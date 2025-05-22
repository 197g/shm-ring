use std::{
    collections::VecDeque,
    ops::Range,
    rc::Rc,
    sync::{atomic, Arc, Mutex},
};

use crate::notify::{CloseableNotifyReader, CloseableNotifyWriter};

use bytes::Bytes;
use shm_pbx::client::{Ring, WaitResult};
use shm_pbx::io_uring::ShmIoUring;
use tokio::sync::Semaphore;
use wasmtime_wasi::p2::{self, Pollable, StreamError, StreamResult};

#[derive(Clone)]
pub struct InputRing {
    inner: Arc<InputInner>,
    name: Option<u64>,
}

struct InputInner {
    /// Signals if the file is closed. Only the file itself has strong ownership, while streams
    /// merely hold it open while they are using the fd. When there are no more strong owners, the
    /// worker in the background can exit as soon as it consumed everything.
    ///
    /// The owner to this is the reader loop.
    notify_produced: CloseableNotifyReader,
    available_space: Semaphore,
    buffer: Mutex<VecDeque<Bytes>>,
    buffer_space_size: u64,
}

pub struct OutputRing {
    inner: Arc<OutputInner>,
    name: Option<u64>,
    #[expect(dead_code)]
    notify_produced: CloseableNotifyWriter,
}

pub struct OutputStream {
    inner: Arc<OutputInner>,
    name: Option<u64>,
}

struct OutputInner {
    /// Signals if the file is closed. Only the file itself has strong ownership, while streams
    /// merely hold it open while they are using the fd. When there are no more strong owners, the
    /// worker in the background can exit as soon as it consumed everything.
    notify_produced: CloseableNotifyReader,
    notify_written: CloseableNotifyReader,
    buffer: Mutex<VecDeque<Bytes>>,
    flush_level: atomic::AtomicUsize,
    flushed: atomic::AtomicUsize,
    /// The number of bytes in the queue to the ring.
    bytes_pushed: atomic::AtomicU64,
    /// The number of bytes actually written by putting a sequence number to the wring.
    bytes_written: atomic::AtomicU64,
    buffer_space_size: u64,
}

struct Available(u64);

impl Extend<u64> for Available {
    fn extend<T: IntoIterator<Item = u64>>(&mut self, iter: T) {
        for item in iter {
            assert!(self.0 <= item, "do not handle stream wrapping");
            self.0 = item;
        }
    }
}

fn usable_power_of_two_size(ring: &Ring) -> u64 {
    usable_power_of_two_size_u64(ring.info().size_data)
}

const fn usable_power_of_two_size_u64(size: u64) -> u64 {
    const _: () = {
        assert!(usable_power_of_two_size_u64(1) == 1);
        assert!(usable_power_of_two_size_u64(2) == 2);
        assert!(usable_power_of_two_size_u64(3) == 2);
        assert!(usable_power_of_two_size_u64(4) == 4);
        assert!(usable_power_of_two_size_u64(256 + 128) == 256);
        assert!(usable_power_of_two_size_u64(511) == 256);
    };

    // All inverted bits *except* the highest set bit
    let and_mask = (!size.reverse_bits() + 1).reverse_bits();
    size & and_mask
}

fn split_ring(available: &Range<&mut u64>, buffer_space_size: u64) -> (Range<usize>, u64) {
    // Split the ring's buffer into two memories, like a VecDeque<u8>.
    let start_offset = *available.start % buffer_space_size;
    // In case we want to a maximum size on each Bytes, just min this.
    let size = (*available.end).checked_sub(*available.start).unwrap();

    assert!(size <= buffer_space_size, "{available:?}");
    let first_capacity = buffer_space_size - start_offset;

    let second_size = size.saturating_sub(first_capacity);
    let first_size = size - second_size;
    (first_size as usize..size as usize, start_offset)
}

impl InputRing {
    pub fn new(mut ring: Ring, io: Rc<ShmIoUring>, local: &tokio::task::LocalSet) -> Self {
        let buffer_space_size = usable_power_of_two_size(&ring);
        let (notify_produced_w, notify_produced) = CloseableNotifyWriter::new();

        let inner = Arc::new(InputInner {
            notify_produced,
            available_space: Semaphore::const_new(32),
            buffer: Mutex::default(),
            buffer_space_size,
        });

        // Time between gratuitous runs of the loop, which might be needed to retry the release of
        // acknowledgements when those are consumed slowly.
        let recheck_time = core::time::Duration::from_micros(1_000);
        let hdl = inner.clone();

        let _task = local.spawn_local(async move {
            let io = io;

            let mut available = Available(0u64);
            let mut sequence = 0u64;
            let mut released = 0u64;
            let mut head_receive = 0;

            let notify_produced = notify_produced_w;
            let mut ready = core::future::ready(());
            let mut is_eof = false;
            let mut permit = None;
            let mut likely_get_more_data = true;

            loop {
                // 1. wait for the next action. FIXME: potentially losing the last message. At
                //    `RemoteInactive` we break before having cleaned up. Should fence post with a
                //    last consumer and consume_stream round.
                tokio::select! {
                    acq = hdl.available_space.acquire(), if permit.is_none() => {
                        match acq {
                            Err(_) => return Ok(()),
                            Ok(acq) => permit = Some(acq),
                        }
                    }
                    wait = io.indicate_for_message(&ring, head_receive, 5 * recheck_time), if permit.is_some() && !likely_get_more_data => {
                        let wait = match wait {
                            Err(io) => return Err(io),
                            Ok(wait) => wait,
                        };

                        match wait {
                            // We successfully waited, or the other half had concurrently advanced anyways
                            WaitResult::Ok | WaitResult::PreconditionFailed => {}
                            // Also alright, nothing fatal and just retry. Could handle blocked a bit
                            // nicer.
                            WaitResult::RemoteBlocked | WaitResult::Restart | WaitResult::Timeout => {}
                            // The remote is no longer here. We stop as soon as we processed all their
                            // data which causes this loop to be longer needed. RAII does all the lifting
                            // of notifying the clients and closing the FD.
                            WaitResult::RemoteInactive => {
                                is_eof = true;
                            }
                            WaitResult::Error => unreachable!(""),
                        }
                    }
                    _ = ready, if permit.is_some() && likely_get_more_data => {}
                }

                ready = core::future::ready(());

                // 2. check for messages on the ring.
                let mut reap = ring.consumer::<8>().unwrap();
                let previous_head = available.0;

                available.extend(
                    reap.iter()
                        .map(u64::from_le_bytes)
                        .inspect(|_| head_receive += 1),
                );

                reap.sync();

                // Do we have the opportunity to read out data?
                if let Some(slot) = permit.take() {
                    let mut guard = hdl.buffer.lock().unwrap();
                    let range = (&mut sequence)..(&mut available.0);
                    let data = Self::consume_stream(&hdl, range, &ring);

                    if !data.is_empty() {
                        guard.push_back(data);

                        notify_produced.notify_waiters();
                        // Permit is restored by the consumer of these bytes.
                        slot.forget();
                    } else {
                        permit = Some(slot);
                    }
                }

                if is_eof && sequence == available.0 {
                    return Ok(());
                }

                // Acknowledge the receipt, free buffer space.
                if sequence != released {
                    let mut produce = ring.producer::<8>().unwrap();

                    if produce.push_many([u64::to_le_bytes(sequence)]) > 0 {
                        released = sequence;
                    }

                    produce.sync();
                }

                // Avoid waiting and syncing if we indeed received more data.
                likely_get_more_data = available.0 != previous_head;
                if let Ok(1..) = io.wake_indicated(&ring).await {
                    {}
                }
            }
        });

        InputRing { inner, name: None }
    }

    pub fn with_name(self, name: u64) -> Self {
        InputRing {
            name: Some(name),
            ..self
        }
    }

    fn consume_stream(inner: &InputInner, available: Range<&mut u64>, ring: &Ring) -> Bytes {
        // In case we want to a maximum size on each Bytes, just min this.
        let (range, start_offset) = split_ring(&available, inner.buffer_space_size);
        let (first_size, size) = (range.start, range.end);

        let mut target = vec![0; size as usize];
        unsafe { ring.copy_from(&mut target[..first_size as usize], start_offset) };
        unsafe { ring.copy_from(&mut target[first_size as usize..], 0) };

        *available.start = *available.end;
        target.into()
    }
}

impl p2::InputStream for InputRing {
    // Required method
    fn read(&mut self, size: usize) -> StreamResult<Bytes> {
        if let Some(name) = self.name {
            eprintln!("{name}: Read up to {size}");
        }

        let mut guard = self.inner.buffer.lock().unwrap();
        let Some(bytes) = guard.front_mut() else {
            return if !self.inner.notify_produced.is_closed() {
                Ok(Bytes::default())
            } else {
                Err(StreamError::Closed)
            };
        };

        let length = bytes.len().min(size);
        let value = bytes.split_to(length);

        if bytes.is_empty() {
            let _ = guard.pop_front();
            self.inner.available_space.add_permits(1);
        }

        Ok(value)
    }
}

#[wasmtime_wasi::async_trait]
impl Pollable for InputRing {
    async fn ready(&mut self) {
        if let Some(name) = self.name {
            eprintln!("{name}: Wait");
        }

        let wakeup = self.inner.notify_produced.notified();

        {
            // Immediately available.
            if !self.inner.buffer.lock().unwrap().is_empty() {
                return;
            }
        }

        let _ = wakeup.await;
    }
}

struct NoMore(&'static str);

impl Drop for NoMore {
    fn drop(&mut self) {
        eprintln!("No more {} left", self.0);
    }
}

impl OutputRing {
    pub fn new(mut ring: Ring, io: Rc<ShmIoUring>, local: &tokio::task::LocalSet) -> Self {
        let buffer_space_size = usable_power_of_two_size(&ring);

        let (notify_produced_w, notify_produced) = CloseableNotifyWriter::new();
        let (notify_written_w, notify_written) = CloseableNotifyWriter::new();

        let inner = Arc::new(OutputInner {
            notify_produced: notify_produced.clone(),
            notify_written,
            buffer: Mutex::default(),
            flush_level: 0.into(),
            flushed: 0.into(),
            bytes_pushed: 0.into(),
            bytes_written: 0.into(),
            buffer_space_size,
        });

        // Time between gratuitous runs of the loop, which might be needed to retry the release of
        // acknowledgements when those are consumed slowly.
        let recheck_time = core::time::Duration::from_micros(500);
        let hdl = inner.clone();

        local.spawn_local(async move {
            let io = io;

            let mut more_data = notify_produced.notified();
            let mut available = Available(buffer_space_size);

            let mut sequence = 0u64;
            let mut released = 0u64;
            let mut head_receive = 0;
            let mut is_eof = false;

            let mut outstanding_flush = 0;
            let mut recheck = tokio::time::interval(recheck_time);
            let mut ready = core::future::ready(());
            let mut immediate = true;
            let mut likely_get_more_data = true;

            let mut no_more_messages = true;

            loop {
                // 1. wait for some data having been produced, by our feed or the remote. Also we
                //    just do ticks sometimes to make sure and sometimes do not wait at all.
                tokio::select! {
                    _ = recheck.tick() => {},
                    _ = more_data, if no_more_messages => {},
                    _ = ready, if immediate || likely_get_more_data || !no_more_messages => {}
                    wait = io.indicate_for_message(&ring, head_receive, 5 * recheck_time), if !likely_get_more_data => {
                        // FIXME: handle wait, particular exit.
                    }
                };

                // Re-ready the immediate.
                ready = core::future::ready(());

                // eprintln!("Can send {released}/{sequence}/{outstanding_flush}");
                let mut reap = ring.consumer::<8>().unwrap();
                let previous_ack = available.0;

                available.extend(
                    reap.iter()
                        .map(u64::from_le_bytes)
                        .map(|n| n.saturating_add(buffer_space_size))
                        .inspect(|_| head_receive += 1),
                );

                reap.sync();
                likely_get_more_data = previous_ack != available.0;

                if outstanding_flush == 0
                    && is_eof
                    && sequence + hdl.buffer_space_size == available.0
                {
                    // We have flushed all data, and the remote has acknowledged it.
                    // We can stop now.
                    break;
                }

                let previous_seq = sequence;

                // Write more data if we're not instructed to flush.
                outstanding_flush = if outstanding_flush == 0 && !is_eof {
                    // Re-arm the notify while holding the guard, ensure nothing is lost.
                    let mut guard = hdl.buffer.lock().unwrap();
                    more_data = notify_produced.notified();
                    let range = (&mut sequence)..(&mut available.0);
                    let flush = Self::fill_stream(&hdl, range, &mut guard, &ring);
                    no_more_messages = guard.is_empty();
                    flush
                // If we've successfully flushed, tell the writer.
                } else {
                    more_data = notify_produced.notified();
                    no_more_messages = false;
                    outstanding_flush
                };

                is_eof |= no_more_messages && notify_produced.is_closed();

                if sequence != released {
                    let mut produce = ring.producer::<8>().unwrap();

                    if produce.push_many([u64::to_le_bytes(sequence)]) > 0 {
                        released = sequence;
                    }

                    produce.sync();

                    hdl.bytes_written.store(released, atomic::Ordering::Relaxed);
                    if sequence == released {
                        notify_written_w.notify_waiters();
                    }
                }

                if released > available.0 - buffer_space_size {
                    if let Ok(1..) = io.wake_indicated(&ring).await {
                        {}
                    }
                }

                if outstanding_flush > 0 && released == sequence {
                    hdl.flushed
                        .fetch_add(outstanding_flush, atomic::Ordering::Release);
                    notify_written_w.notify_waiters();

                    more_data = notify_produced.notified();
                    outstanding_flush = 0;
                }

                /*
                if let Some(name) = hdl.name {
                    eprintln!("End at {released}/{sequence}/{outstanding_flush}");
                } */

                // If we have written anything, don't try to wait by some kind of barrier. Just go
                // again, immediately. We could also do a kind of counting mechanism where we go
                // for n steps immediately just to try to push new data.
                immediate = previous_seq != sequence;
                tokio::task::yield_now().await;
            }
        });

        OutputRing {
            inner,
            notify_produced: notify_produced_w,
            name: None,
        }
    }

    pub fn with_name(self, name: u64) -> Self {
        OutputRing {
            name: Some(name),
            ..self
        }
    }

    pub fn stream(&self) -> OutputStream {
        OutputStream {
            inner: self.inner.clone(),
            name: self.name,
        }
    }

    #[must_use = "Flushes must be tracked"]
    fn fill_stream(
        inner: &OutputInner,
        available: Range<&mut u64>,
        full: &mut VecDeque<Bytes>,
        ring: &Ring,
    ) -> usize {
        let mut flushes = 0;

        loop {
            let Some(frame) = full.front_mut() else {
                break;
            };

            if frame.is_empty() {
                flushes += 1;
                full.pop_front();
                continue;
            }

            let (range, start_offset) = split_ring(&available, inner.buffer_space_size);
            let (first_size, free_space) = (range.start, range.end);

            let split = frame.len().min(first_size);
            let copy_size = frame.len().min(free_space);

            let first = frame.split_to(split);
            let second = frame.split_to(copy_size - split);

            unsafe { ring.copy_to(&first, start_offset) };
            unsafe { ring.copy_to(&second, 0) };
            *available.start += copy_size as u64;

            if !frame.is_empty() {
                // Still some data left to write.
                break;
            }

            full.pop_front();
        }

        flushes
    }
}

impl p2::OutputStream for OutputStream {
    fn write(&mut self, bytes: Bytes) -> StreamResult<()> {
        let Some(notify_produced) = self.inner.notify_produced.upgrade() else {
            return Err(StreamError::Closed);
        };

        if let Some(name) = self.name {
            eprintln!("{name}: Write {}", bytes.len());
        }

        if bytes.is_empty() {
            return Ok(());
        }

        self.inner
            .bytes_pushed
            .fetch_add(bytes.len() as u64, atomic::Ordering::Release);

        {
            let mut guard = self.inner.buffer.lock().unwrap();
            guard.push_back(bytes);
        }

        notify_produced.notify_waiters();
        Ok(())
    }

    fn flush(&mut self) -> StreamResult<()> {
        let Some(notify_produced) = self.inner.notify_produced.upgrade() else {
            return Err(StreamError::Closed);
        };

        if let Some(name) = self.name {
            eprintln!("{name}: Flush");
        }

        self.inner
            .flush_level
            .fetch_add(1, atomic::Ordering::Relaxed);

        {
            let mut guard = self.inner.buffer.lock().unwrap();
            guard.push_back(Bytes::default());
        }

        notify_produced.notify_waiters();
        Ok(())
    }

    fn check_write(&mut self) -> StreamResult<usize> {
        if self.inner.notify_produced.is_closed() {
            return Err(StreamError::Closed);
        };

        if self.inner.flush_pending() {
            Ok(0)
        } else {
            Ok(self.inner.buf_avail())
        }
    }
}

impl OutputInner {
    fn buf_avail(&self) -> usize {
        let written = self.bytes_written.load(atomic::Ordering::Relaxed);
        let pushed = self.bytes_pushed.load(atomic::Ordering::Relaxed);
        // There is a load-store dependency in the queue of byte instances.
        debug_assert!(pushed >= written);

        let in_transit = pushed
            .saturating_sub(written)
            .try_into()
            .unwrap_or(usize::MAX);

        (1usize << 12).saturating_sub(in_transit)
    }

    fn flush_pending(&self) -> bool {
        let flushed = self.flushed.load(atomic::Ordering::Acquire);
        let flush_level = self.flush_level.load(atomic::Ordering::Acquire);

        flushed != flush_level
    }
}

#[wasmtime_wasi::async_trait]
impl Pollable for OutputStream {
    async fn ready(&mut self) {
        if self.inner.notify_produced.is_closed() {
            return;
        };

        if let Some(name) = self.name {
            eprintln!("{name}: Ready");
        }

        loop {
            // Make sure we progress if anything may have changed.
            let notify = self.inner.notify_written.notified();

            let flush_pending = self.inner.flush_pending();
            let avail = self.inner.buf_avail();

            if let Some(name) = self.name {
                eprintln!("{name}: {} {}", flush_pending, avail);
            }

            if !flush_pending && avail > 0 {
                return;
            }

            if notify.await.is_err() {
                return;
            }
        }
    }
}
