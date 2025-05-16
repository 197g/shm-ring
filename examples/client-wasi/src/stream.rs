use std::{
    collections::VecDeque,
    ops::Range,
    sync::{atomic, Arc, Mutex, Weak},
};

use bytes::Bytes;
use shm_pbx::client::{Ring, WaitResult};
use shm_pbx::io_uring::ShmIoUring;
use tokio::sync::{Notify, Semaphore};
use wasmtime_wasi::{HostInputStream, HostOutputStream, StreamResult, Subscribe};

#[derive(Clone)]
pub struct InputRing {
    inner: Arc<InputInner>,
    name: Option<u64>,
}

struct InputInner {
    notify_produced: Notify,
    notify_consumed: Semaphore,
    buffer: Mutex<VecDeque<Bytes>>,
    buffer_space_size: u64,
    /// Signals if the file is closed. Only the file itself has strong ownership, while streams
    /// merely hold it open while they are using the fd. When there are no more strong owners, the
    /// worker in the background can exit as soon as it consumed everything.
    ///
    /// The owner to this is the reader loop.
    open: Weak<()>,
}

pub struct OutputRing {
    inner: Arc<OutputInner>,
    name: Option<u64>,
    /// The owner to [`OutputInner::open`].
    #[expect(dead_code)]
    open: Arc<()>,
}

pub struct OutputStream {
    inner: Arc<OutputInner>,
    name: Option<u64>,
}

struct OutputInner {
    /// Signals if the file is closed. Only the file itself has strong ownership, while streams
    /// merely hold it open while they are using the fd. When there are no more strong owners, the
    /// worker in the background can exit as soon as it consumed everything.
    open: Weak<()>,
    notify_produced: Notify,
    notify_written: Notify,
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
    pub fn new(mut ring: Ring, on: Arc<ShmIoUring>, local: &tokio::task::LocalSet) -> Self {
        let buffer_space_size = usable_power_of_two_size(&ring);
        let open = Arc::new(());

        let inner = Arc::new(InputInner {
            open: Arc::downgrade(&open),
            notify_produced: Notify::new(),
            notify_consumed: Semaphore::const_new(32),
            buffer: Mutex::default(),
            buffer_space_size,
        });

        // Time between gratuitous runs of the loop, which might be needed to retry the release of
        // acknowledgements when those are consumed slowly.
        let recheck_time = core::time::Duration::from_millis(10);
        let hdl = inner.clone();

        let _task = local.spawn_local(async move {
            let mut available = Available(0u64);
            let mut sequence = 0u64;
            let mut released = 0u64;
            let mut head_receive = 0;

            let open = open;

            let result = loop {
                // 1. wait for space on the queue. FIXME: potentially losing the last message. At
                //    `RemoteInactive` we break before having cleaned up. Should fence post with a
                //    last consumer and consume_stream round.
                let Ok(permit) = hdl.notify_consumed.acquire().await else {
                    break Ok(());
                };

                // 2. wait for messages on the ring.
                let mut reap = ring.consumer::<8>().unwrap();

                available.extend(
                    reap.iter()
                        .map(u64::from_le_bytes)
                        .inspect(|_| head_receive += 1),
                );

                reap.sync();

                {
                    let mut guard = hdl.buffer.lock().unwrap();
                    let range = (&mut sequence)..(&mut available.0);
                    let data = Self::consume_stream(&hdl, range, &ring);

                    if !data.is_empty() {
                        guard.push_back(data);

                        hdl.notify_produced.notify_waiters();
                        // Permit is restored by the consumer of these bytes.
                        permit.forget();
                    }
                }

                // Acknowledge the receipt, free buffer space.
                if sequence != released {
                    let mut produce = ring.producer::<8>().unwrap();

                    if produce.push_many([u64::to_le_bytes(sequence)]) > 0 {
                        released = sequence;
                    }

                    produce.sync();
                    ring.wake();
                }

                let wait = match on.wait_for_message(&ring, head_receive, recheck_time).await {
                    Err(io) => break Err(io),
                    Ok(wait) => wait,
                };

                match wait {
                    // We successfully waited, or the other half had concurrently advanced anyways
                    WaitResult::Ok | WaitResult::PreconditionFailed => {}
                    // Also alright, nothing fatal and just retry. Could handle blocked a bit
                    // nicer.
                    WaitResult::RemoteBlocked | WaitResult::Restart | WaitResult::Timeout => {}
                    // The remote is no longer here. We stop as soon as we processed all their
                    // data but this loop is no longer needed.
                    WaitResult::RemoteInactive => {
                        hdl.notify_consumed.close();
                        continue;
                    }
                    WaitResult::Error => unreachable!(""),
                }
            };

            drop(open);
            hdl.notify_produced.notify_waiters();

            result
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

impl HostInputStream for InputRing {
    // Required method
    fn read(&mut self, size: usize) -> StreamResult<Bytes> {
        if let Some(name) = self.name {
            eprintln!("{name}: Read up to {size}");
        }

        let mut guard = self.inner.buffer.lock().unwrap();
        let Some(bytes) = guard.front_mut() else {
            return if self.inner.open.upgrade().is_some() {
                Ok(Bytes::default())
            } else {
                Err(wasmtime_wasi::StreamError::Closed)
            };
        };

        let length = bytes.len().min(size);
        let value = bytes.split_to(length);

        if bytes.is_empty() {
            let _ = guard.pop_front();
            self.inner.notify_consumed.add_permits(1);
        }

        Ok(value)
    }
}

#[wasmtime_wasi::async_trait]
impl Subscribe for InputRing {
    async fn ready(&mut self) {
        if let Some(name) = self.name {
            eprintln!("{name}: Wait");
        }

        let Some(_streamops) = self.inner.open.upgrade() else {
            return;
        };

        // FIXME: a race between that notification and dropping the streamops thing.
        // We're ready to receive notifications after this point.
        let wakeup = self.inner.notify_produced.notified();

        {
            // Immediately available.
            if !self.inner.buffer.lock().unwrap().is_empty() {
                return;
            }
        }

        wakeup.await
    }
}

struct NoMore(&'static str);

impl Drop for NoMore {
    fn drop(&mut self) {
        eprintln!("No more {} left", self.0);
    }
}

impl OutputRing {
    pub fn new(mut ring: Ring, io: Arc<ShmIoUring>, local: &tokio::task::LocalSet) -> Self {
        let buffer_space_size = usable_power_of_two_size(&ring);
        let open = Arc::new(());

        let inner = Arc::new(OutputInner {
            open: Arc::downgrade(&open),
            notify_produced: Notify::new(),
            notify_written: Notify::new(),
            buffer: Mutex::default(),
            flush_level: 0.into(),
            flushed: 0.into(),
            bytes_pushed: 0.into(),
            bytes_written: 0.into(),
            buffer_space_size,
        });

        // Time between gratuitous runs of the loop, which might be needed to retry the release of
        // acknowledgements when those are consumed slowly.
        let recheck_time = core::time::Duration::from_millis(10);
        let hdl = inner.clone();

        local.spawn_local(async move {
            let _io = io;

            let mut more_data = hdl.notify_produced.notified();
            let mut available = Available(buffer_space_size);

            let mut sequence = 0u64;
            let mut released = 0u64;
            let mut head_receive = 0;
            let mut is_eof = false;

            let mut outstanding_flush = 0;
            let mut recheck = tokio::time::interval(recheck_time);

            loop {
                // 1. wait for data having been produced.
                tokio::select! {
                    _ = more_data, if !is_eof => {},
                    // FIXME: when we flush, we wait this tick time to find out if the remote has
                    // accepted the data. This is quite wasteful.
                    _ = recheck.tick() => {},
                };

                // eprintln!("Can send {released}/{sequence}/{outstanding_flush}");
                let mut reap = ring.consumer::<8>().unwrap();

                available.extend(
                    reap.iter()
                        .map(u64::from_le_bytes)
                        .map(|n| n.saturating_add(buffer_space_size))
                        .inspect(|_| head_receive += 1),
                );

                reap.sync();

                if outstanding_flush == 0
                    && is_eof
                    && sequence + hdl.buffer_space_size == available.0
                {
                    // We have flushed all data, and the remote has acknowledged it.
                    // We can stop now.
                    break;
                }

                // Write more data if we're not instructed to flush.
                outstanding_flush = if outstanding_flush == 0 && !is_eof {
                    // Re-arm the notify while holding the guard, ensure nothing is lost.
                    let mut guard = hdl.buffer.lock().unwrap();
                    more_data = hdl.notify_produced.notified();
                    let range = (&mut sequence)..(&mut available.0);
                    let flush = Self::fill_stream(&hdl, range, &mut guard, &ring);
                    flush
                // If we've successfully flushed, tell the writer.
                } else {
                    more_data = hdl.notify_produced.notified();
                    outstanding_flush
                };

                is_eof |= hdl.open.strong_count() == 0;

                if sequence != released {
                    let mut produce = ring.producer::<8>().unwrap();

                    if produce.push_many([u64::to_le_bytes(sequence)]) > 0 {
                        released = sequence;
                    }

                    produce.sync();
                    ring.wake();
                    hdl.notify_written.notify_waiters();
                }

                hdl.bytes_written.store(released, atomic::Ordering::Relaxed);

                if outstanding_flush > 0 && released == sequence {
                    hdl.flushed
                        .fetch_add(outstanding_flush, atomic::Ordering::Release);
                    hdl.notify_written.notify_waiters();

                    more_data = hdl.notify_produced.notified();
                    outstanding_flush = 0;
                }

                /*
                if let Some(name) = hdl.name {
                    eprintln!("End at {released}/{sequence}/{outstanding_flush}");
                } */

                tokio::task::yield_now().await;
            }
        });

        OutputRing {
            inner,
            open,
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

impl HostOutputStream for OutputStream {
    fn write(&mut self, bytes: Bytes) -> StreamResult<()> {
        let Some(_streamops) = self.inner.open.upgrade() else {
            return Err(wasmtime_wasi::StreamError::Closed);
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

        self.inner.notify_produced.notify_waiters();
        Ok(())
    }

    fn flush(&mut self) -> StreamResult<()> {
        let Some(_streamops) = self.inner.open.upgrade() else {
            return Err(wasmtime_wasi::StreamError::Closed);
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

        self.inner.notify_produced.notify_waiters();
        Ok(())
    }

    fn check_write(&mut self) -> StreamResult<usize> {
        let Some(_streamops) = self.inner.open.upgrade() else {
            return Err(wasmtime_wasi::StreamError::Closed);
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
impl Subscribe for OutputStream {
    async fn ready(&mut self) {
        let Some(_streamops) = self.inner.open.upgrade() else {
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

            // FIXME: this is a bit hairy, we assume that eventually there will be a notification
            // here. That works out, probably, but if the coroutine maintaining the ring is just
            // dropped we may get stuck. We should use a kind of (send,recv) here instead.
            notify.await;
        }
    }
}
