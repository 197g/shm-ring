/// A submission structure for a ring, keeping alive memory mapped for the user ring futex
/// operations.
///
/// Similar to pre-registered files this can hold shared reference counts to the memory mapping
/// itself, which makes the futex operations referring to within those regions sound. The rings are
/// kept alive while submitted operations are running on the ring, until the completions are found.
use crate::client::{Client, Ring, WaitResult};
use crate::data::ClientAwaitable;
use crate::frame::Shared;
use crate::server::Server;
use crate::uapi::FutexWaitv;

use alloc::collections::VecDeque;
use alloc::{rc::Rc, sync::Arc};
use core::{cell, sync::atomic, time};

use io_uring::{opcode, IoUring};
use slotmap::{DefaultKey, Key, KeyData, SlotMap};
use tokio::io::unix::AsyncFd;
use tokio::sync::{Notify, Semaphore};

/// Control an io-uring, instrumenting clients and servers as asynchronous waiters.
///
/// This is inherently single-threaded, both for control over the ring queues and since the
/// allocation of identifiers for the completion queue need not happen across threads.
pub struct ShmIoUring {
    shared_user_ring: Shared,
    /// A semaphore describing the available send space. The queue here defines the fairness
    /// criterion between different submission operations.
    fill: Semaphore,
    io_uring: cell::RefCell<IoUring>,
    stable_timeouts: cell::RefCell<TimeoutDeque>,
    ready: Arc<Notify>,
    notify: cell::RefCell<SlotMap<DefaultKey, Rc<Semaphore>>>,
    #[allow(dead_code)] // Only have this for aborting on drop.
    poller: NotifyOnDrop,
}

/// Signal the level of operational readiness of the underlying platform.
#[derive(Clone, Copy, Debug)]
pub enum SupportLevel {
    /// We do not support this platform.
    ///
    /// Expect some operations to fail when utilized. The failure may result in various error codes
    /// but not memory corruption, although errors may be confusing and inconsistent.
    None,
    /// The basic operations work.
    ///
    /// This will work with Linux `6.8`, usually.
    V1,
}

struct NotifyOnDrop(Arc<Notify>);

impl Drop for NotifyOnDrop {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}

struct Submit<'cell> {
    uring: cell::RefMut<'cell, IoUring>,
    n: usize,
    timeout: cell::RefMut<'cell, TimeoutDeque>,
}

struct KeyOwner<'own>(DefaultKey, &'own ShmIoUring, Rc<Semaphore>);

/// Some memory allocation which we reference in `submit`.
///
/// The only relevant feature here is that the type be `Drop`.
trait SubmitAllocation {}

impl<T: 'static> SubmitAllocation for T {}

type TimeoutDeque = VecDeque<Option<Rc<dyn SubmitAllocation>>>;

/// How we block in `wait_for_message`.
enum BlockStrategy {
    /// We only take a futex on the remote liveness and the block flag.
    None,
    /// We set the block flag, then do `None`.
    ///
    /// This ensures that at most one thread is blocked.
    Block,
    /// We set our own indication bit, then do `None`.
    ///
    /// This is for asynchronous use where the other side can discover if a remote wakeup is
    /// required even if it may be blocked itself.
    Indicate,
}

/// Implements the client interfaces through queueing on the wrapped ring.
impl ShmIoUring {
    pub fn new(shared_user_ring: &Shared) -> Result<Self, std::io::Error> {
        let mut io_uring = IoUring::new(0x100)?;
        let fill = io_uring.submission().capacity();

        let eventfd = uapi::eventfd(
            0,
            uapi::c::EFD_SEMAPHORE | uapi::c::EFD_NONBLOCK | uapi::c::EFD_CLOEXEC,
        )?;

        io_uring.submitter().register_eventfd(eventfd.raw())?;
        let eventfd = AsyncFd::new(eventfd)?;

        let ready = Arc::new(Notify::new());
        let notify = ready.clone();
        let abort = Arc::new(Notify::new());
        let abort_notify = abort.clone();

        tokio::spawn(async move {
            let mut eventfd = eventfd;
            let mut buf = [0; 8];

            while let Ok(mut ready) = {
                tokio::select!(
                    v = eventfd.readable_mut() => v,
                    _ = abort_notify.notified() => return,
                )
            } {
                match uapi::read(ready.get_inner_mut().raw(), &mut buf) {
                    Ok(_) | Err(uapi::Errno(uapi::c::EAGAIN)) => {}
                    _ => break,
                };

                ready.clear_ready();
                notify.notify_waiters();
                tokio::task::yield_now().await;
            }
        });

        Ok(ShmIoUring {
            shared_user_ring: shared_user_ring.clone(),
            fill: Semaphore::new(fill),
            io_uring: io_uring.into(),
            ready,
            notify: SlotMap::new().into(),
            stable_timeouts: Default::default(),
            poller: NotifyOnDrop(abort),
        })
    }

    /// Query the kernel, which necessary operations are supported.
    pub fn is_supported(&self) -> Result<SupportLevel, std::io::Error> {
        let mut probe = io_uring::Probe::new();

        self.io_uring
            .borrow_mut()
            .submitter()
            .register_probe(&mut probe)?;

        let support = probe.is_supported(io_uring::opcode::FutexWaitV::CODE)
            && probe.is_supported(io_uring::opcode::FutexWaitV::CODE)
            && probe.is_supported(io_uring::opcode::FutexWake::CODE);

        Ok(match support {
            false => SupportLevel::None,
            true => SupportLevel::V1,
        })
    }

    pub async fn wait_client(
        &self,
        client: &Client,
        awaitable: &ClientAwaitable,
        mut timeout: time::Duration,
    ) -> Result<WaitResult, std::io::Error> {
        let head = client.shared_head();
        let compare = &head.ring_ping.ring_pong.0;

        let key = self.establish_notice();
        let mut now = std::time::Instant::now();

        let mut timespec = Rc::new(uapi::c::timespec {
            tv_nsec: timeout.subsec_nanos().into(),
            tv_sec: timeout.as_secs() as uapi::c::time_t,
        });

        loop {
            let submit = self.non_empty_submission_and_then_sync(2).await?;
            let loaded = compare.load(atomic::Ordering::Acquire);

            if loaded.wrapping_sub(awaitable.bumped) as i32 >= 0 {
                return Ok(WaitResult::Ok);
            }

            let mut wakes = Rc::new([(); 1].map(|_| FutexWaitv::pending()));
            let [fblock] = Rc::get_mut(&mut wakes).unwrap();

            // Safety: we're owning the ring, which the block references. This keeps it alive for as
            // long as this futex wait is in the kernel, i.e. until everything was reaped. If we can't,
            // the Arc is leaked thus keeping the io-uring itself alive with the memory mapping.
            *fblock = unsafe { FutexWaitv::from_u32_unchecked(compare, loaded) };

            let entry = opcode::FutexWaitV::new(Rc::as_ptr(&wakes) as *const _, 1)
                .build()
                .user_data(key.as_user_data())
                .flags(io_uring::squeue::Flags::IO_LINK);

            // FIXME: wouldn't it be nicer to have an absolute timeout?
            let entry_to = opcode::LinkTimeout::new(Rc::as_ptr(&timespec) as *const _).build();
            unsafe { submit.redeem_push(&[entry, entry_to], [wakes, timespec.clone()]) };

            match key.wait().await {
                Ok(0) => return Ok(WaitResult::Ok),
                Err(FutexWaitv::EAGAIN) => {}
                Err(FutexWaitv::ERESTARTSYS) => {}
                Err(FutexWaitv::ETIMEDOUT | FutexWaitv::ECANCELED) => {
                    return Ok(WaitResult::Timeout)
                }
                Err(errno) => return Err(std::io::Error::from_raw_os_error(errno)),
                Ok(_) => return Ok(WaitResult::Error),
            }

            let elapsed = now.elapsed();
            now += elapsed;

            // Account for the remaining time.
            timeout = timeout.saturating_sub(elapsed);
            *Rc::make_mut(&mut timespec) = uapi::c::timespec {
                tv_nsec: timeout.subsec_nanos().into(),
                tv_sec: timeout.as_secs() as uapi::c::time_t,
            };
        }
    }

    pub async fn wait_server(
        &self,
        server: &Server,
        timeout: time::Duration,
    ) -> Result<WaitResult, std::io::Error> {
        assert!(self.shared_user_ring.same_server(server));

        let head = server.head();
        let assertion = &head.ring_ping.ring_ping.0;
        let loaded = assertion.load(atomic::Ordering::Relaxed);
        let cmp = head.ring_ping.ring_pong.0.load(atomic::Ordering::Relaxed);

        if loaded.wrapping_sub(cmp) as i32 > 0 {
            return Ok(WaitResult::Ok);
        }

        let submit = self.non_empty_submission_and_then_sync(2).await?;

        // FIXME: this is a remarkably clean copy of what we do in `lock_for_message`. Is that
        // incidental or structural? I don't like deduplicating it immediately out of concerns the
        // server might want to wake on a different, other signal in this same futex call.
        let mut wakes = Rc::new([(); 1].map(|_| FutexWaitv::pending()));
        let [fblock] = Rc::get_mut(&mut wakes).unwrap();

        // Safety: we're owning the ring, which the block references. This keeps it alive for as
        // long as this futex wait is in the kernel, i.e. until everything was reaped. If we can't,
        // the Arc is leaked thus keeping the io-uring itself alive with the memory mapping.
        *fblock = unsafe { FutexWaitv::from_u32_unchecked(assertion, loaded) };

        let key = self.establish_notice();
        let entry = opcode::FutexWaitV::new(Rc::as_ptr(&wakes) as *const _, 1)
            .build()
            .user_data(key.as_user_data())
            .flags(io_uring::squeue::Flags::IO_LINK);

        let timespec = Rc::new(uapi::c::timespec {
            tv_nsec: timeout.subsec_nanos().into(),
            tv_sec: timeout.as_secs() as uapi::c::time_t,
        });

        // Safety: the reference-counted pointers of wakes and timespec are passed. The entries
        // submitted are also otherwise valid.
        let entry_to = opcode::LinkTimeout::new(Rc::as_ptr(&timespec) as *const _).build();

        unsafe { submit.redeem_push(&[entry, entry_to], [wakes, timespec]) };

        Ok(match key.wait().await {
            Ok(0) => WaitResult::Ok,
            Err(FutexWaitv::EAGAIN) => WaitResult::Error,
            Err(FutexWaitv::ETIMEDOUT | FutexWaitv::ECANCELED) => WaitResult::Timeout,
            Err(FutexWaitv::ERESTARTSYS) => WaitResult::Restart,
            Err(errno) => return Err(std::io::Error::from_raw_os_error(errno)),
            Ok(_) => WaitResult::Error,
        })
    }

    // We do not implement `activate` here, which is a single futex wake call, since semantics of
    // dropping it in progress are not entirely fruitful. Sure, one can just restart that sequence
    // despite already being active but what.

    pub async fn wait_for_remote(
        &self,
        ring: &Ring,
        timeout: time::Duration,
    ) -> Result<WaitResult, std::io::Error> {
        assert!(self.shared_user_ring.same_ring(ring));

        let head = ring.ring_head();
        let blocking = &head.blocked.0;

        let loaded = blocking.load(atomic::Ordering::Relaxed);
        let indicator = head.send_indicator(!ring.side());

        // Line is going down.
        if (loaded as i32) < 0 {
            return Ok(WaitResult::RemoteBlocked);
        }

        if indicator.load(atomic::Ordering::Relaxed) != 0 {
            return Ok(WaitResult::Ok);
        }

        let submit = self.non_empty_submission_and_then_sync(2).await?;

        let mut wakes = Rc::new([(); 2].map(|_| FutexWaitv::pending()));
        let [fblock, fsend] = Rc::get_mut(&mut wakes).unwrap();

        *fblock = unsafe { FutexWaitv::from_u32_unchecked(blocking, loaded) };
        *fsend = unsafe { FutexWaitv::from_u32_unchecked(indicator, 0) };

        let key = self.establish_notice();
        let entry = opcode::FutexWaitV::new(Rc::as_ptr(&wakes) as *const _, 2)
            .build()
            .user_data(key.as_user_data())
            .flags(io_uring::squeue::Flags::IO_LINK);

        let timespec = Rc::new(uapi::c::timespec {
            tv_nsec: timeout.subsec_nanos().into(),
            tv_sec: timeout.as_secs() as uapi::c::time_t,
        });

        // Safety: the reference-counted pointers of wakes and timespec are passed. The entries
        // submitted are also otherwise valid.
        let entry_to = opcode::LinkTimeout::new(Rc::as_ptr(&timespec) as *const _).build();

        unsafe { submit.redeem_push(&[entry, entry_to], [wakes, timespec]) };

        // assertion.
        Ok(match key.wait().await {
            Ok(0) => WaitResult::Restart,
            Ok(1) => WaitResult::Ok,
            Err(FutexWaitv::EAGAIN) => WaitResult::PreconditionFailed,
            Err(FutexWaitv::ETIMEDOUT | FutexWaitv::ECANCELED) => WaitResult::Timeout,
            Err(FutexWaitv::ERESTARTSYS) => WaitResult::Restart,
            Err(errno) => return Err(std::io::Error::from_raw_os_error(errno)),
            Ok(_) => WaitResult::Error,
        })
    }

    pub async fn lock_for_message(
        &self,
        ring: &Ring,
        timeout: time::Duration,
    ) -> Result<WaitResult, std::io::Error> {
        assert!(self.shared_user_ring.same_ring(ring));
        let assertion = match ring.lock_for_message() {
            Ok(guard) => guard,
            Err(err) => return Ok(err),
        };

        let submit = self.non_empty_submission_and_then_sync(2).await?;

        let mut wakes = Rc::new([(); 1].map(|_| FutexWaitv::pending()));
        let [fblock] = Rc::get_mut(&mut wakes).unwrap();

        // Safety: we're owning the ring, which the block references. This keeps it alive for as
        // long as this futex wait is in the kernel, i.e. until everything was reaped. If we can't,
        // the Arc is leaked thus keeping the io-uring itself alive with the memory mapping.
        *fblock = unsafe { FutexWaitv::from_u32_unchecked(assertion.block(), assertion.owner()) };

        let key = self.establish_notice();
        let entry = opcode::FutexWaitV::new(Rc::as_ptr(&wakes) as *const _, 1)
            .build()
            .user_data(key.as_user_data())
            .flags(io_uring::squeue::Flags::IO_LINK);

        let timespec = Rc::new(uapi::c::timespec {
            tv_nsec: timeout.subsec_nanos().into(),
            tv_sec: timeout.as_secs() as uapi::c::time_t,
        });

        // Safety: the reference-counted pointers of wakes and timespec are passed. The entries
        // submitted are also otherwise valid.
        let entry_to = opcode::LinkTimeout::new(Rc::as_ptr(&timespec) as *const _).build();

        unsafe { submit.redeem_push(&[entry, entry_to], [wakes, timespec]) };

        Ok(match key.wait().await {
            Ok(0) => WaitResult::Ok,
            Err(FutexWaitv::EAGAIN) => WaitResult::Error,
            Err(FutexWaitv::ETIMEDOUT | FutexWaitv::ECANCELED) => WaitResult::Timeout,
            Err(FutexWaitv::ERESTARTSYS) => WaitResult::Restart,
            Err(errno) => return Err(std::io::Error::from_raw_os_error(errno)),
            Ok(_) => WaitResult::Error,
        })
    }

    /// Wait until the other side wakes us.
    ///
    /// This asserts three futexes:
    /// - the current remote's head with the value indicated
    /// - that there is no block
    /// - the remote's indicator being active
    ///
    /// If any of them fails a non-Ok `WaitResult` is returned. Returns an appropriate status code
    /// depending on the futex being changed. If either the head or the blocked indicator is woken
    /// then `Ok` is returned. If the remote indicator is changed then `RemoteInactive` is
    /// presumed.
    pub async fn wait_for_message(
        &self,
        ring: &Ring,
        head: u32,
        timeout: time::Duration,
    ) -> Result<WaitResult, std::io::Error> {
        self.block_for_message_maybe_indicated(ring, head, timeout, BlockStrategy::None)
            .await
    }

    /// Signal that we may be woken up to make more progress.
    ///
    /// Contrary to `block_for_message` this is not exclusive, we have another means of progress
    /// and our side is still alive working on the ring (and in particular still capable of waking
    /// the other side as soon as useful).
    pub async fn indicate_for_message(
        &self,
        ring: &Ring,
        head: u32,
        timeout: time::Duration,
    ) -> Result<WaitResult, std::io::Error> {
        self.block_for_message_maybe_indicated(ring, head, timeout, BlockStrategy::Indicate)
            .await
    }

    /// Wait until the other side wakes us, showing a status of doing so.
    pub async fn block_for_message(
        &self,
        ring: &Ring,
        head: u32,
        timeout: time::Duration,
    ) -> Result<WaitResult, std::io::Error> {
        self.block_for_message_maybe_indicated(ring, head, timeout, BlockStrategy::Block)
            .await
    }

    #[expect(unused_assignments)] // A guard value.
    async fn block_for_message_maybe_indicated(
        &self,
        ring: &Ring,
        head: u32,
        timeout: time::Duration,
        block: BlockStrategy,
    ) -> Result<WaitResult, std::io::Error> {
        assert!(self.shared_user_ring.same_ring(ring));
        let submit = self.non_empty_submission_and_then_sync(2).await?;

        let rhead = ring.ring_head();
        let side = ring.side();

        if rhead.select_producer(!side).load(atomic::Ordering::Relaxed) != head {
            return Ok(WaitResult::Ok);
        }

        if rhead.send_indicator(!side).load(atomic::Ordering::Relaxed) != 1 {
            return Ok(WaitResult::RemoteInactive);
        }

        if rhead.blocked.0.load(atomic::Ordering::Relaxed) != 0 {
            return Ok(WaitResult::RemoteBlocked);
        }

        enum Unblock<'lt> {
            None,
            Block(&'lt crate::data::RingHead, crate::data::ClientSide),
            Wait(&'lt crate::data::RingHead, crate::data::ClientSide),
        }

        impl Drop for Unblock<'_> {
            fn drop(&mut self) {
                match self {
                    Unblock::None => {}
                    Unblock::Block(head, side) => {
                        // We do not care if we succeeded here or not, just if we were still blocked then
                        // stop doing that.
                        let _ = head.blocked.unblock(*side);
                    }
                    Unblock::Wait(head, side) => {
                        head.wait_indicator(*side)
                            .store(0, atomic::Ordering::Relaxed);
                    }
                }
            }
        }

        #[expect(unused_variables)]
        let mut blocked = Unblock::None;
        let mut blocked_expected = 0;

        match block {
            BlockStrategy::None => {}
            BlockStrategy::Indicate => {
                rhead
                    .wait_indicator(side)
                    .store(1, atomic::Ordering::Relaxed);

                blocked = Unblock::Wait(rhead, side);
            }
            BlockStrategy::Block => {
                if rhead.blocked.block(side).is_err() {
                    return Ok(WaitResult::RemoteBlocked);
                }

                // Just for `Drop`.
                blocked = Unblock::Block(rhead, side);
                blocked_expected = side.as_block_slot();
            }
        }

        let mut wakes = Rc::new([(); 3].map(|_| FutexWaitv::pending()));
        let [fblock, fsend, fhead] = Rc::get_mut(&mut wakes).unwrap();

        let blocking = &rhead.blocked.0;
        *fblock = unsafe { FutexWaitv::from_u32_unchecked(blocking, blocked_expected) };

        let indicator = rhead.send_indicator(!side);
        *fsend = unsafe { FutexWaitv::from_u32_unchecked(indicator, 1) };

        let producer = rhead.select_producer(!side);
        *fhead = unsafe { FutexWaitv::from_u32_unchecked(producer, head) };

        let key = self.establish_notice();
        let entry = opcode::FutexWaitV::new(Rc::as_ptr(&wakes) as *const _, 3)
            .build()
            .user_data(key.as_user_data())
            .flags(io_uring::squeue::Flags::IO_LINK);

        let timespec = Rc::new(uapi::c::timespec {
            tv_nsec: timeout.subsec_nanos().into(),
            tv_sec: timeout.as_secs() as uapi::c::time_t,
        });

        // Safety: the reference-counted pointers of wakes and timespec are passed. The entries
        // submitted are also otherwise valid.
        let entry_to = opcode::LinkTimeout::new(Rc::as_ptr(&timespec) as *const _).build();

        unsafe { submit.redeem_push(&[entry, entry_to], [wakes, timespec]) };

        match key.wait().await {
            Ok(0) => Ok(WaitResult::Ok),
            Ok(1) => Ok(WaitResult::RemoteInactive),
            Ok(2) => Ok(WaitResult::Ok),
            Err(FutexWaitv::EAGAIN) => Ok(WaitResult::PreconditionFailed),
            Err(FutexWaitv::ETIMEDOUT | FutexWaitv::ECANCELED) => Ok(WaitResult::Timeout),
            Err(FutexWaitv::ERESTARTSYS) => Ok(WaitResult::Restart),
            Err(errno) => Err(std::io::Error::from_raw_os_error(errno)),
            Ok(_) => Ok(WaitResult::Error),
        }
    }

    /// Wake the other side, *if* it is blocked.
    ///
    /// Unblocks the other side before it is woken it. If the queue is not blocked on the other
    /// side's actions, does nothing and returns `Ok(0)`. This is quite a cheap action.
    pub async fn wake_block(&self, ring: &Ring) -> Result<u32, std::io::Error> {
        let rhead = ring.ring_head();
        let side = ring.side();

        if rhead.blocked.unblock(!side).is_ok() {
            self.wake(ring).await
        } else {
            Ok(0)
        }
    }

    /// Wake the other side, *if* it indicates.
    ///
    /// If the other side does not indicate, does nothing and returns `Ok(0)`. This is quite a
    /// cheap action but note that it does not introduce any happens-before order. If the other
    /// side starts waiting after we checked its flag, we may miss it. You will need to check again
    /// regularly, i.e. still be live, to continue communication.
    pub async fn wake_indicated(&self, ring: &Ring) -> Result<u32, std::io::Error> {
        let rhead = ring.ring_head();
        let side = ring.side();

        if rhead.wait_indicator(!side).load(atomic::Ordering::Relaxed) != 0 {
            self.wake(ring).await
        } else {
            Ok(0)
        }
    }

    /// Wait the block futex and the other side's producer futex.
    pub async fn wake(&self, ring: &Ring) -> Result<u32, std::io::Error> {
        assert!(self.shared_user_ring.same_ring(ring));
        let submit = self.non_empty_submission_and_then_sync(2).await?;

        let head = ring.ring_head();
        let side = ring.side();

        let key = self.establish_notice();
        let producer = head.select_producer(side).as_ptr();
        let entry_head = opcode::FutexWake::new(
            producer,
            // Wakeup everyone.
            u32::MAX.into(),
            u32::MAX.into(),
            // On this 32-bit futex.
            FutexWaitv::ATOMIC_U32,
        )
        .build()
        .user_data(key.as_user_data());

        let key = self.establish_notice();
        let producer = head.blocked.0.as_ptr();
        let entry_blocked = opcode::FutexWake::new(
            producer,
            // Wakeup everyone.
            u32::MAX.into(),
            u32::MAX.into(),
            // On this 32-bit futex.
            FutexWaitv::ATOMIC_U32,
        )
        .build()
        .user_data(key.as_user_data());

        // Safety: no reference-counted pointers are needed.
        unsafe { submit.redeem_push(&[entry_head, entry_blocked], []) };

        match key.wait().await {
            Ok(n) => Ok(n as u32),
            Err(errno) => Err(std::io::Error::from_raw_os_error(errno)),
        }
    }

    fn establish_notice(&self) -> KeyOwner<'_> {
        let sem = Rc::new(Semaphore::const_new(0));
        let key = self.notify.borrow_mut().insert(sem.clone());
        KeyOwner(key, self, sem)
    }

    async fn non_empty_submission_and_then_sync(
        &self,
        n: u32,
    ) -> Result<Submit<'_>, std::io::Error> {
        let _permit = self.fill.acquire_many(n).await.expect("Never closed");
        let n = usize::try_from(n).unwrap();

        loop {
            {
                let mut ring = self.io_uring.borrow_mut();
                let submission = ring.submission();
                let avail = submission.capacity() - submission.len();

                if avail >= n {
                    // This is enough to 'own' the non-empty submission queue until calling async task yields.
                    break;
                }
            }

            // We couldn't even submit enough, wait for some events to clear. Re-check for the next
            // time that the submission queue might become longer.
            self.complete().await;
        }

        Ok(Submit {
            uring: self.io_uring.borrow_mut(),
            n,
            timeout: self.stable_timeouts.borrow_mut(),
        })
    }

    /// Drive forward our submission towards the kernel, in the process of ensuring that the
    /// submissions are also completed at some point.
    fn try_submission(&self) {
        let mut ring = self.io_uring.borrow_mut();

        {
            // Make sure we do not block endlessly.
            let submission = ring.submission();
            if submission.is_empty() {
                return;
            }

            // Only do submission if we shouldn't poll more. And here we should process completions
            // anyways.
            if submission.taskrun() {
                return;
            }
        }

        if let Ok(n) = Self::submit(&mut ring, &self.stable_timeouts) {
            // Refill permits available for the fill queue.
            self.fill.add_permits(n);
        }
    }

    async fn complete(&self) {
        self.try_submission();
        let _ = self.ready.notified().await;

        let mut ring = self.io_uring.borrow_mut();
        let mut notify = self.notify.borrow_mut();
        let completion = ring.completion();

        // FIXME: log this somewhere? Some potentially concurrent or even parallel insight into the
        // cycles here might be appreciated.
        let mut _n = 0;
        for entry in completion {
            _n += 1;

            let data = KeyData::from_ffi(entry.user_data());
            let key = DefaultKey::from(data);
            let result: i32 = entry.result();

            // The waiter might already be gone, if the task was cancelled by being dropped.
            if let Some(waiter) = notify.remove(key) {
                // One permit is consumed by the waiter itself, then the rest encodes the actual result.
                // FIXME: on 32-bit systems we might corrupt an `-1 == -EPERM`.
                waiter.add_permits((1 + (result as u32)) as usize);
            }
        }
    }

    fn submit(
        ring: &mut IoUring,
        stable_timeouts: &cell::RefCell<TimeoutDeque>,
    ) -> std::io::Result<usize> {
        let n = ring.submitter().submit()?;
        // After submissions, we no longer need to keep alive the referenced data. There is a
        // one-to-one correspondence between our queue for this and the submitted entries.
        stable_timeouts.borrow_mut().drain(..n).for_each(|_| ());
        Ok(n)
    }
}

impl KeyOwner<'_> {
    fn as_user_data(&self) -> u64 {
        self.0.data().as_ffi()
    }

    async fn wait(&self) -> Result<i64, i32> {
        let permit = loop {
            tokio::select! {
                permit = self.2.acquire() => {
                    break permit;
                }
                () = self.1.complete() => {}
            }
        };

        permit.unwrap().forget();
        let n = self.2.available_permits();

        // Signalled via here, and the kernel signals error with negative values.
        let kernel_result = (n as u32) as i32;
        // eprintln!("Ready {} {}", self.as_user_data(), kernel_result);
        if kernel_result >= 0 {
            Ok(kernel_result as i64)
        } else {
            Err(-kernel_result)
        }
    }
}

impl Submit<'_> {
    /// Safety:
    ///
    /// Some entries contain pointers to data. The kernel requires them to be kept alive, until the
    /// submissions of these entries has been finished. The caller guarantees that all memory
    /// referenced in this way is either kept alive with the io-uring; OR kept alive through
    /// the reference-counted shared ownership passed in the second array.
    unsafe fn redeem_push<const N: usize>(
        mut self,
        entry: &[io_uring::squeue::Entry],
        to: [Rc<dyn SubmitAllocation>; N],
    ) {
        debug_assert_eq!(self.n, entry.len());
        assert!(N <= entry.len());

        let non_timeouts = entry.len() - N;
        self.uring.submission().push_multiple(entry).unwrap();

        self.timeout
            .extend(core::iter::repeat(None).take(non_timeouts));
        self.timeout.extend(to.map(Some));
    }
}

impl SupportLevel {
    pub fn any(self) -> bool {
        match self {
            SupportLevel::None => false,
            _ => true,
        }
    }
}

impl Drop for KeyOwner<'_> {
    fn drop(&mut self) {
        self.1.notify.borrow_mut().remove(self.0);
    }
}
