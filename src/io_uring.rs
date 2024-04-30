/// A submission structure for a ring, keeping alive memory mapped for the user ring futex
/// operations.
///
/// Similar to pre-registered files this can hold shared reference counts to the memory mapping
/// itself, which makes the futex operations referring to within those regions sound. The rings are
/// kept alive while submitted operations are running on the ring, until the completions are found.
use crate::client::{Ring, WaitResult};
use crate::frame::Shared;
use crate::uapi::FutexWaitv;

use alloc::collections::VecDeque;
use alloc::{rc::Rc, sync::Arc};
use core::{cell, time};

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

#[derive(Clone, Copy, Debug)]
pub enum SupportLevel {
    /// We do not support this platform.
    None,
    /// The basic operations work.
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
trait SubmitAllocation {}

impl<T: 'static> SubmitAllocation for T {}

type TimeoutDeque = VecDeque<Option<Rc<dyn SubmitAllocation>>>;

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

                notify.notify_waiters();
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
        let loaded = blocking.load(core::sync::atomic::Ordering::Relaxed);

        // Line is going down.
        if (loaded as i32) < 0 {
            return Ok(WaitResult::RemoteBlocked);
        }

        let submit = self.non_empty_submission_and_then_sync(2).await?;

        let mut wakes = Rc::new([(); 2].map(|_| FutexWaitv::pending()));
        let [fblock, fsend] = Rc::get_mut(&mut wakes).unwrap();

        *fblock = unsafe { FutexWaitv::from_u32_unchecked(blocking, loaded) };
        let indicator = head.send_indicator(!ring.side());
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
        let entry_to = opcode::Timeout::new(Rc::as_ptr(&timespec) as *const _).build();
        unsafe { submit.redeem_push(&[entry, entry_to], [wakes, timespec]) };

        // assertion.
        Ok(match key.wait().await {
            Ok(0) => WaitResult::Restart,
            Ok(1) => WaitResult::Ok,
            Err(FutexWaitv::EAGAIN) => WaitResult::PreconditionFailed,
            Err(FutexWaitv::ETIMEDOUT) => WaitResult::Timeout,
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
        let entry_to = opcode::Timeout::new(Rc::as_ptr(&timespec) as *const _).build();
        unsafe { submit.redeem_push(&[entry, entry_to], [wakes, timespec]) };

        Ok(match key.wait().await {
            Ok(0) => WaitResult::Ok,
            Err(FutexWaitv::EAGAIN) => WaitResult::Error,
            Err(FutexWaitv::ETIMEDOUT) => WaitResult::Timeout,
            Err(FutexWaitv::ERESTARTSYS) => WaitResult::Restart,
            Err(errno) => return Err(std::io::Error::from_raw_os_error(errno)),
            Ok(_) => WaitResult::Error,
        })
    }

    pub async fn wait_for_message(
        &self,
        ring: &Ring,
        head: u32,
        timeout: time::Duration,
    ) -> Result<WaitResult, std::io::Error> {
        assert!(self.shared_user_ring.same_ring(ring));
        let submit = self.non_empty_submission_and_then_sync(2).await?;

        let rhead = ring.ring_head();
        let side = ring.side();

        let mut wakes = Rc::new([(); 3].map(|_| FutexWaitv::pending()));
        let [fblock, fsend, fhead] = Rc::get_mut(&mut wakes).unwrap();

        let blocking = &rhead.blocked.0;
        *fblock = unsafe { FutexWaitv::from_u32_unchecked(blocking, 0) };

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
        let entry_to = opcode::Timeout::new(Rc::as_ptr(&timespec) as *const _).build();
        unsafe { submit.redeem_push(&[entry, entry_to], [wakes, timespec]) };

        match key.wait().await {
            Ok(0) => Ok(WaitResult::RemoteBlocked),
            Ok(1) => Ok(WaitResult::RemoteInactive),
            Ok(2) => Ok(WaitResult::Ok),
            Err(FutexWaitv::EAGAIN) => Ok(WaitResult::PreconditionFailed),
            Err(FutexWaitv::ETIMEDOUT) => Ok(WaitResult::Timeout),
            Err(FutexWaitv::ERESTARTSYS) => Ok(WaitResult::Restart),
            Err(errno) => Err(std::io::Error::from_raw_os_error(errno)),
            Ok(_) => Ok(WaitResult::Error),
        }
    }

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

        for entry in completion {
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
        // eprintln!("Waiting {}", self.as_user_data());
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
