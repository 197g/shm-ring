use core::{alloc, cell, mem, ptr, sync::atomic, time};

use linux_futex::{AsFutex, Futex, Shared};

use crate::{data, frame, ring, uapi};

/// A client accessing an SHM-PBX map.
///
/// Each client, by its identifier that matches a process ID, can attempt to join any number of
/// rings on the map at any side, resulting in a [`Ring`] each.
pub struct Client {
    ring: frame::Shared,
    head: ClientHead,
    tid: data::ClientIdentifier,
}

/// An owning wrapper of a portion of a ring on the SHM-PBX map.
///
/// This structure should not be forgotten. Doing so will effectively block the owned side until
/// the process with which the side was taken is reaped.
pub struct Ring {
    ring: frame::Shared,
    map: RingMap,
    /// The ID which we acquired the slot from.
    ring_id: data::RingIdentifier,
}

/// A borrowing lock between two sides of a ring.
///
/// This allows the two sides to compete for a resource if it's unclear for a protocol which side
/// must be activately attempting to make progress and which is waiting. The assertion can be
/// discarded to the ring itself to signal that a connection is about to enter a closing state and
/// may no longer make progress, just cleaning up any messages still to be processed.
pub struct RingAssertion<'lt> {
    block: &'lt atomic::AtomicU32,
    owner: u32,
}

/// Input data for a [`Client`] to join a [`Ring`] on one side..
pub struct RingRequest {
    pub index: data::RingIndex,
    pub side: data::ClientSide,
}

/// The read-out descriptor of a ring.
///
/// May not change except while the ring is owned by the SHM-PBX maintainer. It is read out with
/// atomic instructions in any case, but is only absolutely coherent if read out while owning at
/// least one side of a ring.
#[derive(Debug)]
pub struct RingAttributes {
    /// The offset at which to find this rings head structure.
    pub offset_head: data::ShOffset,
    /// The offset at which to find this rings slot structure.
    pub offset_ring: data::ShOffset,
    /// The offset at which to find this rings data structure.
    pub offset_data: data::ShOffset,
    /// The byte size of that rings head.
    pub size_head: u64,
    /// The byte size of that rings slot structure.
    pub size_ring: u64,
    /// The byte size of that rings data structure.
    pub size_data: u64,
    /// The byte size of each entry in the rings slot structure.
    pub size_slot_entry: u64,
    /// The advertised identifier of the slot, when we joined it.
    pub ring_id: data::RingIdentifier,
}

#[derive(Debug)]
pub enum ClientError {
    /// This isn't actually a ring at all..
    BadMagic,
    SizeError,
    TooSmall,
    UnalignedMap,
}

#[derive(Debug)]
pub enum RingJoinError {
    BadRingIndex,
    BadRingOffsetHead,
    BadRingOffsetData,
    BadRingOffsetRing,
    Unsupported,
    Taken(data::ClientIdentifier),
    /// The ring is currently not initialized, i.e. being cleaned up.
    Unavailable,
}

#[derive(Debug, PartialEq, Eq)]
#[must_use]
pub enum WaitResult {
    Ok,
    PreconditionFailed,
    RemoteBlocked,
    RemoteInactive,
    Error,
    /// Indetermined, woken up by spurious result.
    Restart,
    Timeout,
}

#[derive(Clone, Copy)]
struct ClientHead {
    /// The frozen head.
    pub head: &'static data::ShmHead,
    // FIXME: not necessarily correct. We only know if a ring is `Rings` if they are V1.
    pub rings: &'static data::Rings,
    // FIXME: missing tail for data pages, and that tail's total offset.
}

struct OwnedRingSlot {
    info: &'static data::RingInfo,
    head: &'static data::RingHead,
    side: data::ClientSide,
    identity: data::ClientIdentifier,
}

struct RingMap {
    pbx_head: &'static data::ShmHead,
    ring_slot: OwnedRingSlot,
}

impl Client {
    pub(crate) unsafe fn new(
        ring: frame::Shared,
        tid: data::ClientIdentifier,
    ) -> Result<Self, ClientError> {
        let info_tail = ring.tail();
        // Safety: we store the shared memory.
        let info_tail = unsafe { &*info_tail };
        let size = mem::size_of_val(info_tail);

        let Some(head) = (unsafe { ring.read_head() }) else {
            return Err(ClientError::BadMagic);
        };

        // Safety: we store the shared memory.
        let head = unsafe { &*head };

        let ring_offset = head
            .ring_offset
            .try_into()
            .map_err(|_| ClientError::SizeError)?;

        let size = size.checked_sub(ring_offset).ok_or(ClientError::TooSmall)?;
        // Safety: size check we just did with checked_sub.
        let info_tail = unsafe { (info_tail.get() as *const u8).add(ring_offset) };

        if data::align_offset::<data::RingInfo>(info_tail) != 0 {
            return Err(ClientError::UnalignedMap);
        }

        let ring_max_count = size / mem::size_of::<data::RingInfo>();
        let max_rings = ring_max_count.try_into().unwrap_or(u64::MAX);

        if max_rings < head.ring_count {
            return Err(ClientError::TooSmall);
        }

        let ring_count = head.ring_count as usize;
        let rings = info_tail as *const data::RingInfo;
        // Safety: memory requirement smaller than `size`, we bailed out everywhere math is not an
        // under approximation of available space.
        let rings = ptr::slice_from_raw_parts(rings, ring_count);

        let head = ClientHead {
            head,
            rings: unsafe { &*(rings as *const data::Rings) },
        };

        Ok(Client { ring, head, tid })
    }

    pub(crate) fn shared_head(&self) -> &data::ShmHead {
        self.head.head
    }

    pub(crate) fn shared_ring(&self) -> &frame::Shared {
        &self.ring
    }

    pub fn join(&self, req: &RingRequest) -> Result<Ring, RingJoinError> {
        // Protects any copy of `self.frame` we create.
        let ring = self.ring.clone();

        let ring_info = self
            .head
            .rings
            .get(req.index)
            .ok_or(RingJoinError::BadRingIndex)?;

        if ring_info.version.load(atomic::Ordering::Acquire) != 1 {
            return Err(RingJoinError::Unsupported);
        }

        let head = ring
            .get_aligned_data_at_offset::<data::RingHead>(ring_info.offset_head)
            .ok_or(RingJoinError::BadRingOffsetHead)?;

        let ring_layout = Self::spec_as_layout(ring_info.size_ring, ring_info.size_slot_entry)
            .ok_or(RingJoinError::BadRingOffsetRing)?;
        let _ = ring
            .get_aligned_data_at_offset_val(ring_info.offset_ring, ring_layout)
            .ok_or(RingJoinError::BadRingOffsetRing)?;

        let data_layout = Self::spec_as_layout(ring_info.size_data, 1u64)
            .ok_or(RingJoinError::BadRingOffsetRing)?;
        let _ = ring
            .get_aligned_data_at_offset_val(ring_info.offset_data, data_layout)
            .ok_or(RingJoinError::BadRingOffsetRing)?;

        let slot = ring_info.select_slot(req.side);
        let ring_id = match slot.insert(self.tid) {
            Ok(id) => id,
            Err(None) => return Err(RingJoinError::Unavailable),
            Err(Some(id)) => return Err(RingJoinError::Taken(id)),
        };

        // Immediately afterwards we are responsible for that region.
        let owned_ring = OwnedRingSlot {
            head: unsafe { &*head },
            identity: self.tid,
            info: ring_info,
            side: req.side,
        };

        let frame = RingMap {
            pbx_head: self.head.head,
            ring_slot: owned_ring,
        };

        Ok(Ring {
            ring,
            map: frame,
            ring_id,
        })
    }

    fn spec_as_layout(size: u64, align: u64) -> Option<alloc::Layout> {
        let size: usize = size.try_into().ok()?;
        let align: usize = align.try_into().ok()?;
        alloc::Layout::from_size_align(size, align).ok()
    }
}

impl Ring {
    /// Create an awaitable object that represents modifications to the ring itself, such as
    /// joining. This is not necessary for correctness, but nice to other parties. It ensures the
    /// server has picked up the structure after that sequential point for instance.
    pub fn awaitable(&self) -> data::ClientAwaitable {
        self.map.pbx_head.client_bump()
    }

    /// Get the identity used when joining the ring.
    pub fn identity(&self) -> data::ClientIdentifier {
        self.map.ring_slot.identity
    }

    /// Give up the ring, in favor of a potentially different client, without de-initializing.
    ///
    /// The ring passes the condition for reaping the ring to that other process.
    ///
    /// Careful with this. Naming some long-lived process that does not care to acquire the donated
    /// ring or is incapable of doing so at the time will leave it blocked.
    pub fn donate(self, _: data::ClientIdentifier) {
        todo!()
    }

    /// Give up this side of the ring, such that another connection to it can be opened.
    ///
    /// FIXME: unclear how this is implementable. The ring needs to be brought into a state as if
    /// we hadn't joined it yet. Our part isn't hard, but ensuring the other side gets there might
    /// be. (I.e. The other can not block, our producer head must be brought back, the consumer
    /// head must be synced up, what to do with dropped messages, etc).
    pub fn relinquish(self) {
        todo!()
    }

    /// Wait on the other side to move the head and wake us.
    ///
    /// Alternatively, the operation may be woken by the coordination authority if the PID
    /// controlling the other half of this ring dropped out abnormally. Returns `true` if the wait
    /// was successfully completed by a `wake`. Returns `false` if the other half already left the
    /// ring, leaves the ring normally while waiting, is reaped by the authority, or a timeout
    /// occurs.
    pub fn lock_for_message(&self) -> Result<RingAssertion<'_>, WaitResult> {
        // Does a bit of a weird dance.. We wait on the _slot_ and not the head counter. The reason
        // here is that it is the _slot_ which must stay constant primarily as one must only wait
        // while there is an active PID on the other side. We rely on the other side to requeue us
        // to the actual head before the notify waiters. Which is absolutely odd, but what can you
        // do.
        //
        // I'd like a `FUTEX_WAIT2` operation which enqueues us to a futex based on the atomic
        // comparison with another futex's value?
        //
        // But really it isn't critical whether the head's value is still as expected, as long as
        // the other half is alive it should periodically re-check even when it has woken on some
        // particular head value. Hence, we do not actually race with an update to the head? This
        // is very surprising..
        //
        // The only real alternative would be us spawning a separate thread wherein we can, for all
        // blocked heads, do a `FUTEX_WAKE_OP` on the producer, with the cached previously loaded
        // value, to wake our futexes blocked on the slot when the producer changed to simulate
        // parts of the effect of having checked two values.
        let block = &self.map.ring_slot.head.blocked.0;
        let owner = self.map.ring_slot.side.as_block_slot();

        let prior = block.compare_exchange_weak(
            0,
            owner,
            atomic::Ordering::Relaxed,
            atomic::Ordering::Relaxed,
        );

        // Do not wait if there is no other side anymore (or yet)..
        match prior {
            Ok(_) => Ok(RingAssertion { block, owner }),
            // FIXME: return an error indicating we're shutting down, other side blocked indefinitely.
            Err(prior) if prior as i32 <= 0 => Err(WaitResult::Error),
            // FIXME: return a different error, already blocked.
            Err(_prior) => Err(WaitResult::Error),
        }
    }

    /// Indicate we are ready to publish messages.
    pub fn activate(&self) -> i32 {
        let indicator = self.map.local_indicator();
        indicator.store(1, atomic::Ordering::Relaxed);
        let slot: &Futex<Shared> = indicator.as_futex();
        slot.wake(i32::MAX)
    }

    /// Indicate we do not intend to publish messages.
    pub fn deactivate(&self) -> i32 {
        let indicator = self.map.local_indicator();
        indicator.store(0, atomic::Ordering::Relaxed);
        let slot: &Futex<Shared> = indicator.as_futex();
        slot.wake(i32::MAX)
    }

    /// Check (relaxed) whether the remote intends to publish messages.
    pub fn is_active_remote(&self) -> bool {
        let indicator = self.map.remote_indicator();
        indicator.load(atomic::Ordering::Relaxed) != 0
    }

    /// Get the static information about the ring we joined.
    pub fn info(&self) -> RingAttributes {
        let info = self.map.ring_slot.info;

        RingAttributes {
            offset_head: info.offset_head,
            offset_ring: info.offset_ring,
            offset_data: info.offset_data,
            size_head: info.size_head,
            size_ring: info.size_ring,
            size_data: info.size_data,
            size_slot_entry: info.size_slot_entry,
            ring_id: self.ring_id,
        }
    }

    /// Does the dangerous portion of borrowing the (improperly) `&'static` references.
    ///
    /// Since we borrow `self` here, the ring itself is ensured to outlive the borrowed map to the
    /// references whose memory is held alive by the ring.
    pub(crate) fn borrow_map(&self) -> Option<ring::Map<'_>> {
        self.map.ring_slot.borrow_map(self.ring.borrow_all())
    }

    pub(crate) fn borrow_data(&self) -> &'_ cell::UnsafeCell<[u8]> {
        self.map
            .ring_slot
            .borrow_data(self.ring.borrow_all())
            .unwrap()
    }

    /// Get the producer head for this ring, given its correct size.
    pub fn producer<const N: usize>(&mut self) -> Option<ring::Producer<'_, N>> {
        ring::Producer::new(self.borrow_map()?)
    }

    /// Get the producer head for this ring, given its correct size.
    pub fn consumer<const N: usize>(&mut self) -> Option<ring::Consumer<'_, N>> {
        ring::Consumer::new(self.borrow_map()?)
    }

    pub unsafe fn copy_to(&self, data: &[u8], at: u64) {
        let ring_data = self.borrow_data();
        let len = data.len().try_into().unwrap();
        let portion =
            OwnedRingSlot::slice_cell(ring_data, data::ShOffset(at), len).expect("Out of bounds");
        core::ptr::copy_nonoverlapping(data.as_ptr(), portion.get() as *mut u8, data.len());
    }

    pub unsafe fn copy_from(&self, data: &mut [u8], at: u64) {
        let ring_data = self.borrow_data();
        let len = data.len().try_into().unwrap();
        let portion =
            OwnedRingSlot::slice_cell(ring_data, data::ShOffset(at), len).expect("Out of bounds");
        core::ptr::copy_nonoverlapping(portion.get() as *mut u8, data.as_mut_ptr(), data.len());
    }

    pub(crate) fn shared_ring(&self) -> &frame::Shared {
        &self.ring
    }

    /// Wait until the remote signals readiness
    pub fn wait_for_remote(&self, timeout: time::Duration) -> WaitResult {
        let mut wakes = [(); 2].map(|_| uapi::FutexWaitv::pending());
        let [fblock, fsend] = &mut wakes;

        let blocking = &self.map.ring_slot.head.blocked.0;
        let loaded = blocking.load(atomic::Ordering::Relaxed);
        let indicator = self.map.remote_indicator();

        // Line is going down.
        if (loaded as i32) < 0 {
            return WaitResult::RemoteBlocked;
        }

        if indicator.load(atomic::Ordering::Relaxed) != 0 {
            return WaitResult::Ok;
        }

        *fblock = uapi::FutexWaitv::from_u32(blocking, loaded);
        *fsend = uapi::FutexWaitv::from_u32(indicator, 0);

        match uapi::futex_waitv(&mut wakes, timeout) {
            Ok(0) => WaitResult::Restart,
            Ok(1) => WaitResult::Ok,
            Err(uapi::FutexWaitv::EAGAIN) => WaitResult::PreconditionFailed,
            Err(uapi::FutexWaitv::ETIMEDOUT) => WaitResult::Timeout,
            Err(uapi::FutexWaitv::ERESTARTSYS) => WaitResult::Restart,
            // ::uapi::write(1, ::alloc::format!("{other}").as_bytes());
            // Must not happen, under the kernel-documented error value cases.
            _x => WaitResult::Error,
        }
    }

    /// Wait, until a message arrives or a timeout.
    ///
    /// The caller provides their expected producer head's sequence number. This call wakes (or
    /// refuses to suspend) if any of the following occurs:
    ///
    /// - Ownership of the blocking indicator is acquired. It's absurd to wait for data if none
    ///   will be coming.
    /// - The other side of the link has de-asserted its sending indicator.
    /// - The head of the list is changed, with the other side waking waiters.
    /// - The timeout is reached.
    pub fn wait_for_message(&self, head: u32, timeout: time::Duration) -> WaitResult {
        if self.map.remote_indicator().load(atomic::Ordering::Relaxed) != 1 {
            return WaitResult::RemoteInactive;
        }

        let blocking = &self.map.ring_slot.head.blocked.0;
        if blocking.load(atomic::Ordering::Relaxed) != 0 {
            return WaitResult::RemoteInactive;
        }

        let mut wakes = [(); 3].map(|_| uapi::FutexWaitv::pending());
        let [fblock, fsend, fhead] = &mut wakes;

        let blocking = &self.map.ring_slot.head.blocked.0;
        *fblock = uapi::FutexWaitv::from_u32(blocking, 0);

        let indicator = self.map.remote_indicator();
        *fsend = uapi::FutexWaitv::from_u32(indicator, 1);

        let producer = self.map.remote_producer();
        *fhead = uapi::FutexWaitv::from_u32(producer, head);

        match uapi::futex_waitv(&mut wakes, timeout) {
            Ok(0) => WaitResult::RemoteBlocked,
            Ok(1) => WaitResult::RemoteInactive,
            Ok(2) => WaitResult::Ok,
            Err(uapi::FutexWaitv::EAGAIN) => WaitResult::PreconditionFailed,
            Err(uapi::FutexWaitv::ETIMEDOUT) => WaitResult::Timeout,
            Err(uapi::FutexWaitv::ERESTARTSYS) => WaitResult::Restart,
            // Must not happen, under the kernel-documented error value cases.
            _x => WaitResult::Error,
        }
    }

    pub fn wake(&self) -> u32 {
        self.map.ring_slot.wake()
    }

    pub(crate) fn ring_head(&self) -> &data::RingHead {
        self.map.ring_slot.head
    }

    pub(crate) fn side(&self) -> data::ClientSide {
        self.map.ring_slot.side
    }
}

impl Drop for Ring {
    fn drop(&mut self) {
        self.deactivate();
    }
}

impl RingAssertion<'_> {
    pub fn wake(self, timeout: time::Duration) -> WaitResult {
        // FIXME: we could turn this into a guard here, returning to the caller. We hold the lock
        // now, and need not immediately wait. However, the guard could be 'stolen' or more
        // precisely locked-up into the invalid state. Then we still own the lock but no longer
        // return it to an unlocked state.

        let slot: &Futex<Shared> = self.block.as_futex();
        // FIXME: a dedicated slot might be better. Let each of the sides declare their intention
        // on whether messages are coming or blocked. And also have at most one side wait on actual
        // messages. Note that such state can be de-initialized just the same during leaving (and
        // also even unblocked by the authority while the slot is empty).
        //
        // This is going to be interesting as we need 3 (or at least two) checks.
        match slot.wait_for(self.owner, timeout) {
            Ok(()) => WaitResult::Ok,
            Err(linux_futex::TimedWaitError::WrongValue) => WaitResult::Error,
            Err(linux_futex::TimedWaitError::Interrupted) => WaitResult::Restart,
            Err(linux_futex::TimedWaitError::TimedOut) => WaitResult::Timeout,
        }
    }

    pub(crate) fn owner(&self) -> u32 {
        self.owner
    }

    pub(crate) fn block(&self) -> &atomic::AtomicU32 {
        self.block
    }
}

impl Drop for RingAssertion<'_> {
    fn drop(&mut self) {
        let _ = self.block.compare_exchange_weak(
            self.owner,
            0,
            atomic::Ordering::Relaxed,
            atomic::Ordering::Relaxed,
        );
    }
}

impl RingMap {
    fn remote_producer(&self) -> &atomic::AtomicU32 {
        self.ring_slot.head.select_producer(!self.ring_slot.side)
    }

    fn local_indicator(&self) -> &atomic::AtomicU32 {
        self.ring_slot.head.send_indicator(self.ring_slot.side)
    }

    fn remote_indicator(&self) -> &atomic::AtomicU32 {
        self.ring_slot.head.send_indicator(!self.ring_slot.side)
    }
}

impl OwnedRingSlot {
    pub fn wake(&self) -> u32 {
        // See `Ring::wait` for an explanation of this dance.
        let slot = &self.head.blocked.0;
        let slot: &Futex<Shared> = slot.as_futex();

        let producer = self.head.select_producer(self.side);
        let producer: &Futex<Shared> = producer.as_futex();
        // First, requeue any waiters that assume we're leaving any second.
        //
        // FIXME: it's not entirely clear why we re-queue all waiters to that if we know the
        // comparison to succeed. We could _merge_ different waiters with this strategy but
        // currently no waiter is taking only the producer futex. In particular, some might wait on
        // a side beginning to produce again (see: fixme in `wait`) while others are signalling
        // when our side is blocked on incoming messages (which is an exclusive condition that only
        // one side must take at a time and which can be stolen when the whole ring is going to be
        // shut down).
        let owner = (!self.side).as_block_slot();

        if slot.cmp_requeue(owner, 0, producer, i32::MAX).is_err() {
            // Oh, the lock wasn't actually taken. Obviously if we'd have taken the lock then we
            // wouldn't be running in this. Right?
        };

        producer.wake(i32::MAX) as u32
    }

    fn borrow_map<'lt>(&self, all: &'lt cell::UnsafeCell<[u8]>) -> Option<ring::Map<'lt>> {
        Some(ring::Map {
            info: self.info,
            head: self.head,
            side: self.side,
            ring: Self::slice_cell(all, self.info.offset_ring, self.info.size_ring)?,
        })
    }

    fn borrow_data<'lt>(
        &self,
        all: &'lt cell::UnsafeCell<[u8]>,
    ) -> Option<&'lt cell::UnsafeCell<[u8]>> {
        OwnedRingSlot::slice_cell(all, self.info.offset_data, self.info.size_data)
    }

    fn slice_cell(
        all: &cell::UnsafeCell<[u8]>,
        offset: data::ShOffset,
        length: u64,
    ) -> Option<&cell::UnsafeCell<[u8]>> {
        let (_, data) = Self::split_cell(all, offset.0)?;
        let (data, _) = Self::split_cell(data, length)?;
        Some(data)
    }

    fn split_cell(
        cell: &cell::UnsafeCell<[u8]>,
        start: u64,
    ) -> Option<(&cell::UnsafeCell<[u8]>, &cell::UnsafeCell<[u8]>)> {
        let available = core::mem::size_of_val(cell);
        let len_first = start.try_into().ok().filter(|n| *n <= available)?;
        let len_second = available.checked_sub(len_first).unwrap();

        let start_first = cell as *const _ as *const cell::UnsafeCell<u8>;
        let start_second = unsafe { start_first.add(len_first) };

        let cell_first = ptr::slice_from_raw_parts(start_first, len_first);
        let cell_second = ptr::slice_from_raw_parts(start_second, len_second);

        Some((
            unsafe { &*(cell_first as *const cell::UnsafeCell<[u8]>) },
            unsafe { &*(cell_second as *const cell::UnsafeCell<[u8]>) },
        ))
    }
}

impl Drop for OwnedRingSlot {
    fn drop(&mut self) {
        self.info.leave_as_owner_with_futex(self.side, self.head);
    }
}
