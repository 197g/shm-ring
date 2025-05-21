//! Defines the central data structures.
use core::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use core::{alloc, cell::UnsafeCell, ops};

use linux_futex::{op as futop, AsFutex, Futex, Shared};

#[repr(C)]
pub struct ShmHead {
    pub ring_magic: RingMagic,
    pub ring_count: u64,
    pub ring_offset: u64,
    pub ring_ping: RingPing,
}

#[derive(Default)]
pub struct RingPing {
    pub ring_ping: RingClientPing,
    pub ring_pong: RingServerPong,
}

#[repr(transparent)]
pub struct RingMagic(pub(crate) u64);

#[derive(Copy, Clone, PartialEq, Eq)]
pub struct RingIndex(pub usize);

#[repr(C, align(4096))]
pub struct Rings([RingInfo]);

/// A 'register' with which clients can ping the server into action, by incrementing. We have a
/// futex waiting on it.
#[derive(Default)]
#[repr(transparent)]
pub struct RingClientPing(pub AtomicU32);

/// A 'register' operated by the server, which acknowledges clients pings.
#[derive(Default)]
#[repr(transparent)]
pub struct RingServerPong(pub AtomicU32);

/// A slot with which a client can register to a ring..
#[repr(C, align(8))]
pub struct ClientSlot {
    /// The current owner of this slot. That is:
    /// - a positive value, always a PID, if the slot is owned by a process.
    /// - `0` if the slot is owned by the coordination authority.
    /// - a negative value if the slot is available, advertising some tag.
    pub owner: AtomicU32,
    /// An additional info advertised by the owner. Only the owner should write here.
    ///
    /// It's discouraged to write here directly, use the provided methods instead to interact
    /// nicely with other clients that might utilize these for detecting a remote pair with certain
    /// assumed behavior.
    pub tag: [AtomicU32; 15],
}

/// An advertised tag, further identifying a client which has acquired a slot.
///
/// The major difference to the owner tag is that this value is larger but can not be modified
/// atomically. It is however large enough to contain a UUID. As a convention, tags should not
/// start with a zero value and end with the same value they were started with. This allows a lossy
/// detection of tags which should be given a second read.
///
/// Also see its `From` implementations.
pub struct ClientTag {
    pub value: [u32; 15],
}

/// Identifies the side of the ring.
///
/// A ring is, from the high-level view, a connection between two equals. There is no ordering
/// relationship here. Of course, specific rings may disagree with that.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ClientSide {
    Left,
    Right,
}

/// An offset within into the head structure of the ring, from the ring info struct (by
/// convention that is the start of the shared memory file).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct ShOffset(pub u64);

/// Number of bytes to have, to avoid cache interference between atomics.
///
/// Assumes that the start of structures with this size are also aligned to that same value. The
/// value *is* used but never at runtime which might make the compiler assume it is not. It
/// protects some index assertions which we can not write with `assert!`, at least not without
/// losing the diagnostics of the actual value on their failure.
#[allow(dead_code)]
const ANTI_INTERFERENCE_ALIGN_AND_SIZE: usize = 256;

/// Published by the server, information on the ring and a slot for registering as a client to the
/// ring via an atomic CAS.
#[repr(C)]
pub struct RingInfo {
    /// Always `1` when this ring is active. Otherwise, `0`.
    ///
    /// NOTE: Maybe this could be used by the server to deactivate a ring while fiddling with its
    /// internals, only when no client is assigned. But how to correctly order the checks in other
    /// fields seems complicated. So this is basically informational.
    pub version: AtomicU64,
    /// The offset at which to find this rings head structure.
    pub offset_head: ShOffset,
    /// The offset at which to find this rings slot structure.
    pub offset_ring: ShOffset,
    /// The offset at which to find this rings data structure.
    pub offset_data: ShOffset,
    /// The byte size of that rings head, should be checked for compatibility.
    pub size_head: u64,
    /// The byte size of that rings slot structure.
    pub size_ring: u64,
    /// The byte size of that rings data structure.
    pub size_data: u64,
    /// The byte size of each entry in the rings slot structure.
    pub size_slot_entry: u64,
    // Here we are at 8 · 8 byte.
    pub _padding0: NoAccess<UnsafeCell<[u64; 24]>>,
    // Here we are at 32 · 8 byte.
    pub lhs: ClientSlot,
    pub _padding2: NoAccess<UnsafeCell<[u64; 24]>>,
    // Here we are at 64 · 8 byte
    pub rhs: ClientSlot,
    pub _padding1: NoAccess<UnsafeCell<[u64; 24]>>,
    // Here we are at 16 · 8 byte
    pub _eos: [u8; 0],
}

const _: () = {
    const ASSERT: [(); 1] = [(); 1];
    ASSERT[(core::mem::offset_of!(RingInfo, lhs) % ANTI_INTERFERENCE_ALIGN_AND_SIZE) as usize];
    ASSERT[(core::mem::offset_of!(RingInfo, rhs) % ANTI_INTERFERENCE_ALIGN_AND_SIZE) as usize];
    ASSERT[(core::mem::offset_of!(RingInfo, _eos) % ANTI_INTERFERENCE_ALIGN_AND_SIZE) as usize];
};

#[repr(C)]
pub struct RingHead {
    pub lhs: RingHeadHalf,
    pub rhs: RingHeadHalf,
    /// Describes one side that is blocked on data produced by the other.
    ///
    /// At most one side can be blocked at the same time. Can be pulled low to signal the end of
    /// the stream, where neither side must be blocked on data.
    ///
    /// While active, this is also a Priority-Inversion futex.
    pub blocked: RingBlockedSlot,
    pub _padding0: NoAccess<UnsafeCell<[u64; 31]>>,
    pub _eos: [u8; 0],
}

const _: () = {
    const ASSERT: [(); 1] = [(); 1];
    ASSERT[(core::mem::offset_of!(RingHead, _eos) % ANTI_INTERFERENCE_ALIGN_AND_SIZE) as usize];

    assert!(
        core::mem::size_of::<RingHead>() <= 4096,
        "Gotta fix code that assumes this rounds up to a single page."
    );
};

#[repr(transparent)]
pub struct RingBlockedSlot(pub(crate) AtomicU32);

impl RingBlockedSlot {
    pub fn block(&self, side: ClientSide) -> Result<(), i32> {
        let block_val = side.as_block_slot();

        match self.0
            .compare_exchange_weak(
                0,
                block_val,
                Ordering::Relaxed,
                Ordering::Relaxed,
            )
        {
            Ok(_) => Ok(()),
            Err(n) if n == block_val => Ok(()),
            Err(u) => Err(u as i32),
        }
    }

    pub fn unblock(&self, side: ClientSide) -> Result<(), i32> {
        match self.0
            .compare_exchange_weak(
                side.as_block_slot(),
                0,
                Ordering::Relaxed,
                Ordering::Relaxed,
            )
        {
            Ok(_) => Ok(()),
            Err(n) if n == 0 => Ok(()),
            Err(u) => Err(u as i32),
        }
    }
}

#[repr(C)]
pub struct RingHeadHalf {
    pub producer: AtomicU32,
    pub _padding0: NoAccess<UnsafeCell<[u32; 63]>>,
    pub consumer: AtomicU32,
    pub _padding1: NoAccess<UnsafeCell<[u32; 63]>>,
    /// A flag, signalling whether the producer is currently active.
    ///
    /// One can futex-wait on this to wait for resumption. Note that this is not the same as
    /// waiting on `blocked`, which signals a situation where progress can happen _exclusively_ by
    /// further messages. Indeed, both sides can disable their flags and wait for each other's
    /// message. The assumption for using this is that a side's activity might depend on some
    /// third-party resource (such as a network socket) and this is merely a courtesy to signal a
    /// situation where that resource is temporarily unavailable.
    ///
    /// This is pulled up to `1` when a send may be occurring, and pulled down to `0` when sending
    /// is deactivate momentarily. (Note this is separate from `RingHead::blocked`'s attribute).
    /// Each transition should wake any futex blocked on the value.
    pub send_indicator: AtomicU32,
    /// A flag signalling whether the producer is asynchronously waiting for more messages.
    ///
    /// Do *not* futex-wait on this. This signal is under full control of the side that sets it.
    /// However do note that suspending while this flag is set may not have semantics you intend.
    /// It can *not* be used for a mechanism that excludes both sides being suspended. Instead, you
    /// can use this if you are also resumed by a signal that causes you to produce new data.
    ///
    /// You may note this is in the same cache line as `send_indicator` as both are controlled by
    /// the same side and the indicator is only rarely toggled.
    pub wait_indicator: AtomicU32,
    pub _padding2: NoAccess<UnsafeCell<[u32; 62]>>,
    pub _eos: [u8; 0],
}

const _: () = {
    const ASSERT: [(); 1] = [(); 1];
    ASSERT[(core::mem::offset_of!(RingHeadHalf, producer) % ANTI_INTERFERENCE_ALIGN_AND_SIZE)
        as usize];
    ASSERT[(core::mem::offset_of!(RingHeadHalf, consumer) % ANTI_INTERFERENCE_ALIGN_AND_SIZE)
        as usize];
    ASSERT[(core::mem::offset_of!(RingHeadHalf, send_indicator) % ANTI_INTERFERENCE_ALIGN_AND_SIZE)
        as usize];
    ASSERT[(core::mem::offset_of!(RingHeadHalf, _eos) % ANTI_INTERFERENCE_ALIGN_AND_SIZE) as usize];
};

/// Wraps memory, not allowing *any* access.
///
/// This allows containers with such fields (for padding) to be `Sync`.
#[repr(transparent)]
pub struct NoAccess<T>(T);

// Safety: no `&T` can even be created, so this is always sound.
unsafe impl<T> Sync for NoAccess<T> {}
// Safety: `Copy` ensures that the value, if any, is completely inert. Since no reference, nor
// owned value, to it can be created after wrapping it in `NoAccess` there can be no code relying
// on any invariants that are broken by viewing the bytes in a different thread.
unsafe impl<T: Copy> Send for NoAccess<T> {}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct ClientIdentifier(pub(crate) u64);

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct RingIdentifier(pub(crate) i32);

impl RingMagic {
    const MAGIC: u64 = 0x9e6c_a4fd8624a738;

    pub fn new() -> Self {
        RingMagic(Self::MAGIC)
    }

    pub fn test(&self) -> bool {
        self.0 == Self::MAGIC
    }
}

impl Rings {
    pub fn get(&self, RingIndex(idx): RingIndex) -> Option<&RingInfo> {
        self.0.get(idx as usize)
    }
}

impl<'lt> IntoIterator for &'lt Rings {
    type Item = &'lt RingInfo;
    type IntoIter = core::slice::Iter<'lt, RingInfo>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl ClientSide {
    pub(crate) fn as_block_slot(self) -> u32 {
        match self {
            ClientSide::Left => 1,
            ClientSide::Right => 2,
        }
    }
}

impl ops::Not for ClientSide {
    type Output = ClientSide;

    fn not(self) -> ClientSide {
        match self {
            ClientSide::Left => ClientSide::Right,
            ClientSide::Right => ClientSide::Left,
        }
    }
}

impl ClientIdentifier {
    pub fn to_slot_id(self) -> u32 {
        let client = self.0 as i32;
        // As promised by the constructor in `uapi.rs`
        assert!(client > 0, "Invalid client ID");
        client as u32
    }
}

impl RingIdentifier {
    pub fn new(id: i32) -> Option<Self> {
        if id < 0 {
            Some(RingIdentifier(id))
        } else {
            None
        }
    }

    pub fn to_slot_id(self) -> u32 {
        self.0 as u32
    }
}

impl ClientSlot {
    pub(crate) fn for_advertisement(owner: RingIdentifier) -> Self {
        ClientSlot {
            owner: (owner.0 as u32).into(),
            tag: [0; 15].map(AtomicU32::new),
        }
    }

    /// Atomically exchange the slot with a request to join with a specific client.
    pub fn insert(
        &self,
        client: ClientIdentifier,
    ) -> Result<RingIdentifier, Option<ClientIdentifier>> {
        let client = client.0 as i32;
        assert!(client > 0, "Invalid client ID");
        let client = client as u32;

        // FIXME: this is a problem if we have heavy contention ABA to a ring. Luckily, we assume
        // that this is not the case.. However, maybe we shouldn't spin-lock forever on this?
        let acquisition =
            self.owner
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n: u32| {
                    Some(client).filter(|_| (n as i32) < 0)
                });

        match acquisition {
            Ok(id) => Ok(RingIdentifier(id as i32)),
            Err(0) => Err(None),
            Err(id) => {
                debug_assert!(id > 0);
                Err(Some(ClientIdentifier(id as u64)))
            }
        }
    }

    /// Check if the server is the authority to write to this slot, i.e. if it is `0`.
    ///
    /// On `true`, the server is the only one allowed to turn it false thus this is also a
    /// non-ephemeral answer. After `true` the server can rely on all effects having been seen.
    pub(crate) fn is_owned_by_server_as_checked_by_server(&self) -> bool {
        if self.owner.load(Ordering::Relaxed) == 0 {
            core::sync::atomic::fence(Ordering::Acquire);
            true
        } else {
            false
        }
    }

    pub fn reinit(&self, ring: RingIdentifier) -> Result<(), Option<ClientIdentifier>> {
        let token: i32 = ring.0;
        assert!(token < 0, "Invalid client ID");
        let token = token as u32;

        let acquisition =
            self.owner
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n: u32| {
                    Some(token).filter(|_| n == 0)
                });

        match acquisition {
            Ok(_id) => Ok(debug_assert_eq!(_id, 0)),
            Err(id) => {
                if id > 0 {
                    Err(Some(ClientIdentifier(id as u64)))
                } else {
                    Err(None)
                }
            }
        }
    }

    pub fn leave(&self, id: ClientIdentifier) -> Result<(), u32> {
        self.owner
            .compare_exchange_weak(
                id.to_slot_id(),
                RingIdentifier::default().to_slot_id(),
                Ordering::AcqRel,
                Ordering::Relaxed,
            )
            .map(|_| ())
    }

    pub fn inspect(&self) -> Result<ClientIdentifier, Option<RingIdentifier>> {
        let id: u32 = self.owner.load(Ordering::Relaxed);
        let id = id as i32;

        match id {
            0 => Err(None),
            1.. => Ok(ClientIdentifier(id as u64)),
            id => Err(Some(RingIdentifier(id))),
        }
    }

    /// Read the tag from this slot.
    ///
    /// Note that the read is not atomic. And without having joined the ring, you need not assume
    /// that it remained unchanged even if fenced by two `inspect` calls that yield the same value
    /// (i.e. ABA problem).
    pub fn tag(&self) -> ClientTag {
        let value = ClientTag::_IDX.map(|idx| self.tag[usize::from(idx)].load(Ordering::Acquire));
        ClientTag { value }
    }
}

impl ClientTag {
    const _IDX: [u8; 15] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14];

    pub fn is_conventional(&mut self) -> bool {
        self.value.first() == self.value.last()
    }
}

impl RingInfo {
    /// Leave a ring, as an owner of a side.
    ///
    /// This will atomically swap the client slot for `0` and wake any futex waiting on the head of
    /// the queue of the side being left, if any.
    pub fn leave_as_owner_with_futex(&self, side: ClientSide, head: &RingHead) {
        let slot: &Futex<Shared> = self.select_slot(side).owner.as_futex();
        let head: &Futex<Shared> = head.select_producer(side).as_futex();

        // Effectively: always assign, always wake since the current value _must_ be our own PID if
        // used correctly. We only really use `> 0` as dummy for bad usage.
        let op = futop::Op::assign(0) + futop::Cmp::ge(0);
        head.wake_op(i32::MAX, slot, op, i32::MAX);
    }

    pub fn select_slot(&self, side: ClientSide) -> &ClientSlot {
        match side {
            ClientSide::Left => &self.lhs,
            ClientSide::Right => &self.rhs,
        }
    }
}

impl RingHead {
    pub fn select_producer(&self, side: ClientSide) -> &AtomicU32 {
        match side {
            ClientSide::Left => &self.lhs.producer,
            ClientSide::Right => &self.rhs.producer,
        }
    }

    pub fn select_consumer(&self, side: ClientSide) -> &AtomicU32 {
        match side {
            ClientSide::Left => &self.lhs.consumer,
            ClientSide::Right => &self.rhs.consumer,
        }
    }

    pub fn send_indicator(&self, side: ClientSide) -> &AtomicU32 {
        match side {
            ClientSide::Left => &self.lhs.send_indicator,
            ClientSide::Right => &self.rhs.send_indicator,
        }
    }

    pub fn wait_indicator(&self, side: ClientSide) -> &AtomicU32 {
        match side {
            ClientSide::Left => &self.lhs.wait_indicator,
            ClientSide::Right => &self.rhs.wait_indicator,
        }
    }

    pub(crate) fn reinit_holding_as_server(&self) {
        self.select_consumer(ClientSide::Left)
            .store(0, Ordering::Relaxed);
        self.select_consumer(ClientSide::Right)
            .store(0, Ordering::Relaxed);

        self.select_producer(ClientSide::Left)
            .store(0, Ordering::Relaxed);
        self.select_producer(ClientSide::Right)
            .store(0, Ordering::Relaxed);

        self.send_indicator(ClientSide::Left)
            .store(0, Ordering::Relaxed);
        self.send_indicator(ClientSide::Right)
            .store(0, Ordering::Relaxed);

        self.blocked.0.store(0, Ordering::Relaxed);
    }
}

pub(crate) fn align_offset<U>(ptr: *const u8) -> usize {
    align_offset_val(ptr, alloc::Layout::new::<U>())
}

pub(crate) fn align_offset_val(ptr: *const u8, layout: alloc::Layout) -> usize {
    let addr = ptr as usize;
    addr.wrapping_neg() % layout.align()
}

#[test]
fn align_offset_is_correct() {
    assert_eq!(align_offset::<u64>(0usize as *const u8), 0);
    assert_eq!(align_offset::<u64>(1usize as *const u8), 7);
    assert_eq!(align_offset::<u64>(2usize as *const u8), 6);
    assert_eq!(align_offset::<u64>(3usize as *const u8), 5);
    assert_eq!(align_offset::<u64>(4usize as *const u8), 4);
    assert_eq!(align_offset::<u64>(7usize as *const u8), 1);
    assert_eq!(align_offset::<u64>(8usize as *const u8), 0);
}
