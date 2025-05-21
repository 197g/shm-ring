//! Implement the types to operate on a ring.
//!
//! Construct these types through a [`crate::client::Ring`], see [`client::Client`] or [`server::Server`].
use core::{cell::UnsafeCell, ptr, sync::atomic};

use crate::data;

/// Crate internal interface for constructing producer and consumers.
///
/// For simplicity we always send the whole ring's informations, and have the component itself
/// figure out which of these pointers to utilize. In particular, this is always borrowed
/// data and such can be mapped down to the right references.
pub(crate) struct Map<'ring> {
    pub(crate) info: &'ring data::RingInfo,
    pub(crate) head: &'ring data::RingHead,
    pub(crate) side: data::ClientSide,
    pub(crate) ring: &'ring UnsafeCell<[u8]>,
}

pub struct Producer<'ring, const N: usize> {
    /// The slot of the local head to indicate how many are filled.
    head: &'ring atomic::AtomicU32,
    /// The slot of the remote to indicate what has been read.
    tail: &'ring atomic::AtomicU32,
    ring: &'ring [UnsafeCell<[u8; N]>],
    offset_mask: u64,
    count: u64,
    cached_head: u64,
    cached_tail: u64,
}

pub struct Consumer<'ring, const N: usize> {
    /// The slot of the remote to indicate what has been read.
    tail: &'ring atomic::AtomicU32,
    /// The slot of the local head to indicate how many are filled.
    head: &'ring atomic::AtomicU32,
    ring: &'ring [UnsafeCell<[u8; N]>],
    offset_mask: u64,
    cached_head: u64,
    cached_tail: u64,
}

pub struct PushError;

impl<'ring, const N: usize> Producer<'ring, N> {
    pub(crate) fn new(map: Map<'ring>) -> Option<Self> {
        if map.info.size_data == N as u64 {
            return None;
        }

        assert!(map.info.size_ring.is_power_of_two());
        assert!(map.info.size_slot_entry.is_power_of_two());
        assert!(map.info.size_slot_entry % map.info.size_slot_entry == 0);

        let count = map.info.size_ring / map.info.size_slot_entry;
        let offset_mask = count - 1;
        let ring = cell_as_arrays(map.ring);

        let head = map.head.select_producer(map.side);
        let tail = map.head.select_consumer(!map.side);

        let cached_head = head.load(atomic::Ordering::Relaxed).into();
        let cached_tail = u64::from(tail.load(atomic::Ordering::Acquire)) + count;

        Some(Producer {
            head,
            tail,
            offset_mask,
            count,
            ring,
            cached_head,
            cached_tail,
        })
    }
}

impl<const N: usize> Producer<'_, N> {
    pub fn push_many<I>(&mut self, iter: I) -> usize
    where
        I: IntoIterator<Item = [u8; N]>,
    {
        let mut iter = iter.into_iter();
        let mut n = self.cached_head;
        // We applied the size as offset to the tail, to mark where we need to stop to avoid
        // overwriting any entries not yet read.
        while n < self.cached_tail {
            let Some(data) = iter.next() else {
                break;
            };

            let idx = n & self.offset_mask;
            let slot = &self.ring[idx as usize];
            unsafe { core::ptr::write(slot.get(), data) };

            n += 1;
        }

        let count = n - self.cached_head;
        self.cached_head = n;
        count as usize
    }

    pub fn sync(&mut self) {
        self.cached_tail = u64::from(self.tail.load(atomic::Ordering::Acquire)) + self.count;
        self.head
            .store(self.cached_head as u32, atomic::Ordering::Release);
    }
}

impl<'ring, const N: usize> Consumer<'ring, N> {
    pub(crate) fn new(map: Map<'ring>) -> Option<Self> {
        if map.info.size_data == N as u64 {
            return None;
        }

        assert!(map.info.size_ring.is_power_of_two());
        assert!(map.info.size_slot_entry.is_power_of_two());
        assert!(map.info.size_slot_entry % map.info.size_slot_entry == 0);

        let count = map.info.size_ring / map.info.size_slot_entry;
        let offset_mask = count - 1;
        let ring = cell_as_arrays(map.ring);

        let head = map.head.select_producer(!map.side);
        let tail = map.head.select_consumer(map.side);

        let cached_head = head.load(atomic::Ordering::Acquire).into();
        let cached_tail = tail.load(atomic::Ordering::Relaxed).into();

        Some(Consumer {
            head,
            tail,
            offset_mask,
            ring,
            cached_head,
            cached_tail,
        })
    }

    pub fn removable(&self) -> core::ops::Range<u64> {
        self.cached_tail..self.cached_head
    }

    pub fn iter(&mut self) -> impl Iterator<Item = [u8; N]> + '_ {
        (self.cached_tail..self.cached_head).map(|idx| {
            let idx = idx & self.offset_mask;
            let slot = &self.ring[idx as usize];
            self.cached_tail += 1;
            unsafe { core::ptr::read(slot.get()) }
        })
    }

    pub fn sync(&mut self) {
        self.cached_head = u64::from(self.head.load(atomic::Ordering::Acquire));
        self.tail
            .store(self.cached_tail as u32, atomic::Ordering::Release);
    }
}

fn cell_as_arrays<const N: usize>(cell: &UnsafeCell<[u8]>) -> &'_ [UnsafeCell<[u8; N]>] {
    let count = core::mem::size_of_val(cell) / N;
    let ring_data: *mut [u8] = cell.get();
    unsafe { &*ptr::slice_from_raw_parts(ring_data as *const UnsafeCell<[u8; N]>, count) }
}
