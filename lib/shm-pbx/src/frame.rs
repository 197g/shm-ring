use ::alloc::sync::{Arc, Weak};
use core::sync::atomic;
use core::{alloc, cell::UnsafeCell, mem, ptr};

use crate::{client, data, server};

use crate::mmap::MmapRaw;

/// A container owning some shared mapped memory.
pub unsafe trait RetainedMemory: Send + Sync {
    /// Return a pointer to memory. This must have provenance to access the memory atomically.
    /// Calling this method twice must *not* invalidate any other previously returned pointer.
    fn data_from_head(&self) -> *mut [u8];
}

#[derive(Clone, Copy)]
struct SharedHead {
    #[allow(dead_code)]
    all: &'static UnsafeCell<[u8]>,
    aligned_tail: &'static UnsafeCell<[u8]>,
    ring: *mut data::ShmHead,
}

// Safety: Any such value must be accompanied by another (shared) owner of the memory backing it,
// the creation of the value is contingent on the caller upholding that requirement. If the head
// itself is sent the backing memory must be part of that transfer, keeping the head valid.
unsafe impl Send for SharedHead {}
// Safety: no shared reference created directly, all other synchronization is guarded by diverse
// and specific preconditions. Callers then turn these assertions (such as: client's upholding the
// basic access guarantees) into shared _references_ to the part of the ring's memory which they
// need for their operations. Those references are required to be send, making this sync.
unsafe impl Sync for SharedHead {}

#[derive(Clone)]
pub struct Shared {
    head: SharedHead,
    _retain: Arc<dyn RetainedMemory>,
}

impl Shared {
    pub fn new(shared: MmapRaw) -> Option<Self> {
        Self::with_retained(Arc::new(shared))
    }

    pub fn with_retained(val: Arc<dyn RetainedMemory>) -> Option<Self> {
        Some(Shared {
            head: unsafe { val.clone().to_head()? },
            _retain: val,
        })
    }

    /// Initialize the mapping as a server.
    ///
    /// # Safety
    ///
    /// The caller must ensure that the mapping is not concurrently accessed, i.e. this value
    /// *owns* the memory. This applies to this process as well as other processes. Any other
    /// existing `Server` or `Client` instance sharing the same mapping is assumed an access. The
    /// server takes ownership of the mapping.
    ///
    /// We can't relax this into a static or ref-counted check due to external processes, hence it
    /// is an `unsafe` function. You should adhere to this constraint by creating the shared memory
    /// as a private mapped shm file and then later linking it into the file system.
    pub unsafe fn into_server(
        &self,
        cfg: server::ServerConfig,
    ) -> Result<server::Server, server::ServerError> {
        // Safety: exactly fulfills the preconditions, ours by definition.
        unsafe { server::Server::new(self.clone(), cfg) }
    }

    /// Access this ring as a client.
    ///
    /// This does not join any specific ring, nor precludes the process from _also_ acting as a
    /// server. Merely it checks if the server structure exists and can be utilized, as well as
    /// resolve the addresses with a consistency check with the underlying mapped memory.
    pub fn into_client(
        &self,
        tid: data::ClientIdentifier,
    ) -> Result<client::Client, client::ClientError> {
        // Safety: exactly fulfills the preconditions, ours by definition.
        unsafe { client::Client::new(self.clone(), tid) }
    }

    /// Get the memory of the file, which is after the head page.
    pub fn tail(&self) -> *const UnsafeCell<[u8]> {
        self.head.aligned_tail as *const UnsafeCell<[u8]>
    }

    pub(crate) fn borrow_all(&self) -> &'_ UnsafeCell<[u8]> {
        self.head.all
    }

    pub fn tail_offset(&self) -> usize {
        let aligned_addr = self.head.aligned_tail as *const _ as *const u8 as usize;
        let head_addr = self.head.all as *const _ as *const u8 as usize;
        aligned_addr - head_addr
    }

    pub(crate) fn get_aligned_data_at_offset<T>(&self, offset: data::ShOffset) -> Option<*const T> {
        let layout = alloc::Layout::new::<T>();
        let ptr = self.get_aligned_data_at_offset_val(offset, layout)?;
        debug_assert_eq!(mem::size_of_val(unsafe { &*ptr }), layout.size());
        Some(ptr as *const u8 as *const T)
    }

    pub(crate) fn get_aligned_data_at_offset_val(
        &self,
        data::ShOffset(offset): data::ShOffset,
        layout: alloc::Layout,
    ) -> Option<*const [u8]> {
        let tail_offset: u64 = self.tail_offset().try_into().ok()?;
        // Re-base offsets etc onto actual tail data.
        let offset = offset.checked_sub(tail_offset)?;
        let tail = self.tail();
        // Safety: still valid, `&self` protects that memory mapping.
        let max_offset = core::mem::size_of_val(unsafe { &*tail }).checked_sub(layout.size())?;

        if offset.try_into().map_or(true, |n: usize| n > max_offset) {
            return None;
        }

        // As verified by comparison, this fits in a usize and is valid within the tail.
        let start = unsafe { UnsafeCell::raw_get(tail).cast::<u8>().add(offset as usize) };

        if data::align_offset_val(start, layout) != 0 {
            return None;
        }

        // Safety: offset + size fits into `size_of_val(tail)`.
        Some(ptr::slice_from_raw_parts(start, layout.size()))
    }

    pub(crate) unsafe fn read_head(&self) -> Option<*const data::ShmHead> {
        let ptr = self.head.ring;
        let magic = (*(ptr as *const atomic::AtomicU64)).load(atomic::Ordering::Relaxed);

        if !data::RingMagic(magic).test() {
            None
        } else {
            // Strengthen the load into an acquire, on success. This ensures all the other struct
            // members and their access synchronize-with the magic, which has also been written as
            // a release.
            atomic::fence(atomic::Ordering::Acquire);
            Some(ptr)
        }
    }

    pub(crate) unsafe fn init(
        &self,
        mut head: data::ShmHead,
    ) -> (*const data::ShmHead, Weak<dyn RetainedMemory>) {
        assert_eq!(Arc::weak_count(&self._retain), 0);
        let ownership = Arc::downgrade(&self._retain);
        let ptr = self.head.ring;

        // To ensure and _ordering_ relation, we write this last and as an atomic with release
        // ordering such that the volatile write of previous struct members is before for any
        // reader that also inspects the magic with an atomic acquire load.
        let magic = core::mem::replace(&mut head.ring_magic.0, 0);

        // Safety: as promised we currently own this address.
        unsafe { ptr::write_volatile(ptr, head) }
        (*(ptr as *const atomic::AtomicU64)).store(magic, atomic::Ordering::Release);

        (self.head.ring, ownership)
    }

    /// Is the client operating on the same underlying file?
    pub fn same_client(&self, client: &client::Client) -> bool {
        Arc::ptr_eq(&self._retain, &client.shared_ring()._retain)
    }

    /// Is the server operating on the same underlying file?
    pub fn same_server(&self, server: &server::Server) -> bool {
        Arc::ptr_eq(&self._retain, &server.shared_ring()._retain)
    }

    /// Is the ring operating on the same underlying file?
    pub fn same_ring(&self, client: &client::Ring) -> bool {
        Arc::ptr_eq(&self._retain, &client.shared_ring()._retain)
    }
}

impl dyn RetainedMemory {
    /// Safety: requires that the caller keep-alive the memory until the return is dropped itself.
    unsafe fn to_head(self: Arc<Self>) -> Option<SharedHead> {
        let data = self.data_from_head();
        let data = unsafe { &*(data as *mut UnsafeCell<[u8]>) };
        let size = mem::size_of_val(data);

        if size < mem::size_of::<data::ShmHead>() {
            return None;
        }

        if (data.get() as *const u8 as usize) % mem::align_of::<data::ShmHead>() != 0 {
            return None;
        }

        // Safety: lifetime enlarged by caller, otherwise this is a no-op.
        let ring = data.get() as *mut data::ShmHead;
        let tail = (data.get() as *const u8).add(mem::size_of::<data::ShmHead>());
        let tail_size = size - mem::size_of::<data::ShmHead>();

        let offset = tail.align_offset(mem::align_of::<data::ShmHead>());

        if offset > tail_size {
            return None;
        }

        let aligned_tail = tail.add(offset);
        let aligned_size = tail_size - offset;

        let aligned_tail = ptr::slice_from_raw_parts(aligned_tail, aligned_size);
        let aligned_tail = unsafe { &*(aligned_tail as *const UnsafeCell<[u8]>) };

        Some(SharedHead {
            all: data,
            ring,
            aligned_tail,
        })
    }
}
