//! Abstract `mmap` call over `uapi`.
//!
//! We use this instead of `memmap2` due to its `no_std` implementation (does not return an
//! `io::Error`). There are several other alternative crates with the same difference, as well as a
//! few crates that are unsound (exposing reference access) and one crate, `safe-mmap`, which only
//! works on immutable files (uses `rustix` underneath).
use uapi::Errno;

/// Holds a unique memory map setup via `mmap`.
pub struct MmapRaw {
    map: *mut u8,
    len: usize,
}

impl Drop for MmapRaw {
    fn drop(&mut self) {
        // SAFETY: was the return value of a non-error mmap call. Also since we are the unique
        // owner this does not unmap any memory that is still referenced.
        unsafe { libc::munmap(self.map as *mut libc::c_void, self.len) };
    }
}

// Safety: does not alias another memory region. Any caller accessing it is responsible for their
// own synchronization requirement; or rather since we are sharing that memory with another process
// they must not rely on any of them.
unsafe impl Send for MmapRaw {}
unsafe impl Sync for MmapRaw {}

// Safety: the memory is unique, and alive for the duration of `self`. The pointers are all valid
// for the whole memory region (as far as that is possible to indicate to Rust).
unsafe impl crate::frame::RetainedMemory for MmapRaw {
    fn data_from_head(&self) -> *mut [u8] {
        // SAFETY: the memory is unique to this object, so we can safely return a pointer to it.
        unsafe { std::slice::from_raw_parts_mut(self.map, self.len) }
    }
}

struct MmapParameter {
    addr: *mut libc::c_void,
    len: libc::size_t,
    prot: libc::c_int,
    flags: libc::c_int,
    fd: libc::c_int,
    offset: libc::off64_t,
}

impl MmapRaw {
    /// Crate a shared mapping of a file, given its length and flags.
    pub fn new_shared(fd: libc::c_int, len: usize, flags: libc::c_int) -> Result<Self, Errno> {
        let len = <libc::size_t>::try_from(len).map_err(|_| Errno(libc::EINVAL))?;

        let addr = Self::syscall_mmap(MmapParameter {
            addr: std::ptr::null_mut(),
            len,
            prot: libc::PROT_READ | libc::PROT_WRITE,
            flags: flags | libc::MAP_SHARED,
            fd,
            offset: 0,
        })?;

        Ok(MmapRaw { map: addr, len })
    }

    #[cfg(feature = "uapi")]
    pub fn from_fd(fd: &impl std::os::fd::AsRawFd) -> Result<Self, Errno> {
        let fd = fd.as_raw_fd();
        let stat = uapi::fstat(fd)?;
        let len = <usize>::try_from(stat.st_size).map_err(|_| Errno(libc::EINVAL))?;
        Self::new_shared(fd, len, 0)
    }

    /// Mmap, but we do not allow full control of the target.
    ///
    /// This ensures that the kernel chooses its own pointer value which does not collide with any
    /// existing address space, thus making this call safe.
    ///
    /// In particular any non-null address sets `MAP_FIXED_NOREPLACE`.
    fn syscall_mmap(
        MmapParameter {
            addr,
            len,
            prot,
            flags,
            fd,
            offset,
        }: MmapParameter,
    ) -> Result<*mut u8, Errno> {
        // Safety: matches the syscall signature.
        //
        // Also the flags are such that the mapping is fresh.

        // If any address is requested, ensure we do not allow replacement.
        let flags = if addr.is_null() {
            if flags & libc::MAP_FIXED != 0 {
                return Err(Errno(libc::EINVAL));
            }

            flags
        } else {
            flags | libc::MAP_FIXED_NOREPLACE
        };

        // Safety: corresponds to mmap64. This does not mess with any existing mapping since we
        // setup the flags such that the returned memory can not alias any existing memory.
        let ptr = unsafe { libc::mmap64(addr, len, prot, flags, fd, offset) };

        if !ptr.is_null() {
            Ok(ptr as *mut u8)
        } else {
            Err(Errno(uapi::get_errno()))
        }
    }
}
