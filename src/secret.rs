use uapi::{Errno, OwnedFd};

/// An open file descriptor of `memfd_secret`.
///
/// A secret file behaves similar to a not mapped to kernel memory.
pub struct MemfdSecret {
    fd: OwnedFd,
}

impl MemfdSecret {
    pub fn new(len: usize) -> Result<Self, uapi::Errno> {
        Self::new_with(len, uapi::c::FD_CLOEXEC as uapi::c::c_uint)
    }

    pub fn new_with(len: usize, memfd_flags: libc::c_uint) -> Result<Self, uapi::Errno> {
        let fd = Self::syscall_memfd_secret(memfd_flags)?;
        uapi::ftruncate(*fd, len as uapi::c::off_t)?;
        Ok(MemfdSecret { fd })
    }

    pub fn as_raw_fd(&self) -> libc::c_int {
        *self.fd
    }

    fn syscall_memfd_secret(flags: uapi::c::c_uint) -> Result<OwnedFd, Errno> {
        // We mask the flags in case any unsound new bits are available in the future.
        let flags = flags & (uapi::c::FD_CLOEXEC as uapi::c::c_uint);
        // SAFETY: int syscall(SYS_memfd_secret, unsigned int flags);
        let status = unsafe { uapi::c::syscall(uapi::c::SYS_memfd_secret, flags) };

        if status > 0 {
            Ok(OwnedFd::new(status as uapi::c::c_int))
        } else {
            Err(Errno(uapi::get_errno()))
        }
    }
}

#[test]
fn secret_memory_works() {
    let secret = MemfdSecret::new(4096).unwrap();
    let fd = secret.as_raw_fd();
    assert!(fd > 0);
    assert_eq!(uapi::fstat(fd).unwrap().st_size, 4096);
}
