mod ring;

use core::mem::MaybeUninit;
use std::os::fd::{AsFd as _, AsRawFd as _};
use std::{rc::Rc, sync::Arc};

use io_uring::opcode;

const COOKIE_SYNC: &str = "120fadb1-ee86-4dde-8cae-20745df78e7c";
const COOKIE_EXIT: &str = "69c5b790-e304-4476-906e-716509513d02";
const COOKIE_SHARED: &str = "c412547c-e64e-4ee5-9ee2-b86f8b0c07ee";
const COOKIE_VAR_FD: &str = "5367cef2-1f77-4e98-baeb-6cb734ae6c9e\0";

fn main() {
    let is_sync = std::env::args_os()
        .filter_map(|arg| arg.to_str().map(String::from))
        .any(|st| st == COOKIE_SYNC);
    let is_exit = std::env::args_os()
        .filter_map(|arg| arg.to_str().map(String::from))
        .any(|st| st == COOKIE_EXIT);
    let is_shared = std::env::args_os()
        .filter_map(|arg| arg.to_str().map(String::from))
        .any(|st| st == COOKIE_SHARED);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    if is_sync || is_exit {
        rt.block_on(main_pipe(is_exit)).unwrap();
    } else if is_shared {
        rt.block_on(main_shared_ring()).unwrap();
    } else {
        rt.block_on(main_coordinator()).unwrap();
    }
}

trait TestInit {
    fn pre_command(
        &self,
        _: &mut tokio::process::Command,
        _: &memmap2::MmapRaw,
    ) -> std::io::Result<Arc<dyn ring::SubmitAllocation>>;
}

async fn main_coordinator() -> Result<(), std::io::Error> {
    let this_exe = std::env::args_os().nth(0).unwrap();

    let file = tempfile::NamedTempFile::new()?;
    file.as_file().set_len(0x100)?;
    let map = memmap2::MmapRaw::map_raw(&file)?;

    let ring = io_uring::IoUring::new(0x100)?;
    let ring = ring::TestCloseRing::new(ring)?;

    struct TestCase {
        cookie: &'static str,
        name: &'static str,
        expected_failure: Option<&'static str>,
        command_init: Option<&'static dyn TestInit>,
    }

    struct SharedTest;

    impl TestInit for SharedTest {
        fn pre_command(
            &self,
            cmd: &mut tokio::process::Command,
            map: &memmap2::MmapRaw,
        ) -> std::io::Result<Arc<dyn ring::SubmitAllocation>> {
            shared_ring_init(cmd, map)
        }
    }

    const TESTS: &[TestCase] = &[
        TestCase {
            cookie: COOKIE_SYNC,
            name: "awaiting child ring",
            expected_failure: None,
            command_init: None,
        },
        TestCase {
            cookie: COOKIE_EXIT,
            name: "forcibly exiting child",
            expected_failure: Some("since the child didn't complete the ring ops"),
            command_init: None,
        },
        TestCase {
            cookie: COOKIE_SHARED,
            name: "sharing ring with child",
            expected_failure: None,
            command_init: Some(&SharedTest),
        },
        TestCase {
            cookie: COOKIE_SYNC,
            name: "awaiting child ring again",
            expected_failure: None,
            command_init: None,
        },
    ];

    for test in TESTS {
        eprintln!("Test {}", test.name);
        let held_for_comms;

        let mut child = {
            let mut cmd = tokio::process::Command::new(&this_exe);

            cmd.arg(file.path()).arg(test.cookie);

            if let Some(init) = &test.command_init {
                held_for_comms = init.pre_command(&mut cmd, &map)?;
            } else {
                held_for_comms = Arc::new(());
            }

            cmd.spawn()?
        };

        let submit = ring.non_empty_submission_and_then_sync(1).await?;

        const CANCELLABLE: u64 = 0;

        let wake =
            opcode::FutexWait::new(map.as_ptr() as *const u32, 0, u32::MAX.into(), 0x02).build().user_data(CANCELLABLE);

        unsafe { submit.redeem_push(&[wake], []) };

        tokio::select! {
            _timeout = async {
                let grace = core::time::Duration::from_millis(1000);
                tokio::time::sleep(grace).await;
            } => {
                if let Some(cause) = test.expected_failure {
                    eprintln!("  [ ] Test failed, as expected, {}", cause);
                } else {
                    eprintln!("  [!] Test failed");
                }

                // Make sure the ring is back to empty for next tests.
                let submit = ring.non_empty_submission_and_then_sync(1).await?;
                let cancel = opcode::AsyncCancel::new(CANCELLABLE)
                    .build();
                unsafe { submit.redeem_push(&[cancel], []) };
                ring.collect().await;

                continue;
            },
            err = async {
                tokio::try_join! {
                    async {
                        ring.collect().await;
                        eprintln!("  [.] Futex awaited");
                        Ok(())
                    },
                    async {
                        child.wait().await.and_then(|out| {
                            if out.success() {
                               eprintln!("  [.] Child reaped");
                                Ok(())
                            } else {
                                Err(std::io::ErrorKind::Other.into())
                            }
                        })
                    },
                }
            } => err?,
        };

        drop(held_for_comms)
    }

    Ok(())
}

fn shared_ring_init(
    command: &mut tokio::process::Command,
    map: &memmap2::MmapRaw,
) -> std::io::Result<Arc<dyn ring::SubmitAllocation>> {
    let ring = io_uring::IoUring::new(0x100)?;
    let ring_fd = ring.as_raw_fd();

    let params = unsafe { map.as_mut_ptr().add(8).cast::<io_uring::Parameters>() };

    assert_eq!(
        params.align_offset(core::mem::align_of::<io_uring::Parameters>()),
        0
    );

    unsafe { core::ptr::copy_nonoverlapping(ring.params(), params, 1) };

    unsafe {
        command.pre_exec(move || {
            let key = COOKIE_VAR_FD.as_bytes().as_ptr();
            let ring_fd = uapi::dup(ring_fd)?.unwrap();

            let val = format!("{}\0", ring_fd);
            let val = val.as_bytes().as_ptr();

            uapi::c::setenv(key as *const _, val as *const _, 1);
            Ok(())
        });
    }

    Ok(Arc::new(ring))
}

async fn main_pipe(exit: bool) -> Result<(), std::io::Error> {
    let file = std::env::args_os().nth(1).unwrap();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(file)?;

    let map = memmap2::MmapRaw::map_raw(&file)?;
    assert!(map.len() >= 4);
    let map = Arc::new(map);

    let (pread, _pwrite) = uapi::pipe2(0)?;

    // 1. Create a pipe.
    // 2. Open shared memory page.
    // 3. Setup linked system calls:
    //     read from pipe,
    //     futex-wake in the shared memory file
    // 4. Close the pipe
    //
    // The expected effect is a read(0) from the pipe and an associated wake. Now try to do the
    // whole thing but close the pipe implicitly by exiting the program and registering the others
    // with the ring.
    let ring = io_uring::IoUring::new(0x100)?;
    let ring = ring::TestCloseRing::new(ring)?;
    ring.collect().await;

    let submit = ring.non_empty_submission_and_then_sync(2).await?;

    let into = Box::leak(Box::new(MaybeUninit::<[u8; 1]>::uninit()));
    let fd = io_uring::types::Fd(pread.as_fd().as_raw_fd());

    let read = opcode::Read::new(fd, into.as_ptr() as *mut _, 1)
        .build()
        .flags(io_uring::squeue::Flags::IO_HARDLINK);

    let wake = opcode::FutexWake::new(
        map.as_ptr() as *const u32,
        u32::MAX.into(),
        u32::MAX.into(),
        0x02,
    )
    .build();

    unsafe { submit.redeem_push(&[read, wake], [Rc::new(map.clone())]) };
    assert_eq!(ring.submit_all()?, 2);

    if exit {
        std::process::exit(0);
    }

    uapi::close(_pwrite)?;
    ring.collect().await;

    Ok(())
}

async fn main_shared_ring() -> Result<(), std::io::Error> {
    let file = std::env::args_os().nth(1).unwrap();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(file)?;

    let map = memmap2::MmapRaw::map_raw(&file)?;
    assert!(map.len() >= 16);
    let map = Arc::new(map);

    let params = unsafe { map.as_ptr().add(8).cast::<io_uring::Parameters>() };

    assert_eq!(
        params.align_offset(core::mem::align_of::<io_uring::Parameters>()),
        0
    );

    let params = unsafe { core::ptr::read(params) };
    let key = COOKIE_VAR_FD.trim_end_matches('\0');

    let fd = std::env::var(key).unwrap().parse().unwrap();
    let ring = unsafe { io_uring::IoUring::from_fd(fd, params) }?;

    let (pread, _pwrite) = uapi::pipe2(0)?;

    // 1. Create a pipe.
    // 2. Open shared memory page.
    // 3. Setup linked system calls:
    //     read from pipe,
    //     futex-wake in the shared memory file
    // 4. Exit the process
    //
    // The expected effect is a read(0) from the pipe and an associated wake, in the context of the
    // parent process which still has the ring open.
    let ring = ring::TestCloseRing::new(ring)?;
    ring.collect().await;

    let submit = ring.non_empty_submission_and_then_sync(2).await?;

    // Wait wtf does this even mean if we die. LMAO.
    let into = Box::leak(Box::new(MaybeUninit::<[u8; 1]>::uninit()));
    let fd = io_uring::types::Fd(pread.as_fd().as_raw_fd());

    let read = opcode::Read::new(fd, into.as_ptr() as *mut _, 1)
        .build()
        .flags(io_uring::squeue::Flags::IO_HARDLINK);

    let wake = opcode::FutexWake::new(
        map.as_ptr() as *const u32,
        u32::MAX.into(),
        u32::MAX.into(),
        0x02,
    )
    .build();

    unsafe { submit.redeem_push(&[read, wake], [Rc::new(map.clone())]) };
    assert_eq!(ring.submit_all()?, 2);

    Ok(())
}
