use shm_pbx::client::{RingRequest, WaitResult};
use shm_pbx::data::{ClientIdentifier, ClientSide, RingIndex};
use shm_pbx::frame::Shared;
use shm_pbx::server::{RingConfig, RingVersion, ServerConfig};
use shm_pbx::MmapRaw;

use std::time::Duration;
use tempfile::NamedTempFile;

#[test]
fn create_server() {
    // Please do not use this in reality. This isn't guaranteed to be in a shared memory region and
    // you'll land in a cache—which isn't compatible with the memory model requirements to
    // communicate effectively. (Or at all, not sure).
    let file = NamedTempFile::new().unwrap();
    file.as_file().set_len(0x1_000_000).unwrap();

    let map = MmapRaw::from_fd(&file).unwrap();
    // Fulfills all the pre-conditions of alignment to map.
    let shared = Shared::new(map).unwrap();

    let rings = [RingConfig {
        version: RingVersion::default(),
        ring_size: 0x10,
        data_size: 0x1234,
        slot_entry_size: 0x8,
        lhs: -1,
        rhs: -1,
    }];

    let shared_client = shared.clone();
    assert!(shared_client.into_client().is_err());

    let shared_server = shared.clone();
    let server = unsafe { shared_server.into_server(ServerConfig { vec: &rings }) };
    let server = server.expect("Have initialized server");

    let shared_client = shared.clone().into_client();
    let client = shared_client.expect("Have initialized client");

    let tid = ClientIdentifier::new();
    let join_lhs = client.join(&RingRequest {
        side: ClientSide::Left,
        index: RingIndex(0),
        tid,
    });

    let tid = ClientIdentifier::new();
    let join_rhs = client.join(&RingRequest {
        side: ClientSide::Right,
        index: RingIndex(0),
        tid,
    });

    let handle = std::thread::spawn(|| {
        let rhs = join_rhs.unwrap();
        let guard = rhs.lock_for_message().expect("Not guarded by rhs");

        // This unlock spuriously whenever the other side notifies us to check for new messages via
        // `wake`. While locked, they can check via a relaxed load whether the lock is taken.
        assert_eq!(guard.wake(Duration::from_millis(1_000)), WaitResult::Ok);
        assert_eq!(rhs.activate(), 1);
    });

    let lhs = join_lhs.unwrap();

    // Does not correspond to our initial assumptions (still inactive), so this must fail.
    assert_eq!(
        lhs.wait_for_message(0, Duration::from_millis(0)),
        WaitResult::PreconditionFailed
    );

    // yes we spin-loop to unlock here, as we don't really expect to produce messages.
    while lhs.wake() == 0 {}

    while {
        let waited = lhs.wait_for_remote(Duration::from_millis(1_000));

        assert!(
            matches!(
                waited,
                // We should expect that a failed precondition is the interception of the toggle by the
                // remote.
                WaitResult::Restart | WaitResult::Ok | WaitResult::PreconditionFailed,
            ),
            "{:?}",
            waited
        );

        !lhs.is_active_remote()
    } {}

    handle.join().expect("Successfully waited");

    let _ = { server };
}
