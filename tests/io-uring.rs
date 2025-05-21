#![cfg(feature = "io-uring")]
use shm_pbx::client::{RingRequest, WaitResult};
use shm_pbx::data::{ClientIdentifier, ClientSide, RingIndex};
use shm_pbx::frame::Shared;
use shm_pbx::io_uring::ShmIoUring;
use shm_pbx::server::{RingConfig, RingVersion, ServerConfig};
use shm_pbx::MmapRaw;

use std::time::Duration;
use tempfile::NamedTempFile;

#[tokio::test(flavor = "multi_thread")]
async fn sync_rings() {
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

    let shared_server = shared.clone();
    let server = unsafe { shared_server.into_server(ServerConfig { vec: &rings }) };
    let _server = server.expect("Have initialized server");

    let shared_client = shared.clone().into_client();
    let client = shared_client.expect("Have initialized client");

    let tid = ClientIdentifier::new();
    let join_lhs = client.join(&RingRequest {
        side: ClientSide::Left,
        index: RingIndex(0),
        tid,
    });

    let join_rhs = client.join(&RingRequest {
        side: ClientSide::Right,
        index: RingIndex(0),
        tid,
    });

    let lhs = join_lhs.expect("Have initialized left side");
    let rhs = join_rhs.expect("Have initialized right side");

    let ring = ShmIoUring::new(&shared).unwrap();

    assert!(
        ring.is_supported().expect("Probing works").any(),
        "Your OS does not support the Futex operations required"
    );

    eprintln!("Sending message");
    let (locked, woken) = tokio::join!(
        ring.lock_for_message(&rhs, Duration::from_millis(1_000)),
        async {
            loop {
                match ring.wake(&lhs).await {
                    Err(e) => break Err(e),
                    Ok(0) => {}
                    Ok(n) => break Ok(n),
                }
            }
        }
    );

    assert!(matches!(
        locked.expect("IO-Uring successful"),
        WaitResult::Ok,
    ));

    assert!(matches!(woken.expect("IO-Uring successful"), 1));

    let hdl = tokio::task::spawn_blocking(move || {
        rhs.activate();
        // Avoid that side deactivating again! This leaks the ownership of the ring but that is
        // fine as far as the data model is concerned. You just can not join that ring queue
        // specifically again, but still drain the queue and demap the memory.
        core::mem::forget(rhs);
    });

    eprintln!("Activating");
    let (joiner, active) = tokio::join!(hdl, async {
        loop {
            let waited = ring
                .wait_for_remote(&lhs, Duration::from_millis(1_000))
                .await;

            let waited = match waited {
                Ok(waited) => waited,
                Err(e) => break Err(e),
            };

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

            if lhs.is_active_remote() {
                break Ok(());
            }
        }
    });

    joiner.expect("Activation successful");
    let () = active.expect("IO-Uring successful");
}
