use core::{num::NonZeroU64, time::Duration};
use std::os::fd::AsRawFd as _;
use std::{fs, path::PathBuf};

use shm_pbx::frame::Shared;
use shm_pbx::io_uring::ShmIoUring;
use shm_pbx::server::{
    RingConfig, RingVersion, ServerConfig, ServerError, ServerTask, TrackedClient,
};
use shm_pbx::MmapRaw;

use quick_error::quick_error;
use serde::Deserialize;
use tempfile::NamedTempFile;
use tokio::{
    io::{unix::AsyncFd, Interest},
    task::JoinSet,
};

#[derive(Deserialize)]
struct Options {
    /// A version number for the configuration.
    ///
    /// Always 1.
    version: u64,
    /// The size of the backing memory to create.
    size: u64,
    /// All rings and their data to generate.
    rings: Vec<RingOptions>,
    /// Duration used when sleeping on a client trigger, in nano seconds.
    ///
    /// Note that this is not a latency but rather a maximum time before waking, not a minimum
    /// time.
    ///
    /// Providing no value uses some arbitrary default below a second. (FIXME: motivation for this
    /// default).
    sleep: Option<NonZeroU64>,
}

#[derive(Deserialize)]
struct RingOptions {
    /// A version number, for differentiating different ring layouts later.
    ///
    /// Always 1.
    version: u64,
    /// The size (in bytes) to use for the entries in the ring.
    ring_size: u64,
    /// The size of an entry in the ring structures.
    slot_entry_size: u64,
    /// The size (in bytes) of auxiliary memory to reserve for the ring's communication partners
    /// which may be used fully between them.
    data_size: u64,
    /// The ID to advertise on the left of this ring when it falls to the server.
    lhs_id: Option<i32>,
    /// The ID to advertise on the right of this ring when it falls to the server.
    rhs_id: Option<i32>,
}

quick_error! {
    #[derive(Debug)]
    pub enum ValidationError {
        InvalidRingSize
    }
}

quick_error! {
    #[derive(Debug)]
    pub enum ServerServeError {
        BadInternalOptions (err: ServerError) {
            from()
        }
        IoUring (err: std::io::Error) {
            from(err: ServeIoUring) -> (err.0)
        }
        UnsupportedOs {}
    }
}

/// An IO-error that originates from serving the IO-uring.
///
/// Distinguishes those at the type-level to make `ServerServeError::from` work.
struct ServeIoUring(std::io::Error);

quick_error! {
    #[derive(Debug)]
    pub enum OptionsError {
        UnsupportedVersion {}
        FailedToUseMap {}
    }
}

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        BadOptions (err: OptionsError) {
            from()
        }
        Serve(err: ServerServeError) {
            from()
        }
        Config (err: serde_json::Error) {
            from()
        }
        Io (err: std::io::Error) {
            from()
        }
    }
}

fn main() -> Result<(), Error> {
    let matches = clap::command!()
        .arg(
            clap::arg!(
                -c --config <FILE> "A json configuration file"
            )
            .required(true)
            .value_parser(clap::value_parser!(PathBuf)),
        )
        .get_matches();

    let Some(config) = matches.get_one::<PathBuf>("config") else {
        panic!("Parser validation failed");
    };

    let file = fs::File::open(config)?;
    let options: Options = serde_json::de::from_reader(file)?;

    let file = NamedTempFile::new()?;
    file.as_file().set_len(options.size)?;
    let map = MmapRaw::from_fd(&file).map_err(std::io::Error::from)?;

    if options.version != 1 {
        return Err(OptionsError::UnsupportedVersion)?;
    }

    if options.rings.iter().any(|opt| opt.version != 1) {
        return Err(OptionsError::UnsupportedVersion)?;
    }

    // Fulfills all the pre-conditions of alignment to map.
    let shared = Shared::new(map).ok_or(OptionsError::FailedToUseMap)?;

    {
        let pid = uapi::getpid();
        let fid = file.as_raw_fd();
        eprintln!("/proc/{pid}/fd/{fid}");
    }

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()?;

    rt.block_on(serve_map_in(shared, options))?;

    Ok(())
}

async fn serve_map_in(shared: Shared, options: Options) -> Result<(), ServerServeError> {
    let uring = ShmIoUring::new(&shared).map_err(ServeIoUring)?;

    if !uring.is_supported().map_err(ServeIoUring)?.any() {
        return Err(ServerServeError::UnsupportedOs);
    }

    let rings: Vec<_> = options
        .rings
        .iter()
        .map(|opt| RingConfig {
            version: RingVersion::default(),
            ring_size: opt.ring_size,
            data_size: opt.data_size,
            slot_entry_size: opt.slot_entry_size,
            rhs: opt.rhs_id.unwrap_or(0),
            lhs: opt.lhs_id.unwrap_or(0),
        })
        .collect();

    let duration = options.sleep.map_or(10_000_000, NonZeroU64::get);
    let duration = Duration::from_nanos(duration);
    let server = unsafe { shared.into_server(ServerConfig { vec: &rings }) }?;
    let mut task = ServerTask::default();

    let mut tasks: JoinSet<Result<_, ServerServeError>> = JoinSet::new();
    let (reap_send, mut reaper) = tokio::sync::mpsc::channel::<TrackedClient>(0x1);

    loop {
        tokio::select! {
            waiter = uring.wait_server(&server, duration) => {
                let _ = waiter.map_err(ServeIoUring)?;
            }
            client = reaper.recv() => {
                let Some(client) = client else {
                    break;
                };

                eprintln!("Reaped dead client pid {:?}", client.identifier());
                server.reap_client(&mut task, client);
            }
        }

        for client in server.track_clients(&mut task) {
            let reap_send = reap_send.clone();

            tasks.spawn(async move {
                let died =
                    AsyncFd::with_interest(client, Interest::READABLE).map_err(ServeIoUring)?;

                died.readable().await.map_err(ServeIoUring)?.clear_ready();
                let _ = reap_send.send(died.into_inner()).await;

                Ok(())
            });
        }

        match server.bring_up(&rings) {
            0 => {}
            n @ 1.. => {
                eprintln!("Reinitialized {} ring pair", n);
            }
        }

        std::hint::spin_loop();
        std::thread::yield_now();
    }

    Ok(())
}
