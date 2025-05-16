mod stream;

use shm_pbx::client::{Client, Ring, RingJoinError, RingRequest, WaitResult};
use shm_pbx::data::{ClientIdentifier, ClientSide, RingIndex};
use shm_pbx::frame::Shared;
use shm_pbx::io_uring::ShmIoUring;
use shm_pbx::server::{RingConfig, RingVersion, ServerConfig};

use memmap2::MmapRaw;
use quick_error::quick_error;
use serde::Deserialize;
use wasmtime::{Module, Store};
use wasmtime_wasi::{preview1, ResourceTable, WasiCtx, WasiCtxBuilder};

use std::{fs, path::PathBuf, sync};

#[derive(Deserialize)]
struct Options {
    modules: Vec<WasmModule>,
    stdin: Option<StreamOptions>,
    stdout: Option<StreamOptions>,
    stderr: Option<StreamOptions>,
}

#[derive(Deserialize)]
pub enum WasmModule {
    WasiV1 {
        module: PathBuf,
        stdin: Option<StreamOptions>,
        stdout: Option<StreamOptions>,
        stderr: Option<StreamOptions>,
    },
}

#[derive(Deserialize)]
pub struct StreamOptions {
    index: usize,
    #[serde(deserialize_with = "de::deserialize_client")]
    side: ClientSide,
}

struct ModuleP1Data {
    ctx: preview1::WasiP1Ctx,
}

struct Program {
    module: Module,
    engine: wasmtime::Engine,
    stdin: Option<Ring>,
    stdout: Option<Ring>,
    stderr: Option<Ring>,
}

struct Host {
    stdin: Option<Ring>,
    stdout: Option<Ring>,
    stderr: Option<Ring>,
}

quick_error! {
    #[derive(Debug)]
    pub enum ClientRunError {
        IoUring (err: std::io::Error) {
            from(err: ClientIoUring) -> (err.0)
        }
        UnsupportedOs {}
        WasmModule (err: wasmtime::Error) {
            from()
        }
    }
}

/// An IO-error that originates from serving the IO-uring.
///
/// Distinguishes those at the type-level to make `ClientRunError::from` work.
struct ClientIoUring(std::io::Error);

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Serve(err: ClientRunError) {
            from()
        }
        Config (err: serde_json::Error) {
            from()
        }
        Io (err: std::io::Error) {
            from()
        }
        WasmModule (err: wasmtime::Error) {
            from()
        }
        Join(err: RingJoinError) {
            from()
        }
    }
}

fn main() -> Result<(), Error> {
    let matches = clap::command!()
        .arg(
            clap::arg!(
                <server> "The serve to join"
            )
            .required(true)
            .value_parser(clap::value_parser!(PathBuf)),
        )
        .arg(
            clap::arg!(
                -c --config <FILE> "A json configuration file"
            )
            .required(true)
            .value_parser(clap::value_parser!(PathBuf)),
        )
        .get_matches();

    let Some(server) = matches.get_one::<PathBuf>("server") else {
        panic!("Parser validation failed");
    };

    let Some(config) = matches.get_one::<PathBuf>("config") else {
        panic!("Parser validation failed");
    };

    let file = fs::File::open(config)?;
    let options: Options = serde_json::de::from_reader(file)?;
    let opt_path = config.parent().unwrap();

    let server = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(server)?;

    let map = MmapRaw::map_raw(&server).unwrap();
    // Fulfills all the pre-conditions of alignment to map.
    let shared = Shared::new(map).unwrap();
    let client = shared.into_client().expect("Have initialized client");

    let tid = ClientIdentifier::new();
    let mod_options = &options.modules[0];

    let mut config = wasmtime::Config::new();
    config.async_support(true);
    config.consume_fuel(true);
    let engine = wasmtime::Engine::new(&config)?;

    let program = new_program(&engine, &mod_options, &client, tid, opt_path.to_path_buf())?;

    let host = host(&options, &client, tid)?;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()?;

    rt.block_on(async {
        let local = tokio::task::LocalSet::new();

        let uring = ShmIoUring::new(&shared).map_err(ClientIoUring)?;
        let uring = sync::Arc::new(uring);

        let host = communicate_host(uring.clone(), host, &local);
        let client = communicate(uring.clone(), program, &local);

        local
            .run_until(async move { tokio::try_join!(host, client,) })
            .await?;

        Ok::<_, ClientRunError>(())
    })?;

    Ok(())
}

fn new_program(
    engine: &wasmtime::Engine,
    options: &WasmModule,
    client: &Client,
    tid: ClientIdentifier,
    opt_path: PathBuf,
) -> Result<Program, Error> {
    let WasmModule::WasiV1 {
        module,
        stdin,
        stdout,
        stderr,
    } = &options;

    let stdin = join_ring(stdin.as_ref(), client, tid)?;
    let stdout = join_ring(stdout.as_ref(), client, tid)?;
    let stderr = join_ring(stderr.as_ref(), client, tid)?;

    let module = opt_path.join(module);
    let module = module.canonicalize()?;

    let module = std::fs::read(module)?;
    let module = Module::new(&engine, module)?;

    Ok(Program {
        module,
        engine: engine.clone(),
        stdin,
        stdout,
        stderr,
    })
}

fn host(options: &Options, client: &Client, tid: ClientIdentifier) -> Result<Host, Error> {
    let stdin = join_ring(options.stdin.as_ref(), client, tid)?;
    let stdout = join_ring(options.stdout.as_ref(), client, tid)?;
    let stderr = join_ring(options.stderr.as_ref(), client, tid)?;

    Ok(Host {
        stdin,
        stdout,
        stderr,
    })
}

fn join_ring(
    options: Option<&StreamOptions>,
    client: &Client,
    tid: ClientIdentifier,
) -> Result<Option<Ring>, RingJoinError> {
    options
        .map(|opt| {
            let mut ring = client.join(&RingRequest {
                index: RingIndex(opt.index),
                side: opt.side,
                tid,
            });

            if let Ok(ring) = &mut ring {
                ring.activate();
            }

            ring
        })
        .transpose()
}

struct Stdin {
    inner: stream::InputRing,
}

struct Stdout {
    inner: stream::OutputRing,
}

impl wasmtime_wasi::StdinStream for Stdin {
    fn stream(&self) -> Box<dyn wasmtime_wasi::HostInputStream> {
        Box::new(self.inner.clone())
    }

    fn isatty(&self) -> bool {
        false
    }
}

impl wasmtime_wasi::StdoutStream for Stdout {
    fn stream(&self) -> Box<dyn wasmtime_wasi::HostOutputStream> {
        Box::new(self.inner.stream())
    }

    fn isatty(&self) -> bool {
        false
    }
}

async fn communicate(
    uring: sync::Arc<ShmIoUring>,
    program: Program,
    local: &tokio::task::LocalSet,
) -> Result<(), ClientRunError> {
    let main = ModuleP1Data {
        ctx: {
            let mut ctx = WasiCtxBuilder::new();

            if let Some(stdin) = program.stdin {
                let stdin = Stdin {
                    #[cfg(debug_assertions)]
                    inner: stream::InputRing::new(stdin, uring.clone(), &local).with_name(0),
                    #[cfg(not(debug_assertions))]
                    inner: stream::InputRing::new(stdin, uring.clone(), &local),
                };

                ctx.stdin(stdin);
            }

            if let Some(stdout) = program.stdout {
                let stdout = Stdout {
                    #[cfg(debug_assertions)]
                    inner: stream::OutputRing::new(stdout, uring.clone(), &local).with_name(1),
                    #[cfg(not(debug_assertions))]
                    inner: stream::OutputRing::new(stdout, uring.clone(), &local),
                };

                ctx.stdout(stdout);
            }

            if let Some(stderr) = program.stderr {
                let stderr = Stdout {
                    inner: stream::OutputRing::new(stderr, uring.clone(), &local),
                };

                ctx.stderr(stderr);
            }

            ctx.build_p1()
        },
    };

    let mut linker = wasmtime::Linker::<ModuleP1Data>::new(&program.engine);

    let mut store = Store::new(&program.engine, main);
    wasmtime_wasi::preview1::add_to_linker_async(&mut linker, |main| &mut main.ctx)?;

    let instance = linker
        .instantiate_async(&mut store, &program.module)
        .await?;

    let main = instance.get_typed_func::<(), ()>(&mut store, "_start")?;

    const FUEL: u64 = 1 << 12;
    store.fuel_async_yield_interval(Some(FUEL))?;
    store.set_fuel(u64::MAX)?;

    main.call_async(&mut store, ()).await?;

    Ok(())
}

async fn communicate_host(
    uring: sync::Arc<ShmIoUring>,
    host: Host,
    local: &tokio::task::LocalSet,
) -> Result<(), ClientRunError> {
    tokio::try_join!(
        async {
            if let Some(stream) = host.stdin {
                move_stdin(uring.clone(), stream, local).await
            } else {
                Ok(())
            }
        },
        async {
            if let Some(stream) = host.stdout {
                move_stdout(uring.clone(), stream, local).await
            } else {
                Ok(())
            }
        },
    )?;

    Ok(())
}

async fn move_stdin(
    uring: sync::Arc<ShmIoUring>,
    ring: Ring,
    local: &tokio::task::LocalSet,
) -> Result<(), ClientRunError> {
    use tokio::io::AsyncBufReadExt as _;
    use wasmtime_wasi::HostOutputStream as _;

    let proxy = stream::OutputRing::new(ring, uring.clone(), &local);
    let mut proxy = proxy.stream();

    let mut stdin = tokio::io::BufReader::new(tokio::io::stdin());

    const SIZE: usize = 1 << 12;

    loop {
        let buf = stdin.fill_buf().await.map_err(ClientIoUring)?;

        if buf.is_empty() {
            break;
        }

        let write = match proxy.check_write() {
            Ok(n) => buf.len().min(n).min(SIZE),
            Err(wasmtime_wasi::StreamError::Closed) => break,
            Err(_err) => unreachable!(),
        };

        if write == 0 {
            match proxy.write_ready().await {
                Ok(_) => {}
                Err(wasmtime_wasi::StreamError::Closed) => break,
                Err(_err) => panic!(),
            }

            continue;
        }

        let bytes = bytes::Bytes::copy_from_slice(&buf[..write]);
        match proxy.write(bytes) {
            Ok(()) => {}
            Err(wasmtime_wasi::StreamError::Closed) => break,
            Err(_err) => panic!(),
        };

        stdin.consume(write);
        tokio::task::yield_now().await;
    }

    Ok(())
}

async fn move_stdout(
    uring: sync::Arc<ShmIoUring>,
    ring: Ring,
    local: &tokio::task::LocalSet,
) -> Result<(), ClientRunError> {
    use tokio::io::AsyncWriteExt as _;
    use wasmtime_wasi::{HostInputStream as _, Subscribe};

    let mut proxy = stream::InputRing::new(ring, uring.clone(), &local);
    let mut stdout = tokio::io::stdout();
    const SIZE: usize = 1 << 12;

    loop {
        let bytes = match proxy.read(SIZE) {
            Ok(bytes) if !bytes.is_empty() => bytes,
            Ok(_) => {
                proxy.ready().await;
                continue;
            }
            Err(wasmtime_wasi::StreamError::Closed) => break,
            Err(_err) => panic!(),
        };

        stdout.write_all(&bytes).await.map_err(ClientIoUring)?;
    }

    Ok(())
}

mod de {
    use serde::{Deserialize, Deserializer};

    #[derive(Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum ClientSide {
        Left,
        Right,
    }

    pub fn deserialize_client<'de, D>(de: D) -> Result<super::ClientSide, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(match ClientSide::deserialize(de)? {
            ClientSide::Left => super::ClientSide::Left,
            ClientSide::Right => super::ClientSide::Right,
        })
    }
}
