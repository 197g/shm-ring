mod notify;
mod stream;

use shm_pbx::client::{Client, Ring, RingJoinError, RingRequest};
use shm_pbx::data::{ClientIdentifier, ClientSide, RingIndex};
use shm_pbx::frame::Shared;
use shm_pbx::io_uring::ShmIoUring;
use shm_pbx::MmapRaw;

use quick_error::quick_error;
use serde::Deserialize;
use wasmtime::{Instance, Module, Store};
use wasmtime_wasi::{p2::StreamError, p2::WasiCtxBuilder, preview1};

use std::{fs, path::PathBuf, rc};

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

    let map = MmapRaw::from_fd(&server).unwrap();
    // Fulfills all the pre-conditions of alignment to map.
    let shared = Shared::new(map).unwrap();
    let tid = ClientIdentifier::from_pid();
    let client = shared.into_client(tid).expect("Have initialized client");

    let mut config = wasmtime::Config::new();
    config.async_support(true);
    config.consume_fuel(true);
    let engine = wasmtime::Engine::new(&config)?;

    let programs = options
        .modules
        .iter()
        .map(|mod_options| new_program(&engine, &mod_options, &client, opt_path.to_path_buf()))
        .collect::<Result<Vec<_>, _>>()?;

    let host = host(&options, &client)?;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()?;

    let mut now = std::time::Instant::now();
    rt.block_on(async {
        let local = tokio::task::LocalSet::new();

        for program in programs {
            let shared = shared.clone();

            // If we could we might run this transparently on another thread. But this can not move
            // after its creation. It is not `Send` due to the uring and localset being alive
            // across await points. Therefore to realize such a system we need to spawn a dedicated
            // thread and send the task description to it.
            let run_module = async move {
                let shared = shared;
                let local = tokio::task::LocalSet::new();
                let uring = rc::Rc::new(ShmIoUring::new(&shared).map_err(ClientIoUring)?);

                let (store, instance) = instantiate(uring.clone(), program, &local).await?;
                local.run_until(communicate(store, instance)).await?;

                Ok::<_, ClientRunError>(())
            };

            local.spawn_local(run_module);
        }

        let uring = rc::Rc::new(ShmIoUring::new(&shared).map_err(ClientIoUring)?);
        let host = communicate_host(uring, host, &local);

        now = std::time::Instant::now();
        local.run_until(host).await?;

        Ok::<_, ClientRunError>(())
    })?;

    eprintln!("Time elapsed: {}ms", now.elapsed().as_millis());

    Ok(())
}

fn new_program(
    engine: &wasmtime::Engine,
    options: &WasmModule,
    client: &Client,
    opt_path: PathBuf,
) -> Result<Program, Error> {
    let WasmModule::WasiV1 {
        module,
        stdin,
        stdout,
        stderr,
    } = &options;

    let stdin = join_ring(stdin.as_ref(), client)?;
    let stdout = join_ring(stdout.as_ref(), client)?;
    let stderr = join_ring(stderr.as_ref(), client)?;

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

fn host(options: &Options, client: &Client) -> Result<Host, Error> {
    let stdin = join_ring(options.stdin.as_ref(), client)?;
    let stdout = join_ring(options.stdout.as_ref(), client)?;
    let stderr = join_ring(options.stderr.as_ref(), client)?;

    Ok(Host {
        stdin,
        stdout,
        stderr,
    })
}

fn join_ring(
    options: Option<&StreamOptions>,
    client: &Client,
) -> Result<Option<Ring>, RingJoinError> {
    options
        .map(|opt| {
            let mut ring = client.join(&RingRequest {
                index: RingIndex(opt.index),
                side: opt.side,
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

impl wasmtime_wasi::p2::StdinStream for Stdin {
    fn stream(&self) -> Box<dyn wasmtime_wasi::p2::InputStream> {
        Box::new(self.inner.clone())
    }

    fn isatty(&self) -> bool {
        false
    }
}

impl wasmtime_wasi::p2::StdoutStream for Stdout {
    fn stream(&self) -> Box<dyn wasmtime_wasi::p2::OutputStream> {
        Box::new(self.inner.stream())
    }

    fn isatty(&self) -> bool {
        false
    }
}

async fn instantiate(
    uring: rc::Rc<ShmIoUring>,
    program: Program,
    local: &tokio::task::LocalSet,
) -> Result<(Store<ModuleP1Data>, Instance), ClientRunError> {
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

    Ok((store, instance))
}

async fn communicate(
    mut store: Store<ModuleP1Data>,
    instance: Instance,
) -> Result<(), ClientRunError> {
    let main = instance.get_typed_func::<(), ()>(&mut store, "_start")?;

    const FUEL: u64 = 1 << 18;
    store.fuel_async_yield_interval(Some(FUEL))?;
    store.set_fuel(u64::MAX)?;

    main.call_async(&mut store, ()).await?;

    Ok(())
}

async fn communicate_host(
    uring: rc::Rc<ShmIoUring>,
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
    uring: rc::Rc<ShmIoUring>,
    ring: Ring,
    local: &tokio::task::LocalSet,
) -> Result<(), ClientRunError> {
    use tokio::io::AsyncBufReadExt as _;
    use wasmtime_wasi::p2::OutputStream as _;

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
            Err(StreamError::Closed) => break,
            Err(_err) => unreachable!(),
        };

        if write == 0 {
            match proxy.write_ready().await {
                Ok(_) => {}
                Err(StreamError::Closed) => break,
                Err(_err) => panic!(),
            }

            continue;
        }

        let bytes = bytes::Bytes::copy_from_slice(&buf[..write]);
        match proxy.write(bytes) {
            Ok(()) => {}
            Err(StreamError::Closed) => break,
            Err(_err) => panic!(),
        };

        stdin.consume(write);
        tokio::task::yield_now().await;
    }

    Ok(())
}

async fn move_stdout(
    uring: rc::Rc<ShmIoUring>,
    ring: Ring,
    local: &tokio::task::LocalSet,
) -> Result<(), ClientRunError> {
    use tokio::io::AsyncWriteExt as _;
    use wasmtime_wasi::p2::{InputStream as _, Pollable as _};

    let mut proxy = stream::InputRing::new(ring, uring.clone(), &local);
    let mut stdout = tokio::io::BufWriter::new(tokio::io::stdout());

    const SIZE: usize = 1 << 12;

    loop {
        let bytes = match proxy.read(SIZE) {
            Ok(bytes) if !bytes.is_empty() => bytes,
            Ok(_) => {
                proxy.ready().await;
                continue;
            }
            Err(StreamError::Closed) => break,
            Err(err) => {
                eprintln!("{err:?}");
                panic!()
            }
        };

        stdout.write_all(&bytes).await.map_err(ClientIoUring)?;
    }

    stdout.flush().await.map_err(ClientIoUring)?;

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
