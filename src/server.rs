use crate::config::Config;
use crate::error::{Error, Result};
use crate::persistence::Persistence;
use crate::protocol::{Command, Response};
use crate::replication::Replication;
use crate::store::Store;
use std::io::{BufReader, BufWriter, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
    Mutex,
};
use std::sync::mpsc::{self, Sender};
use std::thread::{self, JoinHandle};

struct Ctx {
    store: Arc<Store>,
    persistence: Arc<Persistence>,
    replication: Arc<Replication>,
}

struct ThreadPool {
    tx: Option<Sender<TcpStream>>,
    workers: Vec<JoinHandle<()>>,
}

impl ThreadPool {
    fn new(
        num_threads: usize,
        ctx: Arc<Ctx>,
    ) -> Self {
        let (tx, rx) = mpsc::channel::<TcpStream>();
        let rx = Arc
        ::new(Mutex
            ::new(rx));
        let workers = (0..num_threads)
            .map(|_| {
                let rx = rx.clone();
                let ctx = ctx.clone();
                thread::spawn(move || {
                    while let Ok(stream) = rx.lock().unwrap().recv() {
                        if let Err(e) = handle_client(stream, &ctx) {
                            eprintln!("[server] client error: {e:?}");
                        }
                    }
                })
            })
            .collect();
        ThreadPool {
            tx: Some(tx),
            workers,
        }
    }

    fn dispatch(
        &self,
        stream: TcpStream,
    ) -> Result<()> {
        self.tx
            .as_ref()
            .expect("dispatch called after shutdown")
            .send(stream)
            .map_err(|_| Error::Other("thread pool is gone".into()))
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        drop(self.tx.take());
        for worker in self
        .workers
        .drain(..) {
            let _ = worker.join();
        }
    }
}

fn handle_client(
    stream: TcpStream,
    ctx: &Ctx,
) -> Result<()> {
    let mut reader = BufReader::new(&stream);
    let mut writer = BufWriter::new(&stream);
    loop {
        let cmd = match Command::decode(&mut reader) {
            Ok(cmd) => cmd,
            Err(Error::Io(e))
                if e.kind() == std
                ::io
                ::ErrorKind
                ::UnexpectedEof =>
            {
                break;
            }
            Err(_) => {
                Response::Err(0x01).encode(&mut writer)?;
                writer.flush()?;
                continue;
            }
        };
        match cmd {
            Command::Set {
                key,
                value,
            } => {
                ctx.store.set(key.clone(), value.clone())?;
                let cmd = Command::Set {
                    key,
                    value,
                };
                ctx.persistence.log_command(&cmd)?;
                ctx.replication.broadcast_command(&cmd)?;
                Response::Ok.encode(&mut writer)?;
            }
            Command::Get {
                key,
            } => {
                match ctx.store.get(&key)? {
                    Some(val) => Response::Value(val)
                    .encode(&mut writer)?,
                    None => Response::Nil
                    .encode(&mut writer)?,
                }
            }
            Command::Delete {
                key,
            } => {
                let count = ctx.store.delete(&key)?;
                let cmd = Command::Delete {
                    key,
                };
                ctx.persistence.log_command(&cmd)?;
                ctx.replication.broadcast_command(&cmd)?;
                Response::Int(count as u64).encode(&mut writer)?;
            }
            Command::Save => {
                ctx.persistence.trigger_snapshot()?;
                Response::Ok.encode(&mut writer)?;
            }
            Command::Replicate {
                ..
            } => {
                writer.flush()?;
                drop(reader);
                drop(writer);
                ctx.replication.add_slave(stream)?;
                return Ok(());
            }
            Command::Shutdown => break,
        }
        writer.flush()?;
    }
    Ok(())
}

pub struct Server {
    listener: TcpListener,
    local_addr: SocketAddr,
    pool: ThreadPool,
    shutdown: Arc<AtomicBool>,
}

impl Server {
    pub fn start(
        config: &Config,
        store: Arc<Store>,
        persistence: Arc<Persistence>,
        replication: Arc<Replication>,
        shutdown: Arc<AtomicBool>,
    ) -> Result<Self> {
        let listener = TcpListener
        ::bind(format!("127.0.0.1:{}", 
            config.port))?;
        let local_addr = listener.local_addr()?;
        let ctx = Arc::new(Ctx {
            store,
            persistence,
            replication,
        });
        let pool = ThreadPool::new(config.threads, ctx);
        Ok(Server {
            listener,
            local_addr,
            pool,
            shutdown,
        })
    }

    pub fn run(
        &self,
    ) -> Result<()> {
        eprintln!("[server] listening on {}", self.local_addr);
        for stream in self
        .listener
        .incoming() {
            if self
            .shutdown
            .load(Ordering::Acquire) {
                break;
            }
            match stream {
                Ok(s) => {
                    if let Err(e) = self.pool.dispatch(s) {
                        eprintln!("[server] dispatch error: {e:?}");
                    }
                }
                Err(e) => eprintln!("[server] accept error: {e}"),
            }
        }
        Ok(())
    }

    pub fn shutdown(
        &self,
    ) {
        self.shutdown.store(true, Ordering::Release);
        let _ = TcpStream::connect(self.local_addr);
    }
}