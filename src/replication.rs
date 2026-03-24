use std::io::{BufReader, BufWriter, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::config::{Config, Role};
use crate::error::{Error, Result};
use crate::protocol::Command;
use crate::store::Store;

pub struct Replication {
    inner: Inner,
    store: Arc<Store>,
}

enum Inner {
    Master(MasterState),
    Slave(SlaveState),
}

struct MasterState {
    slaves: Arc<Mutex<Vec<SlaveConn>>>,
    cmd_tx: Sender<Vec<u8>>,
    _broadcaster: JoinHandle<()>,
}

struct SlaveState {
    master_addr: String,
    _worker: JoinHandle<()>,
}

struct SlaveConn {
    id: u64,
    writer: BufWriter<TcpStream>,
}

static SLAVE_ID: AtomicU64 = AtomicU64::new(0);

impl Replication {
    pub fn start(
        store: Arc<Store>,
        config: &Config,
    ) -> Result<Self> {
        let inner = match config.role {
            Role::Master => {
                let slaves = Arc::new(Mutex::new(Vec::new()));
                let (cmd_tx, cmd_rx) = mpsc::channel();
                let broadcaster = Self::spawn_broadcaster(
                    cmd_rx,
                    slaves.clone(),
                );
                Inner::Master(MasterState {
                    slaves,
                    cmd_tx,
                    _broadcaster: broadcaster,
                })
            }
            Role::Slave => {
                let addr = config
                    .master_addr
                    .clone()
                    .ok_or_else(|| Error
                        ::Config("slave role requires master_addr"
                            .into())
                        )?;
                let worker = 
                Self::spawn_slave_worker(
                    store.clone(),
                    addr.clone(),
                );
                Inner::Slave(SlaveState {
                    master_addr: addr,
                    _worker: worker,
                })
            }
        };
        Ok(Replication {
            inner,
            store,
        })
    }

    pub fn add_slave(
        &self,
        stream: TcpStream,
    ) -> Result<()> {
        let Inner::Master(ref m) = 
        self.inner else {
            return Err(Error
                ::Config("add_slave called on a slave node"
                .into()));
        };
        m.slaves
            .lock()
            .unwrap()
            .push(SlaveConn {
                id: SLAVE_ID.fetch_add(1, Ordering::Relaxed),
                writer: BufWriter::new(stream),
            });
        Ok(())
    }

    pub fn broadcast_command(
        &self,
        cmd: &Command,
    ) -> Result<()> {
        let Inner::Master(ref m) = self.inner else {
            return Ok(());
        };
        let mut buf = Vec::new();
        cmd.encode(&mut buf)?;
        m.cmd_tx
            .send(buf)
            .map_err(|_| Error
                ::Config("broadcaster thread is gone"
                    .into())
                )
    }

    fn spawn_broadcaster(
        rx: Receiver<Vec<u8>>,
        slaves: Arc<Mutex<Vec<SlaveConn>>>,
    ) -> JoinHandle<()> {
        thread::spawn(move || {
            for data in rx {
                let mut dead = Vec::new();
                let mut guard = slaves
                .lock()
                .unwrap();
                for slave in guard.iter_mut() {
                    if slave
                        .writer
                        .write_all(&data)
                        .and_then(|_| slave.writer
                            .flush())
                        .is_err()
                    {
                        dead.push(slave.id);
                    }
                }
                if !dead.is_empty() {
                    guard.retain(|s| !dead.contains(&s.id));
                }
            }
        })
    }

    fn spawn_slave_worker(
        store: Arc<Store>,
        addr: String,
    ) -> JoinHandle<()> {
        thread::spawn(move || loop {
            if let Err(e) = Self::slave_session(&store, &addr) {
                eprintln!(
                    "[replication] session error, reconnecting in 1s: {e:?}"
                );
            }
            thread::sleep(Duration::from_secs(1));
        })
    }

    fn slave_session(
        store: &Arc<Store>,
        addr: &str,
    ) -> Result<()> {
        let stream = TcpStream::connect(addr)?;
        {
            let mut w = BufWriter::new(&stream);
            Command::Replicate {
                command: Vec::new(),
            }
            .encode(&mut w)?;
            w.flush()?;
        }
        let mut reader = BufReader::new(&stream);
        loop {
            match Command::decode(&mut reader)? {
                Command::Set {
                    key,
                    value,
                } => store.set(key, value)?,
                Command::Delete {
                    key,
                } => {
                    store.delete(&key)?;
                }
                other => {
                    return Err(Error::Protocol(format!(
                        "unexpected command from master: {other:?}"
                    )))
                }
            }
        }
    }
}