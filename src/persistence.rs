use crate::config::Config;
use crate::error::{Error, Result};
use crate::protocol::Command;
use crate::store::Store;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

pub struct Persistence {
    aof_sender: Option<Sender<Vec<u8>>>,
    snapshot_sender: Sender<()>,
    shutdown_tx: Option<Sender<()>>,
    store: Arc<Store>,
    config: Config,
    pub cmd_counter: Arc<AtomicU64>,
    pub snapshot_counter: Arc<AtomicU64>,
    aof_thread: Option<JoinHandle<Result<()>>>,
    snapshot_thread: Option<JoinHandle<Result<()>>>,
}

impl Persistence {
    pub fn start(
        store: Arc<Store>,
        config: &Config,
    ) -> Result<Self> {
        std::fs::create_dir_all(&config.data_dir)?;
        Self::replay(store.clone(), config)?;

        let (aof_tx, aof_rx) = mpsc::channel::<Vec<u8>>();
        let (snapshot_tx, snapshot_rx) = mpsc::channel::<()>();
        let (shutdown_tx, shutdown_rx) = mpsc::channel::<()>();

        let cmd_counter = Arc::new(AtomicU64::new(0));
        let snapshot_counter = Arc::new(AtomicU64::new(0));

        let aof_thread = {
            let path = config.aof_path();
            thread::spawn(move || aof_writer(aof_rx, path))
        };

        let snapshot_thread = {
            let store = store.clone();
            let path = config.snapshot_path();
            let cmd_counter = cmd_counter.clone();
            let snapshot_counter = snapshot_counter.clone();
            let interval = Duration
            ::from_secs(config.snapshot_interval_secs);
            let threshold = config.snapshot_commands;
            thread::spawn(move || {
                snapshot_scheduler(
                    store,
                    path,
                    snapshot_rx,
                    shutdown_rx,
                    cmd_counter,
                    snapshot_counter,
                    interval,
                    threshold,
                )
            })
        };

        Ok(Persistence {
            aof_sender: Some(aof_tx),
            snapshot_sender: snapshot_tx,
            shutdown_tx: Some(shutdown_tx),
            store,
            config: config.clone(),
            cmd_counter,
            snapshot_counter,
            aof_thread: Some(aof_thread),
            snapshot_thread: Some(snapshot_thread),
        })
    }

    pub fn log_command(
        &self,
        cmd: &Command,
    ) -> Result<()> {
        let mut buf = Vec::new();
        cmd.encode(&mut buf)?;
        self.aof_sender
            .as_ref()
            .expect("log_command called after shutdown")
            .send(buf)?;
        self.cmd_counter
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub fn trigger_snapshot(
        &self,
    ) -> Result<()> {
        self.snapshot_sender
            .send(())
            .map_err(Into::into)
    }

    pub fn shutdown(
        &mut self,
    ) -> Result<()> {
        // Dropping the real senders (not clones) closes both channels,
        // which signals each background thread to drain and exit.
        drop(self.aof_sender.take());
        drop(self.shutdown_tx.take());

        if let Some(h) = self.snapshot_thread.take() {
            h.join()
                .expect("snapshot thread panicked")?;
        }
        if let Some(h) = self.aof_thread.take() {
            h.join()
                .expect("aof thread panicked")?;
        }

        Ok(())
    }

    fn replay(
        store: Arc<Store>,
        config: &Config,
    ) -> Result<()> {
        let snapshot_path = config.snapshot_path();
        if snapshot_path.exists() {
            store.load_snapshot(&snapshot_path)?;
        }

        let aof_path = config.aof_path();
        if !aof_path.exists() {
            return Ok(());
        }

        let mut reader = std::io::BufReader::new(File::open(&aof_path)?);
        loop {
            match Command::decode(&mut reader) {
                Ok(Command::Set {
                    key,
                    value,
                }) => store.set(key, value)?,
                Ok(Command::Delete {
                    key,
                }) => {
                    store.delete(&key)?;
                }
                Ok(_) => {}
                // A trailing partial write after an unclean shutdown is
                // normal, anything else is real corruption.
                Err(Error::Io(e))
                    if e.kind() == std::io::ErrorKind::UnexpectedEof =>
                {
                    break;
                }
                Err(e) => return Err(e),
            }
        }

        Ok(())
    }
}

fn aof_writer(
    rx: Receiver<Vec<u8>>,
    path: PathBuf,
) -> Result<()> {
    let mut writer = BufWriter::new(
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?,
    );
    for data in rx {
        writer.write_all(&data)?;
        writer.flush()?;
    }
    Ok(())
}

fn snapshot_scheduler(
    store: Arc<Store>,
    path: PathBuf,
    snapshot_rx: Receiver<()>,
    shutdown_rx: Receiver<()>,
    cmd_counter: Arc<AtomicU64>,
    snapshot_counter: Arc<AtomicU64>,
    interval: Duration,
    threshold: u64,
) -> Result<()> {
    let mut last = Instant::now();

    loop {
        thread::sleep(Duration::from_millis(100));

        // Check shutdown first 
        // so a snapshot burst can't delay a clean stop.
        if shutdown_rx.try_recv().is_ok() {
            break;
        }

        let explicit = snapshot_rx.try_recv().is_ok();
        let timed = last.elapsed() >= interval;
        let over_threshold =
            cmd_counter
            .load(Ordering::Relaxed) >= threshold;

        if explicit || timed || over_threshold {
            match store.save_snapshot(&path) {
                Ok(()) => {
                    last = Instant::now();
                    cmd_counter.store(0, Ordering::Relaxed);
                    snapshot_counter
                        .fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => {
                    eprintln!("[persistence] snapshot failed: {e:?}");
                }
            }
        }
    }

    Ok(())
}