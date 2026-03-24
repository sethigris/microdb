mod config;
mod error;
mod persistence;
mod protocol;
mod replication;
mod server;
mod store;

use crate::config::Config;
use crate::error::Result;
use crate::persistence::Persistence;
use crate::replication::Replication;
use crate::server::Server;
use crate::store::Store;
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread;
use std::time::Duration;

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let config_path = if args.len() > 1 && args[1] == "--config" && args.len() > 2 {
        PathBuf::from(&args[2])
    } else {
        PathBuf::from("./microdb.conf")
    };

    let config = Config::load(&config_path)?;
    eprintln!("Config loaded: {:?}", config);

    let store = Arc::new(Store::new());
    let persistence = Arc::new(Persistence::start(
        store.clone(),
        &config,
    )?);
    let replication = Arc::new(Replication::start(
        store.clone(),
        &config,
    )?);

    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let server = Server::start(
        &config,
        store,
        persistence,
        replication,
        shutdown_flag.clone(),
    )?;

    // Spawn a thread that checks for a shutdown file (simple mechanism)
    let shutdown_flag2 = shutdown_flag.clone();
    let handle = thread::spawn(move || loop {
        if PathBuf::from("./shutdown.txt").exists() {
            shutdown_flag2.store(true, Ordering::Relaxed);
            break;
        }
        thread::sleep(Duration::from_millis(100));
    });

    if let Err(e) = server.run() {
        eprintln!("Server error: {:?}", e);
    }

    shutdown_flag.store(true, Ordering::Relaxed);
    let _ = handle.join();

    // Cleanup (persistence shutdown already handled in drop)
    Ok(())
}