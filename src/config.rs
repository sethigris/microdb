use crate::error::{Error, Result};
use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::str::FromStr;

#[derive(Debug, Clone)]
pub struct Config {
    pub port: u16,
    pub data_dir: PathBuf,
    pub snapshot_interval_secs: u64,
    pub snapshot_commands: u64,
    pub role: Role,
    pub master_addr: Option<String>,
    pub threads: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Role {
    Master,
    Slave,
}

impl FromStr for Role {
    type Err = Error;

    fn from_str(
        s: &str,
    ) -> Result<Self> {
        match s {
            "master" => Ok(Role::Master),
            "slave" => Ok(Role::Slave),
            _ => Err(Error::Config(format!(
                "unknown role '{s}'; 
                expected 'master' or 'slave'"
            ))),
        }
    }
}

impl Config {
    pub fn load(
        path: &Path,
    ) -> Result<Self> {
        let mut cfg = Self::default();

        if path.exists() {
            let reader = BufReader::new(File::open(path)?);
            for line in reader.lines() {
                let line = line?;
                let line = line.trim();
                if line.is_empty() || line.starts_with('#') {
                    continue;
                }
                if let Some((key, value)) = line.split_once('=') {
                    cfg.
                    apply(
                        key.
                        trim(), 

                        value
                        .trim())?;
                }
            }
        }

        // Env vars override the file. Each MICRODB_* name maps
        // directly to its config key so this table stays the single
        // source of truth for both namespaces.
        for (env_key, cfg_key) in [
            ("MICRODB_PORT", "port"),
            ("MICRODB_DATA_DIR", "data_dir"),
            ("MICRODB_SNAPSHOT_INTERVAL", "snapshot_interval"),
            ("MICRODB_SNAPSHOT_COMMANDS", "snapshot_commands"),
            ("MICRODB_ROLE", "role"),
            ("MICRODB_MASTER_ADDR", "master_addr"),
            ("MICRODB_THREADS", "threads"),
        ] {
            if let Ok(val) = env::var(env_key) {
                cfg.apply(cfg_key, &val)?;
            }
        }

        Ok(cfg)
    }

    /// Applies a single key/value pair from either the config file or
    /// an environment variable. Keeping this in one place means the
    /// precedence logic in `load` stays trivial.
    fn apply(
        &mut self,
        key: &str,
        value: &str,
    ) -> Result<()> {
        match key {
            "port" => {
                self.port = parse(value, key)?;
            }
            "data_dir" => {
                self.data_dir = PathBuf::from(value);
            }
            "snapshot_interval" => {
                self.snapshot_interval_secs = parse(value, key)?;
            }
            "snapshot_commands" => {
                self.snapshot_commands = parse(value, key)?;
            }
            "role" => {
                self.role = value.parse()?;
            }
            "master_addr" => {
                self.master_addr = Some(value.to_owned());
            }
            "threads" => {
                self.threads = parse(value, key)?;
            }
            // Silently skip unknown keys so older config files stay
            // compatible when new fields are added.
            _ => {}
        }
        Ok(())
    }

    pub fn aof_path(
        &self,
    ) -> PathBuf {
        self.data_dir.join("appendonly.aof")
    }

    pub fn snapshot_path(
        &self,
    ) -> PathBuf {
        self.data_dir.join("dump.rdb")
    }
}

impl Default for Config {
    fn default() -> Self {
        Config {
            port: 6379,
            data_dir: PathBuf::from("./data"),
            snapshot_interval_secs: 10,
            snapshot_commands: 1000,
            role: Role::Master,
            master_addr: None,
            threads: available_parallelism(),
        }
    }
}

fn parse<T: FromStr>(
    value: &str,
    field: &str,
) -> Result<T> {
    value
        .parse()
        .map_err(|_| Error::Config(format!(
            "invalid value for '{field}': {value:?}"
        )))
}

fn available_parallelism() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}