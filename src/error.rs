use std::fmt;
use std::io;
use std::net::TcpStream;
use std::sync::mpsc;

/// Top‑level error type for microdb.
#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Protocol(String),
    Store(String),
    Persistence(String),
    Replication(String),
    Config(String),
    Send(String),
    Other(String),
}

impl fmt::Display for Error {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "IO error: {}", e),
            Error::Protocol(msg) => write!(f, "Protocol error: {}", msg),
            Error::Store(msg) => write!(f, "Store error: {}", msg),
            Error::Persistence(msg) => write!(f, "Persistence error: {}", msg),
            Error::Replication(msg) => write!(f, "Replication error: {}", msg),
            Error::Config(msg) => write!(f, "Configuration error: {}", msg),
            Error::Send(msg) => write!(f, "Send error: {}", msg),
            Error::Other(msg) => write!(f, "Other error: {}", msg),
        }
    }
}

impl From<io::Error> for Error {
    fn from(
        err: io::Error,
    ) -> Self {
        Error::Io(err)
    }
}

impl From<mpsc::SendError<Vec<u8>>> for Error {
    fn from(
        err: mpsc::SendError<Vec<u8>>,
    ) -> Self {
        Error::Send(err.to_string())
    }
}

impl From<mpsc::SendError<()>> for Error {
    fn from(
        err: mpsc::SendError<()>,
    ) -> Self {
        Error::Send(err.to_string())
    }
}

impl From<mpsc::SendError<TcpStream>> for Error {
    fn from(
        err: mpsc::SendError<TcpStream>,
    ) -> Self {
        Error::Send(err.to_string())
    }
}

pub type Result<T> = std::result::Result<T, Error>;