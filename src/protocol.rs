use crate::error::{Error, Result};
use std::io::{Read, Write};

#[repr(u8)]
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum CommandType {
    Set = 0x01,
    Get = 0x02,
    Delete = 0x03,
    Save = 0x04,
    Replicate = 0x05,
    Shutdown = 0x06,
}

#[repr(u8)]
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum ResponseType {
    Ok = 0x00,
    Err = 0x01,
    Value = 0x02,
    Nil = 0x03,
    Int = 0x04,
}

impl TryFrom<u8> for CommandType {
    type Error = Error;

    fn try_from(b: u8) -> Result<Self> {
        match b {
            0x01 => Ok(Self::Set),
            0x02 => Ok(Self::Get),
            0x03 => Ok(Self::Delete),
            0x04 => Ok(Self::Save),
            0x05 => Ok(Self::Replicate),
            0x06 => Ok(Self::Shutdown),
            _ => Err(Error::Protocol(format!(
                "unknown command type: {b:#04x}"
            ))),
        }
    }
}

impl TryFrom<u8> for ResponseType {
    type Error = Error;

    fn try_from(b: u8) -> Result<Self> {
        match b {
            0x00 => Ok(Self::Ok),
            0x01 => Ok(Self::Err),
            0x02 => Ok(Self::Value),
            0x03 => Ok(Self::Nil),
            0x04 => Ok(Self::Int),
            _ => Err(Error::Protocol(format!(
                "unknown response type: {b:#04x}"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Command {
    Set {
        key: String,
        value: Vec<u8>,
    },
    Get {
        key: String,
    },
    Delete {
        key: String,
    },
    Save,
    Replicate {
        command: Vec<u8>,
    },
    Shutdown,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Response {
    Ok,
    Err(u8),
    Value(Vec<u8>),
    Nil,
    Int(u64),
}

impl Command {
    pub fn encode<W: Write>(
        &self,
        w: &mut W,
    ) -> Result<()> {
        match self {
            Command::Set {
                key,
                value,
            } => {
                write_u8(w, CommandType::Set as u8)?;
                write_string(w, key)?;
                write_bytes(w, value)?;
            }
            Command::Get {
                key,
            } => {
                write_u8(w, CommandType::Get as u8)?;
                write_string(w, key)?;
            }
            Command::Delete {
                key,
            } => {
                write_u8(w, CommandType::Delete as u8)?;
                write_string(w, key)?;
            }
            Command::Save => {
                write_u8(w, CommandType::Save as u8)?;
            }
            Command::Replicate {
                command,
            } => {
                write_u8(w, CommandType::Replicate as u8)?;
                write_bytes(w, command)?;
            }
            Command::Shutdown => {
                write_u8(w, CommandType::Shutdown as u8)?;
            }
        }
        Ok(())
    }

    pub fn decode<R: Read>(
        r: &mut R,
    ) -> Result<Self> {
        match CommandType::try_from(read_u8(r)?)? {
            CommandType::Set => Ok(Command::Set {
                key: read_string(r)?,
                value: read_bytes(r)?,
            }),
            CommandType::Get => Ok(Command::Get {
                key: read_string(r)?,
            }),
            CommandType::Delete => Ok(Command::Delete {
                key: read_string(r)?,
            }),
            CommandType::Save => Ok(Command::Save),
            CommandType::Replicate => Ok(Command::Replicate {
                command: read_bytes(r)?,
            }),
            CommandType::Shutdown => Ok(Command::Shutdown),
        }
    }
}

impl Response {
    pub fn encode<W: Write>(
        &self,
        w: &mut W,
    ) -> Result<()> {
        match self {
            Response::Ok => {
                write_u8(w, ResponseType::Ok as u8)?;
            }
            Response::Err(code) => {
                write_u8(w, ResponseType::Err as u8)?;
                write_u8(w, *code)?;
            }
            Response::Value(data) => {
                write_u8(w, ResponseType::Value as u8)?;
                write_bytes(w, data)?;
            }
            Response::Nil => {
                write_u8(w, ResponseType::Nil as u8)?;
            }
            Response::Int(val) => {
                write_u8(w, ResponseType::Int as u8)?;
                w.write_all(&val.to_le_bytes())?;
            }
        }
        Ok(())
    }

    pub fn decode<R: Read>(
        r: &mut R,
    ) -> Result<Self> {
        match ResponseType::try_from(read_u8(r)?)? {
            ResponseType::Ok => Ok(Response::Ok),
            ResponseType::Nil => Ok(Response::Nil),
            ResponseType::Err => Ok(Response::Err(read_u8(r)?)),
            ResponseType::Value => Ok(Response::Value(read_bytes(r)?)),
            ResponseType::Int => {
                let mut buf = [0u8; 8];
                r.read_exact(&mut buf)?;
                Ok(Response::Int(u64::from_le_bytes(buf)))
            }
        }
    }
}

// wire primitives 

fn read_u8<R: Read>(
    r: &mut R,
) -> Result<u8> {
    let mut b = [0u8; 1];
    r.read_exact(&mut b)?;
    Ok(b[0])
}

fn write_u8<W: Write>(
    w: &mut W,
    b: u8,
) -> Result<()> {
    w.write_all(&[b]).map_err(Into::into)
}

fn write_string<W: Write>(
    w: &mut W,
    s: &str,
) -> Result<()> {
    let len = u8::try_from(s
        .len())
        .map_err(|_| {
        Error::Protocol(format!(
            "key too long: {} bytes (max 255)",
            s.len()
        ))
    })?;
    write_u8(w, len)?;
    w.write_all(s.as_bytes()).map_err(Into::into)
}

fn read_string<R: Read>(
    r: &mut R,
) -> Result<String> {
    let len = read_u8(r)? as usize;
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf)?;
    String::from_utf8(buf).map_err(|e| {
        Error::Protocol(format!("invalid UTF-8 in key: {e}"))
    })
}

fn write_bytes<W: Write>(
    w: &mut W,
    data: &[u8],
) -> Result<()> {
    let len = u32::try_from(data
        .len())
        .map_err(|_| {
        Error::Protocol(format!(
            "value too large: {} bytes (max 4 GiB)",
            data.len()
        ))
    })?;
    w.write_all(&len.to_le_bytes())?;
    w.write_all(data).map_err(Into::into)
}

fn read_bytes<R: Read>(
    r: &mut R,
) -> Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    r.read_exact(&mut len_buf)?;
    let len = u32::from_le_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf)?;
    Ok(buf)
}

/// Tests
#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn roundtrip_cmd(
        cmd: Command,
    ) -> Command {
        let mut buf = Vec::new();
        cmd.encode(&mut buf).unwrap();
        Command::decode(&mut Cursor::new(buf)).unwrap()
    }

    fn roundtrip_resp(
        resp: Response,
    ) -> Response {
        let mut buf = Vec::new();
        resp.encode(&mut buf).unwrap();
        Response::decode(&mut Cursor::new(buf)).unwrap()
    }

    #[test]
    fn roundtrip_all_commands() {
        let cases = vec![
            Command::Set {
                key: "k".into(),
                value: b"v".to_vec(),
            },
            Command::Set {
                key: "k".into(),
                value: vec![],
            },
            Command::Get {
                key: "hello".into(),
            },
            Command::Delete {
                key: "x".into(),
            },
            Command::Save,
            Command::Replicate {
                command: b"\x01\x03foo\x03bar".to_vec(),
            },
            Command::Shutdown,
        ];
        for cmd in cases {
            assert_eq!(roundtrip_cmd(cmd.clone()), cmd);
        }
    }

    #[test]
    fn roundtrip_all_responses() {
        let cases = vec![
            Response::Ok,
            Response::Nil,
            Response::Err(42),
            Response::Value(b"data".to_vec()),
            Response::Value(vec![]),
            Response::Int(0),
            Response::Int(u64::MAX),
        ];
        for resp in cases {
            assert_eq!(roundtrip_resp(resp.clone()), resp);
        }
    }

    #[test]
    fn unknown_command_byte_is_an_error() {
        let mut cur = Cursor::new(vec![0xFF]);
        assert!(Command::decode(&mut cur).is_err());
    }

    #[test]
    fn unknown_response_byte_is_an_error() {
        let mut cur = Cursor::new(vec![0xFF]);
        assert!(Response::decode(&mut cur).is_err());
    }

    #[test]
    fn key_too_long_is_rejected() {
        let long_key = "x".repeat(256);
        let cmd = Command::Get {
            key: long_key,
        };
        let mut buf = Vec::new();
        assert!(cmd.encode(&mut buf).is_err());
    }

    #[test]
    fn truncated_input_is_an_error() {
        let mut cur = Cursor::new(vec![CommandType::Get as u8]);
        assert!(Command::decode(&mut cur).is_err());
    }
}