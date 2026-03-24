use std::env;
use std::io::{Read, Write};
use std::net::TcpStream;

// Protocol definitions (copied from server)
#[derive(Debug, Clone, PartialEq)]
enum Command {
    Set { key: String, value: Vec<u8> },
    Get { key: String },
    Delete { key: String },
    Save,
}

#[derive(Debug, Clone, PartialEq)]
enum Response {
    Ok,
    Err(u8),
    Value(Vec<u8>),
    Nil,
    Int(u64),
}

impl Command {
    fn encode<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        match self {
            Command::Set { key, value } => {
                writer.write_all(&[0x01])?; // Set
                write_string(writer, key)?;
                write_bytes(writer, value)?;
            }
            Command::Get { key } => {
                writer.write_all(&[0x02])?; // Get
                write_string(writer, key)?;
            }
            Command::Delete { key } => {
                writer.write_all(&[0x03])?; // Delete
                write_string(writer, key)?;
            }
            Command::Save => {
                writer.write_all(&[0x04])?; // Save
            }
        }
        Ok(())
    }

    fn decode<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        let mut buf = [0u8; 1];
        reader.read_exact(&mut buf)?;
        match buf[0] {
            0x01 => {
                let key = read_string(reader)?;
                let value = read_bytes(reader)?;
                Ok(Command::Set { key, value })
            }
            0x02 => {
                let key = read_string(reader)?;
                Ok(Command::Get { key })
            }
            0x03 => {
                let key = read_string(reader)?;
                Ok(Command::Delete { key })
            }
            0x04 => Ok(Command::Save),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "unknown command",
            )),
        }
    }
}

impl Response {
    fn encode<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        match self {
            Response::Ok => writer.write_all(&[0x00])?,
            Response::Err(code) => {
                writer.write_all(&[0x01, *code])?;
            }
            Response::Value(data) => {
                writer.write_all(&[0x02])?;
                write_bytes(writer, data)?;
            }
            Response::Nil => writer.write_all(&[0x03])?,
            Response::Int(val) => {
                writer.write_all(&[0x04])?;
                writer.write_all(&val.to_le_bytes())?;
            }
        }
        Ok(())
    }

    fn decode<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        let mut buf = [0u8; 1];
        reader.read_exact(&mut buf)?;
        match buf[0] {
            0x00 => Ok(Response::Ok),
            0x01 => {
                let mut code = [0u8; 1];
                reader.read_exact(&mut code)?;
                Ok(Response::Err(code[0]))
            }
            0x02 => {
                let data = read_bytes(reader)?;
                Ok(Response::Value(data))
            }
            0x03 => Ok(Response::Nil),
            0x04 => {
                let mut buf = [0u8; 8];
                reader.read_exact(&mut buf)?;
                let val = u64::from_le_bytes(buf);
                Ok(Response::Int(val))
            }
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "unknown response",
            )),
        }
    }
}

fn write_string<W: Write>(writer: &mut W, s: &str) -> std::io::Result<()> {
    if s.len() > 255 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "key too long",
        ));
    }
    writer.write_all(&[s.len() as u8])?;
    writer.write_all(s.as_bytes())?;
    Ok(())
}

fn read_string<R: Read>(reader: &mut R) -> std::io::Result<String> {
    let mut len = [0u8; 1];
    reader.read_exact(&mut len)?;
    let len = len[0] as usize;
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf)?;
    String::from_utf8(buf).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

fn write_bytes<W: Write>(writer: &mut W, data: &[u8]) -> std::io::Result<()> {
    let len = data.len() as u32;
    writer.write_all(&len.to_le_bytes())?;
    writer.write_all(data)?;
    Ok(())
}

fn read_bytes<R: Read>(reader: &mut R) -> std::io::Result<Vec<u8>> {
    let mut len = [0u8; 4];
    reader.read_exact(&mut len)?;
    let len = u32::from_le_bytes(len) as usize;
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf)?;
    Ok(buf)
}

fn main() -> std::io::Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: client <host:port> <command> [key] [value]");
        eprintln!("Commands: set, get, delete, save");
        return Ok(());
    }
    let addr = &args[1];
    let cmd = &args[2];
    let key = args.get(3);
    let value = args.get(4);

    let mut stream = TcpStream::connect(addr)?;

    match cmd.as_str() {
        "set" => {
            let key = key.ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "key required")
            })?;
            let value = value.ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "value required")
            })?;
            let cmd = Command::Set {
                key: key.clone(),
                value: value.as_bytes().to_vec(),
            };
            cmd.encode(&mut stream)?;
            let resp = Response::decode(&mut stream)?;
            println!("{:?}", resp);
        }
        "get" => {
            let key = key.ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "key required")
            })?;
            let cmd = Command::Get { key: key.clone() };
            cmd.encode(&mut stream)?;
            let resp = Response::decode(&mut stream)?;
            println!("{:?}", resp);
        }
        "delete" => {
            let key = key.ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "key required")
            })?;
            let cmd = Command::Delete { key: key.clone() };
            cmd.encode(&mut stream)?;
            let resp = Response::decode(&mut stream)?;
            println!("{:?}", resp);
        }
        "save" => {
            let cmd = Command::Save;
            cmd.encode(&mut stream)?;
            let resp = Response::decode(&mut stream)?;
            println!("{:?}", resp);
        }
        _ => eprintln!("Unknown command. Use set, get, delete, save"),
    }
    Ok(())
}
