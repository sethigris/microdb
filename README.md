# microdb – Distributed Key‑Value Store in Rust

A simple distributed key‑value store built with only the standard library. It supports:

- In‑memory storage with thread safety.
- Append‑only log (AOF) and periodic snapshots for persistence.
- Master‑slave replication.
- Binary protocol over TCP.
- Multi‑threaded server with a fixed thread pool.
- Graceful shutdown via a file.

## Requirements

- Rust 1.70 or newer.

## Build

```bash
cargo build --release
```

The binary is `target/release/microdb`.

## Configuration

Configuration can be set in a file (default `microdb.conf`) or via environment variables. Environment variables override file settings.

### Config file format

Lines: `key=value`. Empty lines and lines starting with `#` are ignored.

### Available settings

| Key                   | Description                        | Default          |
|-----------------------|------------------------------------|------------------|
| `port`                | TCP port to listen on              | `6379`           |
| `data_dir`            | Directory for data files           | `./data`         |
| `snapshot_interval`   | Seconds between automatic snapshots| `10`             |
| `snapshot_commands`   | Number of commands between snapshots| `1000`          |
| `role`                | `master` or `slave`                | `master`         |
| `master_addr`         | Address of master (if slave)       | (none)           |
| `threads`             | Number of worker threads           | CPU cores        |

### Environment variables

Prefix with `MICRODB_`. Example: `MICRODB_PORT=6380`.

## Running

### Master

Create `master.conf`:

```
port=6379
data_dir=./data_master
role=master
```

Start:

```bash
./target/release/microdb --config master.conf
```

### Slave

Create `slave.conf`:

```
port=6380
data_dir=./data_slave
role=slave
master_addr=127.0.0.1:6379
```

Start:

```bash
./target/release/microdb --config slave.conf
```

## Client

A simple client is provided in `client.rs`. Compile it:

```bash
rustc client.rs -o client
```

Use it to send commands:

```bash
# Set key "foo" to value "bar"
./client 127.0.0.1:6379 set foo bar

# Get key "foo"
./client 127.0.0.1:6379 get foo

# Delete key "foo"
./client 127.0.0.1:6379 delete foo

# Save a snapshot manually
./client 127.0.0.1:6379 save
```

Responses are printed as Rust debug output.

## Persistence

- **AOF** – Every mutating command (`set`, `delete`) is appended to `appendonly.aof` in the data directory.
- **Snapshots** – A snapshot is saved to `dump.rdb` in the data directory. Snapshots are taken automatically when the command count or time interval is reached, or manually via the `save` command.
- **Recovery** – On startup, the latest snapshot is loaded, then the AOF is replayed.

## Replication

- Master listens for slave connections. A slave connects and sends a `Replicate` command.
- After connection, the master sends every mutating command to all connected slaves.
- Slaves apply the commands to their own store (they do not write to their own AOF – they rely on the master for persistence).

## Graceful Shutdown

Create a file named `shutdown.txt` in the server’s working directory. The server will:

- Stop accepting new connections.
- Flush the AOF.
- Write a final snapshot.
- Exit cleanly.

## Testing

A test script `test_all.sh` is included. It runs a series of tests and reports results.

```bash
chmod +x test_all.sh
./test_all.sh
```

The script covers:
- Basic operations (set, get, delete, save)
- Persistence after restart
- Replication (master‑slave)
- Concurrency (100 simultaneous requests)
- Snapshot scheduling
- Graceful shutdown
- Malformed command handling
- Performance (operations per second)

## Limitations

- Keys are limited to 255 bytes.
- Values can be up to 2³²‑1 bytes.
- Only synchronous I/O.
- No authentication or encryption.

## License

MIT
