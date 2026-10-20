<img src="murmur-sql.png" alt="murmur-sql logo" width="400">

# Murmur-SQL

Murmur-SQL is a thin shell over the embedded Murmur database, accessible
over the PostgreSQL wire protocol. The server speaks PostgreSQL wire and the
SQLite SQL dialect; the embedded Murmur engine (`../MURMUR`) executes queries
against its in-memory SQLite core, while background workers handle encrypted,
compressed persistence in Murmur Spool and masterless peer-to-peer data
replication. Replication and settings live in `config.toml`.

Clients connect using any PostgreSQL wire protocol client or driver and speak the
SQLite SQL dialect. The server provides authentication, TLS/mTLS, per-connection
database unlocking, and database administration directly over SQL.

For interactive Windows/Linux service installation, run `murmur-server /install`
in an elevated console (`sudo ./murmur-server /install` on Linux/systemd).
Use `/uninstall` to remove the service, with separate confirmations for file
and data deletion. See [service installation](docs/USAGE.md#install-as-an-operating-system-service).

## Features

- **PostgreSQL Wire Compatibility**: Connect with standard PostgreSQL clients and drivers (`psql`, Go, Python, Node.js, Rust, etc.) using the SQLite SQL dialect and `$N` parameter binding.
- **In-Memory SQLite Performance**: Executes application SQL on an in-memory SQLite engine without blocking on disk writes or CRDT metadata maintenance during queries.
- **Encrypted Background Persistence**: SQLite session changesets are collected and queued asynchronously, then committed to append-only Spool segments with Zstandard compression and AES-256-GCM encryption.
- **Masterless Peer-to-Peer Replication**: Background QUIC-based data replication between nodes with deterministic Hybrid Logical Clock (HLC) per-column conflict resolution and durable tombstones.
- **JSON Queries**: Use SQLite JSON functions with `TEXT` JSON or `BLOB` JSONB, including extraction, updates and array iteration. See [JSON usage and examples](docs/USAGE.md#json).
- **Vector Search**: Default builds include sqlite-vec. Configure `BLOB` embeddings with `MURMUR SET COLUMN <table> <column> VECTOR true DIMENSIONS <n> [DISTANCE l2|cosine|l1]`. See [vector setup and restrictions](docs/USAGE.md#vector-search).
- **Full-Text Search**: Enable FTS5 with `MURMUR SET COLUMN <table> <column> FTS true`; search indexes rebuild locally from persisted and replicated source rows. See [setup and restrictions](docs/USAGE.md#full-text-search).
- **Per-Table Replication Control**: Tables are local-only by default; selectively enable replication at runtime via `MURMUR SET TABLE <name> REPLICATED true`.
- **Built-in Security & Administration**: Argon2id password authentication, TLS and mutual TLS (mTLS), per-connection key unlocking (`MURMUR UNLOCK`), encrypted file storage (`MURMUR FILE`), and live backup/restore (`MURMUR BACKUP` / `MURMUR RESTORE`).

## Quickstart

### 1. Configure and start the server

Copy the example configuration, create an Argon2 password hash for the user, and start the server:

```sh
cp config.example.toml config.toml
murmur-admin hash-password --password s3cret   # paste the hash into [[auth.users]]
cargo run --bin murmur-server -- --config config.toml
```

### 2. Connect and execute SQL

Connect using `psql` or any PostgreSQL-wire client:

```sh
psql -h 127.0.0.1 -p 5432 -U admin -d murmur
```

Unlock the database and execute queries:

```sql
MURMUR UNLOCK KEY 'database-encryption-key';
CREATE TABLE greetings(id INTEGER PRIMARY KEY, lang TEXT NOT NULL, text TEXT NOT NULL);
INSERT INTO greetings VALUES (1, 'en', 'hello');
SELECT text FROM greetings WHERE lang = 'en';
MURMUR FLUSH;
```

The database encryption key is required before executing application queries: the first
unlock creates the database when the storage directory is empty and opens it otherwise.
An incorrect key fails with SQLSTATE `28000` and never modifies storage.

## Configuration

Everything lives in `config.toml`; see the commented [config.example.toml](config.example.toml). Sections:

| Section | Purpose |
|---|---|
| `server` | Listen address, production mode, TLS certificates |
| `storage` | Data directory, database/node identity |
| `engine` | Capture, queue, Spool, snapshot, scheduler tuning |
| `auth` | Users with Argon2 password hashes |
| `replication` | Peers, schema policy, retention, per-table flags |
| `replication.files` | Encrypted disk file objects and background peer fetching |
| `logging` | Daily-rotated file logs, retention, VictoriaLogs sink |

Unknown keys are rejected. Validate without starting:

```sh
murmur-server --validate-config --config config.toml
```

Per-table replication flags live under `[replication.tables]` (missing means local-only) and can be changed at runtime:

```sql
MURMUR SET TABLE orders REPLICATED true;
MURMUR SHOW TABLES REPLICATION;
```

`SET` rewrites `config.toml` atomically, so flags survive restarts.

## Server operation

Administration happens over SQL sessions and the local `murmur-admin` tool:

| Command | Effect |
|---|---|
| `MURMUR UNLOCK KEY '<hex-or-passphrase>'` | Create-or-open the database for this connection |
| `MURMUR SET TABLE <name> REPLICATED true\|false` | Persist a local/replicated flag |
| `MURMUR SHOW TABLES REPLICATION` | List tables and their flags |
| `MURMUR BACKUP SAVE\|LIST\|UPLOAD\|DOWNLOAD` | Datetime archives, listing, chunked transfer |
| `MURMUR RESTORE FROM '<name>' KEY '<key>'` | Live-restore an archive |
| `MURMUR ROTATE KEY '<previous>' '<new>'` | Rotate the master key |
| `MURMUR FLUSH` | Wait for a durable checkpoint |
| `MURMUR SHOW STATUS` | Read the engine diagnostic snapshot as JSON |
| `MURMUR PEER ADD\|REMOVE\|RETIRE\|SYNC` | Peer membership administration (needs `replication`) |
| `MURMUR SHOW PEERS\|REPLICATION\|RETENTION` | Peer list, local summary, retention state |
| `MURMUR RETENTION COLLECT` | Run one bounded retention pass |
| `MURMUR POLICY ...` | One CRDT counter/set/min/max operation on a single-TEXT-key row |
| `MURMUR BRIDGE RELEASE ...` | Release High ownership of one bridge-shielded field or row (needs `replication` plus a high-receiver bridge) |

Production mode (`production = true`) requires TLS with client certificates (mTLS); plaintext and unverified connections are rejected. Development servers may run plaintext on loopback.

`murmur-admin` works directly on a stopped server's data directory: `status`, `inspect`, `verify`, `backup`, `restore`, `keys`, and `hash-password` for generating `auth.users` entries.

## Write path and durability

Application statements execute on the server's in-memory SQLite connection; the SQLite session extension records changes with no CRDT tables, triggers, or metadata statements on that connection. At a collection boundary the server extracts the committed changeset, assigns its collection-time HLC version, and queues the batch; a background worker commits changeset, versions, tombstones, and clock into Spool as one atomic group (Zstandard compression, then AES-256-GCM encryption).

Statements acknowledge **in-memory commit**, not disk durability. `MURMUR FLUSH` waits for a durable checkpoint. The default one-second collection schedule gives an approximate loss window; a full queue fails writes with `Error::Busy` mapped to a connection error, and storage failure stops writes while reads stay available.

Engine tuning options (`capture_interval`, `queue_bytes`, `spool_cache_bytes`, ...) are configured in `config.toml`; see [USAGE.md](docs/USAGE.md#engine-and-enginescheduler).

## Replication

Replication is masterless peer-to-peer over QUIC, configured in `[replication]`: peers, allowed networks, schema-master policy, anti-entropy, and history retention (minimum age plus minimum count, peer ACKs, offline window). Tables default to local-only and retain their application SQLite primary key. `MURMUR SET TABLE <name> REPLICATED true` converts the application key to a unique index and adds the read-only UUIDv5 primary key `id_murmur_primary_key`, including for existing rows. Disabling restores the application primary key. Local-only tables persist normally and exclude incoming and outgoing replication. Replicated inserts must supply every application-key component. See [USAGE.md](docs/USAGE.md#table-replication-settings) for conversion behavior and restrictions. When a peer cannot recover from retained history, it fetches a resumable encrypted snapshot from an explicitly trusted source.

Protocol version **13** requires upgraded peers with no rolling compatibility. Database format **8** reads and upgrades format-6/7 stores and backups; older formats and downgrades are unsupported. Keep a pre-upgrade backup.

## Clients

Because Murmur speaks the standard PostgreSQL wire protocol, applications can connect using any PostgreSQL-compatible driver or client tool (such as `psql`, or drivers for Go, Python, Node.js, Java, Rust, etc.). SQL is the SQLite dialect; parameters use `$1`, `$2`, etc.

Client setup, API usage, and runnable examples are documented separately:

- [Go client](clients/go/README.md)
- [Python client](clients/python/README.md)
- [Rust client](clients/rust/README.md)

Error codes follow standard SQLSTATE conventions (`28000` auth/locked, `42P01` unknown table, `23505` constraint violation, `42601` syntax error).

## Encrypted file objects

File listing, LIKE search, uploads, and downloads are available through `MURMUR FILE` SQL commands with typed metadata rows, explicit cursors, and BYTEA chunks. Payloads stay in encrypted disk objects; metadata stays in Spool. Enable the `replication` feature and configure `[replication.files]` plus node credentials. See [file usage and limits](docs/USAGE.md#encrypted-file-objects).

## Documentation

- [USAGE.md](docs/USAGE.md) — full server reference: config keys, SQL behavior, MURMUR commands, error codes, TLS/mTLS, logging, replication operations.
- [DESIGN.md](docs/DESIGN.md) — engine and persistence architecture.
- [SCALABILITY.md](docs/SCALABILITY.md) — scaling properties and resource limits.
- [TESTING.md](docs/TESTING.md) — test suites and commands.
- [VENDORS.md](docs/VENDORS.md) — external dependency inventory.
- [TODO.md](docs/TODO.md) — roadmap notes.

## Building and testing

Rust edition 2024 with a C linker is required. The
engine comes from the sibling `../MURMUR` checkout (a `murmur` path
dependency); engine, replication, and Spool tests run in that repository.

```sh
cargo build --bin murmur-server --bin murmur-admin
cargo test
cargo test --features replication
```

Live multi-process scenarios (`tests-live/`: one `murmur-server` child per
node over `murmur-client`) run sequentially via the runner; see
[TESTING.md](docs/TESTING.md) and [tests-live/README.md](tests-live/README.md):

```sh
tests-live/run.sh <scenario>  # one scenario; also: all | gate | soak | stress
```

## License

See the crate manifests for license declarations (Spool is MIT).
