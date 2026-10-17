# Murmur Rust client

Pure-Rust async client over the PostgreSQL wire protocol, built on
[`tokio-postgres`](https://crates.io/crates/tokio-postgres) with `rustls` (ring) for TLS. It does
not link the Murmur engine or use any native library; it is a standalone crate with its own
`Cargo.lock`. Application SQL uses normal `$1, $2, ...` parameters; MURMUR commands are helper methods.

```toml
[dependencies]
murmur-client = { path = "clients/rust" }
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

```rust
use murmur_client::{Client, Config};

#[tokio::main]
async fn main() -> murmur_client::Result<()> {
    let mut cfg = Config::new("admin", "s3cret", "database-encryption-key");
    cfg.host = "127.0.0.1".into(); // defaults: 127.0.0.1:5432, database "murmur"
    let db = Client::connect(&cfg).await?; // runs MURMUR UNLOCK

    db.execute("CREATE TABLE greetings(id INTEGER PRIMARY KEY, lang TEXT, text TEXT)", &[]).await?;
    db.execute("INSERT INTO greetings VALUES ($1, $2, $3)", &[&1i64, &"en", &"hello"]).await?;
    let rows = db.query("SELECT text FROM greetings WHERE lang = $1", &[&"en"]).await?;
    let text: String = rows[0].get(0);
    println!("{text}");
    db.flush().await?; // durable checkpoint
    Ok(())
}
```

A wrong key fails `connect` with SQLSTATE `28000` (`err.code()`, `err.is_locked()`). `db.inner()`
exposes the raw `tokio_postgres::Client`.

| Feature | API |
|---|---|
| Connect and unlock | `Client::connect(&Config)` |
| CRUD with parameters | `query`, `execute`, `simple`, `inner()` |
| Replication flags | `set_table_replicated(table, bool)`, `table_replication()` |
| Automatic replication keys | Managed UUIDv5 keys when replication is enabled; application keys remain required |
| Durability flush | `flush()` |
| Verified TLS | `Config.tls = TlsConfig { mode, ca_file, cert_file, key_file }`; modes `Disable`, `Require`, `VerifyCa`, `VerifyFull` |
| Remote backup and restore | `backup_save`, `backup_list`, `backup_download`, `backup_upload`, `restore` |
| Key rotation | `rotate_key(old, new)`, then reconnect with the new key |
| Engine metrics | `metrics(scope)` |
| Encrypted file transfer and cursors | `upload_file`, `download_file`, `list_files`, `search_files` (`FileCursor`) |
| Replication peers | `peer_add`, `peer_remove`, `peer_retire`, `peer_sync`, `show_peers`, `show_replication` |
| Retention | `show_retention`, `retention_collect` |
| CRDT policy ops | `policy_counter_add`, `policy_set_add`, `policy_set_remove`, `policy_min`, `policy_max` (`PolicyValue`) |
| Bridge ownership release | `bridge_release(table, key, Option<column>) -> bool` |
| Managed FTS and vector indexes | `set_column_fts`, `column_fts`, `set_column_vector`, `column_vectors`, `refresh_indexes` |
| JSON and JSONB | SQLite JSON functions through parameterized SQL |

Uploads take any `tokio::io::AsyncRead`, downloads any `AsyncWrite`.

## Restrictions

- The server autocommits each statement; `BEGIN`/`COMMIT`/`ROLLBACK` are accepted no-ops with no
  multi-statement rollback.
- One `Client` is one connection and one unlocked session; open more `Client`s for parallel work.
- `FileCursor` holds server state until exhausted; call `close().await` if you stop early
  (dropping it cannot run async cleanup).
- File commands need a server built with the `replication` feature, enabled replication with node
  credentials, and `[replication.files]`. Remote-only files fail with SQLSTATE `55000`.
- Peer, retention, policy, and bridge commands need enabled replication too (`55000` without it).
  Policy columns must carry the matching `replication.merge_policies` entry. Bridge release also needs
  a configured `[replication.bridge]` high-receiver (`55000` without it) and addresses single-TEXT-key
  rows only; it returns `false` when the engine is busy (retry).
- Rust parameter types must match the SQLite column affinity the server reports (for example
  `i64` for `INTEGER`); parameterless MURMUR commands use the simple protocol and return text.
- Replicated tables, `id_murmur_primary_key`, and other server SQL rules are in
  [docs/USAGE.md](../../docs/USAGE.md).

## Tests and examples

Tests and examples start a scratch `murmur-server` (cargo builds it on first use; set
`MURMUR_SERVER_BIN` to a prebuilt `--features replication` binary). From `clients/rust`:

```sh
cargo test
cargo run --example basic   # also: tables, backup, metrics, files, tls, keys, replication, policies, bridge, json, search
```

`examples/coverage.json` maps each feature above to its runnable example and is checked by the tests.

## Managed search

```rust,no_run
# async fn example(db: &murmur_client::Client) -> murmur_client::Result<()> {
use murmur_client::{VectorDistance, VectorOptions};
db.set_column_fts("documents", "body", true).await?;
db.set_column_vector("documents", "embedding", Some(VectorOptions {
    dimensions: 2, distance: VectorDistance::L2,
})).await?;
db.refresh_indexes().await?;
let columns = db.column_vectors().await?;
# Ok(())
# }
```

The source table needs one INTEGER application key; FTS columns must be TEXT,
vector columns BLOB. Vectors contain finite little-endian float32 values and
require the server's default `vector` feature. Status is optional for disabled
columns. Discover and SQL-quote index names before querying; see the checked
`search` example for bound MATCH and KNN queries. Refresh waits for local
collected writes. Search indexes rebuild locally and do not replicate.

The client covers the server SQL/MURMUR interface. Server configuration, node
credentials, bridge transport, recovery tooling and scheduling are configured
on the server, not through client-side engine APIs. Embedded Rust callbacks,
subscriptions and custom transport traits are not exposed by the wire protocol.
