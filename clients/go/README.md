# Murmur Go client

A `database/sql` client for Murmur servers, speaking the PostgreSQL wire
protocol through the pgx driver. SQL is the SQLite dialect; parameters use
`$N`. No cgo, no native builds — pure Go plus the `pgx` module.

```go
import (
    "context"

    murmur "github.com/marcgauthier/murmur/clients/go"
)

db, err := murmur.Open(ctx, murmur.Config{
    Host:     "127.0.0.1",
    Port:     5432,
    User:     "app",
    Password: "secret",
    Key:      "encryption-key-or-passphrase",
})
if err != nil {
    log.Fatal(err)
}
defer db.Close()

var greeting string
err = db.QueryRowContext(ctx,
    "SELECT text FROM greetings WHERE lang = $1", "en").Scan(&greeting)
```

## Connection

`Config` takes the server address, an `auth.users` login from the server's
`config.toml`, and the database encryption key (64 hex chars or a
passphrase, hashed like `murmur-admin`). Every pooled connection unlocks
itself with the key before first use; a wrong key fails `Open` with code
`28000`. The `Database` name is accepted and ignored by the server.

TLS mirrors Postgres `sslmode`:

```go
murmur.Config{
    // ...
    TLS: murmur.TLSConfig{
        Mode:     murmur.TLSVerifyFull,
        CAFile:   "/etc/murmur/ca.crt",
        CertFile: "/etc/murmur/client.crt", // mutual TLS
        KeyFile:  "/etc/murmur/client.key",
    },
}
```

`TLSDisable` sends passwords in cleartext. Production servers require TLS
with a client certificate and reject anything else.

## SQL

Use the embedded `*sql.DB` for everything: `ExecContext`, `QueryContext`,
`QueryRowContext`, prepared statements. DDL runs through server-side
migrations; DML autocommits per statement. Explicit `BEGIN`/`COMMIT` is
accepted and each statement stays individually atomic.

`$N` parameters are inferred from context (`INSERT` columns, `WHERE`
comparisons, `IN`, `BETWEEN`) so drivers encode values correctly.

`murmur.CodeOf(err)` extracts the SQLSTATE code (`28000`, `42P01`,
`23505`, `42601`, ...); `murmur.IsLocked(err)` spots statements sent
before unlock on a raw connection.

## Administration

```go
if err := db.Flush(ctx); err != nil { /* ... */ } // durability barrier
if err := db.SetTableReplicated(ctx, "scratch", false); err != nil { /* ... */ }
flags, err := db.TableReplication(ctx) // []TableFlag
metrics, err := db.Metrics(ctx)        // []Metric{Subsystem, Metric, Value}
sqliteMetrics, err := db.Metrics(ctx, "sqlite")
```

Remote backup moves single-file archives through the server's backup folder:

```go
saved, err := db.BackupSave(ctx) // BackupFile{Name, Size}
files, err := db.BackupList(ctx) // []BackupFile
err = db.BackupDownload(ctx, saved.Name, &buf)
err = db.BackupUpload(ctx, "copy.murmur-backup", &buf)
err = db.Restore(ctx, saved.Name, key) // live restore, key re-authenticates
```

Key rotation rewraps the registry under a new master key; open a fresh
handle with the new key afterwards (run `go run ./examples/keys` for a
checked rotation, old-key rejection, and data-preservation example):

```go
if err := db.RotateKey(ctx, oldKey, newKey); err != nil { /* ... */ }
db.Close()
db, err = murmur.Open(ctx, murmur.Config{ /* ... Key: newKey */ })
```

Tables are local-only by default and must declare an application primary key.
Enabling replication rebuilds even a populated table: the application key becomes
a unique index, and `id_murmur_primary_key TEXT NOT NULL PRIMARY KEY` contains an
automatically generated UUIDv5 derived from the table name and application key.
Read it with `SELECT` or `RETURNING`; applications cannot assign or update it.
Supply every application-key component on replicated inserts; application keys
remain immutable while replicated. Text keys, including UUID strings, and
composite keys are supported. Disabling replication removes the managed column
and restores the application primary key. Other server SQL restrictions still apply.
Mode changes pause table access while rebuilding; existing queries should name
columns explicitly because `SELECT *` changes shape.

```go
_, err = db.ExecContext(ctx,
    "CREATE TABLE orders(id TEXT NOT NULL PRIMARY KEY, value TEXT)")
err = db.SetTableReplicated(ctx, "orders", true)
var replicationID string
err = db.QueryRowContext(ctx,
    "INSERT INTO orders(id, value) VALUES($1, $2) RETURNING id_murmur_primary_key",
    "order-123", "ready").Scan(&replicationID)
```

Flags persist into the server's `config.toml`. Run `go run ./examples/tables`
for a complete example with result checks, including populated conversions.

## JSON

Use SQLite JSON functions in ordinary SQL, with no column setting or extension.
Store JSON as `TEXT`, bind JSON strings, and validate them in your application
with `json.Valid` from `encoding/json` before writing. `json_extract`, `->>`, `json_set`,
`json_each` and SQLite JSONB (`BLOB`) are available. `CHECK` constraints,
generated columns and JSON expression indexes are currently rejected by managed
migrations. Replication resolves conflicts on the whole JSON column value.

Run `go run ./examples/json` for checked queries, updates, arrays, bound JSON,
validation and JSONB conversion. See [JSON usage](../../docs/USAGE.md#json).

## Full-text search

```go
err := db.SetColumnFTS(ctx, "documents", "body", true)
err = db.RefreshIndexes(ctx)
columns, err := db.ColumnFTS(ctx) // discover the quoted index identifier for MATCH
_ = columns
```

Source columns must be declared `TEXT` and the table must have one `INTEGER`
application primary key, including when replication uses a managed UUID physical
key. All enabled columns in a table share a `unicode61` FTS5 index. Use ordinary
SQL `MATCH $1` with a bound string; quote the discovered index name as an
identifier, rather than a value parameter. `ColumnFTS` returns nullable pointers
for disabled index/status fields.

Declarations persist as schema metadata and follow existing schema replication
rules. Index data is rebuilt locally, with eventual updates after source writes.
`RefreshIndexes` waits for local collected writes, not peers or durable storage;
use `Flush` for persistence. Default builds include FTS5. See the
[complete restrictions](../../docs/USAGE.md#full-text-search) and run
`go run ./examples/search` for a scratch-server example with result checks.

## Vector search

Vector search is included in ordinary server builds. Declare an index on a
`BLOB` column of a table with one `INTEGER` application primary key:

```go
err := db.SetColumnVector(ctx, "documents", "embedding", &murmur.VectorOptions{
    Dimensions: 384, Distance: murmur.VectorCosine,
})
err = db.RefreshIndexes(ctx)
columns, err := db.ColumnVectors(ctx) // discover the index identifier and status
_ = columns
err = db.SetColumnVector(ctx, "documents", "embedding", nil) // disable
```

An empty distance selects L2; L1 and cosine are also supported. Store embeddings
as little-endian finite float32 bytes using `encoding/binary` and `math.Float32bits`.
Applications supply embeddings; the server does not generate them. Dimensions
must be 1–8192, and source column names must be ASCII SQL identifiers starting
with a letter. Nullable embeddings are excluded from search.

Query the returned, quoted index name with `embedding MATCH $1 AND k=$2`;
bind a JSON vector string and an integer neighbor count. KNN queries accept only
`ORDER BY distance`; equal-distance ordering is unspecified. Explicitly typed
BYTEA query parameters are also accepted by the server. Declarations persist and
replicate like FTS, while indexes rebuild locally. Invalid populated settings
roll back; malformed vectors written later leave source data intact but make the
index unavailable until repaired and refreshed. Use `Flush` for durable storage.

Run `go run ./examples/vectors` for checked neighbors, distances, and disabling.
See [all restrictions](../../docs/USAGE.md#vector-search).

## Encrypted files

File contents live in encrypted disk objects; durable metadata lives in Spool.
The server requires the `replication` Cargo feature, enabled replication with
node credentials, and `[replication.files]`. No peers are required for local use.

```go
info, err := db.UploadFile(ctx, "reports/a.bin", reader)
err = db.DownloadFile(ctx, info.Name, writer)
cursor, err := db.SearchFiles(ctx, "reports/%.bin") // or db.ListFiles(ctx)
if err != nil { /* handle error */ }
defer cursor.Close()
for cursor.Next(ctx) {
    file := cursor.File() // Name, Size, ModifiedMillis, SHA256, Local
    _ = file
}
err = cursor.Err()
```

Uploads replace existing names atomically at completion. Downloads pin the
selected version and authenticate it before streaming. Search uses case-sensitive
LIKE (`%`, `_`, backslash escaping). Metadata cursors are snapshots and pin a
pooled connection until exhausted or closed; serialize calls on each cursor.
Transfers use 64 KiB BYTEA chunks. SQL ROLLBACK does not undo file operations.
Remote-only content returns `CodeOf(err) == "55000"`; retry after peer fetching.
Handles expire after five idle minutes and do not survive reconnect or restore.
The default staging limit is 1 GiB including encrypted overhead, shared with
peer fetches; increase it to upload larger files. Back up the object directory
and file keys separately from the database. See [USAGE](../../docs/USAGE.md#files-over-postgresql-sql)
for SQL commands, limits, and error codes, or run `go run ./examples/files` for
a complete scratch-node example with result checks.

## Replication

Peer management, retention inspection, and CRDT policy operations need a
server built with the `replication` Cargo feature and enabled `[replication]`
credentials; without them the server fails with code `55000`.

```go
status, err := db.ShowReplication(ctx) // ReplicationStatus{Running, Address, ...}
peers, err := db.ShowPeers(ctx)        // []PeerInfo ordered by node id
err = db.PeerAdd(ctx, "203.0.113.11:9001")
err = db.PeerSync(ctx)
err = db.PeerRemove(ctx, nodeID) // reversible with PeerAdd
err = db.PeerRetire(ctx, nodeID) // permanent, releases retention
entries, err := db.ShowRetention(ctx)  // []RetentionEntry
collected, err := db.RetentionCollect(ctx)
```

`PeerAdd` succeeds once the dial is queued; dial failures surface in
`ShowReplication` (`LastError`). Node ids are 32 hex characters and are
validated locally, as are peer addresses. `ShowPeers` is empty when
peerless; nullable columns (`Address`, `LastSeenMillisAgo`, `LastError`,
`Origin`, `Acknowledged`) are nil pointers when the server reports NULL.

Policy operations apply one CRDT op to a single-TEXT-key row whose column
carries the matching `replication.merge_policies` entry:

```go
err = db.PolicyCounterAdd(ctx, "stats", "count", "row-1", 5)
err = db.PolicySetAdd(ctx, "stats", "tags", "row-1", "blue")
err = db.PolicySetRemove(ctx, "stats", "tags", "row-1", "blue")
err = db.PolicyMax(ctx, "stats", "highest", "row-1", 42)
err = db.PolicyMin(ctx, "stats", "lowest", "row-1", -1.5)
```

`PolicyMin`/`PolicyMax` take an int, a finite float, or a string; other
types fail locally. A column without the matching policy fails with `22023`,
a missing row with `02000`. See
[USAGE](../../docs/USAGE.md#murmur-policy-counter_add-set_add-set_remove-min-max)
for merge semantics, and run `go run ./examples/replication` for a checked
two-node example.

On a `[replication.bridge]` high-receiver, local writes shield the touched
cells from later Low updates; `BridgeRelease` drops that shield for one
single-TEXT-key row so Low data flows through again:

```go
released, err := db.BridgeRelease(ctx, "notes", "row-1", "title") // field
released, err = db.BridgeRelease(ctx, "notes", "row-1")          // whole row
```

The optional column is at most one name. `true` means the release applied;
`false` means the engine was busy, retry. A Low exporter, an unknown table,
or a field that is not High-owned fails with `22023`; without enabled
replication or a configured bridge the server fails with `55000`. Run
`go run ./examples/bridge` for a checked single-node example.

## Features

| Feature            | Examples                  |
|--------------------|---------------------------|
| Connect and unlock | basic                     |
| CRUD with parameters | basic, concurrency      |
| DDL migrations     | migrations                |
| Automatic replication keys | tables            |
| Replication flags  | tables                    |
| Durability flush   | tables                    |
| Verified TLS       | tls                       |
| Concurrent use     | concurrency               |
| Remote backup and restore | backup             |
| Backup file transfer | backup                  |
| Key rotation       | keys                      |
| Engine metrics     | metrics                   |
| Column full-text search | search                |
| Column vector search | vectors                    |
| SQLite JSON queries | json                       |
| Encrypted file transfer and cursors | files         |
| Replication peer management | replication          |
| Replication retention | replication              |
| CRDT policy operations | replication             |
| Bridge ownership release | bridge               |

The client covers the server SQL/MURMUR interface. Server configuration, node
credentials, bridge transport, recovery tooling and scheduling stay on the
server. Embedded callbacks, subscriptions and custom transport traits are not
exposed by the PostgreSQL protocol.

Backup downloads return `io.ErrShortWrite` if the destination accepts fewer
bytes than supplied without reporting an error; other writer errors propagate.

Browse the [runnable examples](examples/README.md). Each creates its scratch
server automatically and verifies its result.

## Development

Go 1.24+ and a Rust toolchain (edition 2024) with a C linker are required;
tests and examples build `murmur-server` with `--features replication` via cargo on first use. Set
`MURMUR_SERVER_BIN` to reuse a prebuilt server binary instead; it must include
the `replication` feature for file tests/examples.

From this directory:

```sh
go vet ./...
go test -count=1 -timeout 300s ./...
go test -race -count=1 -timeout 300s ./...
```

`CGO_ENABLED=0` works; nothing here needs cgo.
