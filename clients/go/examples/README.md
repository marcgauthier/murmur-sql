# Murmur Go examples

Runnable SQL examples using standard `database/sql` calls. Each starts its own scratch `murmur-server`
(Go builds it on first use), provisions what it needs, and verifies its
result. Run from `clients/go`:

```sh
go run ./examples/basic
go run ./examples/migrations
go run ./examples/tables
go run ./examples/tls
go run ./examples/concurrency
go run ./examples/keys
go run ./examples/backup
go run ./examples/json
go run ./examples/vectors
go run ./examples/search
go run ./examples/metrics
go run ./examples/files
go run ./examples/replication
go run ./examples/bridge
```

| Example      | Shows                                            |
|--------------|--------------------------------------------------|
| basic        | Connect, unlock, CRUD with `$N` parameters       |
| migrations   | DDL versions: create, alter, index, backfill     |
| tables       | Local defaults, UUIDv5 keys, populated conversions, flush        |
| tls          | Verified TLS against a minted test PKI           |
| concurrency  | Parallel workers sharing one pool                |
| keys         | Key rotation, old-key rejection and preserved data |
| backup       | Save, list, download, upload, restore            |
| json         | JSON extraction, updates, arrays, bound strings, validation and JSONB |
| vectors      | Vector declarations, KNN neighbors, numeric distances and disable |
| search       | Column FTS, parameterized MATCH, refresh and disable |
| metrics      | SQLite/Spool metrics and JSON engine diagnostics            |
| files        | Encrypted binary upload/download and LIKE cursor search |
| replication  | Two-node peer mesh, retention, CRDT policy ops, retire |
| bridge       | High-receiver field release, re-release, row form |

Set `MURMUR_SERVER_BIN` to skip the Go server build and use a prebuilt server.

The tables example reads the managed key, checks that it cannot be changed, and
verifies stable UUIDs after disabling and re-enabling a populated table. Inserts
into replicated tables must supply every application-key component. Local-only
tables retain their application SQLite primary key. Mode changes rebuild the
source table and can pause concurrent SQL. No external peers are required here.

The files example generates temporary node credentials and file keys, enables
replication without peers, and configures a scratch encrypted object directory.
It checks metadata and an arbitrary binary round-trip. Cargo builds the server
with `--features replication`; a prebuilt `MURMUR_SERVER_BIN` must include that
feature. Uploads replace atomically; file operations are independent of SQL
transactions. The example pins a `*sql.Conn` for handles and explicitly closes or aborts them. Default staging
is 1 GiB including encryption overhead; database backups exclude object payloads.

The search example needs no peers or external extensions. It indexes `TEXT`
columns on a table with one `INTEGER` application primary key, discovers the
index name, checks parameterized FTS5 queries and verifies column disabling.
Updates are eventual unless `MURMUR REFRESH INDEXES` is executed; schema declarations
persist and replicate, while index contents rebuild locally.

The vectors example uses default server builds, without peers or external
services. It supplies two-dimensional float32 BLOB embeddings, enables an L2
index, verifies JSON-parameter KNN results and distances, and disables indexing.
Production embeddings must have the declared dimensions and finite float32
values. Updates are eventual unless `MURMUR REFRESH INDEXES` is executed; invalid data is reported through
column status and must be repaired before the index is available again.

The replication example provisions two scratch servers sharing one database
id and CA, dials them together with `MURMUR PEER ADD`, and checks SHOW PEERS,
SHOW REPLICATION, SHOW RETENTION, and RETENTION COLLECT results. It also runs
counter, set add/remove, min and max policy operations against `merge_policies` and verifies
`MURMUR PEER REMOVE` / `MURMUR PEER RETIRE`, including the retired retention row and the
self-retire `22023` rejection. A prebuilt `MURMUR_SERVER_BIN` must include
the `replication` feature.

The json example uses built-in SQLite functions on `TEXT` JSON and `BLOB` JSONB.
It validates bound input with Go's `encoding/json`, checks extraction and updates,
iterates array elements, and verifies JSONB conversion. No peers, custom schema
commands, external extensions, or additional dependencies are needed. JSON
properties are not independently merged during replication; the whole column
value wins. Managed migrations reject JSON expression indexes, `CHECK`
constraints and generated columns.

The bridge example provisions one high-receiver scratch server (no bundles
are exchanged), inserts a row so the High write claims field ownership, and
executes `MURMUR BRIDGE RELEASE`: the first release reports true, a second release of
the same field fails with `22023`, and so do the row form without `COLUMN`
(no row ownership exists) and an unknown table. A prebuilt
`MURMUR_SERVER_BIN` must include the `replication` feature.


The keys example needs no peers. It rotates the master key, closes the old pool,
checks that the old key is rejected, and reconnects with the new key to verify
data. Open a fresh pool immediately after rotation: existing sessions stay
unlocked, but new connections require the new key. Rotation rewraps the key
registry; it does not rewrite every encrypted block.

The keys, replication, and bridge examples inspect server SQLSTATE codes with
`errors.As` and `*murmur.Error`; they require no driver imports.
