# Architecture

Murmur-SQL is a thin PostgreSQL-wire shell over the embedded Murmur engine
(the `murmur` crate in the sibling `../MURMUR` checkout). This repository owns
only the wire shell: `src/server` (PostgreSQL wire protocol, TLS/mTLS, auth,
per-connection `MURMUR UNLOCK`, `MURMUR ...` commands, query fan-out),
`src/config.rs` (`config.toml`, authoritative for replication and settings,
including `[replication.tables]`), `src/logging.rs`, the `murmur-server` and
`murmur-admin` binaries, the `clients/` wire clients, and `tests-live/`.
All storage, replication, conflict-resolution, and search-index behavior lives
in `../MURMUR` and is described in its own docs; the sections below summarize
the behavior the shell projects over the wire.

```text
PostgreSQL-wire client (SQLite dialect)
      |
murmur-server: TLS/mTLS, auth, per-connection MURMUR UNLOCK, MURMUR commands
      |  (this repo: src/server, src/config.rs, src/logging.rs)
murmur::Database API boundary (table-flags sink callback for config.toml)
      |
Embedded Murmur engine (../MURMUR):
one SQLite :memory: connection + session, collection-time versions,
bounded background queue, Spool metadata, Zstd -> AES-256-GCM segments,
QUIC replication mesh
```

Replication and settings live in `config.toml`. Table flag changes made over
the wire (`MURMUR SET TABLE`, schema adoption) flow back through a
`TableFlagsSink` callback that atomically rewrites `config.toml` and mirrors
the running snapshot; the engine never touches the file directly.

The engine behavior below is implemented in `../MURMUR`. The database thread
owns the single SQLite connection and its session. Normal writes execute only
application SQL and transaction control; they do not execute CRDT SQL. Session
extraction and resource admission are the coordination boundary. Compression,
encryption, disk access, metadata generation, and changeset compaction run
behind that boundary. Explicit migrations, exports, and incoming row
application need coordinated database access.

## Collection time

The writing time of a batch is its collection time, assigned immediately after
session extraction. HLC ordering uses physical milliseconds, a logical counter,
and node identity. The counter handles equal or backward clocks; a remote clock
can move the watermark forward. Every captured cell and deletion in the batch
uses this version. Retry and worker delay never replace it with processing time.
Intermediate operations coalesced by SQLite have no independently recoverable time.

## Metadata ownership

Versions, row generations, and tombstones are encrypted Spool records. The worker
reads the relevant records from disk and constructs an atomic update group. It
owns neither a SQL replica nor a persistent Rust map of these values. The Spool
key/location index is the read index; record values are decoded temporarily.

Storage key namespaces:

- `00/identity`: format, database ID, node ID.
- `01/snapshot/head`: published archive boundary and hash.
- `02/snapshot/<sequence>/...`: chunked compacted replay archive.
- `03/log/<sequence>/...`: immutable, chunked ordered events.
- `04/meta/row/<table-id>/<key-hash>`: generation and cell versions.
- `05/meta/tomb/<table-id>/<hash>`: generation tombstones.
- `06/clock`: durable HLC watermark.
- `07/replication/...`: peer ACKs, applied/observed progress, retained logs,
  receipts, retention state, and schema-manifest cursors.
- `08/files/meta/...`: encrypted file-object metadata (payloads live in disk objects).
- `09/bridge/...`: Low-to-High bridge ownership, shadow, provenance, and receipts.

A log event and its metadata mutations share an atomic, synchronous Spool commit.
Chunks are independently encrypted by Spool framing; payload descriptors verify
length, chunk order, and BLAKE3 hash. Application wrapping keys are authenticated
before opening existing storage for mutation. Data-key rotation remains in Spool.
The current persistence format is version 8 (replication protocol version 13).
Format-6/7 logs and snapshots remain readable; opening upgrades the store identity
before new writes. Earlier formats and older network versions are rejected.
Column FTS/vector declarations use a versioned binary schema extension keyed by stable
table/column IDs. The codec preserves the legacy positional column layout,
including nested historical and signed payloads, and removes extensions before
SQL index definitions reach SQLite. Human-readable schemas expose `Column::fts`
and `Column::vector` directly. The declarations are authoritative schema metadata in Spool; managed
FTS5/vector indexes and shadow tables are disposable TEMP objects on the sole in-memory
connection, rebuilt independently on each peer.

## Recovery and compaction

Open reads the committed ordered archive/log and reconstructs the sole SQLite
connection before making it available. Failed-storage retry reloads Spool records
without opening another SQLite database. Pending events retain their original
collection version and are replayed in queue order.

Snapshots compact changesets between schema transitions using SQLite Changegroup,
which processes changeset blobs without a database connection. The snapshot head
is published durably before covered log records are deleted and Spool reclaims
segments. Metadata keys stay live independently of the replay archive. Schema
migrations retain their replay dependencies, so frequent migrations can grow the
archive. Snapshot processing uses temporary buffers, not a continuously resident
copy of SQL rows.

## Conflict resolution

Per-column versions choose a deterministic LWW winner within a row generation.
Deletes retain a tombstone for the generation, so an update of that generation
cannot resurrect it. A subsequent captured INSERT receives a new generation.
A delete/reinsert that SQLite coalesces into UPDATE is indistinguishable from an
ordinary update and keeps its generation.

Peers replicate over QUIC (engine `replication/` crate in `../MURMUR`): gossip or Plumtree dissemination,
membership exchange, anti-entropy, and bounded incoming batches. A peer that
cannot recover from retained history fetches a resumable encrypted snapshot from
a trusted source. Incoming rows are merged against Spool records and applied by
the writer atomically. Offline state export/join remains an engine API
(`export_state`) without a wire surface. Alternate ciphers and operation-based
CRDT types are deferred.

## Table replication

Tables are local-only by default. `MURMUR SET TABLE <name> REPLICATED true|false`
rewrites `[replication.tables]` in `config.toml` atomically; the file is overlaid
onto the in-memory schema at unlock and missing entries never enable replication.
Enabling rebuilds the table with a read-only UUIDv5 `id_murmur_primary_key` and a
unique application-key index; disabling restores the application key. Outgoing
and incoming background paths filter rows, tombstones and table CRDT records by
these flags; local persistence is unaffected and ordinary writes never check them.
Drops and renames of replicated tables and columns are rejected in the
engine's `schema::inspect`.

## Retention

Background workers persist peer ACKs, retirement, snapshot coverage and duplicate
floors in Spool. Logs and receipts are deleted only after a durable snapshot
boundary is published, honoring minimum age and count and protecting unretired
peers within the offline window. Tombstones are kept. Heads are rechecked on the
serialized Spool worker so cleanup cannot overwrite newer appends.

## Schema authority

Schema sync rides the replication session, not the SQLite writer. Hello and
Welcome exchange canonical schema bytes as an unsigned mismatch hint; adoption
happens only through signed manifests (`murmur/sqlite-schema/v1`, Ed25519 over
database, signer, monotonic sequence, and payload digest). When `schema_master`
is configured, exactly that node id is adopted from and the master itself never
adopts; otherwise any trusted origin is adopted (legacy symmetric mode).
Sequence cursors and last-accepted manifests live under
`07/replication/schema/` in Spool, so replays stay idempotent across restarts
and old payloads referencing since-retired tables still apply. Payloads carry
their origin so receivers filter retired tables per sender; unknown senders
keep strict checking. Adoption stays additive (tables, nullable columns,
indexes) and local-only tables are excluded from sent schemas and untouched by
adoption.
