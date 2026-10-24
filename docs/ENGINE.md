# Murmur Engine Integration Contract

## Ownership and Architecture

`engine.Factory` and `engine.Database` define the integration boundary between the PostgreSQL-wire server and the core engine.
Both production binaries inject `engine/native.Factory`, implemented in `engine/native`. Its lifecycle, query/type-inference, command, file-handle,
and offline-admin code uses the embedded engine's public packages (`murmur`, `spool`, `replication`).
`engine.Unavailable` remains an explicit disabled-engine option for protocol fixtures and embedding hosts; it is not selected by either production binary.

The core database engine is fully integrated in this repository under `murmur/`, `spool/`, `replication/`, and `compression/`.
The vendored SQLite amalgamation and DataDog/Meta zstd C libraries live directly in `third_party/`.

The adapter translates operations between PostgreSQL wire commands and public engine APIs. All storage, crypto for durable data, key verification, schema history, replication, search indexing, and merge semantics are managed by the engine. The SQL shell owns YAML configuration, PostgreSQL framing, authentication, SQL command parsing, archive packaging, logging, and OS service deployment.

## Database lifecycle and SQL

- `Factory.Open(ctx, OpenRequest)` receives an owned configuration snapshot, a
  raw 32-byte key, and `TableFlagsSink`. Decide create/open from the engine's
  existing-store rules. A wrong key must not modify storage. Return a usable
  database only after all configuration and table flags have been applied.
- Apply the authoritative stable-ID table flag map before starting replication.
  Missing flags disable replication once configuration owns the map. On a legacy
  store without that marker, the adapter imports existing engine flags through
  the sink once, then marks the map configuration-owned. Every later flag change, including remote
  schema adoption, invokes the callback with the complete map. Callback failure
  must leave the engine's published state unchanged.
- `Database.Unlock` verifies the key against the current instance. The shell
  serializes opening and key verification; successful verification unlocks only
  the requesting session. Rotation must update the key accepted by future calls.
- `Execute` receives one SQLite statement with `?N` bindings and separate values.
  It provides statement-level autocommit and accepts concurrent callers.
  `Migrate` handles DDL through authoritative engine migration APIs.
- `Describe` receives rewritten SQL and client-supplied parameter OIDs. Infer
  missing types from engine schema and safely prepare/probe read-only results.
  Never execute writes to describe a statement. Return one parameter OID per
  positional index. Preserve supplied types or return a type error. Unknown
  parameters fall back to TEXT in the shell.
- Values are nil, bool, int64, float64, string, or []byte. A nil value is SQL NULL;
  a non-nil empty byte slice is an empty BLOB. Columns use PostgreSQL OIDs and
  must match between Describe and Execute, including names and column order.
  SQLite result affinities map to TEXT, INT8, FLOAT8, BYTEA, and BOOL.
- Result rows are buffered by the current contract. Enforce admission/result
  limits in the engine before large allocations. The shell applies its own
  session budget after receiving a result. Set command tags for non-row SQL;
  row results default to `SELECT <count>`. `DescribedDatabase.ExecuteDescribed`
  receives a portal's fixed description so typed bindings cannot change result
  OIDs between Describe and Execute. The adapter prepares and steps statements
  using inferred public return values; it never imports engine internal packages.
- Cancellation is checked before engine calls and between row steps. The current
  public engine API does not expose interruption of an active SQLite step or a
  blocked scheduler admission. Active durable atomic commits are not aborted.
  `Close` drains/checkpoints through the engine; sessions finish before closing it.

## Administration requests

`Database.Command(ctx, connectionID, Command)` receives a canonical uppercase
operation and ordered decoded arguments. Literal strings remain strings; parameters
are never substituted into SQL. The shell assigns monotonically increasing
connection IDs. Use those IDs to isolate and clean up transfer handles.

| Operation | Arguments, in order |
| --- | --- |
| `FLUSH`, `REFRESH INDEXES` | none |
| `SHOW STATUS`, `SHOW PEERS`, `SHOW REPLICATION`, `SHOW RETENTION`, `SHOW TABLES REPLICATION`, `SHOW COLUMNS FTS`, `SHOW COLUMNS VECTOR`, `SHOW HEAD` | none |
| `SHOW METRICS` | optional target string: SQLITE or SPOOL, case-insensitive |
| `SET TABLE` | table name, enabled bool; resolve the stable ID and invoke the sink |
| `SET COLUMN FTS` | table name, column name, enabled bool |
| `SET COLUMN VECTOR` | table name, column name, enabled bool; when enabled: dimensions int64, distance string |
| `ROTATE KEY` | previous raw-key []byte, new raw-key []byte |
| `PEER ADD` | IP:port string |
| `PEER REMOVE`, `PEER RETIRE` | node-ID hex string |
| `PEER SYNC`, `RETENTION COLLECT` | none |
| `WAIT FOR` | origin hex string, sequence int64, optional timeout-seconds int64 (default 30) |
| `WAIT FOR LAG` | lag-bound int64, optional timeout-seconds int64 (default 30) |
| `POLICY COUNTER_ADD` | table, column, single-TEXT row key, int64 delta |
| `POLICY SET_ADD`, `POLICY SET_REMOVE` | table, column, single-TEXT row key, text value |
| `POLICY MIN`, `POLICY MAX` | table, column, single-TEXT row key, int64/float64/string value |
| `BRIDGE RELEASE` | table, single-TEXT row key, optional column name |
| `BACKUP DIRECTORY` | shell-allocated destination path for a consistent checkpoint directory |
| `RESTORE DIRECTORY` | unpacked checkpoint path, current raw-key []byte |
| `FILE LIST` | none |
| `FILE SEARCH` | LIKE pattern text |
| `FILE NEXT` | cursor token, row limit int64 |
| `FILE CLOSE` | connection-owned handle token |
| `FILE UPLOAD BEGIN` | file name |
| `FILE UPLOAD APPEND` | upload token, BYTEA chunk |
| `FILE UPLOAD FINISH`, `FILE UPLOAD ABORT` | upload token |
| `FILE DOWNLOAD` | file name |
| `FILE DOWNLOAD NEXT` | download token, chunk limit int64 |

The adapter owns capability checks: disabled replication, missing bridges, or
unsupported engine features must return their documented SQLSTATE, not success.
All response column names and OIDs are defined by `server.commandColumns` and
validated by the shell before extended results are encoded. Non-row operations
return their operation tags. `SHOW STATUS` is a single TEXT JSON diagnostic row.
The Rust server remains the behavioral reference for row contents and ordering.

`BACKUP DIRECTORY` creates a complete encrypted backup in the destination;
the shell packs it into a Rust-compatible archive. `RESTORE DIRECTORY` must trial
restore/open before closing the live database, preserve the old store under a
replacement name, roll back failed swaps, retain session unlock state, and
invalidate connection-owned file handles. Database keys passed as []byte must
be copied if retained: the shell clears temporary key buffers on return.

## File handle lifecycle

The native adapter implements these host semantics around the engine's encrypted file-object API:

- Bind handles to connection IDs and reject foreign/expired handles with `34000`.
- Expire handles after five idle minutes; allow at most 16 per connection and
  1024 globally. Bound cursor materialization to 16 MiB per connection and
  128 MiB globally.
- Respect file-engine configuration and staging budgets. Encrypted file content
  never becomes an application SQLite table.
- NEXT defaults to 256 rows and is capped at 1000. DOWNLOAD NEXT defaults to
  64 KiB and is capped at 1 MiB. A text literal BYTEA chunk uses `\x` hex.
- `Disconnect(connectionID)` releases all cursors, uploads, and downloads for
  that connection. Restore, file key rotation, and shutdown invalidate stale
  handles. Interrupted uploads must not appear as completed file objects.

These semantics are implemented in `engine/native/files.go`; real-engine
validation remains outstanding.

## Offline administration and errors

`Factory.Admin(ctx, AdminRequest)` handles status, inspect, verify, backup,
restore, and keys without requiring the SQL server. It receives source/destination
paths, the raw key, backup verification mode, and optional fresh node/database
identities. Return a JSON-serializable result or a human-readable string.
The adapter returns structured reports with a human-readable rendering; JSON
field names follow the Rust CLI. Failed verification can return a report and an
error together, so the CLI prints diagnostics before returning failure.

Return `*engine.Error` with a five-character SQLSTATE and safe user message.
Use `28000` for wrong keys, `40001` for engine admission conflicts, `08003` for
closed databases, `22023` for invalid arguments, `XX001` for corruption, `54000`
for limits, `55000` for identity/schema mismatches, and `58000` for storage errors.
Map SQLite failures to `23505` unique, `23502` not-null, `23503` foreign-key,
`23514` check, `42501` authorization, `55P03` busy, `25006` read-only,
`42P01` missing table/index, `42703` missing column, `42883` missing function,
and `42601` syntax. `context.Canceled` maps to `57014` in the shell.
