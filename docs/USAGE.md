# Murmur usage and restrictions

Murmur is a server database. `murmur-server` runs one in-memory SQLite
engine, speaks the PostgreSQL wire protocol, and accepts the SQLite dialect —
Postgres-specific syntax is not translated. SQLite sessions capture
source-table changes; background workers compress and encrypt them in Spool
and, when configured, replicate them between peers. Spool stores durable
conflict versions and tombstones.

This guide describes server operation: configuration, connection, SQL
behavior, administration, and restrictions. Repeated-byte keys and IDs below
are demonstration values; deployments must supply their own keys, identities,
and certificates.

## Contents

- [Quickstart](#quickstart)
- [Server configuration](#server-configuration)
- [Connecting](#connecting)
- [Encryption and unlock](#encryption-and-unlock)
- [MURMUR commands](#murmur-commands)
- [SQL behavior and schema restrictions](#sql-behavior-and-schema-restrictions)
- [JSON](#json)
- [Full-text search](#full-text-search)
- [Vector search](#vector-search)
- [Migrations](#migrations)
- [Transactions](#transactions)
- [Durability, recovery, and backpressure](#durability-recovery-and-backpressure)
- [Error codes](#error-codes)
- [TLS and production mode](#tls-and-production-mode)
- [Logging](#logging)
- [Network replication](#network-replication)
- [Table replication settings](#table-replication-settings)
- [Replication retention and snapshot catch-up](#replication-retention-and-snapshot-catch-up)
- [Counters, sets, minimum, and maximum](#counters-sets-minimum-and-maximum)
- [Encrypted file objects](#encrypted-file-objects)
- [One-way Low-to-High bridge](#one-way-low-to-high-bridge)
- [Backups, restore, and key rotation](#backups-restore-and-key-rotation)
- [Remote backup and restore](#remote-backup-and-restore)
- [Progress, metrics, and writer scheduling](#progress-metrics-and-writer-scheduling)
- [Administration CLI](#administration-cli)
- [Deployment limits](#deployment-limits)

## Quickstart

```sh
cp config.example.toml config.toml
murmur-admin hash-password --password s3cret   # paste the hash into [[auth.users]]
murmur-server --config config.toml
```

Then connect with the [Go client](../clients/go/README.md) (`database/sql`,
no cgo), the [Python client](../clients/python/README.md) (`pg8000`, DB-API, no
libpq), the [Rust client](../clients/rust/README.md) (`tokio-postgres`), or any PostgreSQL-wire client, and unlock:

```sql
MURMUR UNLOCK KEY 'database-encryption-key';
CREATE TABLE greetings(id INTEGER PRIMARY KEY, lang TEXT NOT NULL, text TEXT NOT NULL);
INSERT INTO greetings VALUES (1, 'en', 'hello');
SELECT text FROM greetings WHERE lang = 'en';
MURMUR FLUSH;
```

`murmur-server` accepts `--config <path>` (default `./config.toml`) and
`--validate-config` (load and validate, then exit without serving). Exit
codes: 0 ok, 1 configuration error, 2 bind error.

## Server configuration

Everything lives in `config.toml`; see the commented
[config.example.toml](../config.example.toml). Every section is optional and
omitted keys take engine defaults. Unknown keys are rejected. Durations take
units (`500ms`, `1s`, `5m`, `1h`, `7d`) or bare seconds; byte sizes take
units (`64KiB`, `256MiB`, `1GiB`) or bare bytes.

### `[server]` and `[server.tls]`

| Key | Default | Purpose |
| --- | --- | --- |
| `bind` | `127.0.0.1:5432` | Postgres-wire listen address |
| `production` | `false` | Require TLS with client certificates (mTLS) |
| `tls.enabled` | `false` | Enable TLS on the wire listener |
| `tls.cert_file` | — | PEM server certificate chain |
| `tls.key_file` | — | PEM server private key |
| `tls.ca_file` | — | PEM CA bundle verifying client certificates |

Production mode fails to start without TLS credentials and a CA file, and
rejects plaintext or unverified connections. See
[TLS and production mode](#tls-and-production-mode).

### `[storage]`

| Key | Default | Purpose |
| --- | --- | --- |
| `data_dir` | `./data` | Spool storage directory (created on first unlock) |
| `database_id` | generated | 16-byte hex identity; peers share it |
| `node_id` | generated | 16-byte hex identity; must differ per replica |

Supplied identities are validated on open. A peer clone needs the same
database ID and a fresh node ID matching its provisioned TLS identity; never
run two active replicas with one node identity.

### `[engine]` and `[engine.scheduler]`

Engine tuning options and defaults (binary units):

| Key | Default | Purpose / constraint |
| --- | --- | --- |
| `capture_interval` | `1s` | Nonzero collection interval between owner jobs |
| `capture_bytes` | `8MiB` | Positive capture threshold, at most queue capacity |
| `queue_bytes` | `64MiB` | Captured persistence queue; maximum 128 MiB |
| `transaction_bytes` | `64MiB` | Positive transaction budget, at most queue capacity |
| `segment_bytes` | `256MiB` | Segment target, greater than 64 MiB |
| `spool_block_bytes` | `64KiB` | Positive independent block target, maximum 64 MiB |
| `spool_cache_bytes` | `32MiB` | Byte-bounded hot cache; zero disables |
| `snapshot_deltas` | `128` | Positive persistence snapshot threshold |
| `snapshot_bytes` | `256MiB` | Positive persistence snapshot byte threshold |
| `compression_level` | `3` | zstd compression level |
| `storage_workers` | `2` | Positive background storage worker count |
| `data_key_max_age` | unset | Optional automatic Spool data-key rotation age |
| `scheduler.local_share` | `90` | Writer service share; shares must sum to 100 |
| `scheduler.remote_share` | `10` | Writer service share |
| `scheduler.max_debt` | `1s` | Nonzero bounded debt |
| `scheduler.maintenance_reserve_every` | `100` | Nonzero reservation interval |

Queue settings bound captured payloads, not every allocator byte; cache and
queue limits do not cap total process memory. The whole active database must
fit available RAM.

### `[auth]`

```toml
[[auth.users]]
name = "admin"
password_hash = "$argon2id$v=19$m=19456,t=2,p=1$..."
```

Passwords are Argon2 hashes, never plaintext; generate one with
`murmur-admin hash-password` (reads stdin without `--password`). The server
sends a cleartext-password request like PostgreSQL `password` authentication,
so use TLS outside trusted loopback. Unknown users and wrong passwords fail
with `28000`.

### `[replication]`

| Key | Default | Purpose |
| --- | --- | --- |
| `enabled` | `false` | Start QUIC replication on unlock (needs the `replication` Cargo feature) |
| `bind` | `127.0.0.1:9001` | Local QUIC listen address |
| `advertise` | — | Address announced to remote nodes |
| `bootstrap` | `[]` | Remote node addresses to dial |
| `cert_chain_file` / `private_key_file` / `ca_file` | — | Node TLS credentials and trusted roots |
| `signing_key_hex` / `signing_key_file` | — | Ed25519 signing seed (exactly one when enabled) |
| `trusted_origins` | `{}` | Node ID → signing public key pins |
| `allowed_peers` / `allowed_networks` | empty (no restriction) | Connection allowlists |
| `trusted_snapshot_sources` | `[]` | Donors allowed for snapshot catch-up |
| `membership` / `dissemination` | `true` / `gossip` | Membership exchange; `gossip` or `plumtree` |
| `accept_remote_schema` | `true` | Adopt compatible remote schema changes |
| `master` / `schema_master` | `false` / unset | Single schema authority appointment |
| `fanout` / `repairs` | `4` / `2` | Positive, combined at most sessions |
| `sessions` / `connections` | `32` / `64` | At most 4096 each, connections at least sessions + 8 |
| `queue_bytes` / `peer_queue_bytes` | `64MiB` / `4MiB` | Total at most 128 MiB, peer at least 1 MiB |
| `staging_bytes` / `staging_transfers` | `256MiB` / `4096` | Staging at least maximum transaction |
| `max_transaction_bytes` | `64MiB` | Positive, at most 64 MiB |
| `snapshot_bytes` / `snapshot_staging_bytes` | `512MiB` each | Positive, at most 512 MiB each |
| `snapshot_transfer_lifetime` | `7d` | Positive, not renewed by reconnect |
| `incoming_interval` / `anti_entropy_interval` | `1s` / `10s` | Positive |
| `batch_transactions` / `batch_bytes` | `64` / `4MiB` | 1–1024 transactions, 1–64 MiB |
| `adaptive_sync` | `false` | Opt-in adaptive capture/batch sizing |
| `retention` / `retention_interval` | `1h` / `1m` | Minimum history age / cleanup cadence |
| `minimum_retained` / `maximum_offline` | `1000` / `7d` | Minimum log count / peer offline window |
| `merge_policies` | `{}` | `"table.column" = "counter"\|"set"\|"min"\|"max"` |
| `[replication.tables]` | `{}` | Per-table flags; missing means local-only |
| `[replication.files]` | unset | Encrypted file-object directory, key, fetch tuning |
| `[replication.bridge]` | unset | Low-to-High bridge role and outbox/inbox |

Network and local queue budgets are separate. These limits bound particular
work buffers; they do not guarantee a hard on-disk size limit.

### `[backups]`

| Key | Default | Purpose |
| --- | --- | --- |
| `dir` | `./backups` | Folder for remote-backup archives, created on demand |

`MURMUR BACKUP SAVE` and `UPLOAD` create the folder when missing; `LIST`
reads it without creating anything. See
[Remote backup and restore](#remote-backup-and-restore).

### `[logging]` and `[logging.victorialogs]`

| Key | Default | Purpose |
| --- | --- | --- |
| `dir` | `./logs` | Log folder (created when missing) |
| `level` | `info` | `trace`, `debug`, `info`, `warn`, or `error` |
| `file_prefix` | `murmur` | Log file name prefix |
| `rotation` | `daily` | `daily` or `never` |
| `retention_days` | `14` | Delete rotated files older than this; 0 keeps all |
| `console` | `false` | Also emit logs to stderr |
| `victorialogs.enabled` | `false` | Push batches to a Loki-compatible endpoint |
| `victorialogs.url` | `http://127.0.0.1:9428` | Push base URL (`/loki/api/v1/push`) |
| `victorialogs.username` / `password` / `bearer_token` | — | Optional credentials |
| `victorialogs.labels` | `{}` | Extra Loki stream labels |

The file log is primary; remote push is best-effort and never blocks serving.
Keys and password hashes are never logged.

## Connecting

The server implements PostgreSQL frontend/backend startup, cleartext
authentication, and both the simple and extended query protocols, reporting
`server_version` accordingly. `SELECT version()` and `SHOW server_version`
answer without touching the engine, and comment-only input (such as driver
pings) is accepted as an empty query.

Parameters use `$N` (`$1`, `$2`, …); the server rewrites them to SQLite
bindings and infers their types from context (`INSERT` columns, `WHERE`
comparisons, `IN`, `BETWEEN`, `LIKE`) so typed drivers encode values
correctly. Anonymous `?` placeholders are not supported. Statements without
parameters may be batched with `;` in the simple protocol; result sets are
fully buffered (no portals/cursors yet). Explicit parameter type OIDs from
the client are honored when the server cannot infer better.

SQLite type names map to the closest PostgreSQL wire types (`INTEGER`→INT8,
`REAL`→FLOAT8, `TEXT`→TEXT, `BLOB`→BYTEA, `NUMERIC`/unknown→TEXT), with a
`LIMIT 1` probe resolving untyped expressions. `DATE`/`TIMESTAMP`/`UUID`
parameters sent in binary format are decoded to their text forms for SQLite.

## Encryption and unlock

The server starts locked with storage closed. Every connection must unlock
before any other statement:

```sql
MURMUR UNLOCK KEY '<64-hex-chars-or-passphrase>';
```

When the storage directory is empty, unlock creates an encrypted database;
otherwise it opens the existing one. Verification happens per connection: a
wrong key fails with `28000` and never changes storage. A 64-hex secret is
used as the raw 32-byte key; anything else is derived with BLAKE3 like
`murmur-admin --passphrase` (not a password-hardening KDF). The key is never
logged. Connections from different clients never share unlock state.

Encryption protects persisted data with AES-256-GCM over zstd-compressed
Spool blocks; application rows are plaintext in process memory.

## MURMUR commands

Commands are case-insensitive, take no parameters, and return a command tag
(or rows for `SHOW`):

| Command | Effect |
| --- | --- |
| `MURMUR UNLOCK KEY '<key>'` | Create-or-open the database for this connection |
| `MURMUR FLUSH` | Wait until pending capture and queued persistence are durable locally |
| `MURMUR SET TABLE <name> REPLICATED true\|false` | Persist a local/replicated flag into `config.toml` |
| `MURMUR SET COLUMN <table> <column> FTS true\|false` | Persist and replicate a column FTS declaration in the schema |
| `MURMUR SET COLUMN <table> <column> VECTOR true DIMENSIONS <n> [DISTANCE l2\|cosine\|l1]` | Configure persistent, replicated vector search |
| `MURMUR SET COLUMN <table> <column> VECTOR false` | Remove a column vector declaration and its local index |
| `MURMUR SHOW COLUMNS VECTOR` | List vector declarations and local index status |
| `MURMUR SHOW COLUMNS FTS` | List column declarations and local search-index status |
| `MURMUR REFRESH INDEXES` | Collect pending writes and refresh local managed search indexes |
| `MURMUR SHOW TABLES REPLICATION` | List tables with `table`, `replicated` columns |
| `MURMUR SHOW METRICS [SQLITE\|SPOOL]` | List live engine metrics with `subsystem`, `metric`, `value` columns |
| `MURMUR BACKUP SAVE` | Checkpoint into a datetime-named archive; returns `name`, `size` |
| `MURMUR BACKUP LIST` | List backup files with `name`, `size`, `modified` |
| `MURMUR BACKUP UPLOAD BEGIN\|APPEND\|FINISH\|ABORT ...` | Chunked base64 file upload |
| `MURMUR BACKUP DOWNLOAD '<name>' [OFFSET n] [LIMIT n]` | Byte range as a BLOB row |
| `MURMUR RESTORE FROM '<name>' KEY '<secret>'` | Live-restore an archive |
| `MURMUR ROTATE KEY '<previous>' '<new>'` | Rewrap the registry under a new master key |
| `MURMUR PEER ADD '<address>'` | Dial a peer and admit it to the mesh |
| `MURMUR PEER REMOVE '<node>'` | Disconnect and exclude a peer (reversible with `ADD`) |
| `MURMUR PEER RETIRE '<node>'` | Permanently exclude a peer and release its retention obligation |
| `MURMUR PEER SYNC` | Request progress from all peers now |
| `MURMUR SHOW PEERS` | List connected peers (see columns below) |
| `MURMUR SHOW REPLICATION` | One row with the local replication summary |
| `MURMUR SHOW RETENTION` | List per-peer retention state (see columns below) |
| `MURMUR RETENTION COLLECT` | Run one bounded retention pass; returns `collected` |
| `MURMUR POLICY COUNTER_ADD\|SET_ADD\|SET_REMOVE\|MIN\|MAX ...` | One CRDT operation on a single-TEXT-key row |
| `MURMUR BRIDGE RELEASE <table> '<key>' [COLUMN <column>]` | Release High ownership of one bridge-shielded field or row; returns `released` |

`SET` accepts `true`/`1`/`on`/`yes` and `false`/`0`/`off`/`no`; anything
else fails with `22023`. Unknown verbs and malformed commands fail with
`42601`. Only `UNLOCK` is accepted on a locked connection; everything else
fails with `28000`. Backup names are portable filenames (letters, digits,
`.`, `_`, `-`, no directories or leading dots); anything else fails with
`22023`, and missing backup files fail with `58P01`.
The peer, retention, policy, and bridge commands below need an unlocked
connection and enabled `replication`; without it (or on a server built
without the `replication` Cargo feature) they fail with `55000` after
argument validation, so `42601`/`22023` usage errors surface on any build.

### MURMUR PEER ADD

```sql
MURMUR PEER ADD '203.0.113.11:9001';
```

Queues a dial to the `ip:port` address and admits whoever authenticates
there, clearing any earlier `REMOVE` exclusion for that node id. The command
succeeds once queued; dial and handshake failures surface asynchronously in
`MURMUR SHOW REPLICATION` (`last_error`). Malformed addresses fail with
`22023`. Removing and re-adding is last-write-wins by command order.

After a peer restarts it answers from a fresh address, so re-add the new
address to re-mesh. A session that just completed its handshake replaces a
stale entry for the same node id when the old connection is already closed
or has carried no inbound traffic for over 5 s (for example after a crash,
where no QUIC close reaches the survivor); simultaneous fresh dials still
resolve deterministically by node id order. Without a fresh handshake the
stale entry lingers until the background prune (up to about 30 s), so
`MURMUR SHOW PEERS` may briefly report the dead address as connected.

### MURMUR PEER REMOVE

```sql
MURMUR PEER REMOVE '00112233445566778899aabbccddeeff';
```

Disconnects the peer, records a durable exclusion, and closes its session.
The peer's retention obligation is kept: history it has not acknowledged
stays protected while it is unretired. Re-adding the peer's address clears
the exclusion. Node ids are 32 hex characters; anything else fails with
`22023`.

### MURMUR PEER RETIRE

```sql
MURMUR PEER RETIRE '00112233445566778899aabbccddeeff';
```

Permanently excludes the peer and durably releases its retention
obligation, so history it never acknowledged becomes collectable. The
exclusion and the retention release commit atomically. Retiring this node
fails with `22023`. Node ids are 32 hex characters; anything else fails
with `22023`. Retirement is permanent: there is no un-retire command.

### MURMUR PEER SYNC

```sql
MURMUR PEER SYNC;
```

Asks every connected peer for its progress (and membership, when enabled)
immediately instead of waiting for the next anti-entropy tick. It only
requests state; it does not wait for convergence and is not a cluster
barrier.

### MURMUR SHOW PEERS

```sql
MURMUR SHOW PEERS;
```

One row per connected peer, ordered by node id, with stable columns:

| Column | Type | Meaning |
| --- | --- | --- |
| `node_id` | TEXT | 32-hex peer id |
| `address` | TEXT | Remote socket address, or NULL |
| `connected` | TEXT | `true` while the session is open |
| `last_seen_millis_ago` | INTEGER | Milliseconds since inbound traffic, or NULL |
| `sent_watermark` | INTEGER | Highest sequence sent to this peer for its own origin |
| `schema_agreed` | TEXT | `true` once the peer acked the current schema |
| `push_mode` | TEXT | `true` while this peer receives eager pushes |

With no peers the result is empty but still describes these columns.

The Go client provides `PeerAdd`, `PeerRemove`, `PeerRetire`, `PeerSync`,
and `ShowPeers`; the Rust client mirrors them as `peer_add`,
`peer_remove`, `peer_retire`, `peer_sync`, and `show_peers`. Run
`go run ./examples/replication` from `clients/go` for a checked two-node
example.

### MURMUR SHOW REPLICATION

```sql
MURMUR SHOW REPLICATION;
```

One row with the local summary: `running` (TEXT), `address` (TEXT, the
local QUIC address or NULL), `peers` (INTEGER, connected sessions),
`members` (INTEGER, known gossip members), `staged_transactions`
(INTEGER), `applied_transactions` (INTEGER), `last_error` (TEXT, the
latest background replication error or NULL), and `committed` (INTEGER,
the local engine committed-batch cursor, useful as the produced side of
retained-batch growth: `committed - collected_transactions`). The first
seven column indices are stable; new columns are appended. Counters
saturate at `2^63 - 1` instead of failing.

The Go client returns this row as `ReplicationStatus` from `ShowReplication`;
the Rust client returns `ReplicationStatus` from `show_replication`.

### MURMUR SHOW RETENTION

```sql
MURMUR SHOW RETENTION;
```

One row per (peer, acknowledged origin), ordered by node id then origin.
Peers with no acknowledgements get one row with NULL `origin` and
`acknowledged`:

| Column | Type | Meaning |
| --- | --- | --- |
| `node_id` | TEXT | 32-hex peer id |
| `last_seen_millis` | INTEGER | Unix milliseconds the peer was last seen |
| `retired` | TEXT | `true` once retired |
| `origin` | TEXT | 32-hex acknowledged origin, or NULL |
| `acknowledged` | INTEGER | Highest sequence acked for the origin, or NULL |
| `collected_transactions` | INTEGER | Node-global collected count |
| `last_collection_millis` | INTEGER | Node-global Unix milliseconds of the last pass |

The trailing counters repeat on every row. With no known peers the result
is empty.

The Go client returns these rows as `RetentionEntry` values from
`ShowRetention`; the Rust client returns `RetentionEntry` values from
`show_retention`.

### MURMUR RETENTION COLLECT

```sql
MURMUR RETENTION COLLECT;
```

Runs one bounded collection pass now, using the configured `retention`,
`minimum_retained`, and `maximum_offline` rules, and returns a single
`collected` (INTEGER) row with the number of transaction log records
deleted. Quiet passes delete no logs; they still update the
last-collection timestamp and still drive physical segment reclamation,
which can span several passes (deleting one sealed file can unblock the
next). See [Replication retention](#replication-retention-and-snapshot-catch-up)
for what protects history. The Go client exposes this as `RetentionCollect`;
the Rust client as `retention_collect`.

### MURMUR POLICY COUNTER_ADD, SET_ADD, SET_REMOVE, MIN, MAX

```sql
MURMUR POLICY COUNTER_ADD stats count 'row-1' 5;
MURMUR POLICY SET_ADD stats tags 'row-1' 'blue';
MURMUR POLICY SET_REMOVE stats tags 'row-1' 'blue';
MURMUR POLICY MAX stats highest 'row-1' 42;
MURMUR POLICY MIN stats lowest 'row-1' -1.5;
```

Each form applies one CRDT operation to the row with the single TEXT
application key `'row-1'` and commits immediately. The column must carry
the matching `replication.merge_policies` entry (`counter`, `set`, `min`,
`max`); anything else fails with `22023`, and a missing row fails with
`02000`. Set elements are TEXT. `MIN`/`MAX` literals are INTEGER when the
word parses as `i64`, else REAL when it parses as a finite `f64`, else
TEXT; `NaN` and infinities fail with `22023`. Out-of-`i64`-range integers
become REAL. Deltas must fit `i64`.

Limits: single-TEXT-key rows only (no composite or non-TEXT keys, no
NULL keys); no TEXT/BLOB choice for set elements; one operation per
statement (at most 10,000 intents per engine transaction still applies to
engine-coupled callers); ordinary `UPDATE` to policy columns stays
rejected. See [Counters, sets, minimum, and
maximum](#counters-sets-minimum-and-maximum) for merge semantics. The Go
client provides `PolicyCounterAdd`, `PolicySetAdd`, `PolicySetRemove`,
`PolicyMin`, and `PolicyMax`; the Rust client mirrors them as
`policy_counter_add`, `policy_set_add`, `policy_set_remove`, `policy_min`,
and `policy_max` (with `PolicyValue` literals).

### MURMUR BRIDGE RELEASE

```sql
MURMUR BRIDGE RELEASE notes 'row-1' COLUMN title;
MURMUR BRIDGE RELEASE notes 'row-1';
```

Releases High ownership of one bridge-shielded field on the row with the
single TEXT application key `'row-1'`, so later Low updates to that field
flow through again; without `COLUMN` the whole-row ownership is released
instead. Returns a single `released` (TEXT) row: `true` when the release
applied, `false` when the engine was busy (retry). It needs a
`[replication.bridge]` high-receiver: without enabled replication or a
configured bridge it fails with `55000`, and on a Low exporter with
`22023`. Unknown tables or columns, composite or non-TEXT keys, and fields
that are not High-owned fail with `22023`, as does a second release of the
same field. Table and column names match exactly (case-sensitive).

Limits: single-TEXT-key rows only (no composite or non-TEXT keys, no NULL
keys); one field or row per statement. See [One-way Low-to-High
bridge](#one-way-low-to-high-bridge) for ownership semantics. The Go client
provides `BridgeRelease`; the Rust client mirrors it as `bridge_release`.

## SQL behavior and schema restrictions

These rules apply to the managed server database, rather than every
capability of unmanaged SQLite:

| Supported | Restricted or rejected |
| --- | --- |
| Tables with declared, non-null primary keys | Tables without primary keys or with nullable primary-key components |
| Single declared `INTEGER PRIMARY KEY` | `AUTOINCREMENT`; changing primary-key identity through migration |
| Composite primary keys and `WITHOUT ROWID` | Updating application keys while replicated; assigning the managed UUID key |
| `DROP`/`RENAME` on local-only tables | `DROP`/`RENAME` of tables or columns while replicated |
| Ordinary nonunique indexes; the managed application-key unique index | Other non-primary-key uniqueness constraints/indexes |
| SQLite queries, joins, aggregates, bound parameters | Application transaction-control SQL with options |
| Declared source columns and ordinary constraints such as `NOT NULL` | `CHECK`, foreign-key `REFERENCES`, generated/hidden columns, triggers, views |
| Controlled SQL through sessions | Attaching databases, loading extensions, unsafe functions, modifying connection settings through PRAGMA |

SQLite permits a single `INTEGER PRIMARY KEY` without explicit `NOT NULL`;
Murmur accepts that form. Other primary-key columns must declare `NOT NULL`.
Do not use private `sqlite_` or `_crdt_` names for application objects.
Replicated conflict resolution does not enforce cross-row business
invariants; design those in the application. Values use SQLite's NULL,
integer, real, text, and blob types.

Tables start **local-only**, retaining their application-defined SQLite primary
key. Every table must explicitly declare a primary key. Local integer primary
keys retain SQLite allocation; replicated inserts must supply every key value.

```sql
CREATE TABLE orders(id TEXT NOT NULL PRIMARY KEY, value TEXT);
INSERT INTO orders(id,value) VALUES('order-123','local');
MURMUR SET TABLE orders REPLICATED true;
INSERT INTO orders(id,value) VALUES('order-456','ready')
  RETURNING id_murmur_primary_key;
SELECT id, id_murmur_primary_key FROM orders;
```

Enabling replication converts existing rows and makes
`id_murmur_primary_key TEXT NOT NULL PRIMARY KEY` SQLite's sole primary key.
The application's declared key becomes a unique index, preserving key order and
built-in collations. The UUID column is visible in `SELECT *` and schema inspection
but is reserved and read-only, including during migrations and conflict updates.
Application key values are immutable while replicated. Managed-column assignments
and key updates fail with SQLSTATE `42501`; missing or NULL key components fail
with `23502`. Text (including UUID
strings), numeric, blob and composite keys are supported. Missing or NULL
application-key components are rejected even if the original key had a default.
Use explicit insert column lists; implicit lists address application columns only.

UUIDs use UUIDv5 with the RFC URL namespace
`6ba7b811-9dad-11d1-80b4-00c04fd430c8`. Input starts with
`murmur/replication-primary-key/v1` followed by a zero byte, then the ASCII
case-normalized table name and application key components in declaration order.
Lengths and component counts are unsigned 64-bit big-endian values. Values are
tagged: integer 1 (signed 64-bit big-endian), non-integral real 2 (IEEE-754 bits,
big-endian), text 3 and blob 4 (length followed by bytes). Integral reals in the
signed 64-bit range use the integer encoding. SQLite affinity is applied first;
BINARY, NOCASE and RTRIM text equality is normalized before hashing; other key
collations are unsupported. UUID strings
are ordinary text, with no special parsing. The same table and application key
produce the same UUID across writers; UUIDv5 does not allocate business IDs or
prevent two applications from choosing the same application key.

Disabling replication removes the managed column and restores the application
primary key. Conversion scans and rebuilds populated tables, pauses writes, and
can change ordinary SQLite rowids. Explicit indexes referencing the removed
managed column are dropped when disabling replication; other indexes are retained.
Conversions are subject to configured queue and storage payload limits.
`last_insert_rowid()` remains the physical SQLite rowid; read the UUID with
`RETURNING` or `SELECT`. Name query columns explicitly when a table's mode may change. Mode changes reset subscriptions because the schema changes.
Search still uses a single eligible INTEGER application key;
managed-key captures invalidate the search index for a background rebuild.
Rust policy operations continue accepting application keys. `Table.application_key`
describes the original key while `Column.primary_key` describes SQLite's physical
key. Other schema restrictions below remain in effect.

Default conflicts resolve per column by ordered hybrid-clock versions:
physical milliseconds, logical counter, then node ID. Concurrent edits to
different columns can survive together; competing edits to one column have a
deterministic winner. Row generations and durable deletion tombstones prevent
old updates from reviving deleted generations. This is eventual convergence,
not distributed transactions or linearizable reads.

## JSON

SQLite JSON functions are available in ordinary Murmur SQL in default builds.
No `MURMUR SET COLUMN` setting or extension is required. Store JSON text in a
`TEXT` column, or SQLite JSONB in a `BLOB` column. These are ordinary application
columns and use the normal persistence and table-replication settings. JSON
functions read the stored value directly; they do not need an index refresh.

### Store, query and update JSON

```sql
CREATE TABLE json_documents(id INTEGER PRIMARY KEY, data TEXT NOT NULL);
INSERT INTO json_documents(id,data)
VALUES(1,'{"name":"Alice","age":30,"tags":["sqlite","search"]}');

SELECT data ->> '$.name' AS name,
       json_extract(data,'$.age') AS age,
       json_valid(data) AS valid
FROM json_documents WHERE id=1;
-- Alice | 30 | 1

UPDATE json_documents
SET data=json_set(data,'$.age',31)
WHERE id=1;

SELECT json_extract(data,'$.age') AS age
FROM json_documents WHERE id=1;
-- 31
```

`->>` extracts a scalar as SQLite text, integer, real or NULL; `->` returns its
JSON text representation. `json_extract` also returns objects/arrays as JSON
text. Use `json_valid` to test input: invalid JSON is not automatically rejected
by a `TEXT` declaration, and functions that require valid JSON may raise an
error when given malformed input.

### Arrays and JSON values

Use `json_each` to read array elements; `json_tree` can walk nested values.
When passing an array or object to `json_set`, wrap JSON text in `json(...)` so
it is inserted as a JSON value rather than a quoted string:

```sql
UPDATE json_documents
SET data=json_set(data,'$.tags',json('["sqlite","json"]'))
WHERE id=1;

SELECT CAST(t.value AS TEXT) AS tag
FROM json_documents AS d, json_each(d.data,'$.tags') AS t
WHERE d.id=1
ORDER BY t.key;
-- sqlite
-- json

SELECT json_object('name','Alice','active',json('true'));
-- {"name":"Alice","active":true}
```

Bind JSON strings to `TEXT` columns with normal parameters, for example
`INSERT INTO json_documents(id,data) VALUES($1,$2)`. In Go, validate the input
with `json.Valid` from `encoding/json` and pass a string for `$2`. A bound JSON string used
inside `json_set` likewise needs `json(...)` when it represents an object/array.
JSON expressions can return different SQLite types depending on their values;
use `CAST(... AS TEXT)`, `CAST(... AS INTEGER)` or another explicit type when a
prepared wire query needs a stable result type, especially with empty results
or mixed-type JSON arrays. Missing properties and JSON null yield SQL NULL for
scalar extraction; use `json_type` when you need to distinguish them.

### SQLite JSONB

```sql
CREATE TABLE json_binary(id INTEGER PRIMARY KEY, data BLOB NOT NULL);
INSERT INTO json_binary(id,data)
SELECT id,jsonb(data) FROM json_documents;

SELECT typeof(data) AS storage,
       json_valid(data,8) AS valid_jsonb,
       json_extract(data,'$.name') AS name
FROM json_binary WHERE id=1;
-- blob | 1 | Alice

SELECT json(data) AS json_text FROM json_binary WHERE id=1;
-- {"name":"Alice","age":31,"tags":["sqlite","json"]}
MURMUR FLUSH;
```

JSONB is SQLite's opaque binary representation, not PostgreSQL JSONB. Let
SQLite produce it with `jsonb(...)`; use `json(...)` to return readable JSON.
A raw BLOB result travels over the wire as BYTEA. `json_valid(data,8)` performs
strict SQLite JSONB validation; the one-argument `json_valid(data)` checks JSON
text and should not be used as the JSONB validation example.

Restrictions and replication:

- Murmur has no automatic JSON schema validation. Validate input in your
  application. Managed migrations reject `CHECK(json_valid(...))` constraints,
  generated columns, and JSON expression indexes such as
  `CREATE INDEX ... ON json_documents(json_extract(data,'$.name'))`.
- Use JSON functions in ordinary SELECT/INSERT/UPDATE statements. JSON functions
  are not in the managed migration function allowlist; do data conversion or
  backfills as separate DML after creating the table.
- Normal source-row persistence retains the complete TEXT/BLOB value. On a
  replicated table, default conflict resolution chooses one whole column value;
  concurrent edits to different JSON properties in that column do not merge
  independently. `json_set` is a local SQLite update, not a distributed JSON
  property operation. Store independently edited fields in separate SQL columns
  when you need the existing per-column conflict behavior.
- FTS and vector indexes are configured on ordinary source columns. Murmur does
  not automatically index JSON properties, maintain extracted text columns, or
  convert a JSON array into a float32 embedding BLOB. Applications must maintain
  those source values; see the following search sections for setup and limits.

### Runnable JSON, FTS and vector examples

From the repository root:

```sh
cd clients/go
go run ./examples/json
go run ./examples/search
go run ./examples/vectors
```

Each example starts an isolated scratch server, checks its results and cleans
up. They require Go and a Rust toolchain with a C compiler; the helper builds
the server on first use. No external services or peers are needed.

| Capability | Runnable example | Checked behavior |
| --- | --- | --- |
| JSON | [json](../clients/go/examples/json/main.go) | Scalar extraction, updates, array iteration, bound strings, validation and JSONB |
| FTS5 | [search](../clients/go/examples/search/main.go) | Column enablement, discovered index names, bound MATCH queries, refresh and disabling |
| Vector | [vectors](../clients/go/examples/vectors/main.go) | Float32 BLOBs, column settings, KNN neighbors, numeric distances and disabling |

Rust client equivalents are runnable from the repository root:

```sh
cargo run --manifest-path clients/rust/Cargo.toml --example json
cargo run --manifest-path clients/rust/Cargo.toml --example search
```

The checked Rust search example uses `set_column_fts`, `column_fts`,
`set_column_vector`, `column_vectors` and `refresh_indexes`, then runs
parameterized MATCH and KNN queries. For example:
`db.set_column_fts("documents", "body", true).await?` followed by
`db.refresh_indexes().await?`. These APIs have the same server restrictions
described below: INTEGER application keys, TEXT FTS columns, BLOB vector
columns, and the server's `vector` feature for vector search. Disabled columns
return absent status/configuration; index names must be quoted as SQL identifiers.
See the [Rust client catalog](../clients/rust/README.md) for all wire features,
including replication, retention, policy operations, bridge release and key rotation.
Embedded callbacks and subscriptions are not exposed by the PostgreSQL protocol.

## Full-text search

Enable full-text search on individual `TEXT` columns with `MURMUR SET COLUMN`.
FTS5 is included in the default server build; no extension, Cargo feature, or
external search service is needed.

```sql
CREATE TABLE documents(id INTEGER PRIMARY KEY, title TEXT, body TEXT);
INSERT INTO documents VALUES(1, 'SQLite', 'fast local search');
MURMUR SET COLUMN documents title FTS true;
MURMUR SET COLUMN documents body FTS true;
MURMUR SHOW COLUMNS FTS;
MURMUR REFRESH INDEXES;
```

`SHOW` returns `table TEXT`, `column TEXT`, `enabled TEXT`, `index_name TEXT`,
`ready TEXT`, `rebuilding TEXT`, `pending_keys INTEGER`, `queued_bytes INTEGER`,
and `error TEXT`. Boolean text is `true` or `false`. It includes every application
column; disabled columns have null index/status fields. The managed UUID column
is excluded and cannot receive a column FTS declaration. Enabled columns in one
table share one index and its status.
`queued_bytes` reports the manager's shared queue size, not a per-column allocation.

Use the returned `index_name` as a quoted SQL identifier. The stable generated
name is `murmur_fts_<table-id>`; the placeholder below must be replaced with the
actual name reported by `SHOW`. Its `rowid` equals the INTEGER application key,
including while the source table uses Murmur's managed UUID physical key:

```sql
SELECT d.id, d.title
FROM documents AS d
JOIN temp."murmur_fts_<table-id>" AS f ON f.rowid = d.id
WHERE "murmur_fts_<table-id>" MATCH 'SQLite AND search';
-- Prepared queries may use MATCH $1 with a bound TEXT search expression.

MURMUR SET COLUMN documents body FTS false;
MURMUR FLUSH;
```

Limits and behavior:

- Source columns must be declared exactly `TEXT` (case-insensitive), and the
  application key must be a single column declared `INTEGER`. Text, UUID and
  composite application keys are unsupported. Nullable text is allowed; null
  values contribute no searchable terms. Invalid declarations return `22023`;
  malformed command syntax returns `42601`. In MURMUR commands, names containing
  spaces or quotes use single-quoted words (double embedded single quotes), for
  example `MURMUR SET COLUMN 'doc list' 'full text' FTS true`. Ordinary SELECT
  statements use SQLite identifier quoting with double quotes.
- One index covers all enabled columns, using the built-in `unicode61` tokenizer.
  SQLite FTS5 query syntax, including column filters, is accepted by `MATCH`.
  Server tokenizer customization and SQL column annotations are not supported.
- The declaration is schema metadata persisted in encrypted Spool, rather than
  `config.toml`. It survives restart, backup/restore and replication-mode changes.
  Existing columns default to disabled. Calling SET with the current value is a
  no-op. `MURMUR FLUSH` waits for durable persistence as with other schema changes.
- Declarations travel with replicated tables under existing schema-authority and
  `accept_remote_schema` rules, including enable/disable changes without SQL DDL.
  Apply replicated schema changes on the configured schema master. Local-only
  tables keep local declarations; peers do not receive those tables or settings.
  Locally disabled tables are protected from incoming schema changes.
- Indexes and shadow tables are read-only, disposable TEMP objects in the same
  in-memory SQLite connection. They never spill to plaintext temporary files and
  are never exported, persisted or replicated. Each node rebuilds from its source
  rows, including after recovery, snapshots and incoming replicated changes.
- Ordinary writes collect source changes only; index maintenance runs separately
  in bounded batches. Search is eventually consistent and an unavailable or
  rebuilding index may reject reads. `MURMUR REFRESH INDEXES` waits for local
  writes collected before the call to be indexed; it does not wait for peers or
  durable persistence. Maintenance and schema changes can pause other SQL on the
  single SQLite owner. Inspect `ready`, `rebuilding` and `error` before querying.
- Existing drop/rename restrictions for replicated tables remain. Local drops
  remove derived indexes, and new column/table identities start without FTS.

Rust callers can use `Database::set_column_fts(table, column, enabled)` and
`Column::fts`; `Database::refresh_indexes()` provides the same local barrier.
Explicit `Options::search_indexes` remain node-local and must be supplied again
on reopen. They coexist with schema-derived indexes; conflicting names are
rejected. `set_search_indexes` replaces only explicit definitions.

The Go client provides `SetColumnFTS`, `ColumnFTS`, and `RefreshIndexes`; run
`go run ./examples/search` from `clients/go` for a complete checked example.

## Vector search

Ordinary server builds include sqlite-vec through the default `vector` Cargo
feature. Vector settings use the same schema maintenance and replication paths
as FTS; indexes remain local, disposable TEMP objects on the sole in-memory
SQLite connection. No vector metadata tables, triggers, or index updates are
added to ordinary application writes.

```sql
CREATE TABLE documents(id INTEGER PRIMARY KEY, embedding BLOB);
-- Two float32 components [1,0] and [0,1], encoded little-endian.
INSERT INTO documents VALUES(1,x'0000803f00000000'),(2,x'000000000000803f');
MURMUR SET COLUMN documents embedding VECTOR true DIMENSIONS 2 DISTANCE l2;
MURMUR SHOW COLUMNS VECTOR;
MURMUR REFRESH INDEXES;
```

`SHOW` includes all application columns, with `table TEXT`, `column TEXT`,
`enabled TEXT`, `dimensions INTEGER`, `distance TEXT`, `index_name TEXT`,
`ready TEXT`, `rebuilding TEXT`, `pending_keys INTEGER`, `queued_bytes INTEGER`,
and `error TEXT`. Boolean strings are `true`/`false`; disabled columns have null
configuration and status fields. The generated UUID column is excluded and
cannot receive a declaration. Queue bytes report the shared search queue.

Each column has its own stable `murmur_vec_<table-id>_<column-id>` index. Replace
the placeholder with the actual name from `SHOW`, and quote it as an SQL
identifier. The index `rowid` equals the INTEGER application key, including when
replication uses a managed UUID physical primary key:

```sql
SELECT d.id, v.distance
FROM temp."murmur_vec_<table-id>_<column-id>" AS v
JOIN documents AS d ON d.id = v.rowid
WHERE v.embedding MATCH '[1,0]' AND v.k = 2
ORDER BY v.distance;
-- Prepared queries: MATCH $1 AND v.k = $2; bind a JSON vector string and integer.

MURMUR SET COLUMN documents embedding VECTOR false;
MURMUR FLUSH;
```

Limits and behavior:

- Source type must be declared `BLOB` (case-insensitive), with one `INTEGER`
  application primary key. Source column names must start with an ASCII letter
  and contain only ASCII letters, digits and underscores; sqlite-vec reserves
  internal names such as `rowid`, `distance` and `k`.
- Dimensions must be 1–8192. `DISTANCE` accepts `l2` (default), `cosine`, or `l1`.
  Enabling always requires `DIMENSIONS`; disabling accepts no options. Malformed
  command syntax returns `42601`; invalid targets or settings return `22023`.
  Names with spaces use the same single-quoted MURMUR words as FTS commands.
- Embeddings are application-supplied BLOBs containing exactly dimensions × 4
  bytes of little-endian finite float32 values. NULL contributes no vector.
  Embedding generation and automatic text-to-embedding conversion are absent.
- Native sqlite-vec KNN queries require `k` and a matching-dimensional query
  vector. JSON query vectors are inferred as TEXT; explicitly supplied BYTEA
  parameters also accept little-endian float32 query vectors. Distance results
  are numeric FLOAT8, including prepared queries described against empty indexes.
  Only a single `ORDER BY distance` is supported in KNN queries; tie order is
  unspecified. This exposes the existing sqlite-vec search behavior without an
  additional approximate-neighbor algorithm.
- Identical settings are a no-op. Changing dimensions or metric rebuilds the
  index and validates populated data; a failure preserves the previous setting
  and usable index. Existing data usually needs conversion while indexing is
  disabled before dimensions can change.
- Ordinary SQL writes do not validate vectors. Invalid values written after
  configuration remain persisted/replicated source data; background maintenance
  reports an error and makes the index unavailable. Correct the values, then
  call `MURMUR REFRESH INDEXES` to recover. Inspect SHOW readiness/error fields.
- Updates are eventually consistent; refresh waits for local collected writes
  only. Use FLUSH for durability. Reconfiguration/rebuilds can pause other SQL.
  Indexes and their shadow tables are read-only, stay in RAM, and are rebuilt
  from source rows after recovery, snapshots, and incoming replicated changes.
- Declarations are authoritative schema metadata in encrypted Spool, never
  `config.toml`. They travel with replicated tables under existing schema-master
  and remote-adoption rules, including enable/disable and option changes without
  DDL. Local-only and locally disabled tables retain their existing protection.
  Historical row payloads do not overwrite current search settings.
- Settings survive mode changes, migrations preserving stable identities, and
  backups/restores. Local drops remove derived indexes; new identities start
  disabled. Explicit Rust search definitions coexist and are not replicated.
- `cargo build --no-default-features` omits sqlite-vec. Such binaries reject
  opening or adopting vector-declared schemas, and reject enabling vector search,
  with a feature-required error. Use ordinary builds, or add `--features vector`.

Rust callers use `Column::vector`, `VectorColumnConfig { dimensions, distance }`,
and `Database::set_column_vector(table, column, Some(config))`; `None` disables.
Go callers use `SetColumnVector`, `ColumnVectors`, and `RefreshIndexes`. Run
`go run ./examples/vectors` from `clients/go` for checked nearest-neighbor results.

## Migrations

DDL (`CREATE`, `ALTER`, `DROP`, `REINDEX`) runs through server-side
migrations, which compare before/after schemas, preserve stable identities,
and create durable schema events. Migrations can scan substantial data and
pause application work. Additive changes — `CREATE TABLE`, `CREATE INDEX`,
`ADD COLUMN`, `DROP INDEX` — work on every table. Drops and renames of
tables and columns are rejected with `22023` while the table replicates,
because peers only adopt additive steps and stable identities must not
diverge. Tables are local-only until explicitly enabled, so drops and
renames work by default; they are rejected with `22023` only while the
table replicates. To restructure a replicated table, disable it first:

```sql
MURMUR SET TABLE orders REPLICATED false;
ALTER TABLE orders RENAME TO archive;
```

The wire protocol carries no table/column
rename mappings, so a rename over SQL mints a new table identity that
defaults to local-only. Enable replication explicitly if needed. Type changes and primary-key changes are rejected loudly. Network
schema adoption is limited to compatible additive changes (new tables,
nullable non-key columns, nonunique indexes). Replicated DDL must originate
on the master: a non-master that creates a replicated table will see its own
writes stop applying upstream until the master carries the same definition.

## Transactions

Every statement autocommits individually. Bare `BEGIN`/`COMMIT`/`ROLLBACK`
(plus `START TRANSACTION`, `END`, and `TRANSACTION`/`WORK` variants) are
accepted as no-ops returning their tags, so drivers and ORMs that bracket
work keep functioning — but each statement stays individually atomic and
there is no multi-statement rollback. Anything else starting with those
words falls through to SQLite for an honest error.

## Durability, recovery, and backpressure

| Operation | Guarantee after success |
| --- | --- |
| Statement | Application changes committed in local RAM |
| `MURMUR FLUSH` | Pending source capture and queued persistence are durable in local Spool |
| Clean shutdown | Managed shutdown, including local persistence flush |

`MURMUR FLUSH` is not an all-peer barrier: it waits for local durability,
not for replicas.

By default, collection happens at a one-second interval or an 8 MiB capture
threshold, between owner-thread jobs. A crash can lose unflushed RAM writes;
the interval is not a strict upper bound if SQL work or workers are delayed.
Flush at application durability boundaries. Reopening reconstructs SQLite
from durable Spool records.

Batch conflict timestamps are assigned when the session changeset is
collected, before queuing, and survive worker delays/retries. A capture can
combine several transactions. Repeated writes may collapse to a final session
change; an insert followed by delete can disappear. If a delete/reinsert must
create a distinct replicated row generation, flush between them.

Queues and transaction sizes are bounded. `40001` signals pressure or a
changed merge baseline; retry replayable operations with application backoff.
`54000` requires reducing the operation size or adjusting a valid limit.
Storage failures stop writes while leaving reads available and retain pending
batches; repair the cause and retry. Do not interpret a storage failure as
proof that an earlier RAM commit was rolled back.

Spool uses zstd compression and authenticated AES-256-GCM encryption. Its
default 64 KiB independent blocks keep point reads small; oversized
individual records are isolated and large atomic commits span blocks. A
disposable, byte-bounded hot block cache evicts blocks and wipes decoded
buffers.

## Error codes

Failures carry SQLSTATE codes (`murmur.CodeOf` in Go extracts them):

| Code | Meaning | Typical response |
| --- | --- | --- |
| `28000` | Locked connection, bad credentials, wrong key, TLS/production rejection | Unlock, fix login, supply the correct key, use TLS |
| `42P01` | No such table or index | Fix the name or migrate first |
| `42703` | No such column | Fix the name |
| `42883` | No such function | Fix the name |
| `42601` | Syntax error, bad MURMUR usage, parameters on commands/DDL | Fix the statement |
| `42501` | Statement denied by the authorizer | Use an allowed statement (DDL goes through migrations) |
| `23505` | Unique/primary-key violation | Fix the conflicting key |
| `23502` | NOT NULL violation | Supply the column |
| `23503` | Foreign-key violation | Fix the reference |
| `23514` | Check-constraint violation | Fix the value |
| `23000` | Other constraint violation | Inspect the message |
| `40001` | Queue full or merge baseline changed | Back off and retry |
| `55P03` | SQLite lock timeout | Retry; reduce contention |
| `25006` | Read-only statement in a read context | Fix the statement |
| `22023` | Invalid parameter value | Fix the value |
| `22P02` / `22000` | Value conversion failure | Fix the value or type |
| `42804` | Type mismatch | Fix the type |
| `02000` | Query returned no rows | Handle the empty result |
| `54000` | Limit exceeded | Split work or adjust a budget |
| `55000` | Database identity or schema history mismatch; replication-gated command with replication disabled | Align identities and history; enable `replication` for peer, retention, policy, and bridge commands |
| `58000` | Storage or I/O error | Repair storage, retry persistence |
| `08003` / `08006` | Database closed / connection failure | Reconnect |
| `58P01` | No such backup file | Check `MURMUR BACKUP LIST` for available names |
| `XX001` | Corrupt storage | Restore a verified backup |
| `XX000` | Internal error | Report with logs |

## TLS and production mode

TLS uses rustls with TLS 1.3/1.2. Configure `server.tls` with a PEM
certificate chain, private key, and (for client verification) a CA bundle.
`production = true` fails to start without TLS credentials and a CA file,
and rejects plaintext or unverified connections with `28000`. Development
servers may run plaintext, ideally on loopback only.

Go clients select a mode mirroring Postgres `sslmode`:

```go
TLS: murmur.TLSConfig{
    Mode:     murmur.TLSVerifyFull, // or TLSRequire, TLSVerifyCA, TLSDisable
    CAFile:   "/etc/murmur/ca.crt",
    CertFile: "/etc/murmur/client.crt", // mutual TLS
    KeyFile:  "/etc/murmur/client.key",
}
```

`TLSDisable` sends passwords in cleartext. There is no built-in CA
generation; provision certificates with your own PKI.

## Logging

The server logs to daily-rotated files (`<dir>/<prefix>.YYYY-MM-DD.log` by
default) and deletes rotated files older than `retention_days` at startup
(0 keeps everything). Set `console = true` to also log to stderr, and
`level` to control verbosity. With `victorialogs.enabled`, batches are also
pushed as Loki JSON to `<url>/loki/api/v1/push` with the configured labels
and credentials; push failures are logged and dropped without affecting
serving.

## Network replication

Requires the `replication` Cargo feature and `replication.enabled = true`.
Provision a TLS certificate chain and private key per node, trusted CA
roots, and a separate Ed25519 signing seed. Certificates identify nodes
using a URI SAN `replicateddb://node/<32 hexadecimal node-id characters>`.
Use a distinct certificate/node ID for each active replica and never change
the signing key under an existing node ID.

Configure trust and policy before the first unlock:

- `trusted_origins` pins signing public keys, including origins relayed by
  another peer. TLS trust and origin signing trust are separate.
- Nonempty `allowed_peers` and `allowed_networks` restrict connections by
  node and network; empty collections impose no additional restriction.
- `dissemination` selects `gossip` or `plumtree`; `membership` controls
  membership exchange. Fanout, repair sessions, queues, and connection
  limits bound work.
- `accept_remote_schema` controls remote adoption. To appoint one schema
  authority, set `schema_master` to the master node on peers, and
  `master = true` on that node with its own ID as `schema_master`. Data
  writes remain masterless.
- `trusted_snapshot_sources` explicitly allows snapshot donors. The default
  empty set does not authorize automatic snapshot imports.

Application commits do not wait for network I/O. Incoming application waits
its turn on the single SQLite owner. Network queues can report overload or
transport/authentication errors; inspect the server logs and replication
diagnostics. Peers need compatible protocol versions and schema
configuration. The current signed snapshot protocol is version 13 and
managed database format is 8. Upgrade all peers together: older wire versions
are rejected. Opening a format-6 or format-7 database or backup upgrades its
identity to format 8, preserving historical logs, snapshots, signed payloads,
and existing FTS declarations. Legacy columns start with vectors disabled.
Formats older than 6 and downgrades are unsupported; keep a pre-upgrade backup.

Incoming changes drain continuously in bounded batches (64 transactions, 4
MiB target by default); a larger transaction runs alone. `incoming_interval`
only controls a fallback retry, capped at one second. `adaptive_sync` is
opt-in and defaults to false; it adjusts capture and batch counts within
configured bounds.

## Table replication settings

Per-table flags live in `[replication.tables]`, keyed by stable table IDs:

```toml
[replication.tables]
# Keys are stable table IDs; use MURMUR SET TABLE to write actual entries.
"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef" = true
```

Missing entries mean local-only. `MURMUR SET TABLE <name> REPLICATED
true|false` rewrites `config.toml` atomically and applies the flag to the
running server; `MURMUR SHOW TABLES REPLICATION` lists current flags.
A mode change also converts the physical primary key as described above. Re-enabling
publishes current rows with fresh versions and deletion fences for rows removed
while local. Replicated tombstones are retained in Spool across conversions.
The durable transition record precedes the config update; after an interrupted
transition, close and reopen so the authoritative config selects the mode. The
config file is authoritative: the server overlays it onto the in-memory
schema at unlock. Legacy maps stored at `00/schema/replication` are imported
into the config file once and never written back. Missing flags always mean
local-only; no automatic table opt-in or old-format migration is performed.

Settings are node-local and durable. Local SQL and persistence continue
normally for local-only tables. Background incoming/outgoing paths filter row
data, tombstones, and table policy records; network schema publication also
filters disabled table definitions. Set policies on every relevant node.
Disabling replication does not erase data already delivered, recall exported
artifacts, or retroactively make shared data private. Re-enabling is not a
guarantee that previously ignored incoming changes will replay automatically.

## Replication retention and snapshot catch-up

Requires enabled `replication`. Background cleanup enforces **both** minimum
persistence age (`retention`, default one hour) and minimum retained log
count (`minimum_retained`, default 1000). Peer ACKs, retirements, ages,
snapshot coverage, and duplicate floors are durable Spool records. Unretired
peers within `maximum_offline` (default seven days), pending bridge exports,
and active snapshot transfers protect needed history. Cleanup publishes
durable snapshot coverage before deleting logs/receipts and advancing
duplicate floors. CRDT tombstones remain. Physical segment reclamation runs
after every pass (including quiet ones) as rate-limited maintenance, so
disk space follows logical pruning with a delay; cross-file delete chains
drain over successive passes. A sealed segment is rewritten when its live
value bytes are at most 20% of its total bytes (tombstones excluded).
Retention is not a hard storage quota or backup rotation policy.

If retained logs cannot cover a peer's gap, an authorized snapshot donor
provides a signed, resumable snapshot. Transfers use 64 KiB chunks, durable
staging, and bounded pages; the receiver merges concurrent local changes
rather than replacing its entire database. Completion must be durable before
ACK. Snapshot payload and per-direction aggregate staging budgets default to
512 MiB, with a seven-day fixed transfer lifetime. Larger states require
explicitly larger snapshot and staging budgets; the signed manifest must also
fit the configured peer queue and the 8 MiB frame limit. Snapshot catch-up
needs an explicitly trusted donor.

## Counters, sets, minimum, and maximum

Requires enabled `replication`. Configure policies for existing source
columns with `merge_policies` before unlock, and use the same policies on
peers:

```toml
[replication]
merge_policies = { "stats.count" = "counter", "stats.tags" = "set" }
```

```sql
CREATE TABLE stats(
    id INTEGER PRIMARY KEY, count INTEGER NOT NULL,
    tags BLOB, lowest INTEGER NOT NULL, highest INTEGER NOT NULL
);
INSERT INTO stats VALUES(1, 0, NULL, 100, 0);
```

Policy operations (counter add, set add/remove, min/max intents) are issued
through the engine's intent API; ordinary `UPDATE` assignments to
policy-controlled columns are rejected. Policies cannot target primary keys.
Counter projections require INTEGER and reject overflow. Set projections are
NULL or a postcard-encoded `Vec<Value>` BLOB. Min/max require comparable
values. The row must exist, keys must match declared primary-key order, and
at most 10,000 policy intents fit one transaction. Causal state lives in
Spool; no policy metadata tables or triggers are added to SQLite. Concurrent
inserts of the same key on different nodes union their counter deltas and
set operations once the rows merge, and minimum/maximum columns take the
extremum across all unfenced incarnations. A delete fences the deleted
incarnation's operations, so a reinserted row never resurrects them.

Over the wire, `MURMUR POLICY COUNTER_ADD|SET_ADD|SET_REMOVE|MIN|MAX`
applies one intent per statement to a single-TEXT-key row; see the [POLICY
command](#murmur-policy-counter_add-set_add-set_remove-min-max) for the
argument encoding and limits. Engine-coupled callers keep the multi-intent
transaction API (at most 10,000 intents per transaction).

## Encrypted file objects

Requires enabled `replication` and `[replication.files]` (directory, key,
fetch tuning). Payloads are separate encrypted objects, not SQLite BLOB
rows. Names are logical metadata keys. Replicated metadata can arrive before
a payload is available. Availability is local, remote peers, or unavailable;
background fetching uses configured peers and resumable staging. Objects are
authenticated in 64 KiB chunks with a whole-file length/digest check.

File defaults: generation 0, maximum file size 4 GiB, staging 1 GiB, two
concurrent fetches, fetch interval 30 seconds. File and staging limits
differ; advertised size alone does not guarantee a transfer will fit
available staging. Files have a separate key lifecycle from Spool. Database
backups include durable file metadata, **not the external object directory**.
Preserve that directory and the relevant object keys separately.

Rust embedders can also use the incremental object API (requires `replication`
and a configured file store). Dropping an unfinished upload aborts it;
`list_bounded` bounds metadata collection before sorting, and downloads pin and
authenticate one version:

```rust
use sqlite_session::replication::files::Objects;
use std::sync::Arc;

fn incremental_file(objects: Arc<Objects>) -> Result<(), Box<dyn std::error::Error>> {
    let mut upload = objects.begin_upload("demo.bin")?;
    upload.append(&[0, 255])?;
    let metadata = objects.finish_upload(upload)?;
    assert_eq!(metadata.bytes, 2);
    let mut download = objects.download("demo.bin")?;
    let (offset, bytes, eof) = download.next(65536)?;
    assert_eq!((offset, bytes, eof), (0, vec![0, 255], true));
    let _snapshot = objects.list_bounded(1024 * 1024)?;
    Ok(())
}
```

Get the configured store through `db.replication().and_then(|h| h.files())`.
These operations share the staging limits with background peer fetching.

### Files over PostgreSQL SQL

Build the server with `--features replication`, enable `[replication]`, and
configure its node certificate, signing key/trusted origin, plus
`[replication.files]` in `config.toml`. The file key is separate from the database
key. See `config.example.toml`; the runnable Go `files` example provisions a
standalone scratch node with no peers.

File commands return ordinary typed PostgreSQL rows. No file metadata table is
created in SQLite: names, sizes, hashes, versions and tombstones remain durable
Spool records, and payloads remain encrypted files under `replication.files.directory`.
Names are logical keys (1–4096 UTF-8 bytes, no NUL), never filesystem paths.

```sql
MURMUR FILE LIST;                       -- returns cursor TEXT
MURMUR FILE SEARCH 'reports/%.pdf';      -- returns cursor TEXT
MURMUR FILE NEXT '<cursor>' LIMIT 100;   -- typed metadata rows
MURMUR FILE CLOSE '<cursor>';

MURMUR FILE UPLOAD BEGIN 'reports/a.pdf'; -- returns upload TEXT
-- Extended protocol: bind the token as TEXT and raw bytes as BYTEA.
MURMUR FILE UPLOAD APPEND $1 $2;
-- Simple protocol: a quoted PostgreSQL hex BYTEA value is also accepted.
MURMUR FILE UPLOAD APPEND '<upload>' '\x00ff27';
MURMUR FILE UPLOAD FINISH '<upload>';    -- publishes and returns metadata
-- Or discard an unfinished upload:
-- MURMUR FILE UPLOAD ABORT '<upload>';

MURMUR FILE DOWNLOAD 'reports/a.pdf';    -- download TEXT, name TEXT, size BIGINT
MURMUR FILE DOWNLOAD NEXT '<download>' LIMIT 65536;
-- offset BIGINT, total BIGINT, chunk BYTEA, eof BOOLEAN; repeat until eof.
MURMUR FILE CLOSE '<download>';
```

LIST and SEARCH capture metadata sorted by name. NEXT returns `name TEXT`,
`size BIGINT`, `modified_ms BIGINT` (Unix milliseconds), `sha256 TEXT` (hex), and
`local BOOLEAN`. Changes after capture do not alter the cursor. An empty page
means exhaustion; explicitly CLOSE the cursor. SEARCH matches names only, using
case-sensitive LIKE: `%` matches any sequence, `_` one Unicode character, and
backslash escapes a literal character. A trailing escape is rejected. The Rust
`search_files` API retains its existing literal substring behavior.

Uploads are encrypted incrementally and become visible only after FINISH
synchronizes the object and publishes its metadata. Failed or aborted uploads
preserve the previously published file. Existing names are replaced atomically;
concurrent replacements follow existing file-metadata conflict resolution.
Downloads authenticate the complete immutable object before returning a handle,
then stream bounded BYTEA chunks from that pinned version even if its logical
name is replaced. Opening a large download therefore reads the whole file first.
Remote-only content returns retryable SQLSTATE `55000`; background peer fetching
continues. Authentication/disk failures return `58030`.

Handles belong to one physical, authenticated connection. Keep the same
connection for OPEN/BEGIN, NEXT/APPEND and CLOSE/FINISH. They expire after five
idle minutes (cleanup every 30 seconds) and are discarded on disconnect, restore
or shutdown. Unknown, expired or foreign handles return `34000`. SQL transactions
do not govern files: ROLLBACK cannot undo a completed upload. No restart resume
is provided for SQL handles.

Defaults and bounds: metadata pages default to 256 rows (maximum 1000), binary
chunks to 64 KiB (maximum 1 MiB); at most 16 handles per connection and 1024
server-wide. Metadata snapshots have an accounted size limit of 16 MiB per
connection and 128 MiB server-wide. Snapshot collection also bounds the scanned
metadata before SEARCH filtering, so a search of an oversized catalog can be
rejected. Capacity exhaustion returns `53400`; invalid limits/arguments return
`22023`. File and staging configuration limits apply, with staging capacity
shared by active uploads and peer fetches. Encrypted container overhead counts
against staging: the default 1 GiB staging limit can prevent uploading a file
below the 4 GiB file-size limit. Increase staging capacity for larger uploads.

Go helpers pin a connection and stream without buffering the full file:

```go
info, err := db.UploadFile(ctx, "reports/a.bin", reader)
err = db.DownloadFile(ctx, info.Name, writer)
cursor, err := db.SearchFiles(ctx, "reports/%.bin") // or db.ListFiles(ctx)
if err != nil { /* handle error */ }
defer cursor.Close()
for cursor.Next(ctx) {
    file := cursor.File() // FileInfo{Name, Size, ModifiedMillis, SHA256, Local}
    _ = file
}
err = cursor.Err()
```

Close cursors promptly; an open cursor occupies a pooled connection. FileCursor
calls must be serialized. The helpers clean up handles after cancellation or
failure; a broken connection is cleaned up by the server. File backups still
require preserving the external object directory and its keys separately.

## One-way Low-to-High bridge

Requires enabled `replication` and `[replication.bridge]`. This transports
signed, compressed, recipient-encrypted bundles from a Low database domain
to a distinct High domain. Provision matching schemas/stable mappings and
trusted origin keys on the receiver. Copy completed `.bundle` artifacts from
the outbox to the inbox with your delivery mechanism. Bundles remain
recipient encrypted and signed. Bridge roles enforce the direction; this is
not bidirectional peer replication.

`config.example.toml` shows both roles. The Low node sets
`role = "low-exporter"` with its database `domain`, an outbox `directory`,
the shared `stream`, the High X25519 public `recipient`, and its Ed25519
`signer_hex`. The High node sets `role = "high-receiver"` with its own
`domain`, the Low database in `trusted_domains`, the Low Ed25519 public key
in `trusted_signers`/`trusted_origins`, an inbox `directory`, the same
`stream`, the matching `recipient`, and its X25519 `receiver_secret_hex`.
`domain` must equal each node's own `storage.database_id`.

High local edits establish ownership and shield those cells from later Low
updates, while Low shadow state is retained. Sequence checks and receipts
prevent replay; trust the source database, bundle signer, and embedded
origins. Default bundle limit is 16 MiB, decoded payload 64 MiB, staging
1 GiB. An export's retained-history pin can prevent log cleanup until
delivery is resolved.

Bridge capture/process operations run from engine configuration and are
not exposed over the wire protocol. Ownership release is:
`MURMUR BRIDGE RELEASE <table> '<key>' [COLUMN <column>]` drops the High
shield on one field (or, without `COLUMN`, the whole row) of a
single-TEXT-key row and returns one `released` row (`false` means the
engine was busy, retry). It needs a high-receiver; Low exporters fail with
`22023`. See [MURMUR BRIDGE
RELEASE](#murmur-bridge-release) for the full contract.

## Backups, restore, and key rotation

Use `murmur-admin` against a stopped server (coordinate with the store's
exclusive lock rather than opening a live store independently):

```sh
murmur-admin backup ./data ./backup-001 --key-hex="$MURMUR_KEY_HEX"
murmur-admin verify --backup --key-hex="$MURMUR_KEY_HEX" ./backup-001
murmur-admin restore --fresh-node-id --key-hex="$MURMUR_KEY_HEX" ./backup-001 ./seeded-peer
```

`backup` takes a consistent Spool checkpoint; later commits are not part of
that checkpoint. Same-filesystem checkpoints use hardlinks; cross-filesystem
backups copy through a temporary checkpoint. `verify --backup` checks
without modifying the backup. `restore` requires a new destination. Plain
restore preserves identities. A peer clone needs the same database ID and a
**fresh node ID**, matching its provisioned TLS identity; an independent
database clone can also choose a fresh database ID. Do not run two active
replicas with one node identity.

Backup rotation is deployment-managed: choose distinct checkpoint
directories and remove old ones according to your retention policy. Keep the
master key that can open each backup; later key rotations do not update old
backups.

Data-key rotation selects a new key for subsequent Spool writes. Master-key
rotation rewraps the key registry instead of rewriting every encrypted
block. Retired keys can remain necessary to read old blocks.
`data_key_max_age` optionally enables age-based rotation. Rotate the master
key over SQL (a comma between the keys is also accepted):

```sql
MURMUR ROTATE KEY 'old-passphrase', 'new-passphrase';
```

The previous key is verified first: a wrong key fails with `28000` and
nothing rotates. On success the old key stops working immediately for new
connections and restarts, while already-unlocked sessions keep serving;
point clients at the new key (the Go client opens a fresh handle). Keep the
master key that can open each backup: rotation does not update old backups.
`murmur-admin keys` inspects the key registry of a stopped store.

## Remote backup and restore

While `murmur-admin` manages local backup directories, SQL clients move
single-file archives through the `[backups]` folder:

```sql
MURMUR BACKUP SAVE;                          -- name, size
MURMUR BACKUP LIST;                          -- name, size, modified
MURMUR BACKUP DOWNLOAD 'murmur-20261016-120000.murmur-backup' OFFSET 0 LIMIT 1048576;
MURMUR BACKUP UPLOAD BEGIN 'copy.murmur-backup';
MURMUR BACKUP UPLOAD APPEND 'copy.murmur-backup' '<base64-chunk>';
MURMUR BACKUP UPLOAD FINISH 'copy.murmur-backup';  -- name, size
MURMUR RESTORE FROM 'murmur-20261016-120000.murmur-backup' KEY '<secret>';
```

`SAVE` checkpoints the live database (writes after the checkpoint are not
included) and packs it into `murmur-YYYYMMDD-HHMMSS.murmur-backup` in UTC;
same-second saves gain a `-2`, `-3`, ... suffix. The archive keeps the live
encryption key, so it restores with the same key. `LIST` shows every
regular, non-hidden file in the folder, sorted by name.

`DOWNLOAD` returns one row (`offset`, `total`, `chunk` as BLOB) per call;
loop with increasing offsets until `offset + len == total`. LIMIT defaults
to 1 MiB. `UPLOAD` streams base64 chunks into a hidden partial file that
`LIST` never shows; `FINISH` publishes it atomically and refuses to
overwrite an existing file (`23505`), while `ABORT` discards partial state.
The Go client wraps both loops (`BackupUpload`/`BackupDownload`).

`RESTORE` re-authenticates with the database key, then unpacks the archive,
restores into staging next to the live data dir, trial-opens it with the
running config, and only then closes the live database and swaps
directories. The pre-restore data dir is preserved as
`.<data>.replaced-<utc>` next to the data dir for the operator to inspect
or delete. Sessions stay unlocked and the server keeps serving; queries
in flight during the swap fail with `08003`. A wrong key (`28000`), a
non-archive file (`XX001`), or an incompatible backup all fail before the
live database is touched.

Backup rotation is deployment-managed: remove old archives according to
your retention policy. The server creates archives, upload staging files,
and restore staging files and directories owner-only (`0600` files,
`0700` directories on unix, set at creation regardless of umask); other
platforms use default permissions. Restrict the backup folder like the
data directory as defense in depth.


### Go client coverage

The [Go client feature catalog](../clients/go/README.md#features) covers the
server SQL/MURMUR interface, including managed search, files, replication,
retention, all five CRDT policy operations, bridge release and key rotation.
Server configuration and embedded callbacks/subscriptions are not wire APIs.

Run checked examples from `clients/go`:

```sh
go run ./examples/keys
go run ./examples/replication
```

The keys example rotates with `db.RotateKey(ctx, oldKey, newKey)`, rejects
the old key and verifies data through a fresh pool. Replace the pool immediately
after rotation so newly opened connections use the new key. No peers are needed.
The replication example provisions two scratch nodes and checks counter, set
add/remove, min and max operations; it requires a server built with
`--features replication`. Examples build that server automatically unless
`MURMUR_SERVER_BIN` is set. Backup downloads propagate writer errors and return
`io.ErrShortWrite` when a destination silently accepts only part of a chunk.

### Python client coverage

The [Python client catalog](../clients/python/README.md) includes typed helpers
for managed search, peer management, retention, CRDT policies and bridge release,
in addition to backups, files, key rotation and ordinary SQLite SQL.
For example, `db.set_column_vector("documents", "embedding",
murmur.VectorOptions(2, "l2"))` declares a vector index; call
`db.refresh_indexes()` before querying its discovered, SQL-quoted name.
Search requires one INTEGER application key and TEXT FTS/BLOB vector columns;
vector support requires the server's default `vector` feature.

Run checked examples from `clients/python` after installing `.[test]`:

```sh
python -m examples.search
python -m examples.json
python -m examples.keys
python -m examples.replication
python -m examples.bridge
```

Replication/bridge examples need the `replication` Cargo feature and provision
temporary credentials; a prebuilt `MURMUR_SERVER_BIN` must include it.
Policy operations need matching merge policies and single-TEXT-key rows.
Bridge release needs a high-receiver; `False` means busy and requires retry.
Nullable status fields remain `None`. Downloads handle partial binary writes
and raise `BlockingIOError` on zero/None progress; use blocking binary streams.
Connections are not thread-safe. Embedded subscriptions and callbacks are not
wire APIs. See the client catalog for the complete restrictions.

## Progress, metrics, and writer scheduling

Startup reconstructs authoritative rows from Spool and reports progress
phases (`Opening`, `Rebuilding`, `Indexing`, `Finalizing`, `Ready`,
`Failed`, `Cancelled`) in the server log. Startup authenticates Spool's
committed groups and loads current records through its byte-bounded decoded
block cache, reuses an insert statement per table, then builds indexes and
runs an integrity check. It uses one in-memory SQLite connection;
`storage_workers` does not parallelize SQLite reconstruction.

`murmur-admin status` reports committed/durable cursors, storage health, and
SQLite/Spool metrics for a stopped store. Live servers expose metrics over the
wire via `MURMUR SHOW METRICS` (or scoped to `MURMUR SHOW METRICS SQLITE` and
`MURMUR SHOW METRICS SPOOL`), returning `subsystem`, `metric`, and `value` rows:

```sql
MURMUR SHOW METRICS;
-- subsystem | metric              | value
-- sqlite    | memory_used_bytes   | 1048576
-- sqlite    | database_size_bytes | 2097152
-- spool     | disk_bytes          | 65536
-- spool     | compression_ratio   | 2.4500
```

Health and batch activity are also streamed to server logs and VictoriaLogs.

Writer admission defaults to 90% local and 10% remote service, with bounded
debt and a maintenance reservation every 100 grants (see
`[engine.scheduler]`). Idle classes can lend capacity. The scheduler cannot
preempt a running statement and shares are not a latency guarantee.

## Administration CLI

Build with `cargo build --bin murmur-admin`. Commands accept a storage
directory, not a `.sqlite` file. Provide the store key explicitly for real
storage:

```sh
./target/debug/murmur-admin status --key-hex="$MURMUR_KEY_HEX" ./data
./target/debug/murmur-admin inspect --json --key-hex="$MURMUR_KEY_HEX" ./data
./target/debug/murmur-admin keys --key-hex="$MURMUR_KEY_HEX" ./data
./target/debug/murmur-admin verify --key-hex="$MURMUR_KEY_HEX" ./data
./target/debug/murmur-admin backup --key-hex="$MURMUR_KEY_HEX" ./data ./backup-002
./target/debug/murmur-admin verify --backup --key-hex="$MURMUR_KEY_HEX" ./backup-002
./target/debug/murmur-admin restore --fresh-node-id --key-hex="$MURMUR_KEY_HEX" ./backup-002 ./restored-db
./target/debug/murmur-admin hash-password --password s3cret
./target/debug/murmur-admin --help
./target/debug/murmur-admin --version
```

`status` reports cursors/health; `inspect` summarizes schema and storage;
`keys` shows key inventory; `verify` validates recovery; `backup` creates a
checkpoint; `restore` recreates storage; `hash-password` prints an Argon2
hash for `[[auth.users]]` (reads stdin without `--password`). `--json`
enables machine-readable output. Restore supports
`--fresh-node-id[=<32 hex characters>]` and
`--fresh-db-id[=<32 hex characters>]` for generated or explicit identities.

`--passphrase=...` is also accepted and derives a key with BLAKE3; this is
not a password-hardening KDF. With neither key option, the CLI uses a public
demo key. CLI opens do not supply node-local network configuration.

Windows skips directory fsync for Spool, file objects, and bridge/backup
directories; file contents are synchronized, while directory-entry crash
durability follows the underlying platform.

## Deployment limits

The whole active database must fit available RAM. SQLite work is serialized;
long queries, schema scans, or rebuilds can delay other callers. Background
persistence and networking cannot make SQL parallel or preempt statements.
There is no automatic embedding service, distributed SQL transaction,
all-peer commit barrier, or arbitrary virtual-table replication. Managed
FTS5 and vector indexes are configurable through column schema settings.
Reactive subscriptions,
offline state exchange, and low-level
session helpers remain engine APIs without a wire surface in this release.
Backups, object directories, keys, certificate provisioning, peer trust, and
application-level retry/business rules remain deployment responsibilities.

