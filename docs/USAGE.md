# Usage

## Current availability

Both binaries select `engine/native.Factory`, which uses the public
MURMUR-GO APIs. The adapter covers database creation/opening, query
preparation/execution, migrations, configuration callbacks, diagnostics,
peer administration, policies, search, file transfers, backups/restores,
key rotation, and offline administration. Coverage comes from the Go unit
suites (`go test -race ./...`), the multi-node fault-injection scenarios
under `murmur/tests-live/` (see `murmur/tests-live/run.sh`), and the
Go/Python/Rust client suites.

The embedded engine and vendored C dependencies (SQLite amalgamation and
DataDog/Meta zstd) are fully contained within this repository under
`murmur/` (including `murmur/third_party/`). A C compiler and cgo are
required for the engine, in addition to Go 1.24+.

## Build and launch

```sh
go build -o bin/ ./cmd/murmur-server ./cmd/murmur-admin
cp config.example.yaml config.yaml
bin/murmur-admin hash-password --password example-password
# Add the generated PHC hash to auth.users in config.yaml.
bin/murmur-server --validate-config --config config.yaml
bin/murmur-server --config config.yaml
```

`--validate-config` validates configuration, server TLS certificates, and
configured replication credential files without opening storage. The native factory also maps and validates engine-owned limits without opening
storage. Node identity/signing bindings are enforced when the engine opens. Server
exit codes are 0 for success, 1 for configuration/runtime errors, and 2 for bind
failure. `-h`/`--help` and `-v`/`--version` work on both binaries.

No users are enabled by default:

```yaml
auth:
  users:
    - name: "admin"
      password_hash: "$argon2id$v=19$m=19456,t=2,p=1$..."
```

Password authentication uses PostgreSQL's cleartext password exchange inside TLS
when enabled. Argon2id and Argon2i PHC version 19 hashes are supported. Resource
bounds are 256 MiB memory, 16 iterations, and 32 lanes; hashes exceeding these
bounds fail authentication. Generated hashes use the Rust defaults: Argon2id,
19456 KiB, 2 iterations, 1 lane, a random 16-byte salt, and a 32-byte output.

Development mode permits plaintext. Production requires TLS with verified client
certificates. `server.tls.ca_file` is rejected outside production, matching the
Rust configuration contract. TLS 1.2 is the minimum supported version.

## Configuration

[config.example.yaml](../config.example.yaml) carries the configuration
schema and defaults. Unknown keys, including incorrectly cased keys, are
rejected. Section defaults also apply to optional file and bridge sections.

Durations accept integer seconds or integer unit strings such as `500ms`, `1s`,
`5m`, `1h`, and `7d`. Byte sizes accept integers or strings such as `64KB`,
`64KiB`, and `256MiB`. SI suffixes use powers of 1000; IEC suffixes use 1024.
Overflow, negative values, and fractional quantities are rejected.

`config.yaml` owns the complete table-replication map, keyed by stable table ID.
Missing entries mean local-only. Engine callbacks must persist the map before
publishing a change. Updates use a same-directory temporary file, file sync,
rename, and directory sync on Unix. The YAML AST node update preserves unrelated
formatting and comments.

## Embedded Go library

The engine also embeds directly in Go programs with no server process:

```go
import "github.com/marcgauthier/murmur-sql/murmur"
```

See [murmur/README.md](../murmur/README.md) for the embedded guide (open,
migrations, transactions, replication, search, backup) and
[example_test.go](../murmur/example_test.go) for runnable snippets:

```sh
go test -run Example ./murmur/
```

## SQL and wire interface

Connect with a PostgreSQL client and use the SQLite SQL dialect:

```sql
MURMUR UNLOCK KEY 'database-encryption-key';
CREATE TABLE greetings(id INTEGER PRIMARY KEY, text TEXT);
INSERT INTO greetings VALUES (1, 'hello');
SELECT text FROM greetings WHERE id = 1;
MURMUR FLUSH;
```

Each physical connection starts locked, even when another connection has opened
the database. A 64-character hexadecimal unlock secret is a raw 32-byte key;
other secrets are Argon2id passphrases on new stores. Stores created before
passphrase support keep the legacy SHA-256 mapping; rotating to a passphrase
(`MURMUR ROTATE KEY '<old>' '<new>'`) migrates the envelope to Argon2id, after
which the legacy mapping is rejected. Secrets are not written to server logs.

`$1`, `$2`, etc. are rewritten to SQLite `?1`, `?2`, etc. outside quotes and
comments. Parameter values are passed separately, never interpolated. Simple
queries accept multiple statements. Extended queries accept one statement and
support named/unnamed statements, portals, Describe, Close, Sync, Flush, and
bounded Execute paging. Errors in extended execution discard messages until Sync.
Cancellation requests require the connection's backend PID and secret. The adapter
checks cancellation before engine admission and between row steps; the current
engine API does not expose interruption of an already-running SQLite step.

Each engine statement autocommits. Bare `BEGIN`, `COMMIT`, `ROLLBACK`, their
`TRANSACTION`/`WORK` forms, `START TRANSACTION`, and `END` are compatibility no-ops;
they do not establish a multi-statement transaction. DDL routes to engine migrations.
`SELECT version()` and `SHOW server_version` return the server version.

Binary parameters support booleans, integers, floats, BYTEA, DATE, TIMESTAMP,
TIMESTAMPTZ, and UUID. Dates/timestamps/UUIDs become SQLite text values. Binary
NUMERIC is rejected with `42846`; send text. SQLite result columns use TEXT,
INT8, FLOAT8, BYTEA, or BOOL, with Describe and Execute required to agree.

Messages are limited to 32 MiB. Sessions permit at most 1024 prepared statements,
1024 portals, and a 64 MiB accounting budget for SQL, bindings, and buffered
results. Engine results are buffered. The engine must enforce its own admission
and result budgets before allocating large query results.

## MURMUR commands

These command families are parsed and executed through the native adapter:

| Family | Examples |
| --- | --- |
| Unlock/durability | `MURMUR UNLOCK KEY 'secret'`, `MURMUR FLUSH` |
| Table flags | `MURMUR SET TABLE orders REPLICATED true`, `MURMUR SHOW TABLES REPLICATION` |
| Search | `MURMUR SET COLUMN notes body FTS true`, `MURMUR REFRESH INDEXES` |
| Vectors | `MURMUR SET COLUMN items embedding VECTOR true DIMENSIONS 3 DISTANCE cosine` |
| Search status | `MURMUR SHOW COLUMNS FTS`, `MURMUR SHOW COLUMNS VECTOR` |
| Diagnostics | `MURMUR SHOW STATUS`, `MURMUR SHOW METRICS SQLITE` |
| Peer management | `MURMUR PEER ADD '127.0.0.1:9002'`, `MURMUR PEER REMOVE 'node-id'`, `MURMUR PEER RETIRE 'node-id'`, `MURMUR PEER SYNC` |
| Replication state | `MURMUR SHOW PEERS`, `MURMUR SHOW REPLICATION`, `MURMUR SHOW RETENTION`, `MURMUR RETENTION COLLECT` |
| Read-your-write | `MURMUR SHOW HEAD`, `MURMUR WAIT FOR 'origin' 42`, `MURMUR WAIT FOR LAG 0` |
| Merge policies | `MURMUR POLICY COUNTER_ADD counters value 'key' 1`, `MURMUR POLICY SET_ADD tags values 'key' 'tag'` |
| Extrema | `MURMUR POLICY MIN readings value 'key' 3`, `MURMUR POLICY MAX readings value 'key' 9` |
| Bridge ownership | `MURMUR BRIDGE RELEASE items 'key' COLUMN name` |
| Key rotation | `MURMUR ROTATE KEY 'previous' 'new'` |
| Backups | `MURMUR BACKUP SAVE`, `MURMUR BACKUP LIST`, `MURMUR RESTORE FROM 'name.murmur-backup' KEY 'secret'` |

Vector dimensions are 1–8192, with `l2`, `cosine`, or `l1` distances. Flags accept
true/false, on/off, yes/no, or 1/0. Non-file administration commands do not accept
bound parameters; quote literal secrets/names with SQL single-quote escaping.

## Replication lag

`MURMUR SHOW PEERS` reports one row per connected peer. The lag columns
read as follows:

- `lag_txns`: worst per-origin applied deficit against this node's heads —
  how many transactions the peer still needs to catch up. Peers that never
  reported progress count as fully behind.
- `origins_behind`: how many origins lag at all.
- `last_progress_millis_ago`: age of the peer's last progress report
  (NULL when none arrived yet).
- `queue_messages` / `queue_available_bytes`: outbound queue depth and
  remaining byte budget toward that peer.
- `member_state`: SWIM membership view of the peer
  (`alive`, `suspect`, `dead`, `left`, `unknown`).

Suggested SLO: warn when any peer exceeds 30 seconds without progress or
10,000 transactions behind. Act before the peer stays offline past
`maximum_offline`: retention collection protects connected peers, but a
peer offline past that bound loses its retained tail and must resync
from a full snapshot.

## Read-your-write

Replication is asynchronous: a write flushed on one node is not instantly
readable on another. The read-your-write recipe closes the gap:

```sql
-- on the writer, after MURMUR FLUSH:
MURMUR SHOW HEAD;            -- origin, applied per origin; note yours
-- on the reader:
MURMUR WAIT FOR 'writer-origin-hex' 42;  -- blocks until applied >= 42
SELECT ...;                  -- the write is visible
```

`MURMUR WAIT FOR LAG 0` instead waits until this node is within the
bound of everything its peers observed (bounded staleness with no
cursor to carry). Both accept `TIMEOUT <seconds>` (default 30);
expiry reports `57014`. Waiting for a retired origin fails fast with
`22023` instead of hanging. Heads resolve within about a second, so
sub-second timeouts may expire on a healthy cluster; prefer generous
timeouts and let the wait return early.

## Files over PostgreSQL SQL

File commands accept quoted text or bound parameters, including binary BYTEA:

```sql
MURMUR FILE LIST;
MURMUR FILE NEXT 'cursor' LIMIT 256;
MURMUR FILE SEARCH '%report%';
MURMUR FILE UPLOAD BEGIN 'report.pdf';
MURMUR FILE UPLOAD APPEND $1 $2;
MURMUR FILE UPLOAD FINISH 'upload-handle';
MURMUR FILE DOWNLOAD 'report.pdf';
MURMUR FILE DOWNLOAD NEXT 'download-handle' LIMIT 65536;
MURMUR FILE CLOSE 'handle';
```

`UPLOAD ABORT` is also supported. File pages are limited to 1000 rows; download
chunks are limited to 1 MiB. The native adapter implements connection ownership, five-minute idle expiry,
16 handles per connection, 1024 globally, 16 MiB cursor budgets per connection,
and a 128 MiB global cursor budget. The engine owns encrypted staging budgets.
See the [engine adapter](ENGINE.md).

## Error codes

Engine failures surface as PostgreSQL `SQLSTATE` codes. The embedded
`murmur.Error` kind maps as follows:

| Code | Meaning | Typical cause |
| --- | --- | --- |
| `02000` | No data | Policy operation on a missing row |
| `08003` | Connection does not exist | Database closed |
| `08006` | Connection failure | Replication transport error |
| `22023` | Invalid parameter value | Bad argument, malformed address or command |
| `23000` | Integrity constraint violation | Generic constraint failure |
| `23502` | Not null violation | NULL in a `NOT NULL` column |
| `23503` | Foreign key violation | Dangling reference |
| `23505` | Unique violation | Duplicate key, or file already exists |
| `23514` | Check violation | Failing `CHECK` constraint |
| `28000` | Invalid authorization | Wrong data key, replication auth failure |
| `40001` | Serialization failure | Database busy, retry |
| `42501` | Insufficient privilege | Operation not authorized |
| `42601` | Syntax error | Malformed SQL |
| `42804` | Datatype mismatch | Incompatible column types |
| `42846` | Cannot coerce | Binary NUMERIC parameter (send text) |
| `42703` | Undefined column | Unknown column name |
| `42P01` | Undefined table | Unknown table name |
| `42883` | Undefined function | Unknown function |
| `53400` | Limit exceeded | Engine overloaded, back off and retry |
| `54000` | Program limit exceeded | Engine-side limit hit |
| `55000` | Object not in prerequisite state | Schema mismatch, unavailable object |
| `55P03` | Lock not available | Lock acquisition failed |
| `57014` | Query canceled | Client cancellation or deadline |
| `58000` | System error | Storage or IO failure |
| `58P01` | Undefined file | Backup or file not found |
| `XX001` | Data corrupted | Checksum or integrity failure |
| `XX000` | Internal error | Unclassified engine failure |

## Backup transfers

The shell implements Rust-compatible `MMBK01\n` archives with big-endian 64-bit
name/content lengths. Files are streamed. Extraction rejects traversal,
absolute paths, backslashes, duplicate entries, and symlink escape paths.
Archives and staging files use private permissions on Unix.

```sql
MURMUR BACKUP UPLOAD BEGIN 'saved.murmur-backup';
MURMUR BACKUP UPLOAD APPEND 'saved.murmur-backup' '<base64-chunk>';
MURMUR BACKUP UPLOAD FINISH 'saved.murmur-backup';
MURMUR BACKUP DOWNLOAD 'saved.murmur-backup' OFFSET 0 LIMIT 1048576;
```

`UPLOAD ABORT` removes a partial upload. FINISH refuses overwrites. Download
returns offset, total size, and BYTEA data; its limit defaults to 1 MiB and cannot
exceed 16 MiB. Every transfer requires an unlocked connection. Live restore
verifies the current key before unpacking and delegates the database swap to the
host adapter. It preserves session unlock state; old file handles must expire.

## Offline admin

```sh
bin/murmur-admin status ./data --json --passphrase example
bin/murmur-admin verify ./data --backup --key-hex <64-hex>
bin/murmur-admin restore ./backup ./restored --fresh-node-id --passphrase example
```

Set exactly one of `--key-hex` and `--passphrase`. A 64-character
hexadecimal `--passphrase` is a raw key, matching SQL; other passphrases
take the store's envelope KDF (Argon2id, or the legacy SHA-256 mapping on
pre-migration stores). Without either option, the Rust development key is
selected and a message is printed to stderr. `--fresh-node-id[=<32-hex>]`
and `--fresh-db-id[=<32-hex>]` assign explicit or random identities.
`hash-password` reads one line from stdin when `--password` is omitted.

`status`, `inspect`, `verify`, `backup`, `restore`, `keys`, and `rekey` use
the native engine. Run offline administration against a stopped server's
data directory. `rekey` re-protects the store under `--new-key-hex` or
`--new-passphrase` and reports the resulting envelope KDF:

```sh
bin/murmur-admin rekey ./data --passphrase old-secret --new-passphrase new-secret
```

Migration notes: databases created through SQL before passphrase support
open with the same secret (legacy mapping) and migrate on the first
rotation to a passphrase, whether via `MURMUR ROTATE KEY` or `rekey`. A
pre-migration store whose `--passphrase` was exactly 64 hex characters was
protected by the SHA-256 of that string; open it once with
`--key-hex <sha256-of-that-string>` and `rekey` it. The sibling engine's
standalone admin CLI uses a different passphrase convention; use this
repository's admin binary or an explicit raw key when working with
databases created through SQL.

## Logs and services

Logs use `log/slog` JSON in `<prefix>.YYYY-MM-DD` files with UTC daily rotation.
Retention deletes only matching dated regular files; zero days retains all logs.
Optional console output writes to stderr. VictoriaLogs uses Loki JSON pushes to
`<url>/loki/api/v1/push`, a bounded 1000-line queue, batches of 100 or every two
seconds, and optional Basic/Bearer [REDACTED] Remote delivery is best effort;
local file logs remain primary.

From an elevated console, run `murmur-server /install` or `/uninstall` alone.
Installers prompt for directories and listener, preserve configuration on upgrade,
copy binaries, configure automatic startup/restart, and restrict runtime access.
Fresh installations require empty, separate installation/runtime directories.
Writable paths must be absolute subdirectories of the runtime directory, separate
from its configuration directory. Installation paths cannot contain symlinks.
Run upgrades from a separate distribution directory, with `murmur-admin` beside
the server when it should also be installed. A local deployment transcript is saved.

Linux uses `murmur-sql.service`, user `murmur`, `/opt/murmur-sql`, and
`/var/lib/murmur-sql` by default. Windows uses SCM service `MurmurSQL` under its
virtual service account, with Program Files/ProgramData defaults. Uninstall asks
separately before deleting binaries and configuration/data. macOS supports the
server but has no service installer. Actual systemd/SCM deployment has not been
exercised for the native integration. The engine must be able to create and
rename its database directory, so the installer grants directory-entry access
to the configured storage parent. Previous cross-builds covered the shell only.

MURMUR-GO now uses QUIC replication with mutual TLS. The earlier TLS/TCP port
notes are obsolete. Configuration keys and UDP peer listener semantics follow
the Rust design. Cross-language peer compatibility has not been validated here.
