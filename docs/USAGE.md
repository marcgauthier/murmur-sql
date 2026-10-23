# Usage

## Current availability

Both binaries select `engine/native.Factory`, which uses the public MURMUR-GO
APIs. The adapter covers database creation/opening, query preparation/execution,
migrations, configuration callbacks, diagnostics, peer administration, policies,
search, file transfers, backups/restores, key rotation, and offline administration.
The integration is source-ported and untested. No builds or tests were run for
these changes; earlier protocol-fixture results do not validate the native engine.

The embedded engine and vendored C dependencies (SQLite amalgamation and DataDog/Meta zstd) are fully contained within this repository under `murmur/` (including `murmur/third_party/`). A C compiler and cgo are required for the engine, in addition to Go 1.24+.

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
other secrets are SHA-256 passphrases. Secrets are not written to server logs.

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

## Administration commands

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
| Merge policies | `MURMUR POLICY COUNTER_ADD counters value 'key' 1`, `MURMUR POLICY SET_ADD tags values 'key' 'tag'` |
| Extrema | `MURMUR POLICY MIN readings value 'key' 3`, `MURMUR POLICY MAX readings value 'key' 9` |
| Bridge ownership | `MURMUR BRIDGE RELEASE items 'key' COLUMN name` |
| Key rotation | `MURMUR ROTATE KEY 'previous' 'new'` |
| Backups | `MURMUR BACKUP SAVE`, `MURMUR BACKUP LIST`, `MURMUR RESTORE FROM 'name.murmur-backup' KEY 'secret'` |

Vector dimensions are 1–8192, with `l2`, `cosine`, or `l1` distances. Flags accept
true/false, on/off, yes/no, or 1/0. Non-file administration commands do not accept
bound parameters; quote literal secrets/names with SQL single-quote escaping.

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

`--key-hex` takes precedence over `--passphrase`. Admin passphrases are always
SHA-256 hashed, including strings containing exactly 64 hexadecimal characters.
Without either option, the Rust development key is selected and a message is
printed to stderr. `--fresh-node-id[=<32-hex>]` and
`--fresh-db-id[=<32-hex>]` assign explicit or random identities.
`hash-password` reads one line from stdin when `--password` is omitted.

`status`, `inspect`, `verify`, `backup`, `restore`, and `keys` use the native
engine. Run offline administration against a stopped server's data directory.
SQL passphrases use SHA-256 key derivation; the sibling engine's standalone
admin CLI uses a different passphrase convention. Use this repository's admin
binary or an explicit raw key when working with databases created through SQL.

## Logs and services

Logs use `log/slog` JSON in `<prefix>.YYYY-MM-DD` files with UTC daily rotation.
Retention deletes only matching dated regular files; zero days retains all logs.
Optional console output writes to stderr. VictoriaLogs uses Loki JSON pushes to
`<url>/loki/api/v1/push`, a bounded 1000-line queue, batches of 100 or every two
seconds, and optional Basic/Bearer authentication. Remote delivery is best effort;
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
