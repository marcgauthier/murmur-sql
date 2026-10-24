> Go-port status: the production server now uses MURMUR-GO through the native adapter. This integration is source-ported and has not been built or tested. See [port status](../../docs/PARITY.md).

# Murmur Go client

A pure-Go PostgreSQL-wire connection package using the `github.com/marcgauthier/pq` fork. `murmur.Open` returns
an ordinary `*sql.DB` and unlocks every physical pooled connection before use.
Use standard `database/sql` methods for application SQL and `MURMUR ...`
commands. There are no command helpers, custom database wrapper, or public
result structs. SQL is the SQLite dialect; value parameters use `$N`.

The fork is pinned with a `go.mod` replacement because it still declares the
`github.com/lib/pq` module path. Applications consuming this client must also
include this replacement in their own module; Go does not inherit replacements
from dependencies:

```go
replace github.com/lib/pq => github.com/marcgauthier/pq v0.0.0-20260820222753-451ac10e4e85
```

## Quickstart

Configure and start the server, then create the `greetings` table using the
[root SQL quickstart](../../README.md#quickstart). Run this program with the
same login and encryption key:

```go
package main

import (
    "context"
    "fmt"
    "log"

    murmur "github.com/marcgauthier/murmur-sql/clients/go"
)

func main() {
    ctx := context.Background()
    db, err := murmur.Open(ctx, murmur.Config{
        Host:     "127.0.0.1",
        Port:     5432,
        User:     "admin",
        Password: "s3cret",
        Key:      "database-encryption-key",
    })
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    var greeting string
    err = db.QueryRowContext(ctx,
        "SELECT text FROM greetings WHERE lang = $1", "en").Scan(&greeting)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println(greeting)
}
```

## Connection and pooling

`Config` takes the server address, an `auth.users` login from `config.yaml`,
and the database key (64 hex characters or a passphrase). Each physical
connection executes `MURMUR UNLOCK` before first use; a wrong key fails `Open`
with SQLSTATE `28000`. Host defaults to `127.0.0.1`, port to `5432`, database
name to `murmur`, and connect timeout to 10 seconds. The server accepts but
ignores the database name. User, password, and key are required.

Tune the returned pool with standard `SetMaxOpenConns` and `SetMaxIdleConns`,
or the corresponding `Config` fields. A single manual unlock on `*sql.DB`
would not unlock every connection; the connection hook in `Open` handles that.

`Config.Standbys` lists failover servers, tried in order after `Host`/`Port`:

```go
murmur.Config{
    // Host, User, Password, Key, ...
    Standbys: []murmur.HostPort{{Host: "db2", Port: 5432}},
}
```

Transport failures (refused/reset connections, EOF, class `08` and `57P0x`
errors) move to the next healthy host and black the failed host out with
exponential backoff (200ms doubling, 5s cap); the pool returns to earlier
hosts as they recover. Auth failures never fail over. Retried statements
are at-least-once: an unacknowledged write may already have executed, so
use idempotent statements or fixed primary keys and treat `23505` as
acknowledged. In-flight transactions cannot migrate hosts: a failure
inside `Tx` aborts that transaction, and the next `Begin` lands on a
healthy host.

TLS mirrors PostgreSQL `sslmode`:

```go
murmur.Config{
    // Host, User, Password, Key, ...
    TLS: murmur.TLSConfig{
        Mode: murmur.TLSVerifyFull,
        CAFile: "/etc/murmur/ca.crt",
        CertFile: "/etc/murmur/client.crt",
        KeyFile: "/etc/murmur/client.key",
    },
}
```

The default `TLSDisable` sends passwords in cleartext. Production servers
require TLS with a client certificate. Use verified TLS outside local development.

## SQL and administration

Use `ExecContext`, `QueryContext`, `QueryRowContext`, and `PrepareContext`
directly. Statements autocommit individually. `BEGIN`, `COMMIT`, and `ROLLBACK`
are server no-ops; `database/sql` transactions do not provide multi-statement
atomicity or rollback. Successful writes acknowledge RAM commits. Execute
`MURMUR FLUSH` when local disk durability is required.

```go
_, err := db.ExecContext(ctx, "MURMUR FLUSH")
if err != nil {
    return err
}
_, err = db.ExecContext(ctx, "MURMUR SET TABLE scratch REPLICATED false")
if err != nil {
    return err
}
rows, err := db.QueryContext(ctx, "MURMUR SHOW TABLES REPLICATION")
if err != nil {
    return err
}
defer rows.Close()
for rows.Next() {
    var table string
    var replicated bool
    if err := rows.Scan(&table, &replicated); err != nil {
        return err
    }
    fmt.Printf("%s replicated=%v\n", table, replicated)
}
return rows.Err()
```

Commands returning rows use `QueryContext` or `QueryRowContext`; scan results
into application-owned variables. Use `sql.NullString`, `sql.NullInt64`, or
pointers for nullable columns. `Rows.Scan` can convert the `true`/`false` text
returned by administrative commands into Go booleans.

Ordinary SQL supports bound values; most `MURMUR` commands require literal
arguments. File commands explicitly support parameters. For literal command
arguments, double embedded single quotes; for SQL identifiers, double embedded
double quotes. Placeholders cannot substitute identifiers or command keywords.

## Command examples

Execute these statements using `database/sql`; server setup and restrictions
are in the [SQL reference](../../docs/USAGE.md#murmur-commands).

```sql
MURMUR SHOW STATUS;
MURMUR SHOW METRICS;
MURMUR SHOW METRICS SQLITE;
MURMUR BACKUP SAVE;
MURMUR BACKUP LIST;
MURMUR RESTORE FROM 'saved.murmur-backup' KEY 'database-key';
MURMUR ROTATE KEY 'previous-key' 'new-key';

MURMUR SET COLUMN documents body FTS true;
MURMUR SHOW COLUMNS FTS;
MURMUR SET COLUMN documents embedding VECTOR true DIMENSIONS 384 DISTANCE cosine;
MURMUR SHOW COLUMNS VECTOR;
MURMUR REFRESH INDEXES;

MURMUR PEER ADD '203.0.113.11:9001';
MURMUR PEER SYNC;
MURMUR SHOW PEERS;
MURMUR SHOW REPLICATION;
MURMUR SHOW RETENTION;
MURMUR RETENTION COLLECT;
MURMUR POLICY COUNTER_ADD stats count 'row-1' 5;
MURMUR POLICY SET_ADD stats tags 'row-1' 'blue';
MURMUR POLICY SET_REMOVE stats tags 'row-1' 'blue';
MURMUR POLICY MIN stats lowest 'row-1' -1.5;
MURMUR POLICY MAX stats highest 'row-1' 42;
MURMUR BRIDGE RELEASE notes 'row-1' COLUMN title;
```

Tables are local-only by default. Enabling replication rebuilds a table,
converts its application key into a unique index, and adds the read-only
UUIDv5 `id_murmur_primary_key` column. Supply all application-key components
on inserts, and name columns explicitly when table modes can change.

FTS needs `TEXT` columns and one `INTEGER` application primary key; vectors
need `BLOB` columns, that same key shape, and 1–8192 dimensions. Embeddings
are application-supplied finite little-endian float32 bytes. Discover and
quote the search index name from the SHOW results. Index updates are eventual;
`MURMUR REFRESH INDEXES` waits for local maintenance, while `MURMUR FLUSH`
waits for persistence. Managed migrations reject JSON expression indexes,
`CHECK` constraints, and generated columns. JSON conflicts resolve per column.

Peer/retention/policy commands require a replication-enabled server and node
credentials. Policy operations require a configured matching merge policy and
one TEXT application key. Bridge release additionally needs a high-receiver
bridge. These commands do not provide an all-peer durability barrier.
After key rotation, close the old pool and open a new one with the new key.

## File and backup transfers

Transfers also use SQL directly; no file API is needed. The checked
[files example](examples/files/main.go) demonstrates the complete protocol:

1. Pin a physical connection with `db.Conn(ctx)` and defer `conn.Close()`.
2. Begin an upload, append BYTEA chunks, then finish and scan metadata.
   Abort unfinished uploads using a cleanup context if the operation fails.
3. Open a list/search cursor, fetch pages, and explicitly close the handle.
4. Open a download, read chunks until EOF, and explicitly close the handle.

Use the same `*sql.Conn` throughout each handle's lifetime. Returning a
connection to the pool does not close server-side handles. Close or abort
handles before releasing the connection; after cancellation, use a bounded
cleanup context independent of the canceled request. Physical disconnection
also releases handles. SQL rollback does not undo file operations.

Files require the `replication` feature, enabled replication credentials, and
`[replication.files]`; no peers are required for local use. Handles expire
after five idle minutes. Uploads replace atomically at finish. Downloads pin
one authenticated version. Default staging is 1 GiB including encryption
overhead. Preserve object directories and file keys separately from database
backups. See [file limits](../../docs/USAGE.md#files-over-postgresql-sql).

The [backup example](examples/backup/main.go) shows literal byte ranges for
`MURMUR BACKUP DOWNLOAD` and base64 chunks for `MURMUR BACKUP UPLOAD`.
Application code controls streaming, cancellation, and destination errors.

## Errors

Server-reported errors from `Open` and its returned `*sql.DB` pool automatically
use `*murmur.Error`. Inspect them with standard `errors.As`:

```go
var serverErr *murmur.Error
if errors.As(err, &serverErr) && serverErr.Code == "28000" {
    // Authentication, unlock, or TLS rejection; inspect serverErr.Message.
}
```

Import `errors` and the MURMUR-SQL client as `murmur`. The independent error
type exposes server diagnostics and `SQLState()`; fields may be empty when not
supplied by the server. `Error()` preserves the original text and connection or
unlock context, and `Unwrap()` preserves the cause. Concrete underlying driver
types are not a compatibility guarantee. The `github.com/marcgauthier/pq` fork
remains an internal dependency;
applications do not need to import it. Validation, network, TLS, cancellation,
and other non-server errors retain their existing types and do not receive
fabricated SQLSTATE codes. See the
[SQLSTATE reference](../../docs/USAGE.md#error-codes).

## Features

| Feature            | Examples                  |
|--------------------|---------------------------|
| Connect and unlock | basic                     |
| Server error handling | keys, replication, bridge |
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

The feature catalog maps SQL examples to server capabilities; it is not an
exhaustive test of every SQL form or command argument. Server configuration,
recovery tooling, and embedded callbacks/subscriptions remain outside the
SQL interface. Browse the [runnable examples](examples/README.md); each
creates a scratch server and checks its results.

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
