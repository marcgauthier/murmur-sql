# Language clients

Client implementations live under `clients/<language>` so additional languages
can be added without changing the Rust database layout.

| Language | Client | Examples |
|---|---|---|
| Go | [Pure-Go `database/sql` client](go/README.md) | [Runnable feature examples](go/examples/README.md) |
| Python | [Pure-Python client (pg8000)](python/README.md) | [Runnable feature examples](python/examples/coverage.json) |
| Rust | [Pure-Rust async client (tokio-postgres)](rust/README.md) | [Runnable feature examples](rust/examples/coverage.json) |

The Go module is `github.com/marcgauthier/murmur-sql/clients/go`. It is a pure-Go wire client with no native binding; application SQL,
persistence, replication, and wire formats remain implemented by the server.
Go `Open` returns a standard `*sql.DB`; application and administrative commands
use SQL directly, including file transfers.
