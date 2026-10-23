# MURMUR-SQL for Go

Native Go implementation of the Murmur embedded in-memory database engine and PostgreSQL-wire server.
Go 1.24+ and a C compiler are required for the engine's bundled SQLite and zstd;
no external Rust toolchain or C libraries are required.

The repository provides both:
1. **Embedded Database Library:** Direct Go import via [`murmur`](murmur/) for in-process encrypted persistence, masterless peer-to-peer replication, full-text search, and migrations:
   ```go
   import "github.com/marcgauthier/murmur-sql-go/murmur"
   ```
2. **PostgreSQL-Wire Server & Admin:** Production binaries for PostgreSQL wire protocol framing, YAML configuration, password authentication, and service deployment:
   ```sh
   go build -o bin/ ./cmd/murmur-server ./cmd/murmur-admin
   bin/murmur-server --validate-config --config config.example.yaml
   bin/murmur-admin hash-password --password example-password
   bin/murmur-server --config config.yaml
   ```

Copy `config.example.yaml` to `config.yaml` and add an `auth.users` entry
with the generated password hash before connecting. Each PostgreSQL connection
must issue `MURMUR UNLOCK KEY 'secret'` before application SQL. Statements commit
in memory; `MURMUR FLUSH` waits for the engine's durability boundary.

The standard library and Go project packages are preferred. The only independent
third-party dependency is YAML parsing (`gopkg.in/yaml.v3` v3.0.1) for server
configuration, with database passphrase derivation using standard library `crypto/sha256`.
Low-level SQLite and zstd implementations are vendored directly in `murmur/third_party/`.

- [Usage and operational behavior](docs/USAGE.md)
- [Engine adapter and ownership](docs/ENGINE.md)
- [Testing instructions and current verification status](docs/TESTING.md)
- [Parity checklist](docs/PARITY.md)
- [Dependencies](VENDORS.md)
