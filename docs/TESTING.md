# Verification status and testing instructions

The native engine integration was ported without running builds, tests, vet,
fuzzers, or live servers, at the user's request. Prior shell-only checks used
fixed-response fixtures and do not establish that the new adapter compiles,
links, or behaves correctly with MURMUR-GO.

## Commands for a later validation pass

Run these only when ready to begin testing. A C compiler and cgo are required;
the engine links its bundled SQLite and zstd through cgo.

```sh
go build ./...
go vet ./...
go test ./...
go test -race ./...
go test ./server -run '^$' -fuzz FuzzSQLLexer -fuzztime=5s -parallel=2
```

The Go client is a separate module:

```sh
cd clients/go
go test ./...
go vet ./...
go test -race ./...
```

`TestGoShellExistingDriver` still uses an injected fixed-response engine to
isolate wire behavior. `TestGoShellUnavailableEngine` explicitly covers a disabled
embedding host, not either production binary. Existing real-engine client tests
now run through the Go server launcher without the obsolete missing-engine skip.
The module replacements point to the root SQL module and vendored zstd.

## Live scenarios

The retained Rust scenario sources remain regression specifications. The runner
builds Go server binaries or accepts `MURMUR_SERVER_BIN`; it still requires Rust
for scenario harnesses and clients. Some adversarial fixtures depend on the
original Rust engine's public protocol types.

```sh
go build -o bin/ ./cmd/murmur-server ./cmd/murmur-admin
MURMUR_ENGINE_INTEGRATION=1 tests-live/run.sh three-node-sync
MURMUR_ENGINE_INTEGRATION=1 tests-live/run.sh gate
```

The environment switch is an explicit opt-in to expensive live scenarios, not
an indication that an engine adapter is missing. MURMUR-GO now uses QUIC, but
neither Rust attacker fixture compatibility nor cross-language peering has been
validated by this port.

Priority cases for the later pass: first unlock into absent/empty storage;
wrong-key immutability; concurrent connections; parameter/result type stability;
schema changes; table-flag callbacks; durable restart; master-key rotation;
file handle ownership/expiry and staging limits; backup/restore interruption and
rollback; replication startup/shutdown; and offline JSON/error reporting.

Service deployment requires disposable elevated Linux and Windows VMs. Cover
first creation, permissions on the storage parent, installed credential access,
upgrade recovery, automatic startup, shutdown, and separate uninstall deletion
prompts. Cross-compilation alone cannot validate systemd or SCM behavior.
