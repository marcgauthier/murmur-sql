# Testing Murmur-SQL

This repository holds only the PostgreSQL-wire shell; the engine, replication,
and Spool crates live in the sibling `../MURMUR` checkout and their suites run
there (`cargo test` and `cargo test --features replication` at that root,
plus its `replication/` and `spool/` manifests).

Run the routine shell suites:

```sh
cargo test
cargo test --features replication
cargo test --all-features
```

The suites are `config` (config parsing and atomic flag rewrite),
`server_lifecycle` (startup, logging, exit codes), `pgwire_server` (auth,
lockout, CRUD over both wire protocols, error codes, restart durability,
table flags), `pgwire_tls` (TLS and mTLS), `pgwire_backup` (remote
backup/restore), `pgwire_ddl` (DDL rules and the unsupported-definition block
list), `pgwire_rotate` (master-key rotation), and `logging_victorialogs`
(Loki push shape). Run one focused suite with
`cargo test --test pgwire_server`. The `live_sql_architecture` test guards
the process/SQL-only boundary and complete live-scenario runner catalog.

The Go client has its own module with tests and a feature-coverage test:

```sh
cd clients/go
go vet ./...
go test -count=1 -timeout 300s ./...
go test -race -count=1 -timeout 300s ./...
```

Go tests and examples build `murmur-server` via cargo on first use; set
`MURMUR_SERVER_BIN` to reuse a prebuilt binary. Runnable examples verify
end to end with `go run ./examples/<name>`. The `json`, `search`, and `vectors`
examples verify JSON/JSONB, FTS5, and nearest-neighbor SQL respectively; their
feature mappings are checked by the example coverage test.

Engine suites (`column_vectors`, `column_fts`, `search_indexes`,
`replication_primary_key`, convergence, persistence, retention, snapshot, and
the ignored subprocess crash matrices) run in `../MURMUR`; see its
`docs/TESTING.md`. The Go FTS/vector suites verify quoted identifiers,
prepared parameters, and server status against a scratch `murmur-server`.

## Live multi-process tests (`tests-live/`)

Live scenarios run real `murmur-server` processes (one child per node) and
drive them over the PostgreSQL wire with the `murmur-client` Rust crate
(`clients/rust`: `tokio-postgres` + `rustls`, no engine or native library).
Each node gets its own directory (`runtime/<scenario>.../nodeN/` with
`config.toml`, `tls/`, `data/`, `logs/`, `backups/`). Tests send application
SQL plus `MURMUR ...` admin commands over the wire; replication runs as a
background QUIC mesh between the server children. Stop/restart/kill legs act
on the child processes directly. See `../tests-live/README.md` for the full
topology, harness API, and per-scenario layout, and
`../tests-live/run.sh` for the runner.

All 73 scenarios now use the Rust client and explicit SQL for database work;
none opens an embedded database. Read-only diagnostic assertions use
`MURMUR SHOW STATUS`; fault injection acts on child processes, networks,
configuration, or durable artifacts. The hostile-QUIC fixtures reuse the engine's
protocol implementation without opening embedded databases.

Two harness packages share one import path through a Cargo alias:

- `../tests-live/harness` (`murmur-live-harness`): full mesh/process management through
  `murmur-client` (`NodeClient` with `execute`/`query`/`simple`,
  `add_peer`/`mesh`/`sync_all`, suspend/resume, cert rotation, forensics,
  `RESULT` line, raw-QUIC attacker fixture).
- `../tests-live/sql-harness` (`murmur-live-sql-harness`): smaller process
  harness that returns `murmur-client` connections (`cluster.node(i)` is a
  `&Client`; `cluster.connect(i).await` opens an extra unlocked connection).
  Options are `name`, `num_nodes`, `extra_toml`, `replication_toml`, `files`,
  `manual_peers`, and `local_only`. Each test gets a unique runtime directory;
  passing tests clean up and panics retain server logs and configs.

Scenarios using the SQL harness keep the same Rust import via the alias
convention, so test bodies look identical:

```toml
# classic harness scenario
murmur-live-harness = { path = "../harness" }
# SQL-harness scenario (same `use murmur_live_harness::...` in tests)
murmur-live-harness = { package = "murmur-live-sql-harness", path = "../sql-harness" }
```

Some SQL-harness scenarios also add `murmur-client = { path = "../../clients/rust" }`
for direct client types. Every scenario (and each harness) is a standalone
package: `tests-live/` is not a workspace and is not part of the root package.

`run.sh` runs one scenario at a time, sequentially. It builds `murmur-server`
and `murmur-admin` with `--features replication`, exports `MURMUR_SERVER_BIN`,
and sweeps server processes scoped to its runtime dir on exit:

```sh
tests-live/run.sh <scenario>  # one scenario by directory name
tests-live/run.sh all         # every scenario in ALL_SCENARIOS order
tests-live/run.sh gate        # release-gate subset (GATE_SCENARIOS)
tests-live/run.sh soak        # files-soak + soak-slo + long-running-five-node with duration env knobs
tests-live/run.sh stress      # rolling-restart, partial-mesh, crash-recovery, partition, snapshot-resync x MURMUR_STRESS_COUNT (default 5)
```

Per-scenario compile gate (no execution, exactly one package plus its harness):

```sh
CARGO_TARGET_DIR=$PWD/tests-live/target \
  cargo test --no-run --manifest-path $PWD/tests-live/<scenario>/Cargo.toml
```

`CARGO_TARGET_DIR` is mandatory: without the shared dir every scenario
duplicates the full dependency build (~1.6 GB each). Never use `--workspace`,
never run `run.sh` as a compile check, and never run binaries under `target/`
by hand. Harness-only checks:

```sh
cargo check --manifest-path tests-live/harness/Cargo.toml --all-targets
cargo check --manifest-path tests-live/sql-harness/Cargo.toml --all-targets
```

Runtime validation is `run.sh <scenario>` (or, for one iteration,
`cargo test --manifest-path tests-live/<scenario>/Cargo.toml` with the same
`CARGO_TARGET_DIR`). Honored environment: `MURMUR_SERVER_BIN` (prebuilt
server; both harnesses, else built once per test process),
`MURMUR_ADMIN_BIN` (sql-harness admin override; default is the sibling of
`MURMUR_SERVER_BIN`), `MURMUR_FORMAT_FIXTURE_BIN` (optional prebuilt engine
unit-test executable for the downgrade guard; otherwise the runner builds it), `CARGO_TARGET_DIR` (shared build dir),
`MURMUR_LIVE_RUNTIME` (artifact root, default `tests-live/runtime`),
`MURMUR_LIVE_FAILURES` plus `MURMUR_LIVE_UNLOCK_STALL_SECONDS=120` /
`MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS=1800` (classic harness only).

Wire-command documentation: [USAGE.md](USAGE.md#murmur-commands) is
authoritative for `MURMUR ...` SQL syntax, per-feature sections, and error
codes. Live scenarios issue those statements through the Rust client's
`execute`, `query`, and `simple` methods. Each scenario's `README.md`
records its environment options and pass criteria.

The SQL conversion compiles all 73 scenario packages. Runtime validation
currently exposes a failure in `write-priority`: the measured dual-contention
service exceeded its 25% remote-share limit. The assertion remains enabled;
the cause requires investigation. Compile success does not establish a passing
release gate, and shortened soak runs do not qualify full-duration profiles.


## Service installer validation

`cargo test --all-features --lib service::` exercises phase cancellation,
configuration preservation, staged replacement/rollback, permission failures,
path restrictions, and separately confirmed deletion using a fake service
manager and temporary files. `cargo test --all-features --test service_cli
--test server_lifecycle` covers CLI dispatch and ordinary console operation.

`service_installation` is an ignored, elevated integration test for disposable
Windows or Linux/systemd machines only. It creates the real `MurmurSQL` /
`murmur-sql.service`, tests start/upgrade/declined-stop/stopped-reinstall/removal,
and checks that no database files were created. It refuses to run over an
existing service or installer record. Enable `MURMUR_SERVICE_TESTS=1` and run
`cargo test --all-features --test service_installation -- --ignored
--test-threads=1` as Administrator/root **only on a disposable machine**.
The service-installation workflow builds as the normal user, then executes the
compiled integration test with elevation on disposable GitHub-hosted runners.
Never run that elevated test against a working deployment.
