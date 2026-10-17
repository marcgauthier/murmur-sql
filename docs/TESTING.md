# Testing Murmur

The database, replication, and Spool packages have separate Cargo manifests.
Running tests at the repository root does not run the dependencies' unit tests.

Run the routine suites:

```sh
cargo test
cargo test --features replication
cargo test --all-features
cargo test --manifest-path replication/Cargo.toml
cargo test --manifest-path spool/Cargo.toml
```

The server suites live in the same runs: `config` (config parsing and
atomic flag rewrite), `server_lifecycle` (startup, logging, exit codes),
`pgwire_server` (auth, lockout, CRUD over both wire protocols, error codes,
restart durability, table flags), `pgwire_tls` (TLS and mTLS), `admin_cli`,
`pgwire_backup` (remote backup/restore), `pgwire_ddl` (DDL rules and the
unsupported-definition block list), `pgwire_rotate` (master-key rotation),
and `logging_victorialogs` (Loki push shape). Run one focused suite with
`cargo test --test pgwire_server`.

`replication_primary_key` covers local defaults, required application keys,
UUIDv5 derivation, read-only enforcement, prepared SQL, populated mode changes,
and recovery. Replication fixtures explicitly enable their tables; local tables
retain their application SQLite primary keys.

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

`column_vectors` verifies vector declarations, metrics, rollback, invalid-data
recovery, FTS coexistence, mode transitions and persistence. Prepared wire tests
cover empty-index numeric descriptions and explicitly typed binary vectors.
Feature-disabled tests reject vector declarations, store opening, and schema
adoption without losing settings; legacy fixtures cover format-6/7 stores.
Run `cargo test --no-default-features --lib --test column_vectors` and
`cargo test --no-default-features --features replication --lib --test column_vectors`
to exercise the opt-out builds.

`column_fts` tests schema declarations, validation, local DDL, mode transitions,
restart and backup recovery. The Go FTS suite verifies quoted identifiers,
prepared MATCH parameters and server status. Replication tests cover FTS-only
schema adoption, enable/disable, source updates and snapshot recovery.

Managed search tests cover FTS5 and sqlite-vec maintenance (enabled by default; optional with `--no-default-features`), read-only
virtual/shadow tables, source-only exports, queue overflow, migration rollback,
conflict resolution, replication settings, subscriptions, recovery, and restore.
The replication and snapshot suites also exercise local index publication after
QUIC delivery, peer schema adoption, and snapshot application. Run the focused
suite with `cargo test --all-features --test search_indexes`.

For a manual comparison of source-write cost with and without managed indexes,
run `cargo test --test search_indexes application_write_cost_with_indexes --
--ignored --nocapture`. It reports the median of three runs with capture and
index maintenance deferred; throughput is diagnostic, not a pass/fail threshold.

The new failure tests cover retention limits and durable floors, encrypted file
boundaries and corruption, upload failures, rotation and collection, resumed peer
fetches, bridge gaps/replays and failed completion receipts, local table settings,
seeded partitioned edits, concurrent shutdown, and cache/parser resource bounds.
File and bridge recovery tests reopen actual encrypted Spool stores. Retention
policy tests use a controlled clock and a recording backend; database retention
tests exercise the real serialized Spool worker and recovery snapshots.

The additional retention process-crash matrix is separate from routine runs:

```sh
cargo test --features replication --lib process_crashes_between_retention -- --ignored
```

It terminates child processes after logical snapshot publication, after durable
coverage publication, and after atomic pruning. Recovery must retain SQL state
and tombstones; log deletion and duplicate-floor advancement must agree.

Seeded randomized tests use fixed seeds and compare against independent expected
SQL/counter/set values. Report the test name and seed when a failure occurs.
Network tests use ephemeral loopback ports and poll explicit state with deadlines.
Spool's interoperability suite also exercises Go compatibility when its external
tools/fixtures are available; see `spool/tests/interop.rs` for its requirements.

Snapshot tests cover signed manifests, chunk conflicts and corruption, paginated
resume, deferred/failed apply, durable receipts, staging limits and retention
pins. Database tests reopen both encrypted stores and exercise pruning, local-only
tables and pruned-history catch-up with counters, sets and deletions.

Run the additional snapshot crash matrix separately:

```sh
cargo test --features replication --lib process_crashes_during_snapshot_transfer -- --ignored
```

It exits child processes after outgoing publication, partial receiving, durable
application before acknowledgement, and durable catch-up-target publication.
Reopening must preserve missing-chunk progress, completed state and retention pins.

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

Two harness packages share one import path through a Cargo alias:

- `../tests-live/harness` (`murmur-live-harness`): full mesh management over
  raw `tokio-postgres` (`NodeClient` with `execute`/`query`/`simple`/`flush`,
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
`MURMUR_SERVER_BIN`), `CARGO_TARGET_DIR` (shared build dir),
`MURMUR_LIVE_RUNTIME` (artifact root, default `tests-live/runtime`),
`MURMUR_LIVE_FAILURES` plus `MURMUR_LIVE_UNLOCK_STALL_SECONDS=120` /
`MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS=1800` (classic harness only).

Wire-command documentation: [USAGE.md](USAGE.md#murmur-commands) is
authoritative for `MURMUR ...` SQL syntax, per-feature sections, and error
codes; `../clients/rust/README.md` maps each command to its `murmur-client`
helper (`set_table_replicated`, `flush`, `peer_add`, `policy_*`,
`bridge_release`, file/backup helpers, ...); each scenario's `README.md`
records its env knobs, pass criteria, and deviations from the Go original.
