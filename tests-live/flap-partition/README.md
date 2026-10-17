# flap-partition (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/flap-partition/`
(`README.md` + `flap_partition_test.go`:
`TestFlappingPartitionLosesNoWrites`).

## What it proves

Three fully meshed `murmur-server` processes take continuous writes from
all nodes while node3's links flap through split/heal cycles. A partition
must never fail a write to a live node (all commits are local), and after
the final heal every node must converge on the identical row set with
equal digests. The first split carries an isolation proof: a marker
written on node1 must reach node2 but never node3.

## Pass criteria

- `Cluster::new` starts 3 meshed servers under
  `tests-live/runtime/flap-partition/`.
- The `records` table is created on every node and flagged
  `MURMUR SET TABLE records REPLICATED true` on each one (row
  convergence needs the per-node flag).
- 6 baseline inserts on node 0 converge on all 3 nodes within 150 s
  (honest-path control).
- Continuous writers on every node (50 ms cadence each) run across
  `MURMUR_FLAP_CYCLES` split/heal cycles (split hold
  `MURMUR_FLAP_SPLIT_SECONDS`, heal settle
  `MURMUR_FLAP_HEAL_SECONDS`).
- On the first split, the isolation marker reaches node2 within 10 s
  and never appears on node3 (checked throughout the window plus a
  1 s post-proof leak watch).
- Zero failed writes to live nodes (node3 stays running, only
  unreachable, so its local commits must succeed too).
- After the final idempotent heal, all 3 nodes converge on exactly
  `6 + written` rows with equal ordered contents within 150 s.

## Env knobs

- `MURMUR_FLAP_CYCLES` (default 5): split/heal flap cycles.
- `MURMUR_FLAP_SPLIT_SECONDS` (default 3): split hold per cycle.
- `MURMUR_FLAP_HEAL_SECONDS` (default 2): heal settle per cycle.
- Harness knobs apply: `MURMUR_LIVE_RUNTIME`,
  `MURMUR_LIVE_FAILURES`, `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
  `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`, `MURMUR_SERVER_BIN`.

## Deviations from the Go original

- Multi-process `murmur-server` children over the PostgreSQL wire
  protocol (SQL + `MURMUR PEER ...`) instead of `testnode` daemon
  processes over HTTPS+mTLS (per design).
- Plain SQL (`records(id INTEGER PRIMARY KEY, name TEXT)`) instead of the
  managed typed-records HTTP API (`TypedInsert` / `TypedNames`); writer
  ids are `node * 10_000_000 + seq`, baseline ids start at 90_000_001,
  and the isolation marker uses id 90_001_001, instead of
  server-generated typed IDs.
- The table is explicitly flagged replicated on each node; the Go
  fixture tables replicate by default.
- `waitConverged` sorts each node's names before the ordered comparison
  (Go's `TypedNames` arrives ordered; SQL `SELECT` without `ORDER BY`
  does not guarantee order).
- Goroutine writers become borrowed writer futures inside one
  `tokio::join!` with the flap driver (no `'static` spawn); the stop
  flag is set on every driver exit path so a failed phase cannot hang
  the join. Written/failed counters stay `AtomicI64`.
- Cycle count uses a local `env_int` helper (the harness only provides
  `env_seconds` for duration knobs).
- Directory logging shows label/dir/SQL/replication addresses (plus PIDs
  via `Cluster::node_pid`).

## Compile check

```sh
CARGO_TARGET_DIR=/home/marc/MURMUR-SQL/tests-live/target \
  cargo test --no-run --manifest-path /home/marc/MURMUR-SQL/tests-live/flap-partition/Cargo.toml
```

## Run (pilot scenario: run until green)

```sh
CARGO_TARGET_DIR=/home/marc/MURMUR-SQL/tests-live/target \
  cargo test --manifest-path /home/marc/MURMUR-SQL/tests-live/flap-partition/Cargo.toml
```

`MURMUR_SERVER_BIN` reuses a prebuilt `--features replication` server
instead of the one cargo build the harness triggers.
