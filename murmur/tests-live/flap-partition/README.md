# flap-partition (Go port)

Rust source: `/home/marc/MURMUR/tests-live/flap-partition/`
(`flap_partition.rs`: `flapping_partition_loses_no_writes`).

## What it proves

Three fully meshed nodes take continuous writes from all nodes while
node3's links flap through split/heal cycles. A partition must never fail
a write to a live node (all commits are local), and after the final heal
every node must converge on the identical row set with equal digests. The
first split carries an isolation proof: a marker written on node1 must
reach node2 but never node3.

## Pass criteria

- `NewCluster` opens 3 meshed nodes under
  `tests-live/runtime/flap-partition/`.
- 6 baseline inserts on node 0 converge on all 3 nodes within 150 s
  (honest-path control).
- Continuous writers on every node (50 ms cadence each) run across
  `MURMUR_FLAP_CYCLES` split/heal cycles (split hold
  `MURMUR_FLAP_SPLIT_SECONDS`, heal settle `MURMUR_FLAP_HEAL_SECONDS`).
- On the first split, the isolation marker reaches node2 within 10 s and
  never appears on node3 (checked throughout the window plus a 1 s
  post-proof leak watch).
- Zero failed writes to live nodes (node3 stays running, only
  unreachable, so its local commits must succeed too).
- After the final idempotent heal, all 3 nodes converge on exactly
  `6 + written` rows with equal ordered contents within 150 s.

## Env knobs

- `MURMUR_FLAP_CYCLES` (default 5): split/heal flap cycles.
- `MURMUR_FLAP_SPLIT_SECONDS` (default 3): split hold per cycle.
- `MURMUR_FLAP_HEAL_SECONDS` (default 2): heal settle per cycle.
- Harness knobs apply: `MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
  `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
  `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`, `MURMUR_TEST_DEADLINE_SCALE`.

## Deviations from the Rust source

- In-process multi-`Database` cluster via the Go harness (QUIC mesh,
  ephemeral 127.0.0.1:0), mirroring the Rust topology one for one.
- Plain SQL (`records(id INTEGER PRIMARY KEY, name TEXT)`) instead of the
  managed typed-records API; writer ids are `node * 10_000_000 + seq`,
  baseline ids start at 90_000_001, and the isolation marker uses id
  90_001_001.
- `waitConverged` sorts each node's names before the ordered comparison
  (SQL `SELECT` without `ORDER BY` does not guarantee order).
- Goroutine writers plus `sync.WaitGroup` instead of scoped threads; the
  stop flag is an `atomic.Bool`, and a deferred stop/join replaces the
  drop guard so a failed phase cannot deadlock the join.
- Cycle count uses a local `envInt` helper (the harness only provides
  `EnvSeconds` for duration knobs).
