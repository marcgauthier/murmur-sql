# delete-pruning (Go port)

Rust source: `/home/marc/MURMUR/tests-live/delete-pruning/`
(`delete_pruning.rs`:
`concurrent_deletes_resurrections_and_updates_converge`).

## What it proves

Three encrypted, mutually authenticated nodes share 300 rows
(`MURMUR_DELETE_PRUNING_KEYS`, multiple of 3), then concurrently delete,
update, and resurrect disjoint thirds under aggressive log retention
(retention 1 s, minimum retained 10, maximum offline 20 s). Retention
collection is forced mid-flight and all nodes must converge on exact
counts plus equal row contents.

Scope (same as Rust): delete/resurrect convergence plus log-GC collection
of the delete batches; stored tombstone markers stay in state by design
and are out of scope.

## Pass criteria

- `NewCluster` opens 3 meshed nodes under
  `tests-live/runtime/delete-pruning/` with aggressive retention overlays.
- Node 1 seeds `seed-{0..299}`; all nodes converge on 300 rows with equal
  contents within 60 s.
- Phase 1 (concurrent, one worker per node): node 1 deletes the first
  third, node 2 renames the middle third to `upd-{i}`, node 3 deletes the
  last third. All nodes converge on 100 rows; row `third` reads `upd-{third}`
  (anti-vacuity: the update was not lost).
- Phase 2: node 2 resurrects the first third as `res-{i}`; all nodes
  converge on 200 rows.
- Phase 3: node 1 deletes the middle third; all nodes converge on 100 rows.
- Retention passes run on node 1 until its durable
  `collected_transactions` counter advances (within 150 s); runs and
  collected totals are logged.
- Every node holds exactly 100 rows with identical ordered `(id, name)`
  digests; row 0 reads `res-0` and row `third` is absent on every node.
- A post-prune insert from node 3 (`post-prune`) replicates everywhere
  within 30 s (101 rows).

## Env knobs

- `MURMUR_DELETE_PRUNING_KEYS` (default 300): shared row count; must be a
  multiple of 3.
- Standard harness knobs: `MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
  `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
  `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.

## Deviations from the Rust original

- In-process multi-`Database` cluster (QUIC mesh, ephemeral `127.0.0.1:0`),
  same as Rust.
- Plain SQL (`contention(id TEXT NOT NULL PRIMARY KEY, name TEXT)`);
  row ids keep the Rust `deleteRowID` UUID-shaped format; "not found" is a
  missing SQL row.
- Concurrency via goroutines + a start channel (one worker per node) with
  per-op `Flush()`; worker errors are collected and failed on the test
  goroutine.
- Aggressive retention maps onto the harness
  `ReplicationOptions{Retention, MinimumRetained, MaximumOffline}` overlays.
- GC forcing uses the product's on-demand `CollectRetention()` (each 2 s
  poll runs a pass); `RetentionStatus().CollectedTransactions` and
  `Status().Metrics.RetentionRuns` replace the Rust counter reads.
- Resurrection is a plain SQL re-`INSERT` of the same primary key after the
  delete has converged.
