# tail-repair (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/tail-repair/`
(`README.md` + `tail_repair_test.go`:
`TestPartitionedNodeRepairsTailFromPeerLogs`).

## What it proves

A node partitioned mid-write-burst misses a contiguous range of its
peers' origin logs, then heals and must repair the missing tail from
peer logs WITHOUT the snapshot path: row counts and ordered digests
converge everywhere while the healed node's log-path counter proves
the range arrived as transactions, not as a snapshot.

## Pass criteria

- `Cluster::new` opens 3 meshed `murmur-server` children under
  `tests-live/runtime/tail-repair/` with generous log retention
  (300 s, 10000 retained batches).
- Baseline of 10 rows on node 0 converges on all 3 nodes (counts +
  equal digests) within 30 s.
- After removing all 4 links to/from node3 (both directions), node3
  reports 0 connected peers within 15 s.
- Isolation probe: one row on node 0 reaches node 1 within 15 s but,
  after a 2 s settle margin, node3 still holds exactly the 10 baseline
  rows with an unchanged digest (leak = fail).
- A burst of `MURMUR_TAIL_REPAIR_BURST_ROWS` rows (default 100),
  alternating nodes 0/1, converges on both survivors within 30 s while
  node3 stays frozen at the baseline (anti-vacuity: it must actually
  miss the contiguous `1 + burst` range).
- After re-adding all 4 links, every node reports 2 connected peers
  within 30 s and all 3 converge on the full row set with equal
  digests within 60 s.
- No-snapshot proof: node3's `applied_transactions` advances by at
  least the missed count across the heal (values logged for
  diagnosis), and no snapshot source is trusted anywhere in the
  cluster (see deviations).
- A post-heal insert on the healed node reaches all 3 nodes (counts +
  equal digests) within 30 s.

## Env knobs

- `MURMUR_TAIL_REPAIR_BURST_ROWS` (default 100): burst rows written on
  the survivors while node3 is isolated. Same name and default as Go.
- Harness knobs apply: `MURMUR_LIVE_RUNTIME`,
  `MURMUR_LIVE_FAILURES`, `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
  `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`, `MURMUR_SERVER_BIN`.

## Deviations from the Go original

- Multi-process cluster: one `murmur-server` child per node, driven over
  the PostgreSQL wire (SQL + `MURMUR ...` commands), instead of `testnode`
  daemon processes over HTTPS+mTLS (per design).
- Plain SQL (`records(id INTEGER PRIMARY KEY, name TEXT)`) instead of the
  managed typed-records HTTP API (`TypedContentionInsert` /
  `TypedContentionRows`); marker rows use the Go numeric ids directly
  (0-9 / 9000 / 10000+i / 20000) instead of formatted UUIDs, and the
  digest is the `Debug` rendering of the sorted `(id, name)` pairs
  instead of `%#v` of the typed rows. Every node runs
  `MURMUR SET TABLE records REPLICATED true` (per-node config) before
  any writes.
- `wait_connected_peers` reads the `peers` column of
  `MURMUR SHOW REPLICATION` (live session count) instead of Go's
  `/v1/status connected_peers`.
- Go's `MinLogRetentionMs`/`MinRetainedBatches` map to harness
  `ReplicationOptions{retention: 300 s, minimum_retained: 10000}`;
  Go's `MaxOfflineLogRetentionMs` (600 s) keeps the product default
  (`maximum_offline` = 7 days), which is strictly more generous and
  preserves the test's intent (the missed range stays inside every
  peer's origin log). No harness change was needed.
- Go asserts `spedsql_repl_snapshots_received_total`,
  `spedsql_repl_snapshot_required_received_total` (unchanged on node3)
  and `spedsql_repl_snapshots_sent_total` (zero everywhere), and logs
  `gaps_detected`/`needs_sent`/`batches_received`. The Rust product
  exposes none of these counters over the wire, so the no-snapshot
  criterion is ported as two parts. Structural: the cluster never
  configures `trusted_snapshot_sources`, and the Rust engine only
  requests, serves, or accepts snapshots for explicitly trusted
  sources, so no snapshot could have been taken on this mesh; any
  observed convergence used log repair. Behavioral: the test asserts
  the healed node's `applied_transactions` (via
  `MURMUR SHOW REPLICATION`) advances by at least the missed count
  across the heal and logs the before/after values (the diagnostic
  analog of Go's gap-pull log line); snapshot application bypasses
  that counter, so advancement proves the range arrived as log
  transactions. `received_transactions` has no wire equivalent
  (`MURMUR SHOW METRICS` only covers SQLite/Spool, and
  `staged_transactions` is a gauge), so only the cumulative applied
  counter is asserted; every burst row is its own flushed transaction,
  so the missed row count is also the missed transaction count.
- Isolation proof strengthened: besides Go's frozen row count, node3's
  digest must also be unchanged at both mid-partition checkpoints
  (count-only in Go).

## Compile check (only gate; never run here)

```sh
  go test -count=1 ./tests-live/tail-repair/
```
