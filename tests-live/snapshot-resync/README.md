# snapshot-resync (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/snapshot-resync/`
(`README.md` + `snapshot_resync_test.go` + `snapshot_multisource_test.go`:
`TestStaleNodeRejoinsViaSnapshot`, `TestDualStaleNodesRejoinViaSnapshot`,
`TestStaleNodeRejoinsViaSingleSnapshotSource`).

## What it proves

Three `murmur-server` processes mesh with aggressive log retention (1 s
retention, 20 s offline pin, 10 retained batches). One node stops while the
survivors write past retention and GC expiry, then restarts with the same
durable directory: its needed log ranges are unrecoverable, so it must
rejoin by merging a snapshot without discarding its acknowledged pre-stop
state. A second test stops two nodes and restarts both back-to-back so
their snapshot requests overlap on the survivor. A multi-source case gives
the stale node three snapshot sources concurrently over a 6,000-row
snapshot.

## Pass criteria

- 3 nodes mesh; node3 writes 5 `stale-*` rows; all converge (60 s).
- Node3 stops; node1 writes 60 `fresh-*` rows; survivors converge on 65
  rows (30 s each).
- After the member-deadline wait, each survivor prunes >= 40 transactions
  and no longer pins node3 (proves the rejoin must use the snapshot path,
  since only the newest 10 per origin survive).
- Node3 restarts with the same durable directory; all converge on 65 rows
  with equal digests (90 s) while node3's applied-log growth stays within
  the retained tail and far below the gap (proves the bulk arrived via
  snapshot, which bypasses that counter);
  the 5 pre-stop `stale-*` rows survive the merge.
- Dual leg: same shape with two stale nodes and one survivor; both rejoin
  via snapshots (120 s), both keep their 5 pre-stop rows, and each
  rejoined node reports live replication sessions.
- Multi-source leg: 4 nodes; the victim stops while a survivor writes 6,000
  rows in 100-row batches; all three survivors prune past retention; the
  victim rejoins under three concurrent sources and converges on 6,005
  rows (120 s) via the snapshot path with its 5 pre-stop rows intact.

## Env knobs

- `MURMUR_SNAPSHOT_RESYNC_GC_WAIT_SECONDS` (default 30): member-deadline
  wait before the collection passes. Lower than the Go default (55) because
  this port drives retention with explicit `MURMUR RETENTION COLLECT` passes
  instead of waiting out the engine's 60 s collection interval.
- Harness knobs apply: `MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
  `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
  `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`, `MURMUR_SERVER_BIN`.

## Deviations from the Go original

- Multi-process cluster: one `murmur-server` child per node, driven over
  the PostgreSQL wire (SQL + `MURMUR ...` commands), instead of `testnode`
  daemon processes over HTTPS+mTLS (per design). Process stop/start
  replaces close/reopen; `mesh()` re-dials after each restart because the
  node rebinds fresh ports.
- Plain SQL (`records(id INTEGER PRIMARY KEY, name TEXT)`) instead of the
  managed typed-records HTTP API; digests are newline-joined names.
  Every node runs `MURMUR SET TABLE records REPLICATED true` (per-node
  config) before any writes.
- Retention legs map to wire commands: `collect_retention` becomes
  `MURMUR RETENTION COLLECT` (returns pruned count), and retention pins
  plus prune totals come from `MURMUR SHOW RETENTION`
  (`node_id`/`last_seen_millis`/`collected_transactions` columns).
- No snapshot metrics exist, so the Go counter legs map to behavior:
  `snapshots_received >= 1` becomes "rows converged while the receiver's
  `applied_transactions` growth (via `MURMUR SHOW REPLICATION`) stayed
  within the retained tail and far below the gap" (the retained tail
  legitimately arrives via logs, so a flat counter would be over-strict);
  the dual test's progress/deferral-series presence legs become a
  connected-peer diagnostics assertion; the multi-source suppressed-frames
  leg becomes behavioral contention coverage (three live sources, one
  completed transfer, convergence to 6,005 rows).
- `received_transactions` has no wire equivalent (`MURMUR SHOW METRICS`
  only covers SQLite/Spool, and `staged_transactions` is a gauge, not a
  cumulative counter), so the snapshot-path proof uses the cumulative
  `applied_transactions` column alone; snapshot application bypasses it,
  so the proof is unchanged.
- Survivor writes flush per write/batch: one replication log entry is
  minted per flush batch, and retention needs dozens of entries to prune
  past (a single trailing flush would leave fewer than
  `minimum_retained`).
- The collection proof is explicit (`MURMUR RETENTION COLLECT` passes plus
  a retention-pin age assertion per survivor/victim pair) rather than a
  fixed 55 s sleep, but the member-deadline wait is kept.

## Compile check (only gate; never run here)

```sh
  go test -count=1 ./tests-live/snapshot-resync/
```
