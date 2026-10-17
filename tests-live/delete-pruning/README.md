# delete-pruning (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/delete-pruning/`
(`delete_pruning_test.go`,
`TestConcurrentDeletesResurrectionsAndUpdatesConverge`).

## What it proves

Concurrent deletes, resurrections, and updates on a shared row set
converge under aggressive log retention (1 s retention, 10 minimum
retained batches, 20 s offline memory). A retention pass is driven
explicitly while tombstone batches are live, and afterwards all nodes
agree on contents plus exact row counts.

Scope note (same as Go): there is no stored-tombstone pruning API in
the product (tombstone markers stay in state; only origin-log batches
are collected). This suite proves delete/resurrect convergence plus
log collection of the delete batches, not stored-tombstone reclamation.

## Pass criteria (same as Go)

- 300 seeded rows (multiple of 3) converge on all nodes.
- Phase 1 (concurrent disjoint thirds: delete / update / delete)
  converges on the updated middle third only, with the update value
  intact (anti-vacuity).
- Phase 2 resurrects the first third; phase 3 deletes the middle third;
  survivors are exactly the resurrected rows.
- A retention pass runs (`last_collection_millis` advances) and collects
  log batches (`collected_transactions` grows).
- Identical digests and exact counts everywhere; resurrected content
  present, deleted rows absent, on every node; a post-prune write
  replicates everywhere.

## Env knobs

- `MURMUR_DELETE_PRUNING_KEYS` (default 300): shared row count, must be
  a multiple of 3.
- Harness knobs apply: `MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
  `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
  `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`, `MURMUR_SERVER_BIN`.

## Deviations from the Go original

- Multi-process `murmur-server` children over the PG wire (per design)
  instead of `testnode` daemon processes over HTTPS+mTLS.
- Plain SQL (`rows(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL)`, UUID-shaped
  ids with explicit `NOT NULL` on primary key) instead of the typed-contention
  HTTP API; a missing row reads back as an empty result instead of a 404.
- Retention legs use `MURMUR RETENTION COLLECT` (on-demand bounded pass)
  plus `MURMUR SHOW RETENTION` (`collected_transactions`,
  `last_collection_millis`) instead of polling the 30 s GC ticker's
  `spedsql_gc_runs_total` / `spedsql_gc_log_collected_total` counters.
- One flush per phase op: the product mints one replication log entry per
  flush batch, so unflushed phases would leave too few batches to prune.
- Digests join `id\0name` pairs instead of JSON-marshalling rows (only
  cross-node equality within one process is asserted).

## Verification

```sh
MURMUR_SERVER_BIN=/home/marc/MURMUR-SQL/target/debug/murmur-server \
  CARGO_TARGET_DIR=/home/marc/MURMUR-SQL/target \
  cargo test --manifest-path tests-live/delete-pruning/Cargo.toml -- --nocapture
```
