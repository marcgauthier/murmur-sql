# write-priority (Go port)

Rust source: `/home/marc/MURMUR/tests-live/write-priority/`
(`write_priority.rs`: `local_write_priority`).

## What it proves

Two nodes take continuous writes on both nodes (dense writers with
disjoint 16-byte row-ID ranges) while serving reads, under full-mesh
replication. The test gates on local progress, the replication share of
dual-contention writer grants, and convergence.

## Env knobs

- `MURMUR_LIVE_WRITE_PRIORITY_SECONDS` (default 12): write-phase duration.
- `MURMUR_LIVE_WRITE_PRIORITY_SYNC_TIMEOUT_SECONDS` (default 120):
  convergence deadline after writes stop.
- Harness knobs apply: `MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
  `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
  `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`, `MURMUR_TEST_DEADLINE_SCALE`.

## Pass criteria

1. Local progress: at least 80 rows written and 4 reads served.
2. Priority: at least 100 dual-contention writer grants measured and
   scheduler debt within the 1s bound for the whole contended window.
   The scheduler targets 90/10 local/remote service with a 1s debt
   bound; the sampled debt is the checkable guarantee.
3. Convergence: identical row counts and identical row sets on both nodes
   after writes stop.

## Deviations from the Rust source

- In-process multi-`Database` cluster via the Go harness (QUIC mesh,
  ephemeral 127.0.0.1:0), mirroring the Rust topology one for one.
- Plain SQL (`records(id TEXT NOT NULL PRIMARY KEY, name TEXT)`) via
  `Database.Transaction`/`Read` + `Flush`; one transaction per insert, as
  in Rust. Same key shapes (16-byte node/worker/seq row IDs,
  `v-n<node>-w<worker>-<seq>` names).
- Scheduler assertions read the in-process
  `Database.Scheduler().Snapshot()` instead of scraping Prometheus
  `/metrics`. As in Rust, the port does NOT gate a dual-grant share: the
  scheduler is service-fair with a debt bound, so transient grant shares
  are unconstrained by design, and a service-share gate would reflect
  ticket-cost asymmetry rather than policy. The port gates the guarantee
  the scheduler actually provides: debt within the 1s bound (sampled
  every 50 ms) plus >= 100 dual grants proving contention. Grant/service
  splits are still reported for diagnosis.
- 64 goroutine writers per node, matching the Rust count (still enough
  looping writers to keep both scheduler classes backlogged, where the
  90/10 policy binds).
- Convergence compares the full sorted row sets for equality (strictly
  stronger than a hash over rows) and requires both nodes to hold the
  full set.
