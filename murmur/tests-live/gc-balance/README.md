# gc-balance (Go port)

Rust source: `/home/marc/MURMUR/tests-live/gc-balance/`
(`gc_balance.rs`: `log_gc_keeps_up_with_churn`).

## What it proves

Three encrypted, mutually authenticated nodes mesh with aggressive log
retention (retention 1s, minimum retained 10, maximum offline 20s, 1s
collection cadence) while node 1 updates a fixed 200-key set with eight
parallel updaters over disjoint ranges. After a 20s warmup, a 40s window
(stretched to a 150s cap until a 2000-commit floor is met) must show GC
collecting at least 90% of the origin-log batches the window produced,
i.e. the retained set does not grow; GC runs with zero failures. The
suite ends with exact row counts and identical digests on all nodes (no
loss).

Scope (same as Rust): version churn only; deletes are out of scope
(delete pruning is unimplemented).

## Pass criteria

- `NewCluster` opens 3 meshed nodes under
  `tests-live/runtime/gc-balance/` with aggressive retention overlays.
- Node 1 seeds 200 rows (`v-0`); all nodes converge on 200 rows within
  60s.
- Eight updaters churn disjoint 25-key ranges on node 1; zero update
  errors and at least 5 full sweeps per updater.
- The measurement window produces at least 2000 commits (else stretched
  in 5s steps to a 150s cap); the local writer scheduler serves
  admissions during the window (no starvation).
- At least one GC run is observed in the window; no GC-shaped replication
  error appears across the window (zero GC failures; unrelated transport
  transients under churn do not fail the check).
- Balance: `commits - collected <= commits / 10` after the post-stop
  drain (one post-stop run observed, then 2 stable 5s collected polls).
- A follower observes a churned value for key 1 within 30s
  (end-to-end update flow).
- All nodes hold exactly 200 rows with identical ordered
  `(id, name, phone, score)` contents (polled 60s, then asserted exactly).

## Env knobs

- Standard harness knobs: `MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
  `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
  `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`, `MURMUR_TEST_DEADLINE_SCALE`.
- No scenario-specific knobs (same as Rust: 200 keys, 8 updaters, fixed
  window bounds).

## Deviations from the Rust source

- In-process multi-`Database` cluster via the Go harness (QUIC mesh,
  ephemeral 127.0.0.1:0), mirroring the Rust topology one for one.
- Plain SQL (`contention(id TEXT NOT NULL PRIMARY KEY, name TEXT, phone
  TEXT, score INTEGER)`) via `Database.Transaction`/`Database.Read`
  instead of the managed typed-contention API; keys keep the 1-based
  `%032x` format; only the `name` column churns (as in Rust).
- Concurrency via goroutines + `sync.WaitGroup` (8 workers on the node-1
  handle, `atomic.Bool` stop flag, hang-guard deadline) instead of scoped
  threads; per-op `Flush()` publishes each write to replication; per-op
  errors are counted (`updateErrs`) rather than failing fast, as in Rust.
- Aggressive retention maps onto the harness
  `ReplicationOptions{Retention: 1s, RetentionInterval: 1s,
  MinimumRetained: 10, MaximumOffline: 20s}` overlays, plus a 1s
  collection cadence so background passes run inside the 40s window (the
  product default is 60s).
- Counters are sampled from product APIs on node 1 instead of the
  `/metrics` endpoint: `Status().Committed`,
  `RetentionStatus().CollectedTransactions`, and
  `Status().Metrics.RetentionRuns`. The test also logs its own
  successful-update count as a cross-check.
- No GC-failures counter exists, so the zero-failures check fails only
  when a GC-shaped replication error (retention, collection, pruning,
  GC, snapshot) appears or differs across the window.
- No maintenance-class check exists on the collection path (retention
  runs in the replication engine, bypassing the `WriterScheduler`; the
  maintenance class only serves migrate/rekey). The scheduler-liveness
  check instead asserts local admissions advanced during the window.
- Digest agreement compares full ordered row vectors directly instead of
  SHA-256 hex digests (equivalent, strictly stronger).
- Counter samples retry transient engine-channel refusals for up to 30s:
  the Go retention/collect paths cross a non-blocking engine command
  channel that can briefly refuse under churn, while the Rust calls
  block.
