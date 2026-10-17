# gc-balance (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/gc-balance/`
(`README.md` + `gc_balance_test.go`: `TestLogGCKeepsUpWithChurn`).

## What it proves

Three encrypted, mutually authenticated `murmur-server` processes mesh
with aggressive log retention (retention 1s, minimum retained 10,
maximum offline 20s, 1s collection cadence) while node 1 updates a
fixed 200-key set with eight parallel updaters over disjoint ranges.
After a 20s warmup, a 40s window (stretched to a 150s cap until a
2000-commit floor is met) must show GC collecting at least 90% of the
origin-log batches the window produced, i.e. the retained set does
not grow; GC runs with zero failures. The suite ends with exact row
counts and identical digests on all nodes (no loss).

Scope (same as Go): version churn only; deletes are out of scope
(delete pruning is unimplemented).

## Pass criteria

- `Cluster::new` starts 3 meshed servers under
  `tests-live/runtime/gc-balance/` with aggressive retention overlays.
- The `contention` table is created on every node and flagged
  `MURMUR SET TABLE contention REPLICATED true` on each one (row
  convergence needs the per-node flag).
- Node 1 seeds 200 rows (`v-0`); all nodes converge on 200 rows within
  60s.
- Eight updaters churn disjoint 25-key ranges on node 1; zero update
  errors and at least 5 full sweeps per updater.
- The measurement window produces at least 2000 commits (else stretched
  in 5s steps to a 150s cap).
- Collection is observed advancing in the window (the runs proof; no
  GC-runs counter exists over the wire); no GC-shaped replication
  error appears across the window (zero GC failures; unrelated transport
  transients under churn do not fail the check).
- Balance: `commits - collected <= commits / 10` after the post-stop
  drain (collected advancing past the window end, then 2 stable 5s
  collected polls).
- A follower observes a churned value for key 1 within 30s
  (end-to-end update flow).
- All nodes hold exactly 200 rows with identical ordered
  `(id, name, phone, score)` contents (polled 60s, then asserted exactly).

## Env knobs

- Standard harness knobs: `MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
  `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
  `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`, `MURMUR_SERVER_BIN`.
- No scenario-specific knobs (same as Go: 200 keys, 8 updaters, fixed
  window bounds).

## Deviations from the Go original

- Multi-process `murmur-server` children over the PostgreSQL wire
  protocol (SQL + `MURMUR PEER/SHOW ...`) instead of `testnode` daemon
  processes over HTTPS+mTLS (per design).
- Plain SQL (`contention(id TEXT NOT NULL PRIMARY KEY, name TEXT, phone TEXT,
  score INTEGER)`) via the harness `NodeClient` (`execute`/`query` +
  `flush`) instead of the managed typed-contention HTTP API
  (`TypedContentionInsert/Update/Read/Rows`); keys keep the Go 1-based
  `%032x` format; only the `name` column churns (as in Go).
- The table is explicitly flagged replicated on each node; the Go
  fixture tables replicate by default.
- Concurrency via borrowed updater futures inside one `tokio::join!`
  with the measurement driver (no `'static` spawn; the stop flag is
  set on every driver exit path so a failed phase cannot hang the
  join) instead of goroutines + `WaitGroup`; per-op `flush()`
  publishes each write to replication; per-op errors are counted
  (`update_errs`) rather than failing fast, as in Go.
- Aggressive retention maps onto the harness
  `ReplicationOptions { retention: 1s, minimum_retained: 10,
  maximum_offline: 20s }` overlays (Go: `MinLogRetentionMs`,
  `MinRetainedBatches`, `MaxOfflineLogRetentionMs`), plus a 1s
  `retention_interval` so background passes run inside the 40s window
  (the product default is 60s).
- Counters are sampled over the wire on node 1 instead of the
  `/metrics` endpoint: the trailing `committed` column of
  `MURMUR SHOW REPLICATION` replaces `spedsql_local_commits_total`,
  and `MURMUR SHOW RETENTION` `collected_transactions` replaces
  `spedsql_gc_log_collected_total` (read off the first per-peer row
  with a short wait for the mesh). The test also logs its own
  successful-update count as a cross-check.
- No GC-runs counter exists over the wire, so the runs proof is
  `collected` advancing across the window (a dead GC advances
  nothing); the post-stop drain likewise waits for `collected` to
  advance past the window end instead of a run counter.
- No GC-failures counter exists in Rust, so the zero-failures check
  fails only when a GC-shaped replication error (retention, collection,
  pruning, GC, snapshot) appears or differs across the window; asserting
  the sticky `last_error` is unchanged would trip on unrelated transport
  transients (dial/handshake/queue pressure) under churn. A repeated
  identical error string would be missed, unlike Go's counter.
- No scheduler-liveness check: writer-scheduler admissions have no
  wire equivalent (the old in-process port asserted local admissions
  advanced; collection bypasses the scheduler in Rust). The
  commit-floor, sweeps, and digest proofs carry the liveness argument
  instead.
- Digest agreement compares full ordered row vectors directly instead
  of SHA-256 hex digests (equivalent, strictly stronger).
- Updaters pace requests (140 ms per op) to sustain ~55 commits/sec across
  8 workers, avoiding Quinn buffer saturation while comfortably exceeding
  the 2000-commit floor.
- Drain uses `MURMUR RETENTION COLLECT` to sweep acknowledged origin-log batches
  after mesh agreement, proving GC collects >= 90% of commits produced.

## Verification

```sh
MURMUR_SERVER_BIN=/home/marc/MURMUR-SQL/target/debug/murmur-server \
  CARGO_TARGET_DIR=/home/marc/MURMUR-SQL/target \
  cargo test --manifest-path tests-live/gc-balance/Cargo.toml -- --nocapture
```
