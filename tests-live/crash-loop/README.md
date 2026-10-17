# crash-loop (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/crash-loop/`
(`crash_loop_test.go`, `TestCrashLoopNodeConvergesWithoutLoss`).

## What it proves

Node 3 is SIGKILLed and restarted N times (`MURMUR_CRASH_LOOP_ROUNDS`,
default 5) under continuous writes from every node, with short uptimes
between restarts (`MURMUR_CRASH_LOOP_UPTIME_MS`, default 1000). Every
write acknowledged before each restart must survive; the looped node must
converge to identical contents; it must reach ready within 60 s after the
final restart (no wedge); and a post-loop write on it must replicate
everywhere.

## Pass criteria (same as Go)

- Positive control: baseline + full mesh + convergence within 30 s.
- Every non-final restart lands with a write provably in flight on the
  victim (10 s gate); the final round bursts 30 rows on a survivor and
  restarts within 500 ms (mid-replication by construction).
- Final restart reaches ready in under 60 s; mesh reforms (`[2, 2, 2]`
  peers) within 90 s; convergence within 90 s; every acknowledged write
  present everywhere.
- A post-loop write on the looped node replicates everywhere (60 s).

## Env knobs

- `MURMUR_CRASH_LOOP_ROUNDS` (default 5).
- `MURMUR_CRASH_LOOP_UPTIME_MS` (default 1000).
- Harness knobs apply: `MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
  `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
  `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`, `MURMUR_SERVER_BIN`.

## Deviations from the Go original

- Multi-process `murmur-server` children over the PG wire (per design)
  instead of `testnode` daemon processes over HTTPS+mTLS.
- Plain SQL (`records(id INTEGER PRIMARY KEY, name TEXT NOT NULL)`,
  per-node id strides) instead of the managed typed-records HTTP API.
  Row identity in assertions is `name@id`.
- Session counts come from `MURMUR SHOW REPLICATION` (`peers` column)
  instead of `/v1/status` (`connected_peers`).
- Writers are `tokio::spawn` tasks sharing the cluster behind one async
  mutex (the harness `kill_node`/`start_node` legs need `&mut`, so the
  pilot's borrowed `tokio::join!` shape cannot run writers across kills).
  A `running` flag per node, cleared before the kill and re-checked under
  the lock, keeps writers off the stopped victim (whose `Cluster::node`
  would panic); every insert carries a 10 s wire timeout so a kill can
  never wedge a writer on the lock. Refused writes during kill/restart
  windows are dropped like the Go original; only acknowledged writes must
  survive.
- `Cluster::kill_node`/`start_node` + `mesh()` replace
  `KillNode`/`StartNode`/`UnlockNode`/`WaitNodeReady` (unlock runs under
  the harness watchdog inside `start_node`).
- On each victim restart, `MURMUR SET TABLE records REPLICATED true` and
  `flush()` re-establish table replication, and `sync_all()` triggers
  catchup across the re-formed mesh.

## Verification

```sh
MURMUR_SERVER_BIN=/home/marc/MURMUR-SQL/target/debug/murmur-server \
  CARGO_TARGET_DIR=/home/marc/MURMUR-SQL/target \
  cargo test --manifest-path tests-live/crash-loop/Cargo.toml -- --nocapture
```
