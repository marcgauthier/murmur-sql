# rolling-restart (Go port)

Rust source: `/home/marc/MURMUR/tests-live/rolling-restart/`
(`rolling_restart.rs`).

## What it proves

- `TestRollingRestartLosesNoWrites`: each node stops and rejoins in turn
  while every node keeps accepting writes. Any failed write outside a
  restart window aborts (zero-downtime restart); after each restart the
  writers quiesce and digests must agree exactly; the mesh converges with
  identical digests at the end.
- `TestRollingRestartChurn`: six back-to-back restarts (nodes 1,2,3,1,2,3)
  with no quiesce between, maximizing handshake/attach churn while writes
  flow; regression test for the heal stall (restart N+1 while restart N
  still healing wedged one link forever).

## Pass criteria (same as Rust)

- 5-row baseline converges within 60 s.
- Zero failed writes outside restart windows (main test; the restarting
  node's writer pauses while it is down).
- After each restart: 3 s catch-up, then quiesce proves exact digest
  agreement within 150 s.
- Final: converged counts within 150 s (with settle re-sample), identical
  ordered digests on all nodes (divergence diagnostics on mismatch).

## Env knobs

None scenario-specific. Harness knobs apply: `MURMUR_LIVE_RUNTIME`,
`MURMUR_LIVE_FAILURES`, `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
`MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.

## Deviations from the Rust original

- In-process multi-`Database` cluster; restart is `Close` + reopen of the
  same directory with the same key, followed by an explicit re-mesh (fresh
  ephemeral port), same as Rust.
- Plain SQL replaces the typed-contention HTTP API: table
  `contention(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL)`; ordered
  `(id, name)` vecs replace the SHA-256 digest.
- Writers resolve the current-generation handle per write from a shared
  slot the driver refreshes after every restart.
- A deferred stop guard stops background writers on failure so the
  WaitGroup join cannot deadlock.
