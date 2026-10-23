# crash-recovery (Go port)

Rust source: `/home/marc/MURMUR/tests-live/crash-recovery/`
(`crash_recovery.rs`: `TestAbruptProcessDeathRecoversCommittedRows`).

## What it proves

Two nodes are abruptly restarted at different times while background
writers (2 workers x 3 nodes, 50 ms tick) hammer all nodes. Every
acknowledged write must survive on every node with equal contents, plus
a post-recovery write that replicates everywhere.

Pass criteria: one baseline write per node; full mesh (`[2, 2, 2]`
peers) and baseline convergence within 20 s; each restart lands only
with a write provably in flight on the victim (5 s gate); after each
restart + re-mesh, full peering returns within 90 s under continuing
load; after writers stop, convergence within
`MURMUR_CRASH_SETTLE_SECONDS` (default 60), every acknowledged value
present on every node, equal contents on all nodes; a
`post-recovery-write` then converges (settle default 20); write windows
honor `MURMUR_CRASH_WRITE_SECONDS` (default 12).

## Env knobs

- `MURMUR_CRASH_WRITE_SECONDS` (default 12): crash-window write
  bursts.
- `MURMUR_CRASH_SETTLE_SECONDS` (defaults 60, then 20): convergence
  settle windows.
- Standard harness knobs (`MURMUR_LIVE_RUNTIME`,
  `MURMUR_LIVE_FAILURES`, `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
  `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`).

## Deviations (inherited from Rust)

- In-process multi-`Database` cluster instead of daemon processes;
  restart is `Close` + reopen of the same directory with the same key
  (**clean close, not SIGKILL**: killing one in-process node would kill
  the test driver). Abrupt mid-write death is therefore not exercised;
  durability of acknowledged writes across restart is.
- Plain SQL replaces the typed-records HTTP API: table
  `live_typed_records(name TEXT NOT NULL PRIMARY KEY)`.
- Peer counts come from the replication status, not `/v1/status`.
- Ordered-vec equality replaces the SHA-256 name digest.
- Writers resolve the current-generation handle per write from a shared
  slot the driver refreshes after every restart. Explicit re-mesh after
  each restart (fresh ephemeral port).
