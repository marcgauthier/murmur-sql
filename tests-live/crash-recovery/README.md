# crash-recovery (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/crash-recovery/`
(`crash_recovery_test.go`,
`TestAbruptProcessDeathRecoversCommittedRows`).

## What it proves

Three meshed nodes take continuous application writes (two workers per
node) while node2 and then node3 are SIGKILLed mid-write and restarted
under load. Every acknowledged write survives both restarts with
identical digests on all nodes, and a post-recovery write replicates
everywhere.

## Pass criteria (same as Go)

- Baseline: one row per node converges with a full `[2, 2, 2]` mesh.
- Each SIGKILL lands with a write provably in flight on the victim
  (5 s gate); each victim rejoins under continuing load (`[2, 2, 2]`
  peers within 90 s).
- After both restarts: full convergence, every acknowledged write
  present everywhere, equal digests.
- A post-recovery write on node1 replicates everywhere with equal
  digests.

## Env knobs

- `MURMUR_CRASH_WRITE_SECONDS` (default 8): burst length between
  kill/restart legs.
- `MURMUR_CRASH_SETTLE_SECONDS` (default 90): convergence deadline.
- Harness knobs apply: `MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
  `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
  `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`, `MURMUR_SERVER_BIN`.

## Deviations from the Go original

- Multi-process `murmur-server` children over the PG wire (per design)
  instead of `testnode` daemon processes over HTTPS+mTLS.
- Plain SQL (`records(id INTEGER PRIMARY KEY, name TEXT NOT NULL)`,
  per-writer id strides) instead of the managed typed-records HTTP API.
  Row identity in assertions is `name@id`.
- Session counts come from `MURMUR SHOW REPLICATION` (`peers` column)
  instead of `/v1/status` (`connected_peers`).
- Digests are FNV-1a/64 over sorted names instead of SHA-256 (only
  cross-node equality within one process is asserted).
- Writers are goroutines sharing the cluster behind one mutex (the
  harness `KillNode`/`StartNode` legs need exclusive access, so writers
  cannot run across kills in a borrowed join shape).
  A `running` flag per node, cleared before each kill and re-checked
  under the lock, keeps writers off a stopped node (whose
  `Cluster::node` would panic); every insert carries a 10 s wire timeout
  so a kill can never wedge a writer on the lock. Refused writes during
  kill/restart windows are dropped like the Go original.
- `Cluster::kill_node`/`start_node` + `mesh()` replace
  `KillNode`/`StartNode`/`UnlockNode`/`WaitNodeReady` (unlock runs under
  the harness watchdog inside `start_node`).

## Compile check (only gate; never run here)

```sh
go test -count=1 ./tests-live/crash-recovery/
```
