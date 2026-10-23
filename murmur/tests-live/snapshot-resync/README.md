# snapshot-resync (Go port)

Rust source: `/home/marc/MURMUR/tests-live/snapshot-resync/`
(`snapshot_resync.rs`: `stale_node_rejoins_via_snapshot`,
`dual_stale_nodes_rejoin_via_snapshot`,
`stale_node_rejoins_via_single_snapshot_source`).

## What it proves

Three nodes mesh with aggressive log retention (1 s retention, 20 s offline
pin, 10 retained batches). One node stops while the survivors write past
retention and GC expiry, then restarts with the same durable directory: its
needed log ranges are unrecoverable, so it must rejoin by merging a snapshot
without discarding its acknowledged pre-stop state. A second test stops two
nodes and restarts both back-to-back so their snapshot requests overlap on
the survivor. A multi-source case gives the stale node three snapshot
sources concurrently over a 6,000-row snapshot.

## Pass criteria

- 3 nodes mesh; node3 writes 5 `stale-*` rows; all converge (60 s).
- Node3 stops; node1 writes 60 `fresh-*` rows; survivors converge on 65
  rows (30 s each).
- After the member-deadline wait, each survivor prunes >= 40 transactions
  and no longer pins node3 (proves the rejoin must use the snapshot path,
  since only the newest 10 per origin survive).
- Node3 restarts with the same durable directory; all converge on 65 rows
  with equal digests (90 s) while node3's log-path growth stays within
  the retained tail and far below the gap (proves the bulk arrived via
  snapshot, which bypasses those counters);
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
  wait before the collection passes. This port drives retention with
  explicit `CollectRetention` passes instead of waiting out the engine's
  60 s collection interval.
- Standard harness knobs apply: `MURMUR_LIVE_RUNTIME`,
  `MURMUR_LIVE_FAILURES`, `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
  `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.

## Deviations (inherited from Rust)

- In-process multi-`Database` cluster (QUIC mesh, ephemeral `127.0.0.1:0`)
  instead of daemon processes.
- Plain SQL (`records(id INTEGER PRIMARY KEY, name TEXT)`); digests are
  newline-joined names.
- Close/reopen replaces process stop/start; `Mesh()` re-dials after each
  restart because the node rebinds a fresh ephemeral port.
- Snapshot trust is configured up front (every node trusts every node);
  the empty-trust default is unchanged, so without this overlay no leg
  could request a snapshot.
- No snapshot metrics exist, so the counter legs map to behavior:
  "rows converged while the receiver's log growth stayed within the
  retained tail and far below the gap".
- Survivor writes flush per write/batch: one replication log entry is
  minted per flush batch, and retention needs dozens of entries to prune
  past.
- The collection proof is explicit (`CollectRetention` passes plus a
  retention-pin age assertion per survivor/victim pair) rather than a
  fixed sleep, but the member-deadline wait is kept.
