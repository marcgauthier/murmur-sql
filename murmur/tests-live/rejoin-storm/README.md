# rejoin-storm (Go port)

Rust source: `/home/marc/MURMUR/tests-live/rejoin-storm/`
(`rejoin_storm.rs`, `TestDualKillSimultaneousRejoin`).

## What it proves

Two of three meshed nodes stop at once while the survivor keeps taking
tracked outage writes (`MURMUR_REJOIN_STORM_OUTAGE_ROWS`, default 40),
then both restart simultaneously behind a start barrier. Both must
rejoin un-wedged, every outage write must land everywhere, counts plus
ordered digests must match exactly, each node's rejoin path is
recorded, and post-rejoin writes on both revived nodes replicate
everywhere.

## Pass criteria (same as Rust)

- Positive control: baseline + full mesh + convergence within 30 s.
- The outage is real: both stopped nodes answer no queries.
- Survivor holds exactly `3 + outage` rows.
- Both nodes reach ready in under 60 s after the simultaneous restart.
- All outage writes present on all three with exact counts plus equal
  digests within 90 s; rejoin path recorded per node (never mandated).
- Post-rejoin writes on both revived nodes converge within 60 s.

## Deviations (inherited from Rust)

- In-process multi-`Database` cluster instead of daemon processes; the
  "kill" is `Close()` (**clean close, not SIGKILL**: killing one
  in-process node would kill the test driver); restart reopens the
  same directory with the same key.
- Plain SQL replaces the typed-records HTTP API: table
  `live_typed_records(name TEXT NOT NULL PRIMARY KEY)`.
- Peer counts come from the replication status (`Peers`), not
  `/v1/status`; ordered-vec equality replaces the joined-name digest.
- The outage-real probe checks that no open handle remains
  (`NodeInfo.DB == nil`) instead of an HTTP failure.
- The simultaneous restart uses the harness `StartNodesParallel`
  barrier (concurrent opens joined before rebinding); the 60 s
  no-wedge bound covers the joined restart, which bounds every node.
  One explicit re-mesh follows (fresh ephemeral ports).
- The rejoin path is inferred from the fresh post-restart log
  counters: a flat `(received, applied)` pair means snapshot, an
  advanced pair means log range repair.
