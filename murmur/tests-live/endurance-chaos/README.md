# endurance-chaos (Go port)

Rust source: `/home/marc/MURMUR/tests-live/endurance-chaos/`
(`endurance_chaos.rs`: `endurance_chaos`).

## What it proves

Long-duration combined-fault validation: a live multi-node mesh absorbs
continuous writes while rolling restarts, peer flaps, disk pressure, online
key rotation, snapshot resyncs, and origin-log GC all overlap for a
configured chaos window. The verdict is exact cross-node convergence (equal
row counts plus identical PK-ordered row contents) with counter-proven GC
progress on every node, at least one proven snapshot resync, successful key
rotations, a rejoined full mesh, and a post-chaos write that converges
everywhere.

## Env knobs

Same names and defaults as Rust:

- `MURMUR_ENDURANCE_NODES` (default 5): mesh size, clamped to 3..16.
- `MURMUR_ENDURANCE_DURATION_SECONDS` (default 600): chaos window.
- `MURMUR_ENDURANCE_SETTLE_SECONDS` (default 300): max convergence wait.
- `MURMUR_ENDURANCE_WRITE_INTERVAL_MS` (default 25): pacing per writer
  (up to 8 writers).
- `MURMUR_ENDURANCE_MAX_ROWS` (default 10000): live-id ceiling; past it
  writers recycle via updates/deletes (floor 100).
- `MURMUR_ENDURANCE_SEED` (default 1): RNG seed.
- `MURMUR_ENDURANCE_STATUS_SECONDS` (default 30): heartbeat interval.
- `MURMUR_ENDURANCE_RESTART_SECONDS` (default 90) /
  `MURMUR_ENDURANCE_RESTART_DOWN_SECONDS` (default 8).
- `MURMUR_ENDURANCE_ROTATE_SECONDS` (default 180).
- `MURMUR_ENDURANCE_SNAPSHOT_SECONDS` (default 300) /
  `MURMUR_ENDURANCE_SNAPSHOT_OFFLINE_SECONDS` (default 75, minimum 60).
- `MURMUR_ENDURANCE_IMPAIR_ON_SECONDS` (default 60) /
  `MURMUR_ENDURANCE_IMPAIR_OFF_SECONDS` (default 20);
  `MURMUR_ENDURANCE_IMPAIR_LATENCY_MS` / `MURMUR_ENDURANCE_IMPAIR_LOSS_PCT`
  are parsed and logged but have no effect (flap-only).
- `MURMUR_ENDURANCE_DISK_SECONDS` (default 180) /
  `MURMUR_ENDURANCE_DISK_MB` (default 128) /
  `MURMUR_ENDURANCE_DISK_HOLD_SECONDS` (default 60).
- `MURMUR_ENDURANCE_SKEW_SECONDS` (default 120): parsed and logged but the
  skew leg is degraded away.
- Standard harness knobs: `MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
  `MURMUR_LIVE_UNLOCK_STALL_SECONDS`, `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.

Quick smoke (via `tests-live/run.sh`, never directly):

```sh
MURMUR_ENDURANCE_NODES=3 MURMUR_ENDURANCE_DURATION_SECONDS=180 \
MURMUR_ENDURANCE_SETTLE_SECONDS=120 \
  bash tests-live/run.sh endurance-chaos
```

## Pass criteria

- Baseline row converges everywhere before chaos starts.
- At least one write is acked (anti-vacuous guard).
- After chaos stops, every node is reachable with equal row counts and
  identical PK-ordered row contents within settle.
- GC ran and collected on every node (restart-proof high-water history)
  with no GC-attributed engine error ever observed.
- At least one restart, rotation, impairment cycle, disk hold (unless
  degraded for space), and snapshot window (unless the run is too short)
  executed; at least one snapshot resync proved via the snapshot path.
- A full N-1 peer mesh is back, and a post-chaos write converges
  everywhere.

## Deviations (inherited from Rust)

- Plain SQL `contention` table; FNV-1a log digest instead of sha256.
- Every restart is in-process close + reopen.
- tc/netem degrades to logical peer flaps; clock skew always skipped.
- Rotation is `RotateMasterKey` with a fresh random key; no key IDs.
- Snapshot trust configured up front (every node trusts every node).
- Snapshot proof is event-driven off `snapshots_applied` (up to 240 s per
  window, writers still running).
- GC failures match GC-substring errors only.
- Report written to `runtime/endurance-chaos/` (retained under
  `tests-live/failures/` on FAIL).
