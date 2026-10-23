# backup-under-fire (Go port)

Rust source: `/home/marc/MURMUR/tests-live/backup-under-fire/`
(`backup_under_fire.rs`: `backup_under_fire`).

## What it proves

A 2-node mesh absorbs sustained writes (25 ms ticker per node) for the
configured load window with **zero failed writes**, while a third member
takes repeated **online backups** (node never stops). A truncated backup
remnant must fail closed. After quiescence a fresh agent takes the final
backup. Every completed backup restores; intermediate snapshots are
non-empty, non-decreasing subsets, and the final backup matches the
survivors exactly.

## Env knobs

- `MURMUR_BACKUP_UNDER_FIRE_WRITE_SECONDS` (default 30): sustained-write
  window, same name and default as Rust.
- Standard harness knobs: `MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
  `MURMUR_LIVE_UNLOCK_STALL_SECONDS`, `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.

## Pass criteria

1. Both writers run the full window with zero failed writes.
2. Agent A (cluster node 3, unmeshed at boot) joins mid-load and converges
   on `max(node0 count, 100)` rows within 120 s.
3. Online backups b1, b2 from agent A's live handle verify cleanly.
4. A truncated remnant (copy of b1 with the manifest cut) is rejected by
   both verify and restore, and the failed restore publishes no target dir.
5. Survivors converge on every written row within 90 s of quiescence.
6. Agent B (fresh data dir, fresh node identity, fresh TLS leaf) joins,
   converges on the quiesced total within 120 s, takes b4, closes cleanly.
7. Restored b1/b2 are non-empty, non-decreasing, and subset-free of phantom
   rows; restored b4 equals the survivors exactly (count + ordered names).

## Deviations (inherited from Rust)

- Plain SQL on `live_typed_records(name TEXT NOT NULL PRIMARY KEY)`
  instead of the typed-records mesh API.
- In-process members instead of a backup-agent subprocess: agent A is
  cluster node 3 (opened unmeshed, meshed mid-load); agent B is a fresh
  out-of-band `murmur.Database` open.
- No literal SIGKILL-mid-backup: a truncated remnant must fail closed on
  verify + restore with no target dir left behind.
- Canonical ordered-names comparison instead of a digest.
