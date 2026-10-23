# files-prune (Go scenario)

New Go scenario (no Rust original): file tombstone pruning at scale.

## What it proves

A two-node encrypted mesh (file object key `0x59..`, 4 MiB file cap,
100 ms fetch interval) proves deleted file objects are reclaimed. Sixty
8 KiB files replicate; forty are deleted and their tombstones replicate;
forcing collection reclaims exactly the forty orphaned objects on both
nodes while the twenty survivors keep downloading byte-identical and
search returns exactly the survivors.

## Pass criteria

- `NewCluster` opens 2 meshed nodes under `tests-live/runtime/files-prune/`.
- Within 90 s (50 ms poll) the peer fetches all 60 files; then all 40
  tombstones replicate (metadata reads nil on the peer).
- After forced zero-grace collection on both nodes, each stores exactly
  20 objects, every survivor downloads byte-identical on both nodes, and
  search returns exactly the 20 survivors on both.

## Env knobs

None scenario-specific. Harness knobs apply: `MURMUR_LIVE_RUNTIME`,
`MURMUR_LIVE_FAILURES`, `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
`MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.

## Notes

- `delete-pruning` and `log-boundedness` prove reclamation for SQL
  state; this scenario proves it for the file object store.
