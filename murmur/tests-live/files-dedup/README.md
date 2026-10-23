# files-dedup (Go scenario)

New Go scenario (no Rust original): identical file content deduplicates.

## What it proves

A two-node encrypted mesh (file object key `0x56..`, 4 MiB file cap,
100 ms fetch interval) proves content-addressed file storage. The same
32 KiB bytes uploaded under three names share one digest and one stored
object (`*.spfo`) on each node; every name fetches byte-identical on
the peer. Deleting one name replicates its tombstone while the surviving
names keep downloading from the shared object, and the same content
uploads fine under a new name afterwards.

## Pass criteria

- `NewCluster` opens 2 meshed nodes under `tests-live/runtime/files-dedup/`.
- All three uploads report the same digest; each node stores exactly one
  object.
- Within 60 s (50 ms poll) every name is available and byte-identical on
  the peer with the shared digest.
- After deleting one name, its tombstone replicates (metadata reads nil)
  while the surviving names still download byte-identical, and a new
  name uploads without error.

## Env knobs

None scenario-specific. Harness knobs apply: `MURMUR_LIVE_RUNTIME`,
`MURMUR_LIVE_FAILURES`, `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
`MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.
