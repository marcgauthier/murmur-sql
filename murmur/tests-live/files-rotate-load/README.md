# files-rotate-load (Go scenario)

New Go scenario (no Rust original): file key rotation amid writes.

## What it proves

A two-node encrypted mesh (generation-0 file object key `0x62..`, 4 MiB
file cap, 100 ms fetch interval) proves file generation rotation
converges while writes keep landing. Three 3-file batches interleave
with a rolling rotation to key `0x63..`: batch 0 pre-rotation on node 1,
batch 1 on the rotated node 1 while node 2 still runs generation 0,
batch 2 post-rotation on node 2. Every file settles at generation 1 on
both nodes, available and byte-identical, with clean rotation status.

## Pass criteria

- `NewCluster` opens 2 meshed nodes under
  `tests-live/runtime/files-rotate-load/`.
- Per rotation leg (stop, set generation-1 key material, restart,
  re-mesh): within 60 s (50 ms poll) the node's files reach generation
  1, available and byte-identical.
- Final: all 9 files at generation 1, available and byte-identical on
  both nodes, with rotation status clean (generation 1, no old metadata
  outstanding, no staged transfers) on both.

## Env knobs

None scenario-specific. Harness knobs apply: `MURMUR_LIVE_RUNTIME`,
`MURMUR_LIVE_FAILURES`, `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
`MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.

## Notes

- `files-rekey` proves rotation on a quiet mesh plus dropping the
  retired key; this scenario proves convergence when uploads interleave
  with the roll, including generation-1 writes replicating to a
  not-yet-rotated peer.
