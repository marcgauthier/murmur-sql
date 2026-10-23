# files-rekey (Go scenario)

New Go scenario (no Rust original): file object-key rotation across a mesh.

## What it proves

A two-node encrypted mesh (generation-0 file object key `0x60..`, 4 MiB
file cap, 100 ms fetch interval) proves file generation rotation end to
end. Three files (100 B, 48 KiB, 200 KiB) replicate and fetch under
generation 0; then each node rolls to generation 1 (key `0x61..` plus
the retired key), the engine re-encrypts its objects, digests stay
stable, and the rotated mesh replicates and fetches normally, including
a post-rotation upload. A final leg drops the retired key on both nodes
once rotation status shows no old-generation metadata outstanding.

## Pass criteria

- `NewCluster` opens 2 meshed nodes under `tests-live/runtime/files-rekey/`.
- Baseline: all 3 files available and byte-identical on node 2 at
  generation 0; rotation status clean on both nodes.
- Rolling rotation per node (stop, set generation-1 key material,
  restart, re-mesh): metadata reaches generation 1, objects available,
  downloads byte-identical, digests unchanged (60 s bounds, 50 ms poll).
- Rotated mesh: generation-1 metadata everywhere, rotation status shows
  generation 1 with zero old-generation metadata missing locally, and a
  post-rotation upload from node 2 fetches on node 1 at generation 1.
- Drop-previous: both nodes reopen at generation 1 with no previous key
  (proving no old-generation metadata remains) and every file still
  downloads byte-identical.

## Env knobs

None scenario-specific. Harness knobs apply: `MURMUR_LIVE_RUNTIME`,
`MURMUR_LIVE_FAILURES`, `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
`MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.

## Notes

- Rotation re-encryption runs in the engine (automatic `RotateKeys` on
  the fetch tick); the scenario only moves key material via
  `SetFilesConfig` and waits for convergence.
- `rekey` covers the storage master key; this scenario covers the file
  object key, a separate rotation domain.
