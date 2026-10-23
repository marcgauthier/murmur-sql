# files-resume (Go scenario)

New Go scenario (no Rust original): interrupted file burst resumes.

## What it proves

A two-node encrypted mesh (file object key `0x5B..`, 4 MiB file cap,
100 ms fetch interval) proves an interrupted burst of larger files
converges without stranded state. Node 1 uploads ten 1 MiB files; once
node 2 has partials in flight (staging non-empty, 10 ms poll), node 2
restarts and re-meshes. Every file must converge byte-identical with
drained staging and clean rotation status: no orphaned partials or
identity sidecars survive the interruption.

## Pass criteria

- `NewCluster` opens 2 meshed nodes under `tests-live/runtime/files-resume/`.
- Staging goes non-empty on node 2 within 120 s (partials in flight).
- After the fetcher restart and re-mesh: within 120 s (50 ms poll)
  every file is available with matching digest and byte-identical
  downloads, staging drains to zero, and rotation status reports the
  previous key unused.

## Env knobs

None scenario-specific. Harness knobs apply: `MURMUR_LIVE_RUNTIME`,
`MURMUR_LIVE_FAILURES`, `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
`MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.

## Notes

- `files-crash` kills the fetcher mid-transfer for one file; this
  scenario restarts it mid-burst and additionally proves no staging
  orphan survives across ten resumed transfers.
