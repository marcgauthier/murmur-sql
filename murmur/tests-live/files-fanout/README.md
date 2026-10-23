# files-fanout (Go scenario)

New Go scenario (no Rust original): one-to-many file fanout.

## What it proves

An encrypted mesh with one uploader and several simultaneous fetchers
(file object key `0x54..`, 4 MiB file cap, 100 ms fetch interval) proves
the file-serve path sustains concurrent load. Node 1 uploads two files
(48 KiB, 256 KiB); every other node must fetch both with byte-identical
content. Serve and fetch share two permits, so fetchers queue and retry
across several rounds, and each fetcher's staging drains once done.

## Pass criteria

- `NewCluster` opens N meshed nodes under `tests-live/runtime/files-fanout/`.
- Within 90 s (50 ms poll) every fetcher has both files `FileAvailable`
  with matching digests and byte-identical downloads.
- Every fetcher's staging drains to zero afterwards (no stranded partials
  or identity sidecars).

## Env knobs

- `MURMUR_FILES_FANOUT_NODES` (default 5, clamped to 3..8): mesh size
  (one uploader plus N-1 fetchers).
- Standard harness knobs: `MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
  `MURMUR_LIVE_UNLOCK_STALL_SECONDS`, `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.

## Notes

- Refused permits surface as transient fetch errors and retries under
  fanout pressure, so only convergence and content are asserted, not an
  error-free run (unlike the two-node `files-soak` gate).
