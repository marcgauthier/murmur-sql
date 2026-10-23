# files-burst (Go scenario)

New Go scenario (no Rust original): concurrent multi-file fetch pressure.

## What it proves

A two-node encrypted mesh (file object key `0x53..`, 4 MiB file cap,
100 ms fetch interval) proves the file-fetch path sustains concurrent
load. Node 1 uploads a burst of N distinct files back-to-back (default
12, sizes 1 KiB..~200 KiB, single- and multi-chunk); node 2 must fetch
every object with byte-identical content. A second burst then proves
fetching still works afterwards (no wedged permits, no leaked fetch
state).

## Pass criteria

- `NewCluster` opens 2 meshed nodes under `tests-live/runtime/files-burst/`.
- Per wave: all N uploads succeed; within 60 s (50 ms poll) every file is
  `FileAvailable` on node 2 with matching digest and byte-identical
  downloads; `SearchFiles` returns exactly the N wave files.
- After both waves, node 2 latched no fetch error (`LastFetchError` nil).

## Env knobs

- `MURMUR_FILES_BURST_COUNT` (default 12, clamped to 3..64): files per wave.
- Standard harness knobs: `MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
  `MURMUR_LIVE_UNLOCK_STALL_SECONDS`, `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.

## Notes

- With the default two fetch permits, a 12-file burst needs several
  throttled rounds; an unconverged wave fails the 60 s bound.
- Exact permit accounting is pinned by the
  `TestFetchMissingPermitExhaustion` unit test in
  `murmur/replication/filefetch_test.go`; this scenario proves sustained
  end-to-end throughput under concurrency.
