# files-boundary (Go scenario)

New Go scenario (no Rust original): boundary-size file replication.

## What it proves

A two-node encrypted mesh (file object key `0x5C..`, 1 MiB file cap,
100 ms fetch interval) proves file size boundaries behave: empty,
1-byte, header-sized (40 B), 64 KiB -/+1, exactly 64 KiB, and exactly
the 1 MiB cap all replicate byte-identical with exact metadata sizes,
while one byte over the cap fails cleanly with nothing published.

## Pass criteria

- `NewCluster` opens 2 meshed nodes under `tests-live/runtime/files-boundary/`.
- All seven boundary uploads succeed with exact byte counts; within
  60 s (50 ms poll) every file is available on the peer with exact
  metadata size, matching digest, and byte-identical downloads.
- The over-cap upload returns an error and publishes no metadata.

## Env knobs

None scenario-specific. Harness knobs apply: `MURMUR_LIVE_RUNTIME`,
`MURMUR_LIVE_FAILURES`, `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
`MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.
