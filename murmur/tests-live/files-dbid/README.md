# files-dbid (Go scenario)

New Go scenario (no Rust original): file endpoint database identity.

## What it proves

A two-node encrypted mesh (file object key `0x5A..`, 4 MiB file cap,
100 ms fetch interval) plus a raw file-protocol client proves the file
endpoint enforces database identity. After an ordinary mesh fetch
positive control, a client speaking the file frames over a fresh mTLS
connection: with the cluster's own id streams the exact stored
container bytes; with a foreign id (and a bogus digest) is refused
before any object lookup — no header, no `FileMissing`, the stream
tears down; with the own id and a missing digest still reports
`FileMissing`.

## Pass criteria

- `NewCluster` opens 2 meshed nodes under `tests-live/runtime/files-dbid/`.
- A 48 KiB file replicates and fetches through the product path.
- Raw-protocol probes (30 s each, deterministic, no sleeps): own-id
  request streams bytes equal to the on-disk container; foreign-id
  request yields a connection error, never a frame; own-id missing
  request yields `FileMissing`.

## Env knobs

None scenario-specific. Harness knobs apply: `MURMUR_LIVE_RUNTIME`,
`MURMUR_LIVE_FAILURES`, `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
`MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.

## Notes

- `dbid-isolation` proves replication sessions never cross databases
  (so file sources can never span them either); this scenario proves
  the file endpoint's own identity check for direct dials.
- The probe pins the `murmur-sqlite/files/1` ALPN and the
  check-before-lookup ordering in `serve()`.
