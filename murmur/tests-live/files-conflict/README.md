# files-conflict (Go scenario)

New Go scenario (no Rust original): concurrent same-name file conflict.

## What it proves

A two-node encrypted mesh (file object key `0x57..`, 4 MiB file cap,
100 ms fetch interval) proves same-name file conflicts converge.
Partitioned, each node uploads different 8 KiB bytes under one name;
after healing, both nodes must agree on one winning digest whose
content downloads byte-identical on both, with no phantom second file.
Either side may win; agreement and integrity are asserted, not which.

## Pass criteria

- `NewCluster` opens 2 meshed nodes under `tests-live/runtime/files-conflict/`.
- After the partition takes effect (both sessions down), conflicting
  uploads (20 ms apart for distinct timestamps), and heal: within 60 s
  (50 ms poll) both nodes report the same non-nil digest for the name.
- The winner downloads byte-identical on both nodes, matches one of the
  two uploaded contents, and `SearchFiles` returns exactly the one file
  on both nodes.

## Env knobs

None scenario-specific. Harness knobs apply: `MURMUR_LIVE_RUNTIME`,
`MURMUR_LIVE_FAILURES`, `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
`MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.

## Notes

- `crdt-contention` proves conflict convergence for SQL cells; this
  scenario proves it for file metadata, which resolves in the record
  layer rather than the SQL layer.
