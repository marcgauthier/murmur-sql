# files-source-restart (Go scenario)

New Go scenario (no Rust original): file source restarts mid-transfer.

## What it proves

A two-node encrypted mesh (file object key `0x58..`, 64 MiB file cap,
100 ms fetch interval) proves a fetcher survives its source restarting
mid-transfer. Node 1 uploads a 32 MiB file; once node 2's fetch is
observably in flight (staging non-empty, 10 ms poll), node 1 restarts
(new replication and file-endpoint ports) and re-meshes. Node 2 must
re-resolve the source and converge on byte-identical content with
drained staging.

## Pass criteria

- `NewCluster` opens 2 meshed nodes under
  `tests-live/runtime/files-source-restart/`.
- Metadata arrives on node 2, then staging goes non-empty (fetch in
  flight) within 120 s.
- After a 2 s source outage, restart, and re-mesh: within 120 s (50 ms
  poll) the object is available on node 2 with the upload digest and
  byte-identical downloads, and staging drains to zero.

## Env knobs

None scenario-specific. Harness knobs apply: `MURMUR_LIVE_RUNTIME`,
`MURMUR_LIVE_FAILURES`, `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
`MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.

## Notes

- `files-crash` kills the fetcher mid-transfer; this scenario restarts
  the source, exercising stale source addresses and fetch re-resolution.
