# three-node-sync (Go port)

Rust source: `/home/marc/MURMUR/tests-live/three-node-sync/`
(`three_node_sync.rs`).

## What it proves

Three encrypted, mutually authenticated nodes each commit 30 uniquely-named
rows concurrently; every node must converge on all 90 rows in canonical
ordered sequence within 25 s.

## Pass criteria

- `NewCluster` opens 3 meshed nodes under `tests-live/runtime/three-node-sync/`.
- Each node inserts `node-{i}-row-{00..29}` (one transaction per row).
- Within 25 s (100 ms poll), `SELECT name FROM records ORDER BY name` on
  every node equals the full 90-name canonical list.

## Env knobs

None scenario-specific. Harness knobs apply: `MURMUR_LIVE_RUNTIME`,
`MURMUR_LIVE_FAILURES`, `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
`MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.

## Deviations from the Rust original

- In-process multi-`Database` cluster (QUIC mesh, ephemeral `127.0.0.1:0`),
  same as Rust.
- Plain SQL (`records(id INTEGER PRIMARY KEY, name TEXT)`) instead of the
  managed typed-records HTTP API; canonical order comes from `ORDER BY name`.
- Concurrency via goroutines + a start channel (one worker per node) instead
  of `std::thread::scope` + a start `Barrier`.
