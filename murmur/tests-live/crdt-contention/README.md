# crdt-contention (Go port)

Rust source: `/home/marc/MURMUR/tests-live/crdt-contention/`
(`crdt_contention.rs`:
`multi_process_disjoint_and_shared_cell_contention`).

## What it proves

Three encrypted, mutually authenticated nodes start from one replicated row
(`03141592-...`, name/phone `baseline`, score `0`), apply concurrent
disjoint-column updates from all nodes (name on node 1, phone on node 2,
score on node 3 — all must survive), then apply 40 competing updates per
node to the shared name cell. The shared-cell LWW winner must be identical
on every node, the untouched cells must keep their disjoint values, and the
full logical rows must match.

## Pass criteria

- `NewCluster` opens 3 meshed nodes under
  `tests-live/runtime/crdt-contention/`; each reports 2 live peers within
  20 s.
- Baseline row inserted on node 1 is visible on all nodes within 20 s.
- After the concurrent disjoint updates, every node reads
  `("node1-name", "node2-phone", 42)` within 20 s.
- After 120 competing name updates, every node agrees on one non-empty
  winner from the `node{N}-cell-{seq:03}` write set within 30 s, with phone
  `node2-phone` and score `42` intact.
- Final full-row reads are equal across all three nodes.

## Env knobs

None scenario-specific. Harness knobs apply: `MURMUR_LIVE_RUNTIME`,
`MURMUR_LIVE_FAILURES`, `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
`MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.

## Deviations from the Rust original

- In-process multi-`Database` cluster (QUIC mesh, ephemeral `127.0.0.1:0`),
  same as Rust.
- Plain SQL (`contention(id TEXT NOT NULL PRIMARY KEY, name TEXT, phone TEXT,
  score INTEGER)`); contention resolution is whatever the replicated-SQL
  layer converges to, with unchanged pass criteria (disjoint survival, single
  identical winner, full-row equality).
- Concurrency via goroutines + a start channel (one worker per node) instead
  of `std::thread::scope` + a start `Barrier`.
- Peer-mesh wait reads replication status peers diagnostics (`connected`
  flags).
