# dbid-isolation (Go port)

Rust source: `/home/marc/MURMUR/tests-live/dbid-isolation/`
(`dbid_isolation.rs`: `cross_database_peers_never_connect`).

## What it proves

Nodes from two different databases (distinct DBIDs) never peer with
each other, even when the TLS layer would admit them. Two independent
2-node clusters with byte-identical schema share CA trust (so TLS
cannot be the refusal cause), then are cross-peered in both directions:
every cross edge must stay disconnected for the observation window,
markers must never cross, identity refusals must be observed on all
nodes, and both clusters must stay internally healthy.

## Pass criteria

- `NewCluster` opens clusters `dbid-iso-a` and `dbid-iso-b` (2 nodes
  each) under `tests-live/runtime/dbid-iso-*/` with distinct DBIDs
  (asserted; the test is vacuous otherwise).
- CA trust is exchanged (`CaDER` + `AddTrustRoot`, then all nodes
  restart and re-mesh), mirroring `crossTrustCAs`.
- Positive controls: one marker per cluster converges internally
  within 60 s, and the internal peer shows `Connected=true` within
  30 s.
- Cross-`AddPeer` runs in both directions (full bipartite). Over the
  `MURMUR_DBID_ISO_WINDOW_S` window (minimum 10 s, 500 ms poll) every
  cross entry stays absent or `Connected=false`, no node reports more
  than its one internal live session, and each node holds only its own
  cluster's marker.
- An identity refusal (`last_error` containing `incompatible`, i.e. the
  accept-side `Auth("incompatible hello")` from the Hello/Welcome
  `database == db` check) is observed on all four nodes, with a 30 s
  grace wait after the window.
- Cleanup removes every cross edge; both clusters still converge
  internally and report no cross connection.

## Env knobs

- `MURMUR_DBID_ISO_WINDOW_S` (default 10, minimum 10):
  length of the isolation observation window.
- Harness knobs apply: `MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
  `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
  `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.

## Deviations from the Rust original

- In-process multi-`Database` clusters (QUIC mesh, ephemeral
  `127.0.0.1:0`) like the Rust port (per design). Cross edges are dialed
  directly through the replication `Handle.AddPeer(addr)` /
  `RemovePeer(nodeID)` with the peer cluster's bound address, since
  `Cluster.AddPeer` only addresses intra-cluster indices.
- Plain SQL (`records(id INTEGER PRIMARY KEY, name TEXT)`,
  `cluster-a-marker` / `cluster-b-marker`); canonical order comes from
  `ORDER BY name`.
- Live session state comes from the replication
  `Status().Peers` and `PeersDiagnostics[].Connected`.
- The product has no per-cause handshake-refusal counters, so the
  `identity_refusals advance on all nodes` leg is ported as a latched
  `last_error` probe: every accepted cross-DBID dial records
  `Auth("incompatible hello")`, and the engine's 5 s seed-redial tick
  recurs those dials (hence refusals) for as long as the cross edge is
  configured. A 10 ms background recorder on every node captures the
  latch transitions; sampling through the >= 10 s window plus the 30 s
  grace wait observes at least one refusal per node. This doubles as
  the anti-vacuity guard: had TLS trust failed, dials would die with
  TLS errors instead of `incompatible` and the test would fail.
- Fixed distinct DBIDs (`[0xA5; 16]`, `[0xB6; 16]`) and node ids
  (`[0xA1/0xA2; 16]`, `[0xB1/0xB2; 16]`) instead of random values so
  no cross-cluster identity can alias; the differing-DBIDs assertion
  is kept.
