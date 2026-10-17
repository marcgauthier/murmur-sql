# dbid-isolation (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/dbid-isolation/`
(`README.md` + `dbid_isolation_test.go`:
`TestCrossDatabasePeersNeverConnect`).

## What it proves

Nodes from two different databases (distinct DBIDs) never peer with
each other, even when the TLS layer would admit them. Two independent
2-node clusters with byte-identical schema share CA trust (so TLS
cannot be the refusal cause), then are cross-peered in both directions:
every cross edge must stay disconnected for the observation window,
markers must never cross, identity refusals must be observed on all
nodes, and both clusters must stay internally healthy.

## Pass criteria

- `Cluster::new` opens clusters `dbid-iso-a` and `dbid-iso-b` (2 nodes
  each) under `tests-live/runtime/dbid-iso-*/` with distinct DBIDs
  (asserted; the test is vacuous otherwise).
- CA trust is exchanged (`ca_der` + `add_trust_root`, then all nodes
  restart and re-mesh), mirroring Go `crossTrustCAs`.
- Positive controls: one marker per cluster converges internally
  within 60 s, and the internal peer shows `Connected=true` within
  30 s.
- Cross-`add_peer` runs in both directions (full bipartite). Over the
  `MURMUR_DBID_ISO_WINDOW_S` window (minimum 10 s, 500 ms poll) every
  cross entry stays absent or `Connected=false`, no node reports more
  than its one internal live session, and each node holds only its own
  cluster's marker.
- An identity refusal (`last_error` containing `incompatible`, i.e. the
  accept-side `Auth("incompatible hello")` from the Hello/Welcome
  `database == db` check) is observed on all four nodes, with a 30 s
  grace wait after the window (Go: `waitMetricAdvanced`).
- Cleanup removes every cross edge; both clusters still converge
  internally and report no cross connection.

## Env knobs

- `MURMUR_DBID_ISO_WINDOW_S` (default 10, minimum 10, Go parity):
  length of the isolation observation window.
- Harness knobs apply: `MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
  `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
  `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.

## Deviations from the Go original

- Real SQL servers (`murmur-server` spawned as child processes, PostgreSQL wire protocol)
  managed via `murmur-live-sql-harness`. Cross-database peering is configured via
  `config.toml` bootstrap lists and shared CA certificates. Cross edges are dialed
  directly through QUIC replication endpoints.
- Plain SQL (`records(id INTEGER PRIMARY KEY, name TEXT)`,
  `cluster-a-marker` / `cluster-b-marker`) instead of the managed
  typed-records HTTP API (`TypedInsert`, `TypedNames`); canonical
  order comes from `ORDER BY name`.
- Live session state comes from the replication
  `Handle::status().peers` and `peers_diagnostics[].connected`
  instead of `/v1/status` `connected_peers` and `/v1/debug/peers`.
- The Rust product has no per-cause handshake-refusal counters
  (`ReplicationMetrics` exposes no refusal fields), so the Go
  `identity_refusals advance on all nodes` leg is ported as a latched
  `last_error` probe: every accepted cross-DBID dial records
  `Auth("incompatible hello")`, and the engine's 5 s seed-redial tick
  recurs those dials (hence refusals) for as long as the cross edge is
  configured. Sampling every 500 ms through the >= 10 s window plus
  the 30 s grace wait observes at least one refusal per node. This
  doubles as the Go anti-vacuity guard: had TLS trust failed, dials
  would die with TLS errors instead of `incompatible` and the test
  would fail.
- The Go `schema_refusals stay flat` leg has no Rust counter to read;
  it is covered by construction (byte-identical `migrate` on all four
  nodes, so schema can never be the refusal cause) plus the internal
  convergence positive controls. With shared TLS trust, identical
  schemas, and one product `VERSION`/dissemination mode, the DBID
  check is the only reachable refusal point.
- Fixed distinct DBIDs (`[0xA5; 16]`, `[0xB6; 16]`) and node ids
  (`[0xA1/0xA2; 16]`, `[0xB1/0xB2; 16]`) instead of random values so
  no cross-cluster identity can alias; the differing-DBIDs assertion
  is kept.
- Shared-harness addition required by this scenario (no other way to
  let cross dials pass TLS): `Cluster::ca_der()` and
  `Cluster::add_trust_root(der)` in
  `tests-live/harness/src/cluster.rs`, documented in
  `tests-live/README.md` section 9.

## Compile check (only gate; never run here)

```sh
cargo test --no-run --manifest-path /home/marc/MURMUR/tests-live/dbid-isolation/Cargo.toml
```
