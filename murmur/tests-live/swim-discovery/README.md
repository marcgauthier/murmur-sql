# swim-discovery (Go port)

Rust source: `/home/marc/MURMUR/tests-live/swim-discovery/`
(`swim_discovery.rs`).

## What it proves

- Seed-only discovery: four nodes start with a single SWIM seed address
  and no static peers; a full mesh must form purely via discovery
  (membership view plus a live replication session to every peer), and
  writes from every node must converge with identical contents.
- Seed kill and rediscovery: after baseline convergence the seed is
  stopped, survivors must observe the death and keep converging writes
  while it is down, then the restarted seed rejoins through one live
  member and the full mesh reconverges with identical contents.

## Pass criteria (same as Rust)

- Discovery leg (`swim-discovery` runtime dir): all 4 nodes see 3
  discovered members each and hold a live session to every other node
  (all 12 directed pairs) within `MURMUR_SWIM_DISCOVERY_SECONDS`
  (default 60 s); 10 inserts per node (40 rows) then converge exactly
  (identical ordered names) on all 4 nodes within 150 s.
- Rediscovery leg (`swim-rediscovery` runtime dir): same discovery
  gates; 8 baseline rows on node 3 converge on all 4 nodes; after the
  seed stops, all 3 survivors drop below 3 live sessions each within
  the discovery bound; 12 survivor rows (round-robin over nodes 2-4)
  converge on the 3 survivors; after the seed restarts and rejoins via
  node 2, membership, full session mesh, and all 20 rows reconverge on
  all 4 nodes.

## Env knobs

- `MURMUR_SWIM_DISCOVERY_SECONDS` (default 60): bound for the
  membership, session-mesh, and death-observed gates (same name as Rust).
  Convergence waits keep the 150 s loaded-box bound (fixed).
  Harness knobs apply: `MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
  `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
  `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.

## Deviations from the Rust original

- In-process multi-`Database` cluster (QUIC mesh, ephemeral `127.0.0.1:0`),
  same as Rust.
- Plain SQL (`records(id INTEGER PRIMARY KEY, name TEXT)`); row ids are
  node-disjoint small integers.
- `Status.Members` counts only discovered peers (excludes the local node),
  so the discovery gate is `members == 3`.
- The direct-session probe maps to a `Connected` entry for that node id in
  `Status().PeersDiagnostics`; the session-mesh gate checks all 12 directed
  pairs exactly like the original.
- Seed "kill" is `StopNode` (`Close()`) rather than SIGKILL; survivors
  observe the death via live session teardown (`Peers` drops 3 -> 2).
- The rejoin is one `AddPeer(0, 1)` after `StartNode`: the restarted seed
  dials a single live member and gossip plus discovery dialing rebuild the
  rest of the mesh; no full `Mesh()` call is made, so the re-formed mesh is
  still discovered, not configured.
- The two legs use distinct runtime dirs (`swim-discovery`,
  `swim-rediscovery`) so the two tests never share or wipe one another's
  directory under parallel execution.
