# partial-mesh (Go port)

Rust source: `/home/marc/MURMUR/tests-live/partial-mesh/`
(`partial_mesh.rs`: `chain_peers_converge_via_forwarding`,
`dynamic_bootstrap_discovery`).

## What it proves

- Chain forwarding: three nodes peered only as a chain (node1-node2,
  node2-node3) still converge, with writes on node1 reaching node3 via
  forwarding by node2.
- Bootstrap discovery: three nodes using node 0 as a bootstrap seed
  discover one another with no explicit `AddPeer` calls, form a full
  mesh, and converge exactly on writes from all three origins.

## Pass criteria

- Chain leg (`partial-mesh-chain` runtime dir): 10 inserts on node 0
  converge (identical ordered names) on all 3 nodes within 60 s; no
  direct node1-node3 session exists in either direction; live peer
  counts are exactly {1, 2, 1}.
- Bootstrap leg (`partial-mesh-bootstrap` runtime dir): all 3 nodes
  discover 2 members each and reach 2 live peers each within 30 s with
  zero explicit `AddPeer` calls; 10 + 5 + 5 inserts (one batch per node)
  then converge exactly (identical ordered 20 names) on all 3 nodes
  within 150 s.

## Env knobs

None scenario-specific. Standard harness knobs apply:
`MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
`MURMUR_LIVE_UNLOCK_STALL_SECONDS`, `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.

## Deviations (inherited from Rust)

- In-process multi-`Database` cluster instead of daemon processes.
- Plain SQL (`records(id INTEGER PRIMARY KEY, name TEXT)`).
- Peer count maps to `Status().Peers`; direct-session probe maps to a
  `Connected` entry in `Status().PeersDiagnostics`.
- `Status().Members` excludes self, so the discovery gate is
  `members >= 2` (plus `peers == 2`) instead of `>= 3`.
- The chain leg disables gossip membership (static topology); the
  bootstrap leg keeps it enabled.
- The two legs use distinct runtime dirs so they never share or wipe one
  another's directory under parallel execution.
