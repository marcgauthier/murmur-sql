# addrpolicy (Live SQL test)

Live SQL port of the `addrpolicy` scenario.

## What it proves

Address admission is enforced on the replication mesh: Node 1 admits
loopback peers (`127.0.0.0/8`), Node 2's TEST-NET policy
(`198.51.100.0/24`) excludes every actual loopback socket address, and
Node 3 is unrestricted. The admitted Node 1 - Node 3 pair forms a session
and replicates, while the denied Node 2 keeps its writes local.

## Pass criteria

- `Cluster::new` spawns 3 real `murmur-server` processes.
- Node 1 has `replication.allowed_networks = ["127.0.0.0/8"]`.
- Node 2 has `replication.allowed_networks = ["198.51.100.0/24"]`.
- Node 3 has unrestricted `allowed_networks` (empty/omitted).
- Replicated table `records(id INTEGER PRIMARY KEY, name TEXT NOT NULL)` created and enabled across nodes.
- One row inserted per node (`allowed-loopback`, `denied-testnet`, `unrestricted-loopback`).
- Nodes 1 and 3 converge on both admitted rows (`allowed-loopback`, `unrestricted-loopback`).
- After a settle window, Node 2 still holds only its local row (`denied-testnet`).
- Node 1 holds only the 2 admitted rows.
- Peer topology checked via `MURMUR SHOW PEERS` confirms Node 2 has no connected peers.

## How to run

```sh
MURMUR_SERVER_BIN=../../target/debug/murmur-server cargo test --manifest-path tests-live/addrpolicy/Cargo.toml -- --nocapture
```
