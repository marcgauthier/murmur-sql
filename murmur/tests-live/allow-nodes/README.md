# allow-nodes (Go port)

Rust source: `/home/marc/MURMUR/tests-live/allow-nodes/`
(`allow_nodes.rs`: `allowed_node_ids_across_processes`).

## What it proves

Certificate validity alone does not authorize a peer: node2's identity
allow-list accepts only node1, while node1 and node3 accept every
identity. All three nodes write concurrently for
`MURMUR_ALLOW_NODES_WRITE_SECONDS` (default 180 s); the unauthorized
node3-to-node2 session must stay absent throughout, and afterwards every
node must hold all rows with identical ordered digests (node3's rows
reach node2 through node1).

## Pass criteria

- 3 nodes open under `tests-live/runtime/allow-nodes/`, with node2's
  `allowed_peers` set to `{node1}`.
- Initial topology `[2, 1, 1]` forms within 20 s (50 ms poll).
- Each node inserts `node{n}-write-{seq}` rows (one transaction per row)
  on a 100 ms cadence for the write window; at least 3 rows mesh-wide.
- Within 45 s (100 ms poll) every node holds all rows and the
  NUL-joined ordered digests are identical.
- No direct node2-node3 session exists in either direction, and the
  final topology is still `[2, 1, 1]` (2 s bound).

## Env knobs

- `MURMUR_ALLOW_NODES_WRITE_SECONDS` (default 180): length of the
  concurrent-write window.
- Standard harness knobs apply: `MURMUR_LIVE_RUNTIME`,
  `MURMUR_LIVE_FAILURES`, `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
  `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.

## Deviations (inherited from Rust)

- In-process multi-`Database` cluster (QUIC mesh, ephemeral `127.0.0.1:0`)
  instead of daemon processes.
- Plain SQL (`records(id INTEGER PRIMARY KEY, name TEXT)`, row ids
  node-disjoint via `(node+1)<<32 | seq`); canonical order comes from
  `ORDER BY name`.
- Live session counts come from `Status().Peers` and
  `PeersDiagnostics[].Connected` instead of `/v1/status` and
  `/v1/debug/peers`.
- Writers are goroutines sharing an `atomic.Bool` stop flag (plus
  per-node `atomic.Int64` counters and a first-error slot); a failed
  transaction or flush records the `node{n} write {seq}: {err}` message
  and stops that writer.
- Fixed distinct node ids instead of random values so the allow-list
  overlay can be built before the cluster opens.
