# plumtree-live (Go port)

Rust source: `/home/marc/MURMUR/tests-live/plumtree-live/`
(`plumtree_live.rs`: `plumtree_mesh_converges`,
`mixed_mode_refuses_gossip_peer`).

## What it proves

- `TestPlumtreeMeshConverges`: three nodes in Plumtree dissemination mode
  converge end to end (10 rows from node 0 on every node within 60 s).
- `TestMixedModeRefusesGossipPeer`: two Plumtree nodes meshed with one
  gossip-mode node — the Plumtree pair converges on its 10 rows while the
  gossip node stays isolated in both directions (10/10/1 row counts after a
  5 s leak window), with handshake refusals recorded.

## Pass criteria

- All-Plumtree mesh: `SELECT name FROM records ORDER BY name` on every node
  equals the 10 `plum-{0..9}` names within 60 s (200 ms poll).
- Mixed mesh: nodes 0/1 reach 10 rows within 30 s; after the leak window the
  counts are exactly 10/10/1.
- At least one node records a handshake refusal (`Status().LastError`
  contains `incompatible`); the gossip node holds 0 replication sessions
  while each Plumtree node holds exactly 1.

## Env knobs

None scenario-specific. Harness knobs apply: `MURMUR_LIVE_RUNTIME`,
`MURMUR_LIVE_FAILURES`, `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
`MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`, `MURMUR_TEST_DEADLINE_SCALE`.

## Deviations from the Rust source

- In-process multi-`Database` cluster via the Go harness (`QUIC mesh,
  ephemeral 127.0.0.1:0`), mirroring the Rust topology one for one.
- Plain SQL (`records(id INTEGER PRIMARY KEY, name TEXT)`) via
  `Database.Transaction`/`Database.Read`; canonical order from
  `ORDER BY name`.
- Per-node dissemination uses the harness
  `ClusterOptions.DisseminationByNode` overlay, mirroring the Rust overlay.
- Refusal evidence is `Status().LastError` (a `*string` in Go) containing
  `incompatible`, plus session counts — no Prometheus endpoint exists.
