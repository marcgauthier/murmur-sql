# config-churn (new Go scenario, no Rust original)

## What it proves

Table replication flags are node-local send gates that stay correct and
durable under churn and restarts:

- Gating: with node 0's `records` flag off, rows committed on node 0 do
  not reach its peers, while peer-to-peer control rows still converge
  (proving the mesh is alive and the absence is the gate, not an outage).
  Re-enabling resumes propagation.
- Churn: concurrent flag flips on all nodes, with the survivors flipping
  through node 0's outage, never error and never wedge the storage worker
  (the `MarkTableConfigOwned` blocking-send regression class).
- Durability: deterministic final flags and the config-owned marker
  survive per-node restarts and a full cluster restart with values intact,
  and replication still converges afterwards.

Flags do not converge across nodes (schema adoption force-enables tables
learned from peers), so cross-node flag equality is deliberately not
asserted. Whether the disabled-era backlog flushes on re-enable is
product-defined and likewise not pinned: only the open gate's propagation
is asserted.

## Pass criteria

- 3-node cluster opens meshed under `tests-live/runtime/config-churn/`.
- 10 gated rows stay off both peers while 5 control rows converge.
- 10 post-re-enable rows reach both peers.
- 150 flips across churn phases report zero errors.
- Final flags `[true false true]` and the owned marker survive restarts.
- A final proof row converges after all churn and restarts.

## Env knobs

None scenario-specific. Harness knobs apply: `MURMUR_LIVE_RUNTIME`,
`MURMUR_LIVE_FAILURES`, `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
`MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`, `MURMUR_TEST_DEADLINE_SCALE`.

## Deviations from the Rust original

None: new scenario.
