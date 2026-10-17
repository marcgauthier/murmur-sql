# churn-retirement (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/churn-retirement/`
(`churn_retirement_test.go`).

## What it proves

Three nodes mesh; node3 is excluded on the survivors. The port keeps every
Go pass criterion:

1. Baseline: five rows written on node1 converge identically on all nodes.
2. Sessions to node3 drop on both survivors; each survivor sees exactly one
   open session (the other survivor).
3. Node3's writes stop arriving (survivors keep only the five baseline rows
   across a 5 s hold).
4. Exclusion survives a survivor restart with no rediscovery re-mesh
   (restarted node1 re-meshes only its fellow survivor).
5. Explicit re-adding heals replication (all nodes converge on six rows).
6. The retired peer releases its GC retention pin (non-retired gating count
   drops below baseline; node3's retention record is marked retired).

## Leg mapping (Go -> Rust)

| Go | Rust |
|---|---|
| `Cluster.TypedInsert` / `TypedNames` (HTTP API) | Plain SQL `records(id INTEGER PRIMARY KEY, name TEXT NOT NULL)` via `execute`/`query` + `flush()` |
| `RemovePeer` (exclusion legs) | `Cluster::remove_peer` (durable exclusion, reversible via `add_peer`) |
| Retention-pin release inside `RemovePeer` | `Cluster::retire_peer` (durable `retired=true` retention record) |
| `spedsql_peer_connected{peer=...}` | `MURMUR SHOW PEERS` (`node_id` + `connected`) |
| `spedsql_peer_count` | Open-session rows of `MURMUR SHOW PEERS` |
| `spedsql_gating_members` | Distinct non-retired peers in `MURMUR SHOW RETENTION` |
| `StopNode` / `StartNode` / `UnlockNode` / `WaitNodeReady` | `Cluster::stop_node` / `Cluster::start_node` (unlock runs under the harness watchdog inside `start_node`); explicit `add_peer` re-mesh to the fellow survivor only (fresh ephemeral port) |
| `AddPeer` re-add | `Cluster::add_peer` (clears the exclusion + dials) |

## Deviations

- **Retention-release leg runs after the re-add heal**, not immediately after
  exclusion. Rust splits Go's single `RemovePeer` into reversible exclusion
  (`remove_peer`, which keeps retention) and permanent retirement
  (`retire_peer`, which releases it): session setup rejects retired peers
  and no unretire API exists, so retiring before the re-add leg would make
  the heal leg impossible. Every assertion is preserved; only the order
  changes.
- SWIM suspect/dead/refutation is not covered, same as the Go original
  (memberlist suspicion is not wired into the runtime).
- Timeouts: 60 s convergence / session waits (Go parity for saturated
  parallel boxes), 5 s isolation hold, 5 s post-restart watch.
- Node ids are pinned with the restarted node sorting last, so both
  survivors' pre-restart sessions sit in their tie-break-preferred
  direction and the restart/re-mesh legs deterministically exercise
  zombie replacement instead of flaking on random id order.

## Env knobs

None beyond the standard harness knobs (`MURMUR_LIVE_RUNTIME`,
`MURMUR_LIVE_FAILURES`, `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
`MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`, `MURMUR_SERVER_BIN`).

## Compile check (only gate; never run here)

```sh
CARGO_TARGET_DIR=/home/marc/MURMUR-SQL/tests-live/target \
  cargo test --no-run --manifest-path /home/marc/MURMUR-SQL/tests-live/churn-retirement/Cargo.toml
```
