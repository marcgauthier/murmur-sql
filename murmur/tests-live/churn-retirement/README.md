# churn-retirement (Go port)

Rust source: `/home/marc/MURMUR/tests-live/churn-retirement/`
(`churn_retirement.rs`).

## What it proves

Three nodes mesh; node3 is excluded on the survivors. The port keeps every
pass criterion:

1. Baseline: five rows written on node1 converge identically on all nodes.
2. Sessions to node3 drop on both survivors; each survivor sees exactly one
   peer (the other survivor).
3. Node3's writes stop arriving (survivors keep only the five baseline rows
   across a 5 s hold).
4. Exclusion survives a survivor restart with no rediscovery re-mesh
   (restarted node1 re-meshes only its fellow survivor).
5. Explicit re-adding heals replication (all nodes converge on six rows).
6. The retired peer releases its GC retention pin (non-retired gating count
   drops below baseline; node3's retention record is marked retired).

## Leg mapping (Go HTTP API -> this port)

| Original | This port |
|---|---|
| `TypedInsert` / `TypedNames` (HTTP API) | Plain SQL `records(name TEXT NOT NULL PRIMARY KEY)` via `Transaction` / `Read` + `Flush()` |
| `RemovePeer` (exclusion legs) | `Cluster.RemovePeer` (durable exclusion record, reversible via `AddPeer`) |
| Retention-pin release inside `RemovePeer` | `Database.RetirePeer` (durable `retired=true` retention record) |
| `spedsql_peer_connected{peer=...}` | `Status().PeersDiagnostics` (`NodeID` + `Connected`) |
| `spedsql_peer_count` | `Status().Peers` |
| `spedsql_gating_members` | `Database.RetentionStatus().Peers` entries with `Retired == false` |
| `StopNode` / `StartNode` | `Cluster.StopNode` (`Close`) / `Cluster.StartNode` (reopen same dir+key under the unlock watchdog); explicit `AddPeer` re-mesh to the fellow survivor only (fresh ephemeral port) |
| `AddPeer` re-add | `Cluster.AddPeer` (deletes the exclusion record + dials) |

## Deviations

- **Retention-release leg runs after the re-add heal**, not immediately after
  exclusion. The product splits a single `RemovePeer` into reversible exclusion
  (`RemovePeer`) and permanent retirement (`RetirePeer`): session setup
  rejects retired peers and no unretire API exists, so retiring
  before the re-add leg would make the heal leg impossible. Every assertion
  is preserved; only the order changes.
- SWIM suspect/dead/refutation is not covered, same as the original
  (memberlist suspicion is not wired into the runtime).
- Timeouts: 60 s convergence / peer waits, 30 s retention-pin appearance,
  5 s isolation hold.

## Env knobs

None beyond the standard harness knobs (`MURMUR_LIVE_RUNTIME`,
`MURMUR_LIVE_FAILURES`, `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
`MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`).
