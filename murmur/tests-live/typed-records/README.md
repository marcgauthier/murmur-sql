# typed-records (Go port)

Rust source: `/home/marc/MURMUR/tests-live/typed-records/`
(`typed_records.rs`, seven tests).

## What it proves

Two in-process nodes share one typed table (`live_typed_records`, TEXT
primary key, PN_COUNTER / OR_SET / MAX / MIN policy columns); seven tests
(one cluster each) prove the pass criteria:

| Go test | Rust test |
|---|---|
| `TestTypedRecordReplicatesAndRebuildsAcrossRestart` | `typed_record_replicates_and_rebuilds_across_restart` |
| `TestTypedSchemaMigrationAdoptedByOlderPeerAndRetainsNewFields` | `typed_schema_migration_adopted_by_older_peer_and_retains_new_fields` |
| `TestTypedCRDTWritesConvergeAfterOfflinePartition` | `typed_crdt_writes_converge_after_offline_partition` |
| `TestTypedEmptyPeerCatchesUpThroughSnapshot` | `typed_empty_peer_catches_up_through_snapshot` |
| `TestTypedConcurrentSchemaBranchesMergeAcrossPartition` | `typed_concurrent_schema_branches_merge_across_partition` |
| `TestTypedFilesReplicateAndFetchWithoutSQL` | `typed_files_replicate_and_fetch_without_sql` |
| `TestTypedNodeLocalRowsPersistOnlyOnTheirNode` | `typed_node_local_rows_persist_only_on_their_node` |

Pass criteria: replicated insert + peer subscription update +
explicit-tx commit/rollback + counter/set/extrema convergence + restart
rebuild; additive migration adopted by the older peer with the new field
surviving writes from both sides and a restart; offline counter sum (18)
and set union on both nodes; 12-row snapshot catch-up proven by flat log
counters; union schema plus both branch values on both nodes; file
metadata/object/delete-tombstone replication plus traversal confinement;
node-local row present locally and absent remotely.

## Env knobs

Standard harness knobs only (`MURMUR_LIVE_RUNTIME`,
`MURMUR_LIVE_FAILURES`, `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
`MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`). Convergence waits are 30 s with
50 ms polls, matching the original timeouts.

## Deviations (inherited from Rust)

- Plain SQL plus policy ops (`CounterAdd`, `SetAdd`/`SetRemove`, `Min`,
  `Max`) instead of the managed typed HTTP API.
- Merge policies bind at open through the harness
  `ReplicationOptions.MergePolicies` overlay.
- Schema "epochs" map to column-presence polls plus the
  `SchemaAdoptions` metric.
- The snapshot leg uses the flat `(received, applied)` counters proof;
  the receiver's snapshot donor is pinned via
  `TrustedSnapshotSources` (the harness trusts nobody by default).
- `TypedRename` renames the LWW `title` field rather than the primary
  key, so row identity is preserved without a delete+insert cycle.
- File fetch is automatic (`FetchInterval`); clusters use
  `ManualPeers` + `Mesh` after the local migrates so a local migrate
  never races schema auto-adoption of the identical DDL.
