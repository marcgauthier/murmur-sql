# backup-restore (Go port)

Rust source: `/home/marc/MURMUR/tests-live/backup-restore/`
(`backup_restore.rs`, three tests).

## What it proves

- `TestBackupRestoreRejoinMesh`: three nodes converge on 20 rows; node3
  stops; its data dir is backed up offline through the library, wiped to
  simulate total loss, and restored under a fresh writer identity; node3
  restarts on the restored data and the mesh reconverges with no lost
  rows, with the restarted node's identity equal to the fresh one.
- `TestRestoreRejectsStaleIdentity`: rollback safety — opening restored
  data under the old NodeID is rejected, while the fresh identity opens
  cleanly with the row intact.
- `TestTypedBackupRestoreRoundTripLive`: a typed row written through
  policy ops (PN_COUNTER / OR_SET / MAX / MIN) survives backup of the
  encrypted store, fresh-identity restore, and reopen with identical
  materialized values.

Pass criteria: all three nodes hold the same 20 rows within 90 s of the
restart; `NodeID` on node3 equals the fresh id; a post-rejoin survivor
write converges to 21 rows everywhere; stale open fails, fresh open
succeeds; typed projections after restore equal the pre-backup values
exactly.

## Env knobs

Standard harness knobs only (`MURMUR_LIVE_RUNTIME`,
`MURMUR_LIVE_FAILURES`, `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
`MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`). No scenario-specific knobs.

## Deviations (inherited from Rust)

- In-process multi-`Database` cluster instead of daemon processes; no
  HTTPS/mTLS admin API exists in the product.
- Plain SQL plus policy ops instead of the managed typed-records HTTP
  API.
- `Cluster.StopNode` / `RestartNodeWithIdentity` instead of
  StopNode/StartNode + config rewrite + unlock polling; the offline
  backup/restore legs use `murmur.Open` (no replication) +
  `Database.Backup` / `murmur.RestoreWithOptions`, and the TLS reissue
  + fresh origin key are handled by the harness method.
- Extra post-rejoin survivor write (21st row) proves the restored node
  is a live replication peer again. The restarted node itself must not
  originate until every peer restarts with its fresh origin key.
