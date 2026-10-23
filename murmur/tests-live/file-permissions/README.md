# file-permissions (Go port)

Rust source: `/home/marc/MURMUR/tests-live/file-permissions/`
(`file_permissions.rs`:
`TestFilePermissionsFreshInitAndBackupRestore`).

## What it proves

Product-owned secret paths stay locked down: after fresh init, and
again after backup plus restore, the key-registry file
(`data/keys.enc`) and the data directory (0600 files, 0700 dirs) grant
nothing to group/other. Harness-minted material (`tls/node.key`) and
operator-owned paths are audit-logged, never asserted, since no
product change can alter fixture-chosen modes.

## Pass criteria

- `NewCluster` opens 2 meshed nodes under
  `tests-live/runtime/file-permissions/`.
- Phase 1 (fresh init, before any write): every node's `data/keys.enc`
  exists and both it and the `data/` tree carry no group/other bits;
  the mode table (node dir, data dir, `tls/`, `ca.der`, `node.der`,
  `node.key`) is logged for audit without assertions.
- Positive control: 25 rows written on node 1 converge to both nodes
  within 30 s, proving the permission posture does not break the
  database.
- Phase 2: node 2 stops; node 1 takes an online backup; the backup
  artifact tree, the restored tree (fresh node id), and the restored
  data dir all carry no group/other bits; node 2 restarts, re-meshes,
  and both nodes reconverge on the 25 rows within 60 s.
- Post-restore: every node's secret paths are locked down again.

## Env knobs

None scenario-specific. Harness knobs apply: `MURMUR_LIVE_RUNTIME`,
`MURMUR_LIVE_FAILURES`, `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
`MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.

## Deviations (inherited from Rust)

- In-process multi-`Database` cluster (QUIC mesh, ephemeral
  `127.0.0.1:0`) instead of `testnode` daemon processes over
  HTTPS+mTLS.
- Plain SQL (`live_typed_records(name TEXT NOT NULL PRIMARY KEY)`)
  instead of the managed typed-records HTTP API; canonical order
  comes from `ORDER BY name` rather than the API response.
- `StopNode`/`StartNode` + `Mesh()` instead of `StopNode`/`StartNode`
  + `UnlockNode` + `WaitNodeReady` (no Remote Unlock endpoint).
- Online `Backup` on the surviving node instead of stopping the node
  and opening its directory offline.
- `RestoreWithOptions` with a fresh node id instead of restore-with-
  clone; backup/restore scratch dirs live under the scenario runtime
  dir (retained on failure) instead of a temp dir.
- The restored tree IS the store dir, so the "restored-data" leg
  asserts the restore target itself.
- Symlinks are logged and skipped during the walk (their 0777 link
  modes are not access grants); the walker additionally fails if it
  sees zero paths, so a missing root can never pass vacuously.
- TLS audit names are DER (`ca.der`/`node.der`/`node.key`); the
  harness provisions no logs dir, config file, or PEM certs.
- Windows skips via `t.Skip` (POSIX modes do not apply to NTFS ACLs).
