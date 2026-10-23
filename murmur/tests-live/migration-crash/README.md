# migration-crash (Go port)

Rust source: `/home/marc/MURMUR/tests-live/migration-crash/`
(`migration_crash.rs`: before-store / after-store cases).

## What it proves

An isolated node recovers from a crash at the schema-manifest store
boundary while its two peers stay on the old epoch; afterwards it
rejoins, the peers adopt the new schema and rebind, and a
migrated-field write converges with every node at the new epoch.

Pass criteria: 24 seeded rows converge within 45 s, all epoch 1; node 1
peer-isolated; recovery reopens at epoch 1 (`before-store`) or epoch 2
(`after-store`) while both peers remain on epoch 1; recovered rows
intact before reconnect; the pre-store case retries the migration; the
new field reads the empty (NULL) default; after rejoin, names converge
within 90 s, peers adopt epoch 2 within 45 s, peers rebind to v2, and a
`{phase}-recovered` note write converges with all nodes at epoch 2
within 45 s (250 ms poll).

## Env knobs

Standard harness knobs only (`MURMUR_LIVE_RUNTIME`,
`MURMUR_LIVE_FAILURES`, `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
`MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`). No scenario-specific knobs.

## Deviations (inherited from Rust)

- In-process multi-`Database` cluster instead of daemon processes;
  restart is `Close` + reopen of the same directory with the same key
  (staying isolated, then explicit rejoin via `AddPeer`).
- Plain SQL replaces the typed-records HTTP API: table
  `live_typed_records(name TEXT NOT NULL PRIMARY KEY, counter INTEGER
  NOT NULL)` plus `ALTER TABLE ... ADD COLUMN note TEXT`.
- Epoch assertions are `PRAGMA table_info` column-presence checks.
- **No mid-migration process exit and no exit codes 85/86**:
  `Migrate` is a synchronous call with no crash-hook boundary, so the
  two cases close/reopen on either side of the synchronous store.
- Every migrate is guarded by a column-presence check (DDL errors on a
  duplicate column).
