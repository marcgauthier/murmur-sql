# migration-crash (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/migration-crash/`
(`migration_crash_test.go`, `TestMigratedPeerCrashReopensAndConverges` with
`before-store` / `after-store` subtests).

## What it proves

An isolated node recovers from a crash at the schema-manifest store
boundary while its two peers stay on the old epoch; afterwards it rejoins,
the peers adopt the new schema and rebind, and a migrated-field write
converges with every node at the new epoch.

## Pass criteria (same as Go)

- 24 seeded rows converge within 45 s; all nodes epoch 1.
- Node 1 is peer-isolated; recovery reopens at epoch 1 (`before-store`) or
  epoch 2 (`after-store`) while both peers remain on epoch 1; recovered rows
  are intact before reconnect.
- The pre-store case retries the migration (epoch 2); the new field reads
  the empty (NULL) default.
- After rejoin: names converge within 90 s, peers adopt epoch 2 within
  45 s, peers rebind to v2, and a `{phase}-recovered` note write converges
  with all nodes at epoch 2 within 45 s (250 ms poll).

## Deviations from the Go original

- Multi-process `murmur-server` children over the PostgreSQL wire protocol
  instead of `testnode` daemon processes over HTTPS+mTLS (per design);
  restart is child stop + respawn of the same directory with the same key
  (staying isolated, then explicit rejoin via `add_peer` + `sync_all`).
- Plain SQL replaces the typed-records HTTP API: table
  `live_typed_records(name TEXT NOT NULL PRIMARY KEY, counter INTEGER NOT NULL)` plus
  `ALTER TABLE ... ADD COLUMN note TEXT`. The table is explicitly flagged
  replicated on each node; the Go fixture tables replicate by default.
- Epoch assertions are `PRAGMA table_info` column-presence checks over the
  wire (the server allowlists side-effect-free inspection pragmas).
- **No mid-migration process exit and no exit codes 85/86**: the wire DDL
  is a synchronous call with no crash-hook boundary, so the two cases
  stop/restart on either side of the synchronous store. The
  peer-isolation, recovery-epoch, retry, rejoin, adoption, rebind, and
  migrated-write assertions are otherwise identical to Go.
- Go `MigrateTypedRecords` is an idempotent manifest bind/rebind; Rust DDL
  errors on a duplicate column, so every migrate is guarded by a
  column-presence check.
- The Go empty-string Note default reads as NULL in Rust (`None`).

## Compile check (only gate; never run here)

```sh
CARGO_TARGET_DIR=/home/marc/MURMUR/tests-live/target \
  cargo test --no-run --manifest-path /home/marc/MURMUR/tests-live/migration-crash/Cargo.toml
```
