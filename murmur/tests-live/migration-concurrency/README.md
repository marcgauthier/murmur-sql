# migration-concurrency (Go port)

Rust source: `/home/marc/MURMUR/tests-live/migration-concurrency/`
(`migration_concurrency.rs`: `TestSimultaneousMigrationConverges`).

## What it proves

The same additive Note migration fires simultaneously on two of three
meshed nodes while all keep writing. Both migrating nodes must land
well-defined (old or new, never mixed), mixed-version replication must
flow in both directions during the overlap, and after the straggler
migrates the whole mesh converges fully migrated with identical counts,
digests, and materialized new-column reads on every node.

Pass criteria: `MURMUR_MIGRATION_CONCURRENCY_SEED` rows (default 60)
converge everywhere within 60 s, all old; node 1 peer-isolated; the
simultaneous migrate fires on nodes 2-3 with at least one overlap write
acknowledged; both racers well-defined, at least one new; heal; all
overlap writes converge within 90 s; new-column writes plus an
old-schema `post-race` insert converge; after migrating the third node,
exact counts plus ordered-digest equality, new schema everywhere, and
`seed-0 == 100` / `seed-4 == 104` read identically on all nodes.

## Env knobs

- `MURMUR_MIGRATION_CONCURRENCY_SEED` (default 60): seed row count.
- Standard harness knobs (`MURMUR_LIVE_RUNTIME`,
  `MURMUR_LIVE_FAILURES`, `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
  `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`).

## Deviations (inherited from Rust)

- In-process multi-`Database` cluster instead of daemon processes; peer
  isolation/heal via `RemovePeer`/`AddPeer` (no restarts, no re-mesh
  needed).
- Plain SQL replaces the typed-records HTTP API: table
  `live_typed_records(name TEXT NOT NULL PRIMARY KEY, counter INTEGER
  NOT NULL)` plus `ALTER TABLE ... ADD COLUMN note TEXT`.
- Epoch assertions are `PRAGMA table_info` column-presence checks.
- Every migrate is guarded by a column-presence check (DDL errors on a
  duplicate column).
- Background writers are goroutines with a 25 ms tick and an
  `atomic.Bool` stop flag; the simultaneous trigger uses a start
  channel as the barrier.
