# schema-evolution (Go port)

Rust source: `/home/marc/MURMUR/tests-live/schema-evolution/`
(`schema_evolution.rs`, `TestRollingTypedSchemaMigration`).

## What it proves

Three nodes share five typed rows with counters. Only node 1 registers the
additive Note field; its peers adopt the replicated schema. The new field is
written from node 1 while an older binding on node 2 renames the same row
after adoption (must not erase Note), node 3 inserts a late row, and all
nodes converge on names, the surviving note, and intact counters. Node 3
restarts and names plus counters are verified again.

## Pass criteria (same as Rust)

- Names converge to the five seeded rows within 30 s.
- Migrating node adopts the column within 10 s; each peer within 30 s, with
  the `SchemaAdoptions` metric advancing (column presence + metric stand in
  for the epoch-2 assertions).
- Names converge to the six-row renamed set within 30 s; Note reads
  `added-field-survives` under the renamed key; counters stay `100+i`.
- After the node 3 restart + re-mesh, names and counter `typed-row-4 == 104`
  verify within 30 s.

## Env knobs

None scenario-specific. Harness knobs apply: `MURMUR_LIVE_RUNTIME`,
`MURMUR_LIVE_FAILURES`, `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
`MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.

## Deviations from the Rust original

- In-process multi-`Database` cluster, same as Rust; restart is `Close()` +
  reopen of the same directory with the same key, followed by an explicit
  re-mesh (fresh ephemeral port).
- Plain SQL: table `live_typed_records(name TEXT NOT NULL PRIMARY KEY,
  counter INTEGER NOT NULL)` plus `ALTER TABLE ... ADD COLUMN note TEXT`;
  counter adds are `UPDATE ... SET counter = counter + ?`.
- Epoch waits are expressed as `PRAGMA table_info` column-presence waits (the
  established `typed-records` port pattern).
- Rename is delete + insert copying the row's fields (ordinary transactions
  cannot update primary keys), barriered on the note first: a record-id
  rename would let concurrent field writes follow it, while the SQL copy
  only sees local state.
