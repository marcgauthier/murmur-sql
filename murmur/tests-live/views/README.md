# views (Go port)

Rust source: `/home/marc/MURMUR/tests-live/views/` (`views.rs`,
`TestTypedViewTracksReplicatedRowsAndRebuilds`).

## What it proves

Three nodes form an encrypted mesh. Each inserts one contact (`ann`, `bob`,
`hidden`); `ann` and `bob` are enabled via a counter add. The two qualifying
rows must appear in the managed view on all replicas while `hidden` stays
out. Node 3 then restarts and its rebuilt view must contain the same two rows.

## Pass criteria (same as Rust)

- View on nodes 1-3 converges to exactly `[ann, bob]` within 20 s.
- After the node 3 restart + re-mesh, its view again converges to `[ann, bob]`
  within 20 s.

## Env knobs

None scenario-specific. Harness knobs apply: `MURMUR_LIVE_RUNTIME`,
`MURMUR_LIVE_FAILURES`, `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
`MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.

## Deviations from the Rust original

- In-process multi-`Database` cluster, same as Rust.
- Plain SQL replaces the typed-RIME managed query: base table
  `contacts(name TEXT NOT NULL PRIMARY KEY, score INTEGER NOT NULL)` plus
  the same `SELECT name FROM contacts WHERE score > 0 ORDER BY name` run
  directly (the product rejects application `VIEW`s, so no view object is
  created). `score` plays the role of the enabling PN counter.
- Same schema is migrated locally on every node (no shared schema manifest).
