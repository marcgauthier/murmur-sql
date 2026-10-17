# downgrade-guard (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/downgrade-guard/`
(`README.md` + `downguard_test.go`:
`TestDowngradeGuardRefusesNewStore`).

## What it proves

A store written by a NEWER release is refused fail-closed instead of
being opened into a mixed state: the current build rejects it loudly
with the `FORMAT` guard in `storage.rs`
(`Error::Corrupt("unsupported database format")`), leaves the refused
store byte-untouched, and keeps both compatibility directions working
(a fresh store boots at the current format; the unmodified store
reopens with an identical digest and identity).

## Pass criteria

- `Cluster::new` opens cluster `downgrade-guard` (1 node) under
  `tests-live/runtime/downgrade-guard/`; 25 `row-*` rows are seeded
  (ids `3000+i`) and digested (sha256 over `id:name` lines, id order).
- Positive control: a fresh store created by the current build opens,
  holds 1 row across a close/reopen, and carries the same format
  marker as the node store — so the later refusal can only be caused
  by the bumped version.
- The newer store (stopped node dir copied to `future-store`, then
  `00/identity` re-encoded with `format + 1` through the raw spool
  API) is refused with `Corrupt("unsupported database format")`; any
  other outcome (including a successful open) fails the test.
- The refused store is content-untouched: every file present before
  and after the refusal is byte-identical (additions/removals are
  logged, not failed, mirroring Go's Spool-rotation tolerance), and
  the identity record still carries the bumped format with unchanged
  database/node ids.
- The unmodified store reopens via `start_node` with exactly the 25
  seeded rows, an identical digest, and the same node id; the newer
  store stays refused afterwards.

## Env knobs

- Harness knobs apply: `MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
  `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
  `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.
- No `MURMUR_PREV_REF` / git-history mechanism: there is no
  previous-release Rust daemon to build (see deviations).

## Deviations from the Go original

- The Go suite builds the previous-release daemon from git history
  (`MURMUR_PREV_REF`) and has it refuse the current store. The Rust
  port inverts the mechanism: with no previous release to build, it
  crafts a store one format generation NEWER than the current build
  and requires the current build to refuse it. The guarded property
  is the same (a version skew across the store-format boundary fails
  closed and loudly), exercised against the product's `FORMAT` guard
  in `storage.rs` instead of the Go `StrictFormat` daemon check.
- The newer store is staged with the raw spool API (the harness
  re-exports `spool`): the test registers a read-path mirror of the
  product's zstd block codec (id 128) to decode product-written
  blocks, re-encodes the postcard `00/identity` record with
  `format + 1`, and commits it with `Durability::Sync`. The test
  never names the product's private `FORMAT` constant — it reads the
  current store's own marker — so the port stays green across
  product format bumps.
- Refusal is reported during `MURMUR UNLOCK` over PostgreSQL wire from
  `murmur-server`: opening the storage returns `Error::Corrupt("unsupported database format")`,
  the connection fails with SQLSTATE error, and the server refuses to serve queries.
- Plain SQL (`records(id INTEGER PRIMARY KEY, name TEXT)`,
  `ORDER BY id`) instead of the managed typed-records HTTP API
  (`TypedContentionInsert`, `TypedContentionRows`); the node-id leg
  compares `Cluster::node_id` hex instead of the `spedsql_info`
  metric label.
- No shared-harness addition was required by this scenario.

## Compile check (only gate; never run here)

```sh
cargo test --no-run --manifest-path /home/marc/MURMUR/tests-live/downgrade-guard/Cargo.toml
```
