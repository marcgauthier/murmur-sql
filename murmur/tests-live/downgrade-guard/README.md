# downgrade-guard (Go port)

Rust source: `/home/marc/MURMUR/tests-live/downgrade-guard/`
(`tests/downgrade_guard.rs`, `downgrade_guard`), itself a port of the
original Go `downgrade-guard` scenario
(`TestDowngradeGuardRefusesNewStore`).

## What it proves

A store written by a NEWER release is refused fail-closed instead of
being opened into a mixed state: the current build rejects it loudly
with the `FORMAT` guard in `storage.go` ("unsupported database
format"), leaves the refused store byte-untouched, and keeps both
compatibility directions working (a fresh store boots at the current
format; the unmodified store reopens with an identical digest and
identity).

## Pass criteria (same as Rust)

- 1-node cluster `downgrade-guard`; 25 `row-*` rows seeded (ids
  `3000+i`) and digested (sha256 over `id:name` lines, id order).
- Positive control: a fresh store created by the current build opens,
  holds 1 row across a close/reopen, and carries the same format marker
  as the node store.
- The newer store (stopped node dir copied to `future-store`, then
  `00/identity` re-encoded with `format + 1` through the raw spool API)
  is refused with `Corrupt("unsupported database format")`; any other
  outcome (including a successful open) fails the test.
- The refused store is content-untouched: every file present before and
  after the refusal is byte-identical (additions/removals are logged,
  not failed), and the identity record still carries the bumped format
  with unchanged database/node ids.
- The unmodified store reopens via `StartNode` with exactly the 25
  seeded rows, an identical digest, and the same node id; the newer
  store stays refused afterwards.

## Deviations

None from the Rust port: the newer store is staged with the raw spool
API (the test registers the product's zstd block codec for reads and
never names the private `FORMAT` constant), the refusal is
`murmur.Open` returning `KindCorrupt`, and plain SQL
(`records(id, name)`) replaces the managed typed-records HTTP API.
