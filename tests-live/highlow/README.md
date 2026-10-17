# highlow (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/highlow/`
(`highlow_test.go`, `ownership_reorder_test.go`, `README.md`).

## What it proves

Eight async `#[tokio::test]` fns (distinct `runtime/` names so parallel
execution stays isolated) port the eight Go tests one for one over a
`contacts(id, name, score)` table plus an `email` column for the schema
leg. Low and High are separate `murmur-server` clusters driven over the
PostgreSQL wire protocol:

- `disconnected_transfer_with_one_way_role_gating`
  (`TestHighLowDisconnectedTransferLive`): distinct Low/High DBIDs,
  structural one-way role gating (nothing is ever ferried back, so High
  state can only change via ferried Low bundles), and Low/High
  contact-state equality after export+import.
- `gap_delivery_with_duplicate_recovery`
  (`TestHighLowDuplicateReorderRecoveryLive`): three rounds export three
  bundles; withholding the middle one applies only the first (gap, no
  skip-ahead); restoring it converges all three; duplicate redelivery
  changes nothing and quarantines nothing.
- `two_receivers_converge` (`TestHighLowTwoReceiversConvergeLive`): two
  High receivers import the same staging independently and the High mesh
  converges both to the Low state.
- `forgeries_rejected_without_partial_apply`
  (`TestHighLowForgeriesRejectedLive`): rogue-domain, wrong-recipient,
  tampered, and oversized artifacts each fail closed alone (quarantined,
  High stays empty, no progress recorded); wrong-extension artifacts
  (`.txt`, dotfile) are ignored in place; the genuine bundle still
  applies afterwards. (The unreadable-file `Err` leg is dropped: the
  background importer swallows the error with no wire-visible signal.)
- `restart_resumes_without_recapture_or_reapply`
  (`TestHighLowRestartResumeLive`): a Low restart exports nothing new
  (bundle file set unchanged); a High restart re-applies nothing (both
  contacts held exactly).
- `schema_hold_survives_restart_and_releases_on_local_migration`
  (`TestHighLowSchemaHoldMigrationLive`): the unknown `email` column holds
  the bundles (High stays empty, no DDL crosses); the hold survives a High
  restart; a local `ADD COLUMN` migration releases the held bundles and
  the email reads back.
- `signer_rotation_and_outage_catch_up`
  (`TestHighLowKeyRotationAndOutageResumeLive`): round 1 imports under the
  predecessor key; both sides rotate (config edit plus restart) and round
  2 imports on top; a retired-signer replay is quarantined while a
  current-key duplicate stages cleanly; then a 5-event outage queues
  durably across a Low restart and catches up to exactly
  `{ann: 1, bob: 2, backlog: 4}`.
- `high_owned_field_reorder_converges_across_peers`
  (`TestHighLowOwnershipReorderAcrossPeersLive`): peer A
  (override-then-import) and peer B (import-then-override) converge over
  the mesh on the High bytes with High-owned provenance (a further Low
  update arrives yet stays suppressed on both). (The explicit-release
  convergence leg is dropped: `Bridge::release` has no wire
  equivalent.)

Pass criteria: per-test contact maps converge exactly as listed; gap,
hold, forgery, and suppression legs assert exact visible row sets;
restart legs assert exact file-set/row stability; rotation asserts
quarantine placement for the replay and clean staging for the duplicate.
Adversarial HLC ordering stays pinned at the DB layer by
`TestBridgeShadowReorderConvergence`, as in Go.

## Env knobs

Standard harness knobs only (`MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
`MURMUR_LIVE_UNLOCK_STALL_SECONDS`, `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`,
`MURMUR_SERVER_BIN`).
Convergence waits are 30 s with 50 ms polls, matching the Go timeouts.

## Deviations from the Go original

- Multi-process `murmur-server` children over the PostgreSQL wire
  protocol instead of `testnode` daemon processes over HTTPS+mTLS
  (per design).
- Plain SQL via the harness `NodeClient` (`execute`/`query` + `flush`)
  replaces the typed HTTP API; there is no HTTP server in the Rust
  product. Sequential single-writer `score` increments replace
  `TypedCounterAdd` (identical convergence); `email` replaces the
  typed note field.
- The `contacts` table is explicitly flagged `MURMUR SET TABLE ...
  REPLICATED true` on each node (row convergence and bridge capture
  need the per-table flag); the Go fixture tables replicate by
  default.
- `BridgeExport`/`BridgeImport` have no wire equivalent: the
  replication engine drives `capture()`/`process()` automatically
  every apply tick (~1 s). Export drains become staging-dir
  quiescence waits (bundle set still for 2.5 s); positive legs poll
  row state with the usual 30 s deadline; negative legs (gap hold,
  schema hold, forgery no-apply, duplicate stability, suppression)
  sleep 3 s (several background ticks) and assert exact unchanged
  state instead of bounded explicit pumps.
- Separate Low/High staging dirs plus a one-way test ferry replace Go's
  shared staging dir: the Rust engine also drives capture/process in the
  background, so only ferried bundles may be visible to High. This keeps
  withhold/forgery/outage legs deterministic; bundle filenames sort in
  export order, so the middle-bundle withhold mirrors Go.
- Per-call bundle counts and `BridgeStatus` progress (`Applied`, `Missing`,
  `Holds`, `pending_events`) have no public Rust equivalent and are racy
  under background driving: all are proven behaviorally (exact row sets,
  file-set stability, quarantine placement, exact converged totals).
- `Bridge::process()` quarantines bad artifacts into
  `staging/quarantine/` and returns `Ok` (Go: import error); the forgery
  legs assert quarantine + empty High + no recorded progress instead of an
  error.
- Dropped legs (no observable wire form; the old in-process port covered
  them via the direct `Bridge` API): the API-boundary role-gating asserts
  (`process()` on the exporter / `capture()` on the importer returning
  zero), the unreadable-staging-file `Err` leg, and the explicit
  `Bridge::release` convergence leg.
- Rust traps schema holds internally and returns `Ok`, so there is no
  `waiting-schema` error to assert; the hold is proven by empty High plus
  `email` still absent (no DDL crossed), then release after migration.
  Column presence is probed via a zero-row select (no guaranteed PRAGMA
  wire form).
- Rust pins a single signer per source node, so Go's overlap trust during
  rotation is inexpressible: round 1 is imported before the rotation
  instead of after, and the retired-signer replay plus current-key
  duplicate distinguish signature failure from dedup. Rotation uses the
  harness `Cluster::set_bridge_config` (stored; next start applies) plus
  restart on both sides, Go `rewriteNodeConfig` parity.
- Field provenance (`Provenance` owner tags) has no public Rust API, so
  High-owned is proven behaviorally by a drained Low update arriving yet
  staying suppressed on both peers.
- Bridge keys are fixed test constants shared by all tests in this file
  (signer seeds, X25519 secrets, domain and node IDs) following the
  `tests/bridge_files.rs` fixed-key convention; clusters never interact
  across tests. Each test owns 2-4 `runtime/` names (one `RESULT` line
  each). Public keys are derived with scenario-only registry deps
  (`ed25519-dalek`, `x25519-dalek`, same versions as the product).

## Compile check

```sh
CARGO_TARGET_DIR=/home/marc/MURMUR-SQL/tests-live/target \
  cargo test --no-run --manifest-path /home/marc/MURMUR-SQL/tests-live/highlow/Cargo.toml
```
