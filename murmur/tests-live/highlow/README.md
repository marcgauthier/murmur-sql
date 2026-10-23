# highlow (Go port)

Rust source: `/home/marc/MURMUR/tests-live/highlow/`
(`highlow.rs`, eight `#[test]` fns).

## What it proves

Eight `TestXxx` fns (distinct `runtime/` names so execution stays isolated)
port the eight Rust tests one for one over a `contacts(id, name, score)`
table plus an `email` column for the schema leg:

- `TestHighLowDisconnectedTransferLive`: distinct Low/High DBIDs, one-way
  role gating (exporter `Process()` and importer `Capture()` are API-level
  no-ops), and Low/High contact-state equality after export+import.
- `TestHighLowDuplicateReorderRecoveryLive`: three rounds export three
  bundles; withholding the middle one applies only the first (gap, no
  skip-ahead); restoring it converges all three; duplicate redelivery
  changes nothing and quarantines nothing.
- `TestHighLowTwoReceiversConvergeLive`: two High receivers import the same
  staging independently and the High mesh converges both to the Low state.
- `TestHighLowForgeriesRejectedLive`: rogue-domain, wrong-recipient,
  tampered, and oversized artifacts each fail closed alone (quarantined,
  High stays empty, no progress recorded); wrong-extension artifacts
  (`.txt`, dotfile) are ignored in place; an unreadable file fails the
  import with an error and no partial apply; the genuine bundle still
  applies afterwards.
- `TestHighLowRestartResumeLive`: a Low restart exports nothing new
  (bundle file set unchanged); a High restart re-applies nothing (both
  contacts held exactly).
- `TestHighLowSchemaHoldMigrationLive`: the unknown `email` column holds
  the bundles (High stays empty, no DDL crosses); the hold survives a High
  restart; a local `ADD COLUMN` migration releases the held bundles and
  the email reads back.
- `TestHighLowKeyRotationAndOutageResumeLive`: round 1 imports under the
  predecessor key; both sides rotate (config edit plus restart) and round
  2 imports on top; a retired-signer replay is quarantined while a
  current-key duplicate stages cleanly; then a 5-event outage queues
  durably across a Low restart and catches up to exactly
  `{ann: 1, bob: 2, backlog: 4}`.
- `TestHighLowOwnershipReorderAcrossPeersLive`: peer A
  (override-then-import) and peer B (import-then-override) converge over
  the mesh on the High bytes with High-owned provenance (a further Low
  update arrives yet stays suppressed on both); release on A converges
  both back to the latest Low value.

Pass criteria: per-test contact maps converge exactly as listed; gap,
hold, forgery, and suppression legs assert exact visible row sets;
restart legs assert exact file-set/row stability; rotation asserts
quarantine placement for the replay and clean staging for the duplicate.

## Env knobs

Standard harness knobs only (`MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
`MURMUR_LIVE_UNLOCK_STALL_SECONDS`, `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`,
`MURMUR_TEST_DEADLINE_SCALE`). Convergence waits are 30 s with 50 ms
polls, matching the Rust timeouts.

## Deviations from the Rust source

- In-process multi-`Database` clusters via the Go harness (QUIC mesh,
  ephemeral 127.0.0.1:0; `ManualPeers` with explicit `Mesh`/`AddPeer`),
  mirroring the Rust topology one for one. Bridge configs use
  `replication.NewBridgeExporter`/`NewBridgeReceiver`; rotation uses
  `Cluster.SetBridgeConfig` plus restart.
- Plain SQL via `Database.Transaction`/`Database.Read`; sequential
  single-writer `score` increments replace `TypedCounterAdd` (identical
  convergence); `email` replaces the typed note field.
- `BridgeExport`/`BridgeImport` map to direct `Bridge.Capture()` (Low) and
  `Bridge.Process()` (High) calls.
- Separate Low/High staging dirs plus a one-way test ferry replace a
  shared staging dir: the engine also drives capture/process in the
  background, so only ferried bundles may be visible to High. Bundle
  filenames sort in export order, so the middle-bundle withhold mirrors
  Rust.
- Per-call bundle counts and bridge progress have no public equivalent
  and are racy under background driving: all are proven behaviorally
  (exact row sets, file-set stability, quarantine placement, exact
  converged totals).
- `Bridge.Process()` quarantines bad artifacts into
  `staging/quarantine/` and returns nil; the forgery legs assert
  quarantine + empty High + no recorded progress instead of an error.
  Only the unreadable-file leg returns an error. That leg is created
  unreadable atomically (no background staging window) and logs a skip
  when the file stays readable (e.g. running as root).
- Schema holds are trapped internally and return nil, so there is no
  `waiting-schema` error to assert; the hold is proven by empty High plus
  `email` still absent (no DDL crossed), then release after migration.
  Column presence is probed with a tolerant `SELECT` (a missing column
  fails prepare) rather than `PRAGMA table_info`.
- A single signer per source node is pinned, so overlap trust during
  rotation is inexpressible: round 1 is imported before the rotation
  instead of after, and the retired-signer replay plus current-key
  duplicate distinguish signature failure from dedup.
- Field provenance has no public API, so High-owned is proven by a
  drained Low update staying suppressed on both peers, and release
  convergence by both peers showing the Low value. The release key is
  `murmur.EncodeValues([]Value{TextValue(row)})`.
- Bridge keys are fixed test vectors shared by all tests in this file
  (signer seeds, X25519 secrets, domain and node IDs); clusters never
  interact across tests. Each test owns 2-4 `runtime/` names.
- Test failure handling uses the harness `failed` flag + deferred
  `DumpForensics`/`Finish` pattern (a `t.Fatalf` runs the deferred
  cleanup); cluster setup failures abort before the defer registers,
  matching the Rust setup-outside-catch behavior.
