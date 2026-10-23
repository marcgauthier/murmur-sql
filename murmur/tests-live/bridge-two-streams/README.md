# bridge-two-streams (Go port)

Rust source: `/home/marc/MURMUR/tests-live/bridge-two-streams/`
(`bridge_two_streams.rs`).

## What it proves

Two independent single-node Low clusters (distinct DBIDs, CAs, keys,
streams, staging dirs) export managed rows into `stream-a` and `stream-b`;
a two-node High cluster imports stream-a on node0 and stream-b on node1
(per-node receiver configs) and converges both domains' rows over its mesh
(imported shadows/owners replicate as auxiliary records). One test
(`TestTwoLowDomainsIntoOneHigh`) proves the pass criteria: round 1 lands
5 A-rows + 5 B-rows on both High nodes with exact names; a drain-to-idle
leg proves duplicate delivery applies nothing twice (receipt idempotence);
a per-domain Low update flows through to both High nodes, proving
Low-owned provenance; the names are restored; a stream-a-only round 2
(a-5..a-7) arrives; and a stream-b-only round 3 (b-5, b-6) delivers exactly
once, proving stream-b progress was undisturbed by the stream-a round.

Pass criteria: both High nodes converge on exactly the 10 round-1 names,
still exactly those 10 after idle pumps, the 2 renamed probe values then
the restored 10, exactly the 13 round-2 names (`a-0..a-7`, `b-0..b-4`),
and exactly the 15 round-3 names (plus `b-5`, `b-6`).

## Env knobs

Standard harness knobs only (`MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
`MURMUR_LIVE_UNLOCK_STALL_SECONDS`, `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`).
Convergence waits are 60 s with 200 ms polls, matching the original timeouts.

## Deviations from the Rust original

- Plain SQL replaces the typed HTTP API; there is no HTTP server in the
  product. Row IDs keep the `%032x` shape as the `TEXT NOT NULL PRIMARY KEY`.
- Export/import map to direct `Bridge.Capture()` (Low) and
  `Bridge.Process()` (High) pumps, with a pump inside every wait poll.
  Per-call bundle counts are not asserted: the replication engine also
  drives capture/process in the background, so progress is proven by row
  convergence instead of `published`/`imported` counters.
- Bridge progress has no public API (the applied cursor lives in internal
  `09/bridge/applied/<db>/<stream>` spool keys), so the byte-identical
  progress assertion is replaced by behavior: exact B-row content after the
  stream-a-only round, plus the added stream-b-only round 3, which can only
  deliver exactly once from an intact cursor.
- Provenance has no public API (ownership/shadow records are internal), so
  the low-owned assertion is replaced by behavior: a Low update to one row
  per domain must become visible on both High nodes. A High-owned field
  would suppress the Low update, so flow-through proves Low ownership.
- The duplicate-delivery gate is an explicit drain-to-idle leg: after
  round-1 convergence every bridge is pumped to idle (bounded 100-pump
  loop) and both High nodes must still show exactly the 10 names.
- `ManualPeers` + `Mesh()` after the local migrates so a local migrate
  never races schema auto-adoption; bridge keys are fixed test constants;
  each staging dir lives under its Low's runtime dir so it is retained on
  failure and cleaned on pass with the cluster dirs.
- Three clusters in one test means three `RESULT` lines (one per cluster
  name) and three forensics dumps on failure.

## `TestCrossDomainIdentityCollisionFailsClosed` (ported with a product extension)

The original gate is ported: domain A lands first, domain B's same-ID row
fails closed with a collision error, and first-writer state stands intact
on both High nodes. This needed a Go-port product extension (the Rust
product has no row-identity collision detection, so the Rust scenario
leaves this leg unported):

- `murmur.HighLowImport` runs `checkBridgeIdentityCollision` before
  joining: any stored shadow for the same `(table, key)` from a different
  source domain, or a live High row with no import provenance, rejects
  the bundle with `replication.KindCollision` ("collides"). The check
  runs before anything is committed, so rejections leave state intact.
- `Bridge.Process()` surfaces `ErrBridgeIdentityCollision` to the caller
  instead of trapping it into `09/bridge/hold/`; all other import
  failures keep hold semantics, and the rejected bundle stays pending
  (never marked applied), so the failure is deterministic across retries
  and background engine pumps (which only record it via `recordError`).

Deviations from the original `prepareBundle` gate: no stream-mismatch
refinement (shadows carry no stream) and no High-owner refinement
(High-owned cells keep projecting through ownership); file records keep
hold semantics. Counts are never asserted (background engine pumps race
explicit ones); the collision itself is retry-deterministic.
