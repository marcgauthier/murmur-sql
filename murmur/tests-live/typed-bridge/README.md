# typed-bridge (Go port)

Rust source: `/home/marc/MURMUR/tests-live/typed-bridge/`
(`typed_bridge.rs`: `TestTypedBridgeImportsIntoNativeRIME`).

## What it proves

Two single-node clusters form the Low (exporter) and High (importer)
bridge domains sharing one staging directory
(`runtime/typed-bridge-low/staging/`, stream `typed-stream`). A typed
row inserted on Low is exported and imported through High's typed
durable path; a High write overrides the field (automatic ownership via
promote); a Low update while the field is High-owned is imported but
suppressed; after release the next Low update flows through; and the
imported result survives a High-node restart.

Pass criteria: High converges to 1 row titled `bridge-native-record`
after export+import; after the High override plus a drained Low update,
High still shows `high-owned-record` = 1 and
`low-update-before-release` = 0; after release and a second Low update,
High shows `low-after-release` = 1, including after the restart.

## Env knobs

Standard harness knobs only (`MURMUR_LIVE_RUNTIME`,
`MURMUR_LIVE_FAILURES`, `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
`MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`). Convergence waits are 30 s with
50 ms polls, matching the original timeouts.

## Deviations (inherited from Rust)

- Plain SQL instead of the typed HTTP API; `TypedRename` updates the
  LWW `title` field rather than the primary key (promote only claims
  ownership of non-PK cells).
- `BridgeExport`/`BridgeImport` map to direct `Bridge.Capture()` (Low)
  and `Bridge.Process()` (High) calls; progress is proven by row
  convergence (a sync pump runs inside every wait poll) instead of
  `published`/`imported` counters.
- The suppression leg drains the bridge to idle before asserting.
- `Release` takes the encoded PK value vector
  (`EncodeValues([]Value{TextValue(name)})`).
- Bridge keys are fixed test constants; the staging dir lives under
  Low's runtime dir so it is retained on failure and cleaned on pass.
- Two clusters in one test means two `RESULT` lines (one per cluster
  name) and two forensics dumps on failure.
