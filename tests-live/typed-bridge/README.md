# typed-bridge (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/typed-bridge/`
(`typed_bridge_test.go`; the Go dir has no README.md).

## What it proves

Two single-node `murmur-server` clusters form the Low (exporter) and High
(importer) bridge domains sharing one staging directory
(`runtime/typed-bridge-low/staging/`, stream `typed-stream`). One
`#[tokio::test]` (`typed_bridge_imports_into_native_rime`, from
`TestBridgeImportsIntoNativeRIME`) proves the Go pass criteria: a typed row
inserted on Low is exported and imported through High's typed durable path
(`high_low::import`, with schema/column mapping and transaction
verification); a High write overrides the field (automatic ownership via
`promote`); a Low update while the field is High-owned is imported but
suppressed; `MURMUR BRIDGE RELEASE` drops the ownership so the next Low
update flows through; and the imported result survives a High-node restart
(process stop + start).

Pass criteria: High converges to 1 row titled `bridge-native-record` after
export+import; after the High override plus an exported Low update, High
still shows `high-owned-record` = 1 and `low-update-before-release` = 0;
after the release plus another Low update, High shows
`low-update-after-release` = 1 and `high-owned-record` = 0; the
post-release value is still 1 after the restart.

## Env knobs

Standard harness knobs only (`MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
`MURMUR_LIVE_UNLOCK_STALL_SECONDS`, `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`,
`MURMUR_SERVER_BIN`, `CARGO_TARGET_DIR`). Convergence waits are 30 s with
50 ms polls, matching the Go timeouts.

## Deviations from the Go original

- Multi-process clusters: one `murmur-server` child per domain, driven
  over the PostgreSQL wire (SQL + `MURMUR ...` commands), instead of
  `testnode` daemon processes over HTTPS+mTLS (per design). Bridge
  configs come from the harness `[replication.bridge]` rendering
  (`BridgeNodeConfig`: role/domain/directory/stream/recipient/secrets/
  trust maps); key derivation (ed25519 signer key, X25519 recipient)
  uses scenario-local `ed25519-dalek`/`x25519-dalek` dev-dependencies
  mirroring the product's derivation. Both domains run
  `MURMUR SET TABLE live_typed_records REPLICATED true` before any
  writes so row content enters the replication log the bridge exports.
- Plain SQL over the wire replaces the typed HTTP API; there is no HTTP
  server in the Rust product.
- `TypedRename` updates the LWW `title` field rather than the primary key,
  so row identity is preserved (the `typed-records` port convention); this
  is also required because `promote` only claims ownership of non-PK
  cells.
- `BridgeExport`/`BridgeImport` have no wire equivalent; the replication
  engine drives capture/process on its 1 s background tick in each server,
  so export/import progress is proven by row convergence (a wait poll)
  instead of `published`/`imported` counters. The suppression leg waits
  for a new bundle file in the shared staging dir (proves Low exported
  the update) plus a 10 s settle covering several High import ticks,
  then asserts the High-owned value held and nothing leaked.
- `BridgeRelease` maps to `MURMUR BRIDGE RELEASE live_typed_records
  'bridge-native-record' COLUMN title` (single-TEXT-key row, like
  `MURMUR POLICY`); the leg retries while the engine reports busy
  (`released` = false), then proves the next Low update flows through
  and replaces the High-owned value.
- Bridge keys are fixed test constants (signer seed, X25519 secret,
  domain and node IDs) following the `tests/bridge_files.rs` fixed-key
  convention; the staging dir lives under Low's runtime dir so it is
  retained on failure and cleaned on pass together with the cluster
  dirs.
- Two clusters in one test means two `RESULT` lines (one per cluster
  name) and two forensics dumps on failure; cluster setup itself is
  outside the body, so a setup failure panics without artifact
  retention, matching the single-cluster template behavior.

## Compile check (only gate; never run here)

```sh
CARGO_TARGET_DIR=/home/marc/MURMUR-SQL/tests-live/target \
  cargo test --no-run --manifest-path /home/marc/MURMUR-SQL/tests-live/typed-bridge/Cargo.toml
```
