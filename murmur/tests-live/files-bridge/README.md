# files-bridge (Go port)

Rust source: `/home/marc/MURMUR/tests-live/files-bridge/`
(`files_bridge.rs`: `two_low_two_high_file_bridge_live`).

## What it proves

A two-node Low mesh and a two-node High mesh (separate DBIDs, CAs,
storage keys, and file object keys: `0x11..` on Low, `0x22..` on High)
move one 384 KiB file (`shared/field-map.bin`, stream `field-files`)
across a shared staging directory: Low upload, Low mesh metadata
convergence plus digest-verified peer fetch, signed export of sealed
file chunks, staged artifacts free of document plaintext, High import
with re-encryption under the High object key, High mesh metadata plus
digest-verified peer fetch, metadata search returning the one
replicated file, and a delete cascade ending with the file reading
deleted on all four nodes.

Pass criteria: Low-1 downloads byte-identical content with the upload
digest; staging holds at least 2 sealed `*.bundle` files and none
contains any of the three 1 KiB payload windows; High-0 downloads
byte-identical content with the same digest while its at-rest `*.spfo`
bytes differ from Low-0's; High-1 downloads byte-identical content;
`SearchFiles("shared/")` on High-1 returns exactly the one file with
the upload digest and byte count; after the Low delete, all four nodes
read the file as deleted.

## Env knobs

Standard harness knobs only (`MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
`MURMUR_LIVE_UNLOCK_STALL_SECONDS`, `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`).
Convergence waits are 30 s with 50 ms polls; peer fetch runs on a
100 ms `fetch_interval`.

## Deviations (inherited from Rust)

- No HTTP: uploads, downloads, metadata, search, and deletes use the
  direct `Database` file API. File name, stream, payload bytes, payload
  pattern, object keys, and 4 MiB file cap keep the original values.
- `BridgeExport`/`BridgeImport` map to `Bridge.ExportFile` plus direct
  `Bridge.Capture()` (Low-0) / `Bridge.Process()` (High-0) pumps, with
  a pump inside every bridge wait poll. Per-call bundle counts are not
  asserted: the replication engine also drives capture/process in the
  background, so progress is proven by file state instead.
- No explicit fetch call exists in the product: peer fetch is automatic
  on `fetch_interval`, so fetch steps become `FileAvailable` waits.
- The digest gate is digest consistency plus full byte equality: the
  upload digest must match every later metadata digest and every
  download must be byte-identical.
- The no-plaintext scan covers `*.bundle` files only: the receiver's
  transient `.file-*.part` reassembly state lives in the shared staging
  dir too once import starts and is plaintext by design.
- The delete tombstone crosses the bridge as a captured file-metadata
  transaction (`Capture`/`Process` pumps); `ExportFile` only handles
  live files.
- Fixed test seeds replace generated bridge key files (Low
  bundle-signer seed, High X25519 receiver secret); the High receiver
  pins the Low signer public key and trusts both Low origin keys.
