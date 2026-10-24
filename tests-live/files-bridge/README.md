# files-bridge (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/files-bridge/`
(`files_bridge_test.go` + `README.md`).

## What it proves

A two-node Low mesh and a two-node High mesh (separate DBIDs, CAs,
storage keys, and file object keys: `0x11..` on Low, `0x22..` on High;
four `murmur-server` processes over the PostgreSQL wire protocol)
move one 384 KiB file (`shared/field-map.bin`, stream `field-files`)
across a shared staging directory. One async test
(`two_low_two_high_file_bridge_live`, from
`TestTwoLowTwoHighFileBridgeLive`) proves the Go pass criteria in Go
order: Low upload, Low mesh metadata convergence plus digest-verified
peer fetch, signed export of sealed file chunks, staged artifacts free
of document plaintext, High import with re-encryption under the High
object key, High mesh metadata plus digest-verified peer fetch, and
metadata search returning the one replicated file. The Go delete
cascade is dropped: there is no file-delete wire command (see below).

Pass criteria: Low-1 downloads byte-identical content with the upload
digest; staging holds at least 2 sealed `*.bundle` files and none
contains any of the three 1 KiB payload windows; High-0 downloads
byte-identical content with the same digest while its at-rest `*.spfo`
bytes differ from Low-0's; High-1 downloads byte-identical content;
wire `SEARCH 'shared/%'` on High-1 returns exactly the one file with
the upload digest and byte count.

## Env knobs

Standard harness knobs only (`MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
`MURMUR_LIVE_UNLOCK_STALL_SECONDS`, `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`,
`MURMUR_SERVER_BIN`).
Convergence waits are 30 s with 50 ms polls, matching the Go timeouts;
peer fetch runs on a 100 ms `fetch_interval` (the `typed-records` files
pattern).

## Deviations from the Go original

- Multi-process `murmur-server` children over the PostgreSQL wire
  protocol instead of `testnode` daemon processes over HTTPS+mTLS
  (per design).
- No HTTP: uploads, downloads, metadata, and search use `MURMUR FILE`
  wire commands (`UPLOAD BEGIN` / chunked `UPLOAD APPEND` / `UPLOAD
  FINISH`; `DOWNLOAD` / `DOWNLOAD NEXT`; `LIST`/`SEARCH` + `NEXT` +
  `CLOSE`). File name, stream, payload bytes, payload pattern, object
  keys, and 4 MiB file cap keep the Go values.
- `BridgeExport`/`BridgeImport` have no wire equivalent: the
  replication engine drives `capture()`/`process()` automatically
  every apply tick (~1 s), so the test only polls file state (no
  explicit pumps like the old in-process port). Progress is proven
  by file state instead of `published`/`imported` counters.
- No explicit fetch call exists in the Rust product: peer fetch is
  automatic on `fetch_interval`, so the Go `FetchFile` steps become
  `local`-flag waits. The Go negative fetch interval (disabled
  background worker) has no Rust equivalent (`fetch_interval` must
  be non-zero); 100 ms keeps the steps deterministic while
  converging fast.
- The SHA-256 gate is digest consistency plus full byte equality: the
  upload digest (product-computed SHA-256 hex) must match every later
  metadata digest and every download must be byte-identical. No
  independent hash recomputation.
- The no-plaintext scan covers `*.bundle` files only. The receiver's
  transient `.file-*.part` reassembly state lives in the shared staging
  dir too once import starts and is plaintext by design, so scanning
  every file would race the background import; sealed bundles are the
  actual staged artifacts.
- The Go delete cascade is dropped: the Rust wire protocol has no
  file-delete command (`Objects::delete` is in-process only), so the
  `DeleteFile` leg and its tombstone-across-the-bridge proof have no
  equivalent. The old in-process port covered them via the direct
  `Database` API.
- Fixed test seeds replace the Go generated bridge key files (Low
  bundle-signer seed, High X25519 receiver secret); the High receiver
  pins the Low signer public key and trusts both Low origin keys.
  Public keys are derived with the in-module `murmur/replication`
  helpers (`PublicKey`/`RecipientPublic`, same logic as the product).

## Compile check

```sh
  go test -count=1 ./tests-live/files-bridge/
```
