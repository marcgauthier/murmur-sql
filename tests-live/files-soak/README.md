# files-soak (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/files-soak/`
(`files_soak_test.go` + `README.md`).

## What it proves

A two-node encrypted mesh (file object key `0x51..`, 4 MiB file cap,
two `murmur-server` processes over the PostgreSQL wire protocol) soaks
64 KiB file uploads for `MURMUR_FILES_SOAK_DURATION_SECONDS`
(default 5 s), one upload every `MURMUR_FILES_SOAK_INTERVAL_SECONDS`
(default 1 s). One async test (`encrypted_file_fetch_soak`, from
`TestEncryptedFileFetchSoak`) proves the Go pass criteria in Go order:
each round uploads `soak/file-NNNN.bin` (payload
`data[i] = (round*31 + i*17) % 251`) to node 1, waits for replicated
metadata on node 2, fetches, downloads, verifies the digest, and
searches the replicated names; afterwards the fetch gate shows every
file completed with zero failures and the first file reads
existing + available.

Pass criteria: at least one upload round; every uploaded file is
locally available on the peer with its upload digest; the fetcher
recorded no error (behavioral: availability + digest, since the wire
exposes no fetch-error signal); `soak/file-0000.bin` exists and is
available; per-round search returns exactly the files uploaded so
far; upload p95 stays under 5 s and metadata-to-verified-fetch p95
under 20 s.

## Env knobs

- `MURMUR_FILES_SOAK_DURATION_SECONDS` (default 5): soak wall-clock
  budget. Long soak: `MURMUR_FILES_SOAK_DURATION_SECONDS=600
  MURMUR_FILES_SOAK_INTERVAL_SECONDS=60`.
- `MURMUR_FILES_SOAK_INTERVAL_SECONDS` (default 1): sleep between
  rounds (skipped after the last round).
- Standard harness knobs (`MURMUR_LIVE_RUNTIME`,
  `MURMUR_LIVE_FAILURES`, `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
  `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`, `MURMUR_SERVER_BIN`).

Convergence waits are 20 s with 50 ms polls, matching the Go
`WaitFileAvailable` bound; peer fetch runs on a 100 ms
`fetch_interval`.

## Deviations from the Go original

- Multi-process `murmur-server` children over the PostgreSQL wire
  protocol instead of `testnode` daemon processes over HTTPS+mTLS
  (per design).
- No HTTP: uploads, downloads, metadata, and search use `MURMUR FILE`
  wire commands (`UPLOAD BEGIN` / chunked `UPLOAD APPEND` over the
  extended protocol with raw BYTEA / `UPLOAD FINISH`; `DOWNLOAD` /
  `DOWNLOAD NEXT` pages; `LIST` / `SEARCH` + `NEXT` + `CLOSE`).
  File names, payload bytes, payload pattern, object key, and the 4
  MiB file cap keep the Go values.
- No explicit fetch call exists in the Rust product: peer fetch is
  automatic on `fetch_interval`, so the Go `FetchFile` steps become
  `local`-flag waits. The Go negative fetch interval (disabled
  background worker) has no Rust equivalent (`fetch_interval` must
  be non-zero); 100 ms keeps the steps deterministic while
  converging fast.
- The Go `FetchStats` gate (completed count plus zero failures) maps
  to per-file availability + digest checks over every uploaded file.
  The old in-process port also asserted `last_fetch_error() is None`;
  that sticky error is process memory with no wire equivalent, so the
  zero-failures leg is proven behaviorally instead.
- The Go `StatFile` exists + available gate maps to a `LIST` hit for
  `soak/file-0000.bin` plus its `local` flag.
- The SHA-256 gate is digest consistency plus full byte equality:
  the upload digest (product-computed SHA-256 hex) must match the
  peer metadata digest and the download must be byte-identical. No
  independent hash recomputation and no new registry dep.
- `search_files("file-")` (literal substring) becomes wire
  `SEARCH '%file-%'` (case-sensitive LIKE over whole names).
- The Go `t.Logf` soak summary (rounds, p95s, fetched bytes) maps to
  an `eprintln!` line without the fetched-bytes counter (no Rust
  equivalent); the latency SLO gates are unchanged.

## Compile check

```sh
  go test -count=1 ./tests-live/files-soak/
```
