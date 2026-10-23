# files-soak (Go port)

Rust source: `/home/marc/MURMUR/tests-live/files-soak/`
(`files_soak.rs`: `encrypted_file_fetch_soak`).

## What it proves

A two-node encrypted mesh (file object key `0x51..`, 4 MiB file cap)
soaks 64 KiB file uploads for `MURMUR_FILES_SOAK_DURATION_SECONDS`
(default 5 s), one upload every `MURMUR_FILES_SOAK_INTERVAL_SECONDS`
(default 1 s). Each round uploads `soak/file-NNNN.bin` (payload
`data[i] = (round*31 + i*17) % 251`) to node 1, waits for replicated
metadata on node 2, fetches, downloads, verifies the digest, and
searches the replicated names; afterwards the fetch gate shows every
file completed with zero failures and the first file reads
existing + available.

Pass criteria: at least one upload round; every uploaded file is
locally available on the peer with its upload digest and the fetcher
recorded no error; `soak/file-0000.bin` exists and is available;
per-round search for `"file-"` returns exactly the files uploaded so
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
  `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`).

Convergence waits are 20 s with 50 ms polls; peer fetch runs on a
100 ms `fetch_interval`.

## Deviations (inherited from Rust)

- No HTTP: uploads, downloads, metadata, and search use the direct
  `Database` file API. File names, payload bytes, payload pattern,
  object key, and the 4 MiB file cap keep the original values.
- No explicit fetch call exists in the product: peer fetch is automatic
  on `fetch_interval`, so fetch steps become `FileAvailable` waits.
- The `FetchStats` gate maps to per-file `FileAvailable` + digest checks
  over every uploaded file plus `LastFetchError()` returning nil.
- The digest gate is digest consistency plus full byte equality: the
  upload digest must match the peer metadata digest and the download
  must be byte-identical.
