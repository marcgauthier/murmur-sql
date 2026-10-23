# files-crash (Go port)

Rust source: `/home/marc/MURMUR/tests-live/files-crash/`
(`files_crash.rs` + `README.md`).

## What it proves

A two-node encrypted mesh (file object key `0x42..`, 4 MiB file cap)
proves file crash consistency. One test
(`TestFileCrashLeavesNoPartialOrPhantom`) follows the Rust phases in Rust
order: a positive control (upload/replicate/fetch/verify plus a clean
baseline inventory), phase 1 (uploader killed mid-upload), phase 2
(fetcher killed mid-fetch, then the interrupted fetch completes
cleanly after restart), and phase 3 (full recovery proof: metadata
settles identically, every listed object downloads byte-complete and
digest-verified on both nodes, and every on-disk payload is
attributable to a known attempt and bounded in count).

Pass criteria: a valid mid-upload kill and a valid mid-fetch kill
each land within 3 rounds (else the test fails loudly, as in Rust);
afterwards metadata converges exactly with no phantoms and no lost
acked uploads; every `.spfo` is referenced by metadata or
attributable to a known upload (orphans bounded by errored uploads);
upload stage files exist only on the uploader and are bounded by
errored uploads; fetch stage files exist only on the fetcher, are all
attributable, and are bounded by fetch attempts + 2.

## Env knobs

- `MURMUR_FILES_CRASH_BIG_MB` (default 3): size in MiB of the
  interrupted fetch payload (Rust: same name and default).
- Standard harness knobs (`MURMUR_LIVE_RUNTIME`,
  `MURMUR_LIVE_FAILURES`, `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
  `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`).

Convergence waits are 30 s with 50 ms polls (60 s for the final
metadata settle), matching the Rust bounds; peer fetch runs on a
100 ms `fetch_interval`. Kill coordination polls every 5 ms with a
15 s bound, as in Rust.

## Deviations from the Rust original

- In-process multi-`Database` cluster, same as Rust.
- No HTTP: uploads, downloads, metadata, and search use the direct
  `Database` file API (`UploadFile`/`GetFile`/`FileMetadata`/
  `ListFiles`); file names, sizes, object key, and the 4 MiB file cap
  keep the Rust values.
- No explicit fetch call exists in the product: peer fetch is
  automatic on `fetch_interval`, so fetch steps become `FileAvailable`
  waits. Each availability wait counts one fetch attempt for the
  inventory bound.
- Signals cannot target one in-process node, so the two kill legs
  are modeled in-process:
  - Mid-upload kill: one upload per round runs over a byte stream
    that aborts after 256 KiB; the product must drop the staged
    temp and fail the upload, and the uploader is then
    closed/reopened (crash aftermath) with a re-mesh.
  - Mid-fetch kill: the fetcher is closed/reopened while a
    `.part` transfer file is on disk in its `objects/staging`
    dir (polled every 5 ms).
- Upload staging has the product name: inventory collects
  `.upload-*`/`.rotate-*`. Completed fetches rename the `.part` into
  place, so settled runs normally hold zero `.part` files; the bound
  (fetch attempts + 2) is kept for tolerance.
- Digests of errored uploads are unknown: the payload pattern is
  name-keyed (`(len*31 + name[i%len] + i*17) % 251`, no hash
  dependency) and expected bytes are regenerated for comparison,
  so only acknowledged uploads contribute known digests.
- The Rust metadata digest becomes a direct string comparison of the
  same canonical `name|digest|size|deleted;` listing.
