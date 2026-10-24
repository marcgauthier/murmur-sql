# files-crash (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/files-crash/`
(`files_crash_test.go` + `README.md`).

## What it proves

A two-node encrypted mesh (file object key `0x42..`, 4 MiB file cap,
two `murmur-server` processes over the PostgreSQL wire protocol)
proves file crash consistency. One async test
(`file_crash_leaves_no_partial_or_phantom`, from
`TestFileCrashLeavesNoPartialOrPhantom`) follows the Go phases in Go
order: a positive control (upload/replicate/fetch/verify plus a clean
baseline inventory), phase 1 (uploader killed mid-upload), phase 2
(fetcher killed mid-fetch, then the interrupted fetch completes
cleanly after restart), and phase 3 (full recovery proof: metadata
settles identically, every listed object downloads byte-complete and
digest-verified on both nodes, and every on-disk payload is
attributable to a known attempt and bounded in count).

Pass criteria: a valid mid-upload kill and a valid mid-fetch kill
each land within 3 rounds (else the test fails loudly, as in Go);
afterwards metadata converges exactly with no phantoms and no lost
acked uploads; every `.spfo` is referenced by metadata or
attributable to a known upload (orphans bounded by errored uploads);
upload stage files exist only on the uploader and are bounded by
errored uploads; fetch stage files exist only on the fetcher, are all
attributable, and are bounded by fetch attempts + 2.

## Env knobs

- `MURMUR_FILES_CRASH_BIG_MB` (default 3): size in MiB of the
  interrupted fetch payload (Go: same name and default).
- Standard harness knobs (`MURMUR_LIVE_RUNTIME`,
  `MURMUR_LIVE_FAILURES`, `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
  `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`).

Convergence waits are 30 s with 50 ms polls (60 s for the final
metadata settle), matching the Go bounds; peer fetch runs on a
100 ms `fetch_interval` (the `files-soak` pattern). Kill
coordination polls every 5 ms with a 15 s bound, as in Go. The
post-crash fetch wait alone is 90 s (deviation below).

## Deviations from the Go original

- Multi-process `murmur-server` children over the PostgreSQL wire
  protocol instead of `testnode` daemon processes over HTTPS+mTLS
  (per design).
- No HTTP: uploads, downloads, and metadata use `MURMUR FILE` wire
  commands (`UPLOAD BEGIN` / chunked `UPLOAD APPEND` / `UPLOAD
  FINISH`; `DOWNLOAD` / `DOWNLOAD NEXT`; `LIST` + `NEXT` + `CLOSE`).
  File names, sizes, object key, and the 4 MiB file cap keep the Go
  values.
- No explicit fetch call exists in the Rust product: peer fetch is
  automatic on `fetch_interval`, so the Go `FetchFile` steps become
  `local`-flag waits. The Go negative fetch interval (disabled
  background worker) has no Rust equivalent (`fetch_interval` must
  be non-zero); 100 ms keeps the steps deterministic while
  converging fast. Each availability wait counts one fetch attempt
  for the inventory bound. Node restart is stop/respawn plus
  re-mesh (fresh SQL ports; the unlock watchdog runs inside
  `Cluster::start_node`).
- Both kill legs are real process kills (the old in-process port
  modeled them with an aborting byte stream, since signals cannot
  target one in-process node):
  - Mid-upload kill: one upload per round appends 256 KiB, then the
    uploader is killed before FINISH; the product must drop the
    staged upload and never publish it (asserted after restart),
    and the uploader is then restarted with a re-mesh (crash
    aftermath). A round counts as a valid kill when the kill
    lands, as in Go.
  - Mid-fetch kill: the fetcher is killed while a `.part`
    transfer file is on disk in its `objects/staging` dir
    (polled every 5 ms; the Go `FetchStats.InFlight`
    equivalent). A round that completes the fetch first retries,
    and the test fails loudly if no valid kill lands in 3
    rounds, as in Go.
- Upload staging has the Rust name: inventory collects
  `.upload-*`/`.rotate-*` (Go: `.stage-*`). Rust completed fetches
  rename the `.part` into place (Go leaves them), so settled runs
  normally hold zero `.part` files; the Go bound (fetch attempts
  + 2) is kept for tolerance. The 32-byte `.identity` resume
  sidecars have no Go equivalent and are not payloads, so
  inventory ignores them.
- Digests of errored uploads are unknown: the payload pattern is
  name-keyed (`(len*31 + name[i%len] + i*17) % 251`, no hash
  dependency) and expected bytes are regenerated for comparison,
  so only acknowledged uploads contribute known digests. This is
  sound because the Rust uploader removes its staged temp on a
  stream error (verified in `replication/src/files.rs`), leaving
  no debris to attribute; the orphan bound (orphans <= errored
  uploads) is kept unchanged.
- The Go metadata digest (SHA-256 over the canonical listing)
  becomes a direct string comparison of the same canonical
  `name|digest|size;` listing: no new registry dep (wire LIST shows
  nondeleted files only, so there is no `deleted` column).
- Standard harness knobs add `MURMUR_SERVER_BIN`.
- The post-crash fetch wait is 90 s instead of Go's 30 s. Killing
  the fetcher with both transfers in flight (the norm: 2 fetch
  permits, several missing files) leaves two zombie serve tasks on
  the uploader holding its whole serve semaphore until the dead
  QUIC connections idle out (30 s `max_idle_timeout`); every new
  file connection is refused until then, so recovery lands at
  ~31 s. The product fails closed and self-heals; the port waits
  out the recovery tail.

## Compile check

```sh
  go test -count=1 ./tests-live/files-crash/
```
