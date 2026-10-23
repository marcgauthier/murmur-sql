# files-corrupt-source (Go port)

Rust source: `/home/marc/MURMUR/tests-live/files-corrupt-source/`
(`files_corrupt_source.rs` +
`TestCorruptSourceFetchFailsVerification`).

## What it proves

A two-node encrypted mesh (file object key `0x42..`, 4 MiB file cap)
proves that fetching from a corrupt source fails verification. One
test follows the Rust phases in Rust order: a positive control
(upload/replicate/fetch/verify), honest publication of a victim
object, corruption of its stored container bytes on the source node
while stopped (same length, 16 flipped bytes in the middle of the
container, i.e. inside chunk ciphertext), restart and re-mesh, then
the failing fetch legs.

Pass criteria: the peer fetch fails with the verification sentinel
(`object authentication`); the fetch-error signal advances from none
to that error; the fetching node reports the victim unavailable,
refuses its download, and holds neither an object file for the
unverified digest nor any staging residue; victim metadata stays
intact (original digest/size, not deleted) on both nodes; the
corruption is persistent (still failing the same verification after
more background retry cycles); and honest objects still fetch
afterwards.

## Env knobs

- `MURMUR_FILES_CORRUPT_VICTIM_KB` (default 256): victim payload size
  in KiB (same name and default).
- Standard harness knobs (`MURMUR_LIVE_RUNTIME`,
  `MURMUR_LIVE_FAILURES`, `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
  `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`).

Metadata/fetch waits are 30 s with 50 ms polls; peer fetch runs on
a 100 ms `fetch_interval`. The retry-persistence pacing sleep is
500 ms. The fail-closed watch and the retry-recurrence watch span
60 s each (see below).

## Deviations (inherited from Rust)

- No HTTP: uploads, downloads, metadata, and search use the direct
  `Database` file API (`UploadFile`/`GetFile`/`FileMetadata`/
  `ListFiles`). File names, sizes, object key, and the 4 MiB file cap
  keep the original values.
- No explicit fetch call exists: peer fetch is automatic on
  `fetch_interval`, so fetch steps become `FileAvailable` waits. The
  negative fetch interval (disabled background worker) has no
  equivalent (`fetch_interval` must be non-zero); instead the fetcher
  is stopped while the victim is published and corrupted, then
  restarted, so no fetch can start before the corruption lands.
  Node restart is close/reopen plus re-mesh (fresh ephemeral ports).
- The verification failure surfaces as the `object authentication`
  error (flipped chunk ciphertext fails AES-GCM decryption during
  fetch verification), observed via `LastFetchError`. There is no
  fetch-failed counter; the test asserts the error signal advances
  from none (clean-control baseline) to that sentinel.
- The second explicit fetch becomes a retry-persistence observation:
  after a 500 ms pacing sleep a recurring auth-sentinel sample must
  be observed (proving retries still fail verification) and the
  victim must still be unavailable, proving the corruption is
  persistent rather than transient.
- Go-only: the fetch-error latch is single-slot last-writer-wins
  shared with benign transport flakes (clean QUIC closes surface as
  recorded errors even on success), so every latch leg matches ANY
  sample in its window instead of a single sample; the clean-control
  baseline asserts no AUTH error rather than no error at all.
- Go-only: the fail-closed proof and the staging-residue proof share
  one joint watch (no `.part` while the latch holds the auth
  failure, which also excludes the pre-fetch initial state), off a
  single verify-failure: failed verification synchronously discards
  the `.part` plus its `.identity` sidecar. Any `.part` counts as
  residue (successful fetches rename the `.part` into place).
- Go-only windows: fetch attempts take seconds here (Rust: ms), so
  the joint fail-closed watch and the retry-recurrence watch span
  60 s each to cover several verify-failures; the joint poll is
  10 ms to catch the ~100 ms post-removal gap before the next tick
  starts a new attempt. All metadata/fetch waits keep the 30 s
  bounds.
- Known product sensitivity (not a test gap): the file server closes
  the transfer connection immediately after serving, which races
  already-buffered stream data on the client; under machine load the
  close systematically wins, attempts die with the benign close
  error, and fetches slow from seconds to minutes or stall. Slow
  runs should be retried on a quiet machine (or with
  `MURMUR_TEST_DEADLINE_SCALE` raised, which scales every wait).
- Digests are compared via replicated-metadata equality and
  byte-exact download round-trips: the payload pattern is name-keyed
  (`(len*31 + name[i%len] + i*17) % 251`, no hash dependency).
