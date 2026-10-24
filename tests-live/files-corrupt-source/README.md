# files-corrupt-source (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/files-corrupt-source/`
(`files_corrupt_source_test.go` + `README.md`).

## What it proves

A two-node encrypted mesh (file object key `0x42..`, 4 MiB file cap,
two `murmur-server` processes over the PostgreSQL wire protocol)
proves that fetching from a corrupt source fails verification. One
async test (`corrupt_source_fetch_fails_verification`, from
`TestCorruptSourceFetchFailsVerification`) follows the Go phases in Go
order: a positive control (upload/replicate/fetch/verify), honest
publication of a victim object, corruption of its stored container
bytes on the source node while stopped (same length, 16 flipped bytes
in the middle of the container, i.e. inside chunk ciphertext), restart
and re-mesh, then the failing fetch legs.

Pass criteria: the peer fetch fails verification with no partial
install: with victim metadata honestly replicated (fetch is attempted
every 100 ms), the fetching node never reports the victim locally
available across a full 30 s window and refuses its download; it
holds neither an object file for the unverified digest nor any
staging residue; victim metadata stays intact (original digest/size,
still listed) on both nodes; the corruption is persistent (still
unavailable after more background retry cycles); and honest objects
still fetch afterwards. (The old in-process port asserted the
`object authentication` fetch-error sentinel; that sticky error is
process memory with no wire equivalent, so the new port proves the
failure behaviorally.)

## Env knobs

- `MURMUR_FILES_CORRUPT_VICTIM_KB` (default 256): victim payload size
  in KiB (Go: same name and default).
- Standard harness knobs (`MURMUR_LIVE_RUNTIME`,
  `MURMUR_LIVE_FAILURES`, `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
  `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`, `MURMUR_SERVER_BIN`).

Convergence waits are 30 s with 50 ms polls, matching the Go bounds;
peer fetch runs on a 100 ms `fetch_interval` (the `files-soak`
pattern). The staging-quiescence wait is 5 s with 10 ms polls, and
the retry-persistence window is 500 ms (~5 background fetch cycles).

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
  be non-zero); instead the fetcher is stopped while the victim is
  published and corrupted, then restarted, so no fetch can start
  before the corruption lands. This is deterministic where stopping
  the source after metadata arrival would race the first fetch
  tick. Node restart is stop/respawn plus re-mesh (fresh SQL ports;
  the unlock watchdog runs inside `Cluster::start_node`).
- The Go `objectstore: invalid encrypted object` sentinel
  (`Error::Auth("object authentication")` in the product, recorded
  in the sticky `last_fetch_error`) is process memory with no wire
  equivalent, so verification failure is proven behaviorally: the
  victim never becomes locally available across a full 30 s window
  while its metadata is present, and downloads fail. The Go
  fetch-failed counter has no Rust equivalent either.
- The Go second explicit `FetchFile` becomes a retry-persistence
  observation: after ~5 more background fetch cycles the victim
  must still be unavailable, proving the corruption is persistent
  rather than transient.
- Staging-residue checks observe a quiescent moment between 100 ms
  retry ticks (failed Rust fetches synchronously discard the
  `.part` plus its `.identity` sidecar in `publish_staged`,
  verified in `replication/src/filefetch.rs`), then require no
  `.part` anywhere under the fetching node dir. Any `.part` counts
  as residue (successful Rust fetches rename the `.part` into
  place), which is stronger than Go's digest-prefixed match. The
  32-byte `.identity` resume sidecars have no Go equivalent and are
  removed on the same failure path.
- Digests are compared via replicated-metadata equality and
  byte-exact download round-trips: the payload pattern is
  name-keyed (`(len*31 + name[i%len] + i*17) % 251`, the
  `files-crash` pattern, no hash dependency) instead of Go's
  SHA-256-seeded bytes.

## Compile check

```sh
  go test -count=1 ./tests-live/files-corrupt-source/
```

