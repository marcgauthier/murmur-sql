# Recovery cache

Optional, disposable read cache that accelerates database restarts.
Available since murmur v1.1. Disabled by default.

## What it is

When enabled, murmur keeps sorted, compressed row runs under
`<data_dir>/recovery/`, rebuilt in the background from immutable
Spool checkpoints. On the next open, murmur restores SQLite from
these sequential runs plus a small overlay of rows that changed
since the capture, instead of decoding every row out of Spool.

What the cache does not do:

- It never changes the authoritative format. Spool segments,
  `manifest`, `keys.enc`, replication logs, CRDT metadata, and
  tombstones are untouched. An old binary opens a database with a
  `recovery/` directory exactly as before (it ignores the sidecar).
- It does not avoid SQLite inserting rows or rebuilding indexes;
  it only makes the row source sequential instead of random.
- It does not avoid opening Spool. Spool open time is unchanged;
  see the benchmark notes below.

## Enabling

`config.yaml`:

```yaml
engine:
  recovery:
    enabled: true
    auto_build: true
    build_interval: "1h"
    target_run_bytes: "256MiB"
    target_block_bytes: "256KiB"
    max_build_memory: "256MiB"
    retain_generations: 2
```

Go API:

```go
options := murmur.DefaultOptions()
ro := murmur.DefaultRecoveryOptions()
ro.Enabled = true
ro.AutoBuild = true
options.Recovery = &ro
db, err := murmur.Open(path, key, options)
```

A nil `options.Recovery` (for example code that builds `Options`
by hand) keeps the legacy path with no behavior change.

First open after enabling uses legacy recovery, then builds the
cache in the background. The database is available before the
build finishes. Later opens use the cache automatically.

## Safety

- **Deletion is always safe.** `rm -rf <data_dir>/recovery` on a
  closed database loses no data: the next open falls back to
  legacy recovery and rebuilds the cache. Backups (`CopyStoreDir`,
  `RestoreBackup`) never include the directory.
- **Corruption fails open to legacy.** A missing, truncated,
  tampered, half-published, or schema-mismatched cache is ignored
  with a recorded reason; authoritative Spool errors still fail
  the open loudly.
- **Encryption follows the store.** An encrypted Spool produces
  an encrypted cache, encrypted sort spills, and an authenticated
  manifest (0600 files, 0700 directories). A plaintext cache on an
  encrypted store (or vice versa) is rejected.
- **Key rotation is safe.** Rotating the data key keeps the cache
  readable while the old key is retained. If maintenance later
  prunes the key the cache was sealed with, the open falls back
  and rebuilds under the current key.
- **Schema changes invalidate.** A generation only restores when
  its schema digest matches exactly; after a migration the open
  falls back once and rebuilds.
- **Writes never wait for the cache.** Commits, capture, remote
  apply, and replication never open, update, or block on recovery
  files. A failed or cancelled build is recorded in status and
  retried later; it is never a database error.

## Observability

`db.Status().Recovery`:

| Field | Meaning |
|-------|---------|
| `Outcome` | `cache-hit`, `legacy-fallback`, or `legacy-disabled` |
| `Reason` | why the cache missed (fallback only) |
| `Generation` | cache generation used (hit only) |
| `ChangedRows` / `TotalRows` | overlay size / restored rows |
| `CacheBytes` | generation size on disk |
| `CaptureCursor` | durable SQL cursor at capture |
| `CacheAgeSeconds` | generation age at open |
| `LastBuild` | latest background build (rows, bytes, seconds, error) |

`db.Status().OpenStages` splits the open into `SpoolOpen`,
`RecoveryDiff` (key/sequence reconciliation), `RowInsert`,
`IndexBuild`, `IntegrityCheck`, and `SearchInit`.
`murmur.RecoveryFallbackTotal()` counts fallback opens in the
process.

## Tuning

- `target_run_bytes` (default 256 MiB): one sorted run per table
  split at this size. Larger tables split into several runs.
- `target_block_bytes` (default 256 KiB): uncompressed block
  plaintext inside a run. 64 KiB / 1 MiB remain plan starting
  points for future tuning; only the 256 KiB default is measured
  in v1.1.
- `max_build_memory` (default 256 MiB): builder memory; overflow
  spills to encrypted temp runs. It also bounds the restore
  overlay; an overlay beyond the bound falls back to legacy.
- `retain_generations` (default 2): published generations kept
  across replacement. The generation `CURRENT` points at is
  always retained.
- `build_interval` (default 1h): minimum spacing between
  automatic rebuilds. Zero disables throttling (tests).
- The cache codec tracks `compression_level` (zstd). Level 1 vs 3
  showed no measurable build-time or ingestion-impact difference
  at 100k rows; level 3 is retained for size.

Startup always pays the Spool open cost (two-pass segment scan),
which dominates once the restore portion shrinks; further
whole-startup wins need a Spool index checkpoint (future work,
plan section 9.A), not more restore tuning.

Measured numbers, method, and hardware are in
[RECOVERY_BASELINE.md](RECOVERY_BASELINE.md).

## Limitations in v1.1

- Default off. Default-on awaits extended soak/chaos evidence;
  `enabled: false` remains a supported escape hatch either way.
- No `MURMUR RECOVERY` SQL administration statements yet;
  status is API-only. Cache management is: enable it, or delete
  the directory.
- No per-write delta log: every open enumerates live Spool key
  metadata for reconciliation (O(N) key scan, no value reads).
  A persistent changed-key feed is future work (plan 9.D).
