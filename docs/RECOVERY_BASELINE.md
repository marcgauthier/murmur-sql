# Recovery cache — PR0 legacy baseline

Recorded for the rebuildable-recovery-cache project (murmur v1.1 plan, PR0).
Fixture: `recoveryFixtureSpec` in `murmur/recovery_baseline_test.go`, seed 42:
`docs` (TEXT UUID-style PK, TEXT/BLOB/INTEGER/REAL columns),
`events` (INTEGER PK + secondary index), `pairs` (composite PK).
Inserts-only loads; the mutation mix (updates/deletes/reinserts) is covered
at small scale by `TestLegacyRebuildMatchesSpool`.

Encryption on (32-byte master key), zstd level 3 (defaults), warm OS page
cache. Every benchmark iteration re-verifies per-table counts and
first/last-25 boundary spot checks against the pre-close state.

## Environment

- CPU: Intel i5-6500 @ 3.20GHz, 4 threads (`-4`)
- OS: linux/amd64, Go 1.24.7, CGO on (vendored SQLite)
- git: `553b8d5` + PR0 instrumentation (stage timings only, no behavior change)
- Command: `CGO_ENABLED=1 go test ./murmur/ -run '^$'
  -bench 'BenchmarkRecoveryLegacy_10k|100k|1M' -benchtime=1x -count=1`

## Results (LEGACY mode, single reopen)

| Dataset | Startup | Spool open | Row insert | Idx build | Integrity | Search | Heap | Sys | Disk |
|---------|---------|------------|------------|-----------|-----------|--------|------|-----|------|
| 10k rows | 213 ms | 103 ms | 103 ms | 0 ms | 4 ms | 0 ms | 39 MB | 70 MB | 4.8 MB |
| 100k rows | 8,496 ms | 3,948 ms | 4,475 ms | 9 ms | 63 ms | 0 ms | 172 MB | 253 MB | 47.8 MB |
| 1M rows | 152,945 ms | 69,735 ms | 82,140 ms | 128 ms | 937 ms | 0 ms | 404 MB | 1,507 MB | 476.7 MB |

Stage share at 1M: spool open ~46%, row insert ~54%, index + check ~1%.

## Decision-gate reading

Spool open (`OpenAndLoadOrdered` two-pass scan + replay) is roughly half of
startup; SQLite row/insert rebuild is the other half. The sorted-run
recovery cache (plan PR1–PR4) targets the SQLite portion only — it cannot
beat ~2x whole-startup while Spool still scans segments. If whole-startup
needs more after PR4, the Spool index checkpoint (plan §9.A) is the
identified follow-up. No speedup is claimed before measurement; the
CACHE_EXACT / CACHE_STALE rows will be added here by PR6.
