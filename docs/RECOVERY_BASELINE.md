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

## PR6 results (v1.1, same hardware, warm page cache)

Same fixture/seed/encryption as above, single timed reopen
(`-benchtime=1x`), per-table counts + boundary spots re-verified
every iteration. STALE applies post-build writes to `docs` only:
1% = 1% updates + 1% inserts + 1% deletes; 10% likewise; 100% =
every docs row updated (worst-case full overlay). Timed cache
opens disable auto-rebuild so a stale hit cannot trigger a build
mid-measurement. Cold-cache numbers were not taken (no permission
to drop page cache); all rows below are warm.

### 100k rows

| Mode | Startup | Spool open | Diff scan | Row insert | Idx build | Integrity | Changed | Heap | Cache |
|------|---------|------------|-----------|------------|-----------|-----------|---------|------|-------|
| LEGACY | 8,118 ms | 3,771 ms | n/a | 4,276 ms | 9 ms | 60 ms | n/a | 134 MB | n/a |
| CACHE_EXACT | 4,910 ms | 3,772 ms | 108 ms | 972 ms | 8 ms | 46 ms | 0 | 168 MB | 30.3 MB |
| CACHE_STALE 1% | 5,034 ms | 3,879 ms | 108 ms | 989 ms | 9 ms | 46 ms | 1,788 | 168 MB | 30.3 MB |
| CACHE_STALE 10% | 5,618 ms | 4,365 ms | 151 ms | 1,047 ms | 6 ms | 46 ms | 16,884 | 177 MB | 30.3 MB |
| CACHE_STALE 100% | 9,257 ms | 4,089 ms | 3,681 ms | 1,430 ms | 8 ms | 45 ms | 60,000 | 326 MB | 30.3 MB |

Whole-startup 1.65x (8.1s → 4.9s). Restore portion
(diff + rows + index): 4,285 ms → 1,088 ms, **3.9x**, past the 2x
acceptance target for that portion. Spool open is unchanged by
design (~3.8s) and now dominates. STALE 1% costs +2.5% over
exact; STALE 10% +14%; STALE 100% lands at rough legacy parity
(+14%, the 3.7s diff stage fetching 60k current values) —
correct, but no win at full churn.

### 1M rows

| Mode | Startup | Spool open | Diff scan | Row insert | Idx build | Integrity | Changed | Heap | Cache |
|------|---------|------------|-----------|------------|-----------|-----------|---------|------|-------|
| LEGACY | 147,166 ms | 66,838 ms | n/a | 79,290 ms | 128 ms | 906 ms | n/a | 485 MB | n/a |
| CACHE_EXACT | 88,884 ms | 74,567 ms | 1,642 ms | 12,015 ms | 130 ms | 525 ms | 0 | 492 MB | 301.8 MB |
| CACHE_STALE 1% | 81,193 ms | 68,816 ms | 1,380 ms | 10,384 ms | 118 ms | 491 ms | 17,893 | 361 MB | 301.8 MB |

Whole-startup 1.66x (147s → 89s). Restore portion: 79,418 ms →
13,787 ms, **5.8x**. Row insert alone drops 79s → 12s. Spool open
(67–75s run-to-run) is the remaining wall — it also explains why
STALE 1% (81s) reads slightly faster than EXACT (89s) here: the
restore portions are at parity (11.9s vs 13.8s) and the delta is
spool-open noise between runs, not a stale-is-faster effect.
The Spool index checkpoint (plan §9.A) is the identified next
step for whole-startup. Cache size scales linearly: ~302 B/row
(30.3 MB @ 100k, 301.8 MB @ 1M), ~63% of the Spool bytes.
5M rows were not run (fixture + single-iteration time).

### Ingestion while the builder runs (100k store)

`BenchmarkRecoveryBuildUnderWrites`: 1,000 single-row commits
idle, then commits until the 100k build finishes (~29k commits
overlapped the ~3.2s build). Unfsynced micro-commits, the worst
case for relative interference:

| Phase | ops/s | p50 | p95 | p99 |
|-------|-------|-----|-----|-----|
| builder idle | 10,764 | 0.087 ms | 0.124 ms | 0.138 ms |
| builder running (zstd-3) | 7,881 | 0.091 ms | 0.125 ms | 0.156 ms |
| builder running (zstd-1) | 7,831 | 0.089 ms | 0.126 ms | 0.164 ms |

Absolute interference is microseconds: +4µs p50, +1µs p95,
+18µs p99. Relatively that reads as −27% throughput / +13% p99,
missing the plan's 5%/10% targets — but only because unfsynced
micro-commits are sub-100µs; any fsync-backed workload buries
this delta. zstd level 1 vs 3 shows no measurable build-time
(3.21s vs 3.26s) or interference difference: codec CPU is not
the driver, so level 3 is retained for size. The post-build
reopen in this benchmark hits the cache with ~23k overlay rows
and verifies, proving overlapping writes reconcile.

### What was not measured

Cold page cache (no root to drop caches), per-operation read
bytes / files opened / AES-vs-zstd CPU split (stage timings and
build duration bound the analysis instead), and multi-writer /
replication-under-build at scale beyond the two-node
`TestReplicationWithRecoveryCache` smoke test. Default-on awaits
that extended soak evidence; v1.1 ships the cache opt-in.
