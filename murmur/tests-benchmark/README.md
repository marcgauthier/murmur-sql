# Murmur Micro-Batched Benchmark (Go)

Go port of `tests-benchmark` from the Rust workspace, runnable via
`go test -bench`. It evaluates sustained throughput and latency under a
realistic 80% read / 20% write workload against Murmur's in-memory SQLite
core persisting to disk via Spool, using **micro-batching** (up to 50
requests or 5 ms).

## Schema

Same 10-column `users` table with a primary key and two secondary indexes:

```sql
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    username TEXT NOT NULL,
    email TEXT NOT NULL,
    full_name TEXT NOT NULL,
    role TEXT NOT NULL,
    status INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    login_count INTEGER NOT NULL,
    bio TEXT NOT NULL
);

CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_users_status_created ON users(status, created_at);
```

## Workload

* **Pre-seeding:** 20,000 rows by default, inserted in batches of 1,000.
* **Micro-batching:** requests buffer until 50 accumulate or 5 ms elapse;
  reads flush in one `Read` call, writes in one `Transaction` call, both
  using prepared statements.
* **80% reads:** 50% primary-key lookups, 50% secondary-index (`email`)
  lookups.
* **20% writes:** 50% profile updates (touching `updated_at`,
  `login_count`, `bio`, `status`, exercising `idx_users_status_created`),
  50% inserts of new accounts.
* **Persistence:** `capture_interval=1s`, `capture_bytes=8 MiB`,
  `queue_bytes=128 MiB`, `transaction_bytes=128 MiB`, then explicit
  `Flush`, clean shutdown, disk-size measurement, and a reopen pass that
  reports recovery time.

## Running

From the repository root:

```sh
# Full 30 s workload (defaults: 20 000 rows, 50 reqs / 5 ms):
go test ./tests-benchmark/ -bench MicroBatchWorkload -benchtime 1x -v

# Quick check (2 s, 2 000 rows):
go test ./tests-benchmark/ -bench MicroBatchWorkload -benchtime 1x -short -v

# Customize parameters:
go test ./tests-benchmark/ -bench MicroBatchWorkload -benchtime 1x -v \
  -args -bench-duration 30s -bench-seed 20000 \
  -bench-batch-size 50 -bench-batch-timeout 5ms

# Focused operation benchmarks (N-driven, standard benchtime applies):
go test ./tests-benchmark/ -bench 'ReadPK|ReadEmail|WriteUpdate|WriteInsert|Batch' -v

# Smoke test only (no benchmarks):
go test ./tests-benchmark/ -v
```

Notes:

* `-benchtime 1x` is recommended for `MicroBatchWorkload`: its run length
  is governed by `-bench-duration`, not `b.N`, so extra benchmark
  iterations would just repeat the full timed run.
* The workload reports `ops/s`, `reads/s`, `writes/s`, read/write
  `p50/p95/p99` latency, plus flush, disk-size, and recovery lines in
  verbose output.
* CPU/memory profiling uses the standard Go toolchain, e.g.
  `go test ./tests-benchmark/ -bench . -cpuprofile cpu.prof` then
  `go tool pprof cpu.prof`.

## Benchmarks

| Benchmark | What it measures |
|---|---|
| `BenchmarkMicroBatchWorkload` | Full timed 80/20 workload with micro-batching |
| `BenchmarkReadPK` | Single primary-key lookup per op |
| `BenchmarkReadEmail` | Single secondary-index lookup per op |
| `BenchmarkWriteUpdate` | Single profile update transaction per op |
| `BenchmarkWriteInsert` | Single insert transaction per op |
| `BenchmarkBatchWrite50` | One 50-request write micro-batch per op |
| `BenchmarkBatchRead50` | One 50-request read micro-batch per op |

## Tools from the Rust workspace

`tests-benchmark/tools` in the Rust workspace ships helpers that were
deliberately **not** ported; this section documents the decision:

* `component_cpu.py` classifies `pprof --collapsed` stacks using
  Rust-symbol rules (`spool::`, `rusqlite::`, `quinn::`, ...). Those rules
  do not match Go binaries, so a line-for-line port would misclassify
  everything as `Other/unresolved`. Use `go tool pprof -top` /
  `-list` on a `-cpuprofile` capture instead; the interesting Go frames
  live under `murmur-go/murmur`, `murmur-go/spool`, and
  `murmur-go/internal/sqlite`.
* `profile_mesh.py` samples Linux `/proc` counters around the multi-node
  mesh replication benchmark (`meshbench` processes, `baseline` vs
  `replication` phases). It is specific to that mesh driver, which has no
  Go counterpart in this package; for the single-node workload here, Go's
  `-cpuprofile`/`-memprofile` plus `/proc`-based `pidstat`/`iostat`
  captures cover the same ground.
* `profile_threads.c` is a tiny C helper for the mesh profiler and is
  likewise mesh-specific.

If a Go mesh benchmark is added later, porting `profile_mesh.py`'s
sampling loop to Go (or invoking the Python script unchanged against the
Go mesh binary) is the natural next step.
