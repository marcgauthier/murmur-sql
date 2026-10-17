# Local throughput benchmark

Measures Murmur through the pure-Rust client (`murmur-client`) over the
PostgreSQL wire protocol: the benchmark spawns a scratch `murmur-server`
(non-replicated, default options, background persistence on) and drives it
from a single client connection. Use it to see how fast the database is
when mostly in memory, including real wire and server overhead.

```sh
cargo run --release --manifest-path tests-benchmark-local/Cargo.toml -- [options]
```

Always use `--release` for the benchmark. The benchmark builds a **release**
`murmur-server` + `murmur-admin` itself on first run (one-time cost of a
few minutes) unless `MURMUR_SERVER_BIN` points at a prebuilt server; it
never benchmarks a debug server. Options:

| Flag | Default | Purpose |
|---|---|---|
| `--seconds N` | `60` | Measurement time per scenario |
| `--preload N` | `100000` | Rows loaded before measuring |
| `--dir PATH` | temp dir | Run directory (must not exist; kept for inspection) |
| `--json PATH` | — | Also write machine-readable results |
| `--quick` | — | Smoke run: 5 s per scenario, 10 k rows |

With defaults the full run takes about 11 minutes (14 scenarios × 60 s)
plus preload.

## Workload

Schema: `items(id INTEGER PRIMARY KEY, sku TEXT, qty INTEGER, payload TEXT)`
with an index on `sku`. Keys are drawn uniformly from a deterministic RNG,
so runs are reproducible. Reads and single-row writes use prepared
statements (one round trip each, directly comparable); larger write batches
use one multi-row `INSERT` or one multi-statement batch per round trip.

| Scenario | What it measures |
|---|---|
| `insert-txn-1/10/100/1000` | Bulk insert rows/s at four batch sizes |
| `select-by-id` | Prepared point lookup by primary key |
| `select-indexed` | Secondary-index lookup (`sku = ? LIMIT 10`) |
| `select-scan` | Unindexed aggregate (`count(*) WHERE qty = ?`, full scan) |
| `select-large-1k/10k/100k` | PK-range scans returning 1k/10k/100k full rows (+ payload MiB/s) |
| `update-txn-1/10/100` | Point updates by primary key at three batch sizes |
| `mixed-80-20` | 35% by-ID + 25% indexed + 20% unindexed reads, 10% inserts + 10% updates |

No explicit flush happens during measurement; one final `flush()` runs at
the end before reporting the row count and on-disk size. Latency columns
(`avg`/`p50`/`p99`) sample 1 in 16 operations; throughput counts every
operation. Units are rows for multi-row results, queries/ops otherwise;
`avg`/`p50`/`p99` describe per-round-trip latency.

## Interpreting results

- `insert-txn-1` vs `insert-txn-1000` shows per-round-trip overhead versus
  steady-state row throughput over the wire.
- `select-by-id` is the ceiling for single-connection read paths;
  `select-indexed` adds B-tree + row-fetch cost; `select-scan` scales with
  table size and shows why hot predicates need indexes.
- `mixed-80-20` approximates a read-heavy service. Its read/write split is
  printed and should stay near 80/20.
- Background persistence (1 s capture interval by default) consumes CPU
  and disk during the run; that load is part of the measurement, matching
  production. Compare runs on the same machine, filesystem, and preload.
- Everything runs over one client connection; add connection parallelism
  to measure multi-client scaling.

## Layout

- `Cargo.toml` — standalone crate (`murmur-client` by path, `tokio`, `tempfile`).
- `src/main.rs` — server spawn, client driver, scenarios, reporting.
