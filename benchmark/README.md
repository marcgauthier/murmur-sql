# Murmur-SQL pgwire benchmark

Compares **1, 100, and 1,000 items per SQL statement** over PostgreSQL wire.
Each statement is one request/response: a bulk lookup, bulk UPDATE, or multi-row
INSERT. Writes run in one real engine transaction; reads run in one engine
read call. There are no per-item awaits and no BEGIN/COMMIT wrappers.

## Running

```sh
# All three sizes, 30 seconds each, 8 clients, its own release server:
cargo run --release --manifest-path benchmark/Cargo.toml

# Explicit comparison and configuration:
cargo run --release --manifest-path benchmark/Cargo.toml -- \
  --batches 1,100,1000 --duration 30 --seed 20000 --clients 8 --capture 1s

# One size (--batch is an alias of --batches):
cargo run --release --manifest-path benchmark/Cargo.toml -- --batch 1000

# Reuse a prebuilt server:
MURMUR_SERVER_BIN=/path/to/murmur-server cargo run --release \
  --manifest-path benchmark/Cargo.toml

# External scratch server accepting the benchmark credentials/key:
cargo run --release --manifest-path benchmark/Cargo.toml -- \
  --host 127.0.0.1 --port 5432 --duration 60
```

**Use a disposable database.** Every size drops and recreates `users`, including
when driving an external server. Batch sizes must be 1–1,000. `--duration` is
seconds per size. `--capture` configures only a spawned server.

## Workload and measurement

- Seed 20,000 rows by default in a 10-column `users` table with indexes on email
  and `(status, created_at)`. Flush before each measurement.
- Choose the request kind randomly: 40% ID lookups, 40% email lookups, 10%
  updates, 10% inserts. Every request has exactly the selected number of items,
  so the expected item mix is also 80% reads / 20% writes. Short runs can vary.
- Lookups join a VALUES table to `users`, preserving repeated requested keys.
  All ten result columns cross the wire and are consumed by the client.
- Updates use UPDATE FROM a VALUES table with distinct seeded IDs, changing
  `updated_at`, `login_count`, `bio`, and `status`.
- Inserts use one multi-row INSERT with fresh IDs reserved across workers.
- Use typed prepared pgwire statements. Connections and preparation finish
  before timing. The server still prepares SQLite statements per execution.
- Each client waits for its whole batch before sending another. Client request
  construction, encoding, decoding, and result consumption are included.
- Throughput counts **successful items/sec**, separately from requests/sec.
  Read/write totals and failed request counts are printed. Failed items do not
  inflate successful throughput; any request errors make the run exit nonzero.
- Latency p50/p99/max is **whole-batch latency**, not individual item latency.
  Timing includes requests completing after the duration deadline.
- Every successful lookup/write checks its returned/affected row count. After
  timing, the final table count must match seeded plus successfully inserted
  rows. A final `MURMUR FLUSH` is outside timing.

Tables remain local-only. This measures RAM commit throughput with background
capture/persistence, not synchronous durable writes or replication. Each size
reseeds the same running server; use separate `--batch` invocations for fully
isolated server/storage lifetimes. Prefer release builds and repeat runs on an
otherwise idle machine before comparing results.

## Interpreting the comparison

The older benchmark's `--batch` wrapped individually awaited operations in
BEGIN/COMMIT. Those commands are server no-ops, so that option batched neither
network round trips nor engine transactions. Its roughly 12–13k items/sec
results cannot establish the throughput of actual bulk transactions.

The embedded benchmark in `../MURMUR/tests-benchmark` groups separate operations
inside engine calls (50 items by default). This benchmark uses set-based SQL
to achieve one engine call and one wire request per batch within the server's
existing API. It helps measure the benefit of amortizing overhead, but differs
from embedded execution in SQL shape, wire encoding and client work. It does
not isolate TCP's share of total overhead.

## Example measured comparison

2026-10-20, Intel Core i5-6500 (4 cores), Linux, loopback, release builds,
8 clients, 20,000 seeded rows, 30 seconds per size, capture interval 1s:

| Items per batch | Successful items/s | Requests/s | Batch p50 (ms) | Batch p99 (ms) | Failed requests |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 7,900 | 7,900 | 0.774 | 4.300 | 0 |
| 100 | 118,819 | 1,188 | 5.246 | 27.051 | 0 |
| 1,000 | 166,269 | 166 | 35.442 | 166.044 | 0 |

All per-request row counts and final table counts passed. Another CPU-heavy
process was active during this run, so these figures are indicative rather
than an isolated performance baseline. Larger batches amortize overhead while
increasing whole-request latency. These results do not establish equivalence
to the embedded benchmark, which uses a different execution strategy.
