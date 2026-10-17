# overload-budgets (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/overload-budgets/`
(`overload_budgets_test.go`, `README.md`).

## What it proves

Two nodes run with small commit budgets (64 KiB transactions). An oversize
write is rejected cleanly with no partial row on either node and no writer
poisoning; then 8 concurrent writers x 50 inserts fully converge with p99
insert latency under 10 s while both nodes stay responsive.

## Pass criteria

- A single insert carrying a value past the 64 KiB commit budget fails with
  `Error::Limit("... budget exceeded")`, and both nodes still hold 0 rows.
- A small post-rejection write succeeds and converges on both nodes.
- 8 x 50 concurrent inserts on node 0 all succeed, p99 insert latency is
  under 10 s, and both nodes converge on identical digests of all 401 rows.
- Both nodes finish responsive (full peer set + a successful read probe).

## Deviations from the Go original

- Multi-process `murmur-server` children over the PostgreSQL wire
  protocol instead of `testnode` daemon processes over HTTPS+mTLS (per
  design). The rejection surfaces as a PG-wire error (SQLSTATE 54000)
  whose message still names `budget exceeded`.
- The 2 MiB POST / 1 MiB HTTP body-cap leg is dropped: the Rust product has
  no HTTP server, so there is no body cap to trip.
- The 5 KiB value / 4 KiB per-value-budget leg is adapted to the
  `transaction_bytes` commit budget (one insert with a 64 KiB + 1 B value):
  the Rust product has no per-value-bytes limit.
- Plain SQL (`records(id, name, phone, score)`) replaces the typed-contention
  HTTP API; the table is explicitly flagged replicated on each node, and the
  convergence digest is a sorted `Debug` row dump rather than a SHA-256 over
  ordered lines (same criterion: identical ordered row sets).
- The 8 blast writers are borrowed async futures inside one
  `tokio::join!` (no thread scope); per-insert latencies aggregate the
  same p99 bound.
- Each blast writer flushes every 10 inserts: Rust session capture can
  combine pending transactions, so the 400 unflushed rows would sign as
  one capture past the 64 KiB replication budget (`58000`), while Go
  captures per HTTP write. The flush restores Go's per-write capture
  granularity; the post-rejection probe retries replayable `40001`
  pressure per the documented back-off-and-retry contract.
- `WaitNodeReady` maps to a `MURMUR SHOW REPLICATION` peer-count plus
  read-probe check; close/reopen is unused here (the scenario never
  restarts a node).

## Compile check (only gate; never run here)

```sh
CARGO_TARGET_DIR=/home/marc/MURMUR-SQL/tests-live/target \
  cargo test --no-run --manifest-path /home/marc/MURMUR-SQL/tests-live/overload-budgets/Cargo.toml
```

## Env knobs

None; the standard harness knobs (`MURMUR_LIVE_RUNTIME`,
`MURMUR_LIVE_FAILURES`, unlock watchdog) apply unchanged.
