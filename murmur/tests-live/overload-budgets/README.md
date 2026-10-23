# overload-budgets (Go port)

Rust source: `/home/marc/MURMUR/tests-live/overload-budgets/`
(`tests/overload_budgets.rs`).

## What it proves

Two nodes run with small commit budgets (64 KiB transactions). An oversize
write is rejected cleanly with no partial row on either node and no writer
poisoning; then 8 concurrent writers x 50 inserts fully converge with p99
insert latency under 10 s while both nodes stay responsive.

## Pass criteria (same as Rust)

- A single insert carrying a value past the 64 KiB commit budget fails with
  a `KindLimit` error naming `budget exceeded`, and both nodes still hold
  0 rows.
- A small post-rejection write succeeds and converges on both nodes.
- 8 x 50 concurrent inserts on node 0 all succeed, p99 insert latency is
  under 10 s, and both nodes converge on identical digests of all 401 rows.
- Both nodes finish responsive (full peer set + a successful read probe).

## Deviations

- Same as the Rust port: no HTTP body-cap leg (no HTTP server exists), the
  per-value leg is adapted to the `transaction_bytes` commit budget, plain
  SQL replaces the typed-contention API, and readiness is a peer-count plus
  read-probe check.
