# soak-slo (Go port)

Rust source: `/home/marc/MURMUR/tests-live/soak-slo/`
(`tests/soak_slo.rs`).

## What it proves

Three encrypted nodes sustain independent managed-record writes for
`MURMUR_SLO_DURATION_SECONDS` (default 5 s smoke; two hours for SLO
acceptance). Each node owns a bounded 1,000-row range: it inserts until the
range fills, then updates those rows so the working set stays bounded.
Afterwards all three nodes must converge to an identical table digest within
`MURMUR_SLO_SETTLE_SECONDS`, with write p95 under 5 s, maximum write latency
under 20 s, replication queues under 5,000 staged transactions, and spool
disk usage under 512 MiB per node.

## Env knobs (same names as Rust)

- `MURMUR_SLO_DURATION_SECONDS` (default 5): soak length.
- `MURMUR_SLO_SETTLE_SECONDS` (default 30): convergence deadline.
- Standard harness knobs: `MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
  `MURMUR_LIVE_UNLOCK_STALL_SECONDS`, `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.

Two-hour SLO acceptance form (run via `tests-live/run.sh`, never directly):

```sh
MURMUR_SLO_DURATION_SECONDS=7200 \
MURMUR_SLO_SETTLE_SECONDS=300 \
  bash tests-live/run.sh soak-slo
```

## Pass criteria (same as Rust)

- Every node holds 2 live peer sessions before the soak starts.
- No writer goroutine reports a transaction/flush failure.
- At least 60 application writes committed mesh-wide.
- All three nodes converge to exactly the owned row count with identical
  sorted row contents.
- Every node has replication running, is fully materialized, reports at
  most 5,000 staged transactions, and reports `spool disk_bytes <= 512 MiB`.
- Write p95 <= 5 s and maximum write latency <= 20 s (same nearest-rank
  p95 position as the original).

## Deviations

- Same as the Rust port: plain SQL instead of the typed-contention API,
  in-process writers, quiesced status instead of materialized-generation
  equality, `staged_transactions` for queue depth, FNV-1a log digest, and
  directed-pair session probes over `peers_diagnostics`.
