# resource-exhaustion (Go port, 3 of 5 legs)

Rust source: `/home/marc/MURMUR/tests-live/resource-exhaustion/`
(`tests/resource_exhaustion.rs`).

## What it proves

Each ported leg squeezes real nodes and demands the same contract: fail
closed under pressure, stay exactly converged after.

- `TestStalledCompactionStaysCorrect`: a 65 MiB-segment victim piles sealed
  `.spool` debt under 64 KiB update churn while the 256 MiB control does
  not (stall proof: victim >= 3 segments and ahead); exact convergence
  holds under the stall; after dropping the override + restart + pumped
  retention collection, debt holds steady (no leak, reclaim live) and the
  digest is stable.
- `TestSlowPeerConvergesWithoutWedge`: replication crosses userspace UDP
  throttle proxies both directions (default 256 KiB/s, no privileges);
  the slow node must make steady progress (90 s without progress = wedge
  = FAIL) until converged within the bound, with byte-count proof the
  data crossed the proxies; direct re-peer heals.
- `TestHugeTransactionsAtDefaultBudgets`: an 8 MiB value replicates
  byte-exact; an oversize transaction is rejected naming the budget with
  no partial row and a healthy writer; 20 x 512 KiB converge exactly.

## Not ported (blocked, not skipped lightly)

- Near-OOM leg: pins `GOMEMLIMIT` + a small block cache on the victim
  *process* and caps RSS via a cgroup on its PID. In-process nodes share
  the driver's heap and cgroup, so a victims-only squeeze is impossible.
- FD-exhaustion leg: squeezes the victim PID's `RLIMIT_NOFILE` via
  `prlimit` and holds idle TLS connections against its HTTPS API. Both
  halves are missing (a whole-process squeeze would break the driver too,
  and no HTTPS API exists).

## Pass criteria (same as Rust, per ported leg)

- Compaction: `MURMUR_RX_COMPACT_ROWS` updates (default 2000) over 200
  keys; stall proven within 2 min; convergence within 5 min; 2 min drain
  soak holds debt steady with reclaim live; digest stable.
- Slow peer: knobs `MURMUR_RX_SLOW_RATE` (262144),
  `MURMUR_RX_SLOW_ROWS` (600), `MURMUR_RX_SLOW_VALBYTES` (1024),
  `MURMUR_RX_SLOW_BOUND_SECONDS` (300); baseline crosses the throttle;
  throttled bytes >= half the churn payload (bypass guard); heal + 10
  rows converge within 1 min.
- Huge transactions: 8 MiB byte-exact both nodes; rejection names
  `budget exceeded` with count still 1 on both nodes; probe write works;
  22 rows converge within 3 min.

## Deviations

- Same as the Rust port: in-process multi-`Database` clusters, plain SQL,
  ordered-vec equality instead of SHA-256 digests, autonomous-reclaim
  drain hygiene instead of shrinkage, transaction-budget rejection
  instead of per-value, and deterministic xorshift64* for incompressible
  values.
- The UDP proxy sender uses `select` over the two lane channels (Go has
  `select`; the 1 ms park is a Rust-std limitation) and kernel-default
  socket buffers.
- The drain-phase reclaim-liveness assert is adapted: `Database.SpoolMetrics`
  does not map the `Reclaims` pass counter (`spoolMetricsFrom` drops it),
  so instead of "reclaim counter advances" the port requires a
  Go-observable reclaim effect — `Compactions` advancing or `Segments`
  decreasing across the drain (same intent: reclaim keeps running).
