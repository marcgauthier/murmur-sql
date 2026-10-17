# resource-exhaustion (Rust port, 3 of 5 legs)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/resource-exhaustion/`
(`oom_test.go`, `fd_test.go`, `compaction_test.go`, `slow_peer_test.go`,
`proxy_test.go`, `huge_txn_test.go`, `helpers_test.go`).

## What it proves

Each ported leg squeezes real nodes and demands the same contract: fail
closed under pressure, stay exactly converged after.

- `stalled_compaction_stays_correct`: background reclamation stalled
  (`retention_interval` 3600 s, Go: `ReclaimIntervalSeconds`), a
  66 MiB-segment victim piles sealed `.spool` debt under 64 KiB update
  churn while the 256 MiB control does not (stall proof: victim >= 3
  segments and ahead); exact convergence holds under the stall; after
  dropping the override + restart + retention collection, dead segments
  drain and the digest is stable.
- `slow_peer_converges_without_wedge`: replication crosses userspace UDP
  throttle proxies both directions (default 256 KiB/s, no privileges);
  the slow node must make steady progress (90 s without progress = wedge
  = FAIL) until converged within the bound, with byte-count proof the
  data crossed the proxies; direct re-peer heals.
- `huge_transactions_at_default_budgets`: an 8 MiB value replicates
  byte-exact; an oversize transaction is rejected naming the budget with
  no partial row and a healthy writer; 20 x 512 KiB converge exactly.

## Not ported (blocked, not skipped lightly)

- Near-OOM leg: Go pins `GOMEMLIMIT` + a small block cache on the victim
  *process* and caps RSS via a cgroup on its PID. In-process nodes share
  the driver's heap and cgroup, so a victims-only squeeze is impossible;
  port when the harness fixture gains a replicating mode runnable under
  its own limits.
- FD-exhaustion leg: Go squeezes the victim PID's `RLIMIT_NOFILE` via
  `prlimit` and holds idle TLS connections against its HTTPS API. Both
  halves are missing in Rust (a whole-process squeeze would break the
  driver too, and no HTTPS API exists); same fixture requirement as OOM.

## Pass criteria (same as Go, per ported leg)

- Compaction: `MURMUR_RX_COMPACT_ROWS` updates (default 2000) over 200
  keys; stall proven within 2 min; convergence within 3 min; debt drains
  within 4 min of retention collection; digest stable.
- Slow peer: knobs `MURMUR_RX_SLOW_RATE` (262144),
  `MURMUR_RX_SLOW_ROWS` (600), `MURMUR_RX_SLOW_VALBYTES` (1024),
  `MURMUR_RX_SLOW_BOUND_SECONDS` (300); baseline crosses the throttle;
  throttled bytes >= half the churn payload (bypass guard); heal + 10
  rows converge within 1 min.
- Huge transactions: 8 MiB byte-exact both nodes; rejection names
  `budget exceeded` with count still 1 on both nodes; probe write works;
  22 rows converge within 3 min.

## Deviations from the Go original

- Multi-process `murmur-server` children over the PostgreSQL wire
  protocol instead of `testnode` daemon processes over HTTPS+mTLS (per
  design); plain SQL replaces the typed-contention HTTP API
  (`contention(id, name)`). Tables are explicitly flagged replicated on
  each node. The rejection surfaces as a PG-wire error (SQLSTATE 54000)
  whose message still names `budget exceeded`.
- Ordered-vec equality replaces SHA-256 digests (identical strength).
- Go's victim-only `ReclaimIntervalSeconds: 3600` stall maps to a global
  `retention_interval` of 3600 s: every retention checkpoint, prune, and
  physical reclaim is collect-gated (the capture-path snapshot
  thresholds, 128 deltas / 256 MiB, never fire here), so the hourly
  cadence piles debt on both nodes while "re-enable" maps to explicit
  `MURMUR RETENTION COLLECT` (which drives the internal reclaim and
  ignores the cadence) after dropping the override + restart.
- Rejection trips the default 64 MiB *transaction* budget via one
  statement carrying a 64 MiB + 1 B value (Rust has no per-value
  limit). A multi-row BEGIN/COMMIT cannot express this: wire
  transaction words are accepted no-ops, so each statement would
  autocommit and only trip retryable 40001 pressure mid-stream.
- The UDP proxy sender polls its two lane channels (std has no `select`;
  1 ms park when empty; priority preference is otherwise exact) and uses
  kernel-default socket buffers (std has no read-buffer API).
- Deterministic xorshift64* replaces `math/rand` for incompressible
  values (no new dependencies).
- The compaction leg uses the harness `SpoolOptions`/`spool_by_node` +
  `set_spool_options` overlays (same as Go's `SpoolByNode`); the
  slow-peer leg dials through the proxies with `add_peer_addr`.
- The compaction victim uses 66 MiB segments (Go: 32 MiB): the Rust
  product rejects `segment_bytes <= 64 MiB`. Spool write overhead
  lands ~3x logical bytes, so 1000 updates of 64 KiB churn
  (~77 MiB, Go: 2000) still seal ~4 victim segments while the
  256 MiB control holds ~1 (the count is a means to pile debt, not
  a pass criterion). Writes flush every 20 instead of per-write
  (same bytes and supersede pattern; each ~1.3 MiB capture stays
  far under budget): per-write barriers would cost past the 600 s
  run cap at debug-build flush throughput. Each batch paces to
  the control's applied counter (lag 5) so the debug-build control
  never faces an unbounded backlog (background retention is stalled,
  so the backlog is pruned only by the explicit drain collects).

## Compile check (only gate; never run here)

```sh
CARGO_TARGET_DIR=/home/marc/MURMUR/tests-live/target \
  cargo test --no-run --manifest-path /home/marc/MURMUR/tests-live/resource-exhaustion/Cargo.toml
```
