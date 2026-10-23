# loadshare (Go port)

Rust source: `/home/marc/MURMUR/tests-live/loadshare/`
(`loadshare.rs`, `TestWriterSharesUnderLoad`).

## What it proves

Two nodes form an encrypted mesh. Node1 absorbs 240 local writes
across four concurrent writers while node2 replicates 20 rows. The
test requires full convergence (no remote starvation on the saturated
node), acquisitions in both scheduler classes, and service debt within
the 1s bound, and logs the local-commit latency distribution
(p50/p95/max).

## Env knobs

None. Deadlines and bounds are the Rust constants: 60 s convergence
deadline (50 ms poll), 10 ms pacing between peer writes, 1 s
service-debt bound, 30 s absurd-latency tripwire.

## Pass criteria

1. Both nodes converge on all 260 rows (node1 applies the 20 remote
   rows despite local saturation; node2 absorbs the 240 local rows).
2. On node1, local-class scheduler acquisitions >= 240 and
   remote-class acquisitions > 0 (the remote class is never starved).
3. Node1 service debt <= 1 s.
4. Max local-commit latency <= 30 s; p50/p95/max logged.

## Deviations (inherited from Rust)

- Plain SQL (`records` table via `Transaction`/`Read` + `Flush`)
  replaces the managed typed-RIME HTTP API; same row counts and key
  shapes (`local-<w>-<i>`, `remote-<i>`).
- Scheduler assertions read the in-process
  `Scheduler().Snapshot()` (`Local`/`Remote` acquisitions,
  `DebtNanos`) instead of scraping scheduler counters from
  Prometheus `/metrics`.
- The convergence wait covers both nodes holding 260 rows
  (full convergence, explicit on both sides). `SELECT COUNT(*)`
  replaces the names length.
- In-process `Cluster` nodes instead of `testnode` daemons;
  forensics via `DumpForensics` on failure.
- Go-only: the product reentrancy guard (`Database.Call`) is
  database-global rather than goroutine-local, so concurrent writers
  are routinely rejected as "cannot reenter". The writers stay
  genuinely concurrent and retry that rejection (any other error
  still fails); serializing them behind a client mutex would vacate
  the saturation leg.
