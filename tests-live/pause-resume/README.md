# pause-resume (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/pause-resume/`
(`pause_resume_test.go`, `TestPauseResumeLosesNoWrites`).

## What it proves

Three nodes mesh while all take writes; node 2 is frozen for 15-30 s
while the survivors keep writing, then resumes with zero data loss and
converging digests. The freeze must be *detected* (alive-view dip or
probe failures), the frozen member must not be evicted, and the catch-up
path must be proven (gap-pull needs).

## Pass criteria (same as Go)

- Full mesh (2 sessions + 2 members excl. self) within 60 s; 5-row
  baseline converges within 30 s.
- Freeze window honors `MURMUR_PAUSE_RESUME_STOP_SECONDS` (default 20,
  15 s floor) with writers flowing (25 ms tick) on the survivors.
- Detection proof: survivors' alive view dips below 2, the frozen peer
  goes stale (> 5 s unseen), or its session drops during the freeze
  (else the freeze was undetected and the test fails); at least one
  write acks during the freeze.
- After resume: every acknowledged write present everywhere within
  180 s; sessions + membership re-admitted within 90 s; no eviction
  (2 connected peers + full member view on every node).
- Catch-up proven via the log path: the frozen node's cumulative
  `applied_transactions` counter advances (snapshot application
  bypasses it and local commits never move it).
- A `post-resume` write converges within 60 s.
- Env-gated: SKIPs on non-unix platforms (no SIGSTOP/SIGCONT).

## Deviations from the Go original

- Multi-process `murmur-server` children over the PostgreSQL wire
  protocol instead of `testnode` daemon processes over HTTPS+mTLS (per
  design). The freeze is a true process freeze: SIGSTOP/SIGCONT on the
  child (unix only), so no iptables privilege is needed.
- Plain SQL replaces the typed-records HTTP API
  (`live_typed_records(name)`); the table is explicitly flagged
  replicated on each node.
- Peer/member assertions use `MURMUR SHOW REPLICATION` (`peers`,
  `members` excl. self) and `MURMUR SHOW PEERS` (per-peer `connected` /
  `last_seen_millis_ago`) instead of `/metrics` gauges. The wire has no
  probe-failure counter, so staleness and session state take the place
  of Go's `probes_failed` detection leg.
- Writers are borrowed async futures inside one `tokio::join!` with the
  freeze driver (no thread scope); the frozen node's writer pauses while
  it is suspended.
- Resume needs only SIGCONT plus an explicit re-mesh (no auto-redial).
- With default retention the log still holds everything, so catch-up
  must arrive over the log path (an `applied_transactions` delta on
  the frozen node); the gap-pull `Need` counter has no wire
  equivalent, and a snapshot here would mean ranges vanished early.
  (`staged_transactions` is a live gauge that drains to zero on
  convergence, so it cannot prove anything after the fact.)

## Compile check (only gate; never run here)

```sh
CARGO_TARGET_DIR=/home/marc/MURMUR-SQL/tests-live/target \
  cargo test --no-run --manifest-path /home/marc/MURMUR-SQL/tests-live/pause-resume/Cargo.toml
```
