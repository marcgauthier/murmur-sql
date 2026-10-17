# impaired-network (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/impaired-network/`
(`impaired_network_test.go`, `TestImpairedNetworkConverges`).

## What it proves

A 3-node mesh (static peers plus seed bootstrap) keeps converging while
tc/netem impairs ONLY the replication ports: (a) +150 ms latency, (b) 3%
packet loss, (c) a 1 Mbit cap during a stale-node snapshot resync under
sustained writes. Membership stays stable (no false evictions) and
per-row visibility latencies are recorded with relaxed p95 bounds.

## Pass criteria (same as Go)

- Baseline converges unimpaired within 150 s (honest-path control).
- Latency phase: 8 timed rows (`MURMUR_IMPAIRED_NETWORK_ROWS`), 30 s
  per-row visibility, p95 within 15 s.
- Loss phase: same rows, 45 s per-row visibility, p95 within 30 s.
- Every timed row samples stable membership on all nodes.
- Capped phase: 5 stale rows, node 3 stops; 60 survivor rows; retention
  pin lapses + one GC pass; restart into the 1 Mbit cap with 20
  sustained writes; convergence within 180 s with the snapshot path
  proven (not merely convergence); membership restored within 60 s.
- Env-gated: SKIPs without tc/netem + CAP_NET_ADMIN on lo (refusing to
  disturb a foreign root qdisc); `MURMUR_IMPAIRED_NETWORK_FORCE=1` runs
  the workload unimpaired with loud FORCE disclaimers; qdisc 77: is
  always removed, on success and on failure.

## Deviations from the Go original

- Multi-process `murmur-server` children over the PostgreSQL wire
  protocol instead of `testnode` daemon processes over HTTPS+mTLS (per
  design); plain SQL replaces the typed-contention HTTP API
  (`contention(id, name)`); ordered-vec equality replaces the digest.
  The table is explicitly flagged replicated on each node.
- Go subtests become sequential phases in one async test (same
  assertions, same order).
- Membership assertions use the `members` column of
  `MURMUR SHOW REPLICATION` (excluding self, so want is 2); the
  alive-count check has no Rust equivalent (evictions still fail the
  member view).
- GC passes run on demand (`MURMUR RETENTION COLLECT` in the poll loop,
  observed via the `last_collection_millis` marker of
  `MURMUR SHOW RETENTION`) instead of waiting for the background tick.
- The snapshot-path proof uses the flat post-restart log-counter pair
  (`staged_transactions`, `applied_transactions` from
  `MURMUR SHOW REPLICATION`).
- The stale-node restart rebinds a fresh replication port, so the tbf
  filters are re-steered after the restart (otherwise the rejoining
  node's new port would bypass the cap).

## Compile check (only gate; never run here)

```sh
CARGO_TARGET_DIR=/home/marc/MURMUR-SQL/tests-live/target \
  cargo test --no-run --manifest-path /home/marc/MURMUR-SQL/tests-live/impaired-network/Cargo.toml
```
