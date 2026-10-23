# tail-repair (Go port)

Rust source: `/home/marc/MURMUR/tests-live/tail-repair/`
(`tail_repair.rs`: `partitioned_node_repairs_tail_from_peer_logs`).

## What it proves

A node partitioned mid-write-burst misses a contiguous range of its peers'
origin logs, then heals and must repair the missing tail from peer logs
WITHOUT the snapshot path: row counts and ordered digests converge
everywhere while the healed node's log-path counters prove the range
arrived as transactions, not as a snapshot.

## Pass criteria

- 3 meshed nodes under `tests-live/runtime/tail-repair/` with generous log
  retention (300 s, 10000 retained batches).
- Baseline of 10 rows on node 0 converges on all 3 nodes (counts + equal
  digests) within 30 s.
- After removing all 4 links to/from node3 (both directions), node3 reports
  0 connected peers within 15 s.
- Isolation probe: one row on node 0 reaches node 1 within 15 s but, after
  a 2 s settle margin, node3 still holds exactly the 10 baseline rows with
  an unchanged digest (leak = fail).
- A burst of `MURMUR_TAIL_REPAIR_BURST_ROWS` rows (default 100),
  alternating nodes 0/1, converges on both survivors within 30 s while
  node3 stays frozen at the baseline (anti-vacuity: it must actually miss
  the contiguous `1 + burst` range).
- After re-adding all 4 links, every node reports 2 connected peers within
  30 s and all 3 converge on the full row set with equal digests within
  60 s.
- No-snapshot proof: node3's `received_transactions` and
  `applied_transactions` each advance by at least the missed count across
  the heal (values logged for diagnosis), and no snapshot source is trusted
  anywhere in the cluster.
- A post-heal insert on the healed node reaches all 3 nodes (counts +
  equal digests) within 30 s.

## Env knobs

- `MURMUR_TAIL_REPAIR_BURST_ROWS` (default 100): burst rows written on the
  survivors while node3 is isolated. Same name and default as Rust.
- Harness knobs apply: `MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
  `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
  `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`, `MURMUR_TEST_DEADLINE_SCALE`.

## Deviations from the Rust source

- In-process multi-`Database` cluster via the Go harness (QUIC mesh,
  ephemeral 127.0.0.1:0), mirroring the Rust topology one for one.
- Plain SQL (`records(id INTEGER PRIMARY KEY, name TEXT)`) instead of the
  managed typed-records API; marker rows use the same numeric ids
  (0-9 / 9000 / 10000+i / 20000), and the digest is the rendering of the
  sorted `(id, name)` pairs.
- `waitConnectedPeers` reads `Replication().Status().Peers` (live session
  count).
- The maximum-offline knob keeps its 7-day product default (strictly more
  generous than the original 600 s), preserving intent: the missed range
  stays inside every peer's origin log.
- No-snapshot proof as in Rust: structural (no trusted snapshot source is
  configured, and the engine only snapshots for explicit sources) plus
  behavioral (both log-path counters advance by at least the missed count;
  snapshot application bypasses both). The applied leg reads the
  top-level `AppliedTransactions`, which advances synchronously, rather
  than the laggy metrics mirror.
