# chaos-load (Go port)

Rust source: `/home/marc/MURMUR/tests-live/chaos-load/`
(`chaos_load.rs`: `continuous_writes_across_partition_and_healing`).

## What it proves

Three encrypted, mutually authenticated QUIC nodes write concurrently
while node3 is cut off from the connected node1/node2 pair. The split
groups must show divergent row counts and ordered state digests; after
re-admitting peers while writes continue, all three nodes must converge
on identical counts and digests, and a post-heal write must replicate
everywhere.

## Pass criteria

- Baseline insert on node 0 reaches all 3 nodes within 20 s.
- After removing all 4 cross-partition peer links, the mesh reports
  live-peer counts `[1, 1, 0]` within 20 s.
- During the partition window all three nodes keep writing (25 ms cadence
  each); at the measurement barrier node1/node2 hold
  `1 + writes1 + writes2` rows, node3 holds `1 + writes3` rows, the two
  groups' ordered-name digests differ, and node3 reports 0 connected peers.
- After re-adding all 4 links under continuing load, the mesh reports
  `[2, 2, 2]` within 30 s and all 3 nodes converge on identical counts
  and digests within 60 s.
- No application write ever fails (failures are collected and fail the
  test after the writers join).
- A post-heal insert on node 1 reaches all 3 nodes within 20 s with
  identical digests.

## Env knobs

- `MURMUR_CHAOS_PARTITION_SECONDS` (default 12): partitioned write window.
- `MURMUR_CHAOS_HEAL_SECONDS` (default 12): post-heal write window.
- Standard harness knobs: `MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
  `MURMUR_LIVE_UNLOCK_STALL_SECONDS`, `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.

## Deviations (inherited from Rust)

- In-process multi-`Database` cluster instead of daemon processes.
- Plain SQL (`records(id INTEGER PRIMARY KEY, name TEXT)`); writer ids
  are `node * 10_000_000 + seq`; digest is sorted names joined by newline.
- Writer goroutines share the cluster; stop/pause/active-counter
  semantics are unchanged.
- Connected-peer counts come from replication `peers_diagnostics`.
