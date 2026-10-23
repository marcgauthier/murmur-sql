# long-running-five-node (Go port)

Rust source: `/home/marc/MURMUR/tests-live/long-running-five-node/`
(`tests/long_running_five_node.rs`).

## What it proves

Five encrypted nodes run a continuous per-node write soak across a full mesh
for `MURMUR_FIVE_NODE_DURATION_SECONDS` (default 30 s smoke; one hour for
capacity acceptance). Node2 and node4 restart in place mid-run (at 1/3 and
2/3 of the soak) while writes continue; their persisted rows must survive
each reopen. Afterwards all five nodes must converge to the same row set,
every node must hold fully materialized state within a 2 GiB spool budget,
and one final application write must converge everywhere.

## Env knobs (same names as Rust)

- `MURMUR_FIVE_NODE_DURATION_SECONDS` (default 30): soak length.
- `MURMUR_FIVE_NODE_INTERVAL_MS` (default 50): per-node write cadence.
- `MURMUR_FIVE_NODE_SETTLE_SECONDS` (default 120): convergence deadline.
- Standard harness knobs: `MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
  `MURMUR_LIVE_UNLOCK_STALL_SECONDS`, `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.

One-hour capacity acceptance form (run via `tests-live/run.sh`, never
directly):

```sh
MURMUR_FIVE_NODE_INTERVAL_MS=200 \
MURMUR_FIVE_NODE_DURATION_SECONDS=3600 \
MURMUR_FIVE_NODE_SETTLE_SECONDS=300 \
  bash tests-live/run.sh long-running-five-node
```

## Pass criteria (same as Rust)

- Every node holds 4 live peer sessions before, between, and after restarts.
- Each node acknowledges >= 10 rows; >= 100 rows acknowledged mesh-wide.
- Each restarted node holds at least its pre-restart row count after rejoin.
- All five nodes converge to identical sorted row sets with at least the
  acknowledged row count.
- Every node is fully materialized and reports `spool disk_bytes <= 2 GiB`.
- The post-soak write converges to `final + 1` identical rows on all nodes.

## Deviations

- Same as the Rust port: plain SQL instead of the typed-records API,
  in-process close()+reopen instead of daemon restarts, re-mesh after each
  restart (fresh ephemeral ports), quiesced status instead of
  materialized-generation equality, FNV-1a log digest, and directed-pair
  session probes over `peers_diagnostics`.
- Writer goroutines share a handle table swapped on restart; an RWMutex
  additionally fences writer ops against close/reopen so no op runs on a
  half-restarted handle.
