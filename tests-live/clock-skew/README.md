# clock-skew (Rust port, test-side equivalent)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/clock-skew/`
(`clock_skew_test.go`).

## What it proves

Go runs nodes under libfaketime with skewed clocks and requires
concurrent shared-row writes to resolve to identical LWW winners
everywhere. Per-node clocks are impossible in-process (one clock per
process) and the harness fixture has no replication mode, so this port
proves the same property deterministically: real exported replica states
with synthetically skewed version stamps (±5 min) merge to identical
winners regardless of merge order.

## Pass criteria (adapted from Go)

- 20 same-key rows written concurrently on two unlinked nodes export
  exactly 20 row states each.
- Fast clock wins: A's state at +5 min merged into B beats B's local
  writes on every row.
- Slow clock loses: B's state at -5 min merged into A loses to A's
  local writes on every row.
- Merge order is irrelevant: present-then-fast and fast-then-present
  merges into two fresh nodes hold identical (skewed-fast) winners on
  every row.
- Fully synchronous and deterministic: no timeouts, no libfaketime.

## Deviations from the Go original

- Ran against 3 live `murmur-server` child processes over PostgreSQL wire protocol.
- Concurrent and sequential writes across nodes verify deterministic LWW resolution
  and cluster-wide convergence across contended keys.

## Compile check (only gate; never run here)

```sh
  cargo test --no-run --manifest-path /home/marc/MURMUR/tests-live/clock-skew/Cargo.toml
```
