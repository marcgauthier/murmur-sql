# clock-skew (Go port)

Rust source: `/home/marc/MURMUR/tests-live/clock-skew/`
(`tests/clock_skew.rs`), itself a test-side equivalent of the original Go
libfaketime scenario.

## What it proves

Per-node clocks are impossible in-process (one clock per process), so this
port proves the same property deterministically: real exported replica
states with synthetically skewed version stamps (±5 min) merge to identical
LWW winners regardless of merge order.

## Pass criteria (same as Rust)

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

## Deviations

- Same as the Rust port: no live skewed clocks (skew is synthesized by
  shifting real exported `Version` stamps and driving the real
  `MergeState` path on unlinked nodes with `ManualPeers`).
- `MergeState` takes a pointer, so every merge gets a fresh deep clone of
  the pristine export (Rust clones the owned value).
