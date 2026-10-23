# compression (Go port)

Rust source: `/home/marc/MURMUR/tests-live/compression/`
(`compression.rs`: `spool_compression_sizes`).

## What it proves

Each (`none` / `deflate`) x (`oltp-small` / `text-heavy` / `json-docs` /
`blobs-random`) cell opens a fresh encrypted store, writes a seeded
deterministic dataset in batches of 500 rows, updates every 10th row where
applicable, closes cleanly, and measures the on-disk store directory.
In-process by design: no replication or SQL participates.

## Env knobs

- `MURMUR_LIVE_COMPRESSION_SCALE` (default 1): multiplies every profile's
  row count.
- `MURMUR_LIVE_RUNTIME` / `MURMUR_LIVE_FAILURES`: runtime and
  failure-artifact roots.

## Pass criteria

- Every cell holds the expected row count with an identical seeded-ID
  checksum across modes.
- `text-heavy` under `deflate` is strictly smaller than under `none`.
- `blobs-random` under `deflate` stays within 10% of `none`.
- Every cell contains segment bytes (non-empty store directory).

## Deviations (inherited from Rust)

- Cells write equivalent seeded KV traffic directly to `spool.Store`
  instead of managed records on a full database: `none` vs `deflate`
  cannot be selected through `murmur.Open`. Same 64 KiB target blocks,
  same AES-256-GCM encryption, same traffic shapes and row counts.
- No cluster: single-store cells use `PrepareRuntimeDir`/`Finish`
  directly. On failure the cell stores are retained as-is.
- ID checksum is FNV-1a/64 instead of SHA-256 (only cross-mode equality
  within one process is asserted).
