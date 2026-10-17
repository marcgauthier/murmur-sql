# compression (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/compression/`
(`README.md`, `compression_test.go`, `TestSpoolCompressionSizes`).

## What it proves

Each (level 1 / level 12) x (`oltp-small` / `text-heavy` / `json-docs` /
`blobs-random`) cell runs a fresh single-node `murmur-server` on real
disk, writes a seeded deterministic dataset through plain SQL in batches
of 500 rows, updates every 10th row where applicable, shuts down cleanly,
and measures the on-disk data directory.

## Env knobs

- `MURMUR_LIVE_COMPRESSION_SCALE` (default 1): multiplies every profile's
  row count.
- `MURMUR_LIVE_RUNTIME` / `MURMUR_LIVE_FAILURES`: runtime and
  failure-artifact roots (harness convention).

## Pass criteria (Go parity, adapted to levels)

- Every cell holds the expected row count with an identical seeded-ID
  checksum across levels.
- `text-heavy` at level 12 is strictly smaller than at level 1.
- `blobs-random` at level 12 stays within 10% of level 1.
- Every cell contains store bytes (non-empty data directory).

## Deviations from Go

- Cells compare Spool `compression_level` 1 vs 12 (via per-cell
  `[engine]` config) instead of `none` vs `deflate`: the Rust `Database`
  hardcodes its Spool block codec to zstd with only a level knob, so
  `none` vs `deflate` cannot be selected. Same 64 KiB target blocks,
  same AES-256-GCM encryption, same traffic shapes and row counts.
- Cells are single-node server processes (plain SQL over the PG wire)
  instead of in-process `db.Open` handles; each cell owns its
  `runtime/compression-<profile>-l<level>/` dir with its own `RESULT`
  line, and the data dir is measured after a clean stop.
- ID checksum is FNV-1a/64 instead of SHA-256, and the PRNG is an inline
  xorshift64* instead of `math/rand` (only cross-level equality within
  one process is asserted, not cross-language dataset bytes).
- The Go suite's absolute size table is a workload measurement, not a
  gate; only the relational gates above are asserted.

## Compile check (only gate; never run here)

```sh
CARGO_TARGET_DIR=/home/marc/MURMUR-SQL/tests-live/target \
  cargo test --no-run --manifest-path /home/marc/MURMUR-SQL/tests-live/compression/Cargo.toml
```
