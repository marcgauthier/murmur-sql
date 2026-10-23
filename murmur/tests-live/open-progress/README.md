# open-progress (Go port)

Rust source: `/home/marc/MURMUR/tests-live/open-progress/`
(`tests/open_progress.rs`).

## What it proves

A real encrypted store holding 6000 rows x 2 tables (~30 MB) is closed and
reopened while `OnOpenProgress` callbacks record every event. Each
reopen must report a sane terminal snapshot, a monotonic non-overlapping
event stream with exactly one terminal event, and the full phase sequence;
failed opens must report `Failed` without a handle; and the store must
recover afterwards with identical contents. Extra legs cover per-table
rebuild tracking, rebuild heartbeating, empty-database opens, per-phase
timing, and status-snapshot isolation.

## Pass criteria (same as Rust)

- `populate`: fresh encrypted DB takes 6000 `requests` + 6000 `events`
  rows (1000/batch), flushes, closes.
- `reload-1` / `reload-2-recovery` (under the unlock watchdog): terminal
  `OpenProgress` is `Ready` with `rows_inserted == 12000`,
  `rows_skipped == 0`, `processed_items >= rows_inserted`, cleared
  `current_table`, no error; both tables verify by count + ordinal sum;
  callbacks never overlap; stream is non-empty, monotonic
  (`processed_items`, `elapsed`), has exactly one terminal event ending in
  `Ready`, and collapses to
  `Opening, Rebuilding, Indexing, Finalizing, Ready`.
- `open-failure-short-key` (3-byte key) / `open-failure-wrong-key`
  (wrong 32-byte key): open fails with no handle; stream starts at
  `Opening` and ends in `Failed` with exactly one terminal event.
- `table-tracking`: terminal shows `rows_inserted == 12000` and a cleared
  `current_table`; both tables countable at 6000.
- `heartbeat`: at least one `Rebuilding`-phase event.
- `empty-db`: fresh create fires the full phase sequence with zero
  `processed_items`/`rows_inserted`/`rows_skipped` and ends `Ready`.
- `phase-elapsed`: every event has `phase_elapsed <= elapsed`.
- `status-snapshot`: 10 consecutive `Status().OpenProgress` snapshots are
  all `Ready`, mutually independent (mutating one never affects the
  others), and stable in `processed_items`, with
  `rows_inserted == 12000`.

## Env knobs

None scenario-specific. Harness knobs apply: `MURMUR_LIVE_RUNTIME`,
`MURMUR_LIVE_FAILURES`, `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
`MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS` (reload opens run under
`OpenWithWatchdog`).

## Deviations

- Same as the Rust port: in-process close()+reopen per leg (isolated
  subdirs under `runtime/open-progress/`), no `cancel` leg, the
  `Opening, Rebuilding, Indexing, Finalizing, Ready` phase sequence,
  spool-records-plus-rows processed-items semantics, plain SQL schema,
  and direct use of the harness artifacts/forensics/unlock helpers
  instead of `Cluster`.
