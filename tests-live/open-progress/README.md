# open-progress (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/open-progress/`
(`open_progress_test.go`: `TestEmbeddedOpenProgressLive` and
`TestEmbeddedOpenProgressLiveExtra`).

## What it proves

A real encrypted store holding 6000 rows x 2 tables (~30 MB) is
populated, closed and reopened under the unlock watchdog, and every
reopen must recover identical contents (count + ordinal sum per table).
Failed unlocks must report the failure without a handle, an unlock that
can never succeed must fail closed after the stall window, the store
must recover afterwards, and a fresh empty database must open under the
watchdog with zero rows.

## Pass criteria

- `populate`: one `murmur-server` process takes 6000 `requests` + 6000
  `events` rows, flushes, verifies by count + ordinal sum.
- `reload-1` / `reload-2-recovery` (stop + restart under
  `connect_with_watchdog`): both tables verify by count + ordinal sum
  after each reopen.
- `open-failure-wrong-key` (wrong 32-byte key): `MURMUR UNLOCK` fails
  naming `incorrect key`; `open-failure-short-key` (3-byte key): unlock
  fails; a fresh connection with the correct key still unlocks after.
- `watchdog`: `connect_with_watchdog` with a wrong key and a 2 s stall
  window fails closed instead of hanging.
- `empty-db`: a fresh single-node cluster opens under the watchdog; a
  new table holds 0 rows.

## Env knobs

None scenario-specific. Harness knobs apply: `MURMUR_LIVE_RUNTIME`,
`MURMUR_LIVE_FAILURES`, `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
`MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS` (restarts run under
`connect_with_watchdog`; the watchdog leg tightens the stall knob to
2 s for its own duration and restores it).

## Deviations from the Go original

- Multi-process `murmur-server` children over the PostgreSQL wire
  protocol (one node here; no replication legs) instead of re-execed
  child processes per role (`MURMUR_PROGRESS_CHILD`), per the
  tests-live design. Restarts are child stop + respawn under the unlock
  watchdog.
- No `cancel` leg: wire opens cannot be cancelled.
- No phase sequences, heartbeats, per-table rebuild tracking,
  per-phase timing, or status-snapshot isolation: there is no
  intra-open progress API over the wire. The watchdog observes redial
  progress (any error change resets the stall timer); one `MURMUR
  UNLOCK` runs to completion, so a wedged open surfaces as an overall
  timeout and an unopenable unlock as a stall timeout. The port asserts
  the wire-level intent instead: exact contents after every reopen,
  clean failure reporting, fail-closed watchdog, and empty-DB open.
- Plain SQL schema (`requests`/`events` with `id/message/ordinal`)
  instead of Go typed models with field IDs; count + `SUM(ordinal)`
  replaces `Find` + ordinal summation.

## Compile check (only gate; never run here)

```sh
CARGO_TARGET_DIR=/home/marc/MURMUR-SQL/tests-live/target \
  cargo test --no-run --manifest-path /home/marc/MURMUR-SQL/tests-live/open-progress/Cargo.toml
```
