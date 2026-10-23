# graceful-shutdown (Go port)

Rust source: `/home/marc/MURMUR/tests-live/graceful-shutdown/`
(`graceful_shutdown.rs`: `graceful_shutdown_mid_write_and_mid_snapshot`).

## What it proves

Phase A stops node 1 mid-write under load; phase B restarts node 3 stale
(log ranges GC'd, forcing snapshot-resync) and stops it again mid-transfer.
Both restarts must be clean and fast, lose no acknowledged rows, and the
mesh must reconverge with identical contents; the snapshot path is proven
via the receiver's counters, not merely by convergence.

## Pass criteria (same as Rust)

- 5-row baseline converges within 30 s; 4 s load pre-roll.
- Phase A stop lands with a write provably in flight (5 s gate); restart
  under 60 s; load converges within 90 s with every acked row present.
- 100 survivor rows + 8 fat (~256 KB) rows converge on both survivors
  within 30 s each.
- Member-deadline sleep (`MURMUR_GRACEFUL_MEMBER_DEADLINE_SECONDS`,
  default 25) plus a GC pass that observably collects log batches (a
  post-expiry pass that collects nothing fails loudly: the snapshot path
  would not be forced).
- The rejoin transfer is caught in flight (row count strictly between the
  stale count and the target); a completed-before-interrupt transfer fails
  loudly ("widen the payload").
- Post-interrupt restart under 60 s; convergence within 120 s; the bounded
  post-restart log pair proves the snapshot path (log catch-up would hide
  a snapshot-path regression; the retained tail legitimately arrives via
  logs, so the proof bounds it instead of requiring zero); all acked rows
  present.
- A `post-recovery` write converges within 60 s.

## Env knobs

- `MURMUR_GRACEFUL_MEMBER_DEADLINE_SECONDS` (default 25): member-deadline
  sleep before the post-expiry GC passes. Same name and default as Rust.
- Harness knobs apply: `MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
  `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
  `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`, `MURMUR_TEST_DEADLINE_SCALE`.

## Deviations from the Rust source

- In-process multi-`Database` cluster via the Go harness instead of daemon
  processes; the "SIGTERM" is `Close()` + reopen of the same directory
  with the same key, followed by an explicit re-mesh (fresh ephemeral
  port).
- Plain SQL: table `contention(id TEXT NOT NULL PRIMARY KEY, name TEXT
  NOT NULL, phone TEXT NOT NULL DEFAULT '')`; ordered-name slices replace
  the joined-name digest.
- Clean shutdown is proven by completed handle release (`NodeInfo(i).DB`
  nil after stop; `StopNode` logs close errors) instead of daemon log
  markers, which have no in-process equivalent; the no-loss assertions
  carry the real proof.
- GC passes run on demand (`CollectRetention` in the poll loop) instead of
  waiting for the background tick; run/collection counters are the same
  proof. Transient collect refusals keep polling rather than failing: the
  Go collect path crosses a non-blocking engine command channel, while
  the Rust call blocks. Retention-status samples retry for up to 30s for
  the same reason.
- Transfer-in-flight detection uses row counts (strictly between stale
  and target); the snapshot-path proof bounds the log-counter pair by the
  retained tail instead of requiring zero (the retained tail legitimately
  arrives via logs).
- GC passes run on every survivor (one survivor's pass is not enough: the
  rejoining node would pull the other's intact log). Fixed node ids with
  the stale node trusting both survivors as snapshot donors (the harness
  trusts nobody by default).
- Writers resolve the current-generation handle per write from a shared
  mutex-protected slot refreshed after every restart; a deferred
  stop/join releases them on failure.
