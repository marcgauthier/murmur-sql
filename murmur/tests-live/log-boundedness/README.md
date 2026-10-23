# log-boundedness (Go port)

Rust source: `/home/marc/MURMUR/tests-live/log-boundedness/`
(`log_boundedness.rs`: `TestLogBoundednessUnderSustainedLoad`).

## What it proves

Two encrypted, mutually authenticated nodes absorb sustained
alternating inserts (zero-based `%032x` keys, ~200-byte
`payload-<seq>-...` values) for `MURMUR_LOG_BOUNDEDNESS_SECONDS`
(default 75 s) while log-GC runs. Per-node retained origin-log batches
must stay within a floor-plus-slope ceiling (the in-process analog of
the `128 KiB + 16 bytes/row` log-file ceiling), no replication-engine
error may be recorded during the load window, per-node on-disk growth
must stay under 8 KiB/row with no second-half acceleration past
`2 * first_half + 1 MiB`, and the mesh must still converge on exactly
the acked rows with identical contents.

## Pass criteria

- `NewCluster` opens 2 meshed nodes under
  `tests-live/runtime/log-boundedness/` with aggressive retention
  overlays.
- Fixed-window sustained inserts alternate nodes (`seq % 2`); at
  least 100 rows are acked (else the proof is vacuous and the test
  fails).
- Start/mid/end samples per node: retained batches
  (`committed - collected_transactions`) and data-dir bytes.
- Per-node retained-batch growth `<= 512 + produced / 4` (floor
  absorbs the minimum-retained batches and in-flight unacknowledged
  batches; the slope is far below the dead-GC slope of ~1 retained
  batch per produced row).
- Per-node replication `last_error` is unchanged across the load
  window.
- Per-node data-dir growth `<= acked * 8 KiB`, and second-half growth
  `<= 2 * first-half growth + 1 MiB`.
- Both nodes converge on exactly the acked rows with identical
  ordered `(id, name, phone, score)` contents within 90 s.

## Env knobs

- Standard harness knobs: `MURMUR_LIVE_RUNTIME`,
  `MURMUR_LIVE_FAILURES`, `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
  `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.
- `MURMUR_LOG_BOUNDEDNESS_SECONDS` (default 75): sustained-load
  window length, same name and default.

## Deviations (inherited from Rust)

- In-process multi-`Database` cluster (QUIC mesh, ephemeral
  `127.0.0.1:0`) instead of `testnode` daemon processes over
  HTTPS+mTLS.
- Plain SQL (`contention(id TEXT NOT NULL PRIMARY KEY, name TEXT,
  phone TEXT, score INTEGER)`) via `Transaction`/`Read` instead of
  the managed typed-contention HTTP API; keys keep the zero-based
  `%032x` format and values keep the `payload-<seq>-` + 200-character
  body shape; phone/score keep their zero values. Per-op `Flush()`
  publishes each write to replication.
- No daemon text log exists in-process, so the log-file ceiling maps
  onto retained origin-log batches sampled from product APIs
  (`Status().Committed` and
  `RetentionStatus().CollectedTransactions`): each sustained insert
  appends one origin-log batch, so dead GC grows the retained set ~1
  batch/row while working GC holds it ~flat. The ceiling keeps the
  floor-plus-slope shape (`512 + produced / 4` batches per node).
- No log file exists to grep for panic/fatal, so the no-errors check
  watches the replication `last_error` latch (collection and engine
  errors land there) with a 100 ms background recorder: any observed
  value that is neither the baseline nor understood
  connection-lifecycle churn fails. The churn mask covers exactly
  `duplicate connection` (mutual-dial tie-break loss, recorded on
  the dialing side), `handshake timeout`, and `stale session`; a
  wedged peer would still fail the convergence leg. The recorder is
  strictly stronger than endpoint equality (a transient engine error
  that later churn overwrote is still caught). A mesh settle wait
  (one peer per node plus latch silence) opens the window so setup
  dial races land in the baseline.
- Disk growth measures the node `data/` dir (database storage) via a
  recursive walk instead of the daemon dir; caps and the acceleration
  check are unchanged.
- Aggressive retention overlays (`retention: 1s`,
  `retention_interval: 1s`, `minimum_retained: 10`) instead of daemon
  defaults: the product defaults (1h retention, 60s cadence) would
  collect nothing inside the 75 s window, so the scenario could not
  observe GC.
- Digest agreement compares full ordered row vectors directly instead
  of SHA-256 hex digests (equivalent, strictly stronger); the
  expected vector is rebuilt deterministically from the acked
  key/value functions.
- If a trivially short window never crosses the halfway sample, the
  midpoint pins to the end values; the acceleration check then
  degrades to a pass.
