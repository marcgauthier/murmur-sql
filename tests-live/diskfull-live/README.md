# diskfull-live (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/diskfull-live/`
(`diskfull_live_test.go`, `TestDiskFullFailsClosedThenRecovers`).

## What it proves

One node is stopped, its directory is moved onto a tiny tmpfs, and it is
restarted there; writes past ENOSPC must fail closed (error, no partial
row, node keeps answering reads) with pre-full data intact, and freeing
space plus a restart must recover to exact convergence.

## Pass criteria (same as Go)

- 20-row baseline converges within 30 s; survive the tmpfs move and
  converge again within 30 s.
- The fs proves tiny (< 5 MiB free) before filling (else vacuous).
- Fixed filler lands first; the fill loop records every acked/errored
  outcome and stops after 5 errors; at least one error is required
  (ENOSPC must actually hit).
- While still full: reads answer; a probe write either acks (tracked) or
  errors cleanly.
- After freeing space + restart: every acked row intact everywhere,
  every errored row absent-on-all or whole-on-all (never torn).
- Post-recovery writes on both nodes converge within 30 s.
- Knobs: `MURMUR_DISKFULL_TMPFS_SIZE_MB` (8),
  `MURMUR_DISKFULL_FILLER_SIZE_MB` (1), `MURMUR_DISKFULL_VALUE_SIZE_KB`
  (128). SKIPs (restoring the dir) when the mount fails; SKIPs on
  non-Unix. Teardown never leaks the mount (unmount failure is loud).

## Deviations from the Go original

- Multi-process `murmur-server` children over the PostgreSQL wire
  protocol instead of `testnode` daemon processes over HTTPS+mTLS
  (per design); plain SQL replaces the typed-records HTTP API
  (`live_typed_records(name, value)`).
- The table is explicitly flagged `MURMUR SET TABLE ... REPLICATED true`
  on each node (row convergence needs the per-node flag); the Go
  fixture tables replicate by default.
- Go's daemon-log panic scan maps to wire behavior: a server panic
  fails the test via the wire error instead, and the server logs are
  kept as failure artifacts.
- On restarts (tmpfs move and post-recovery), table replication is
  explicitly re-enabled via `MURMUR SET TABLE live_typed_records REPLICATED true`
  and mesh anti-entropy is re-synced.
- Free space via libc `statvfs` (new scenario-only dep `libc`, same
  version as the harness); mount/copy/unmount via `mount`/`cp`/`umount`.
- On failure the tmpfs content is copied aside before the unmount so the
  `finish` snapshot still captures it.

## Verification

```sh
MURMUR_SERVER_BIN=/home/marc/MURMUR-SQL/target/debug/murmur-server \
  MURMUR_ENGINE_INTEGRATION=1 ./tests-live/run.sh diskfull-live
```
