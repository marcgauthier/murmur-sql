# endurance-chaos (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/endurance-chaos/`
(`endurance_chaos_test.go`: `TestEnduranceChaos`, plus the
`faults_test.go` helpers and `statfs_*_test.go` free-space probes).

## What it proves

Long-duration combined-fault validation: a live multi-node mesh of
`murmur-server` processes (PostgreSQL wire protocol) absorbs continuous
writes while rolling restarts, peer flaps, disk pressure, online key
rotation, snapshot resyncs, and origin-log GC all overlap for a
configured chaos window. The verdict is exact cross-node convergence
(equal row counts plus identical PK-ordered row contents) with
counter-proven GC progress on every node, at least one proven snapshot
resync, successful key rotations, a rejoined full mesh, and a post-chaos
write that converges everywhere.

## Env knobs

Same names and defaults as Go:

- `MURMUR_ENDURANCE_NODES` (default 5): nodes in the mesh (clamped to 3..16).
- `MURMUR_ENDURANCE_DURATION_SECONDS` (default 600): chaos window.
- `MURMUR_ENDURANCE_SETTLE_SECONDS` (default 300): max convergence wait.
- `MURMUR_ENDURANCE_WRITE_INTERVAL_MS` (default 25): pacing per writer
  task (up to 8 writers).
- `MURMUR_ENDURANCE_MAX_ROWS` (default 10000): live-id ceiling; past it
  writers recycle via updates/deletes.
- `MURMUR_ENDURANCE_SEED` (default 1): RNG seed.
- `MURMUR_ENDURANCE_STATUS_SECONDS` (default 30): heartbeat interval.
- `MURMUR_ENDURANCE_RESTART_SECONDS` (default 90): mean rolling-restart
  interval; `MURMUR_ENDURANCE_RESTART_DOWN_SECONDS` (default 8): downtime
  per restart.
- `MURMUR_ENDURANCE_ROTATE_SECONDS` (default 180): online key-rotation
  interval (round-robin).
- `MURMUR_ENDURANCE_SNAPSHOT_SECONDS` (default 300):
  `MURMUR_ENDURANCE_SNAPSHOT_OFFLINE_SECONDS` (default 75, minimum 60):
  snapshot offline windows.
- `MURMUR_ENDURANCE_IMPAIR_ON_SECONDS` (default 60) /
  `MURMUR_ENDURANCE_IMPAIR_OFF_SECONDS` (default 20): impairment phase
  length and clear gap. `MURMUR_ENDURANCE_IMPAIR_LATENCY_MS` (default 150)
  and `MURMUR_ENDURANCE_IMPAIR_LOSS_PCT` (default 3) are parsed and logged
  but have no effect (flap-only; no tc privilege model for child
  processes).
- `MURMUR_ENDURANCE_DISK_SECONDS` (default 180) /
  `MURMUR_ENDURANCE_DISK_MB` (default 128) /
  `MURMUR_ENDURANCE_DISK_HOLD_SECONDS` (default 60): disk-pressure holds.
- `MURMUR_ENDURANCE_SKEW_SECONDS` (default 120): parsed and logged but the
  skew leg is degraded away (no daemon for libfaketime).
- Standard harness knobs: `MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
  `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
  `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`, `MURMUR_SERVER_BIN`.

Quick smoke (run via `tests-live/run.sh`, never directly):

```sh
MURMUR_ENDURANCE_NODES=3 MURMUR_ENDURANCE_DURATION_SECONDS=180 \
MURMUR_ENDURANCE_SETTLE_SECONDS=120 \
  bash tests-live/run.sh endurance-chaos
```

## Pass criteria

- The baseline row converges on every node before chaos starts.
- At least one write is acked during the run (anti-vacuous guard).
- After chaos stops, every node is reachable with equal row counts and
  identical PK-ordered row contents within settle.
- GC ran and collected on every node (restart-proof high-water history)
  with no GC-attributed engine error ever observed.
- At least one restart, rotation, impairment cycle, disk hold (unless
  degraded for space), and snapshot window (unless the run is too short
  to fit one) executed; at least one snapshot resync proved via the
  snapshot path (see deviations).
- A full N-1 peer mesh is back, and a post-chaos write converges
  everywhere.

Transient write errors during chaos are counted, never fatal.

## Deviations from the Go original

- Multi-process `murmur-server` children over the PostgreSQL wire
  protocol instead of `testnode` daemon processes over HTTPS+mTLS
  (per design).
- Plain SQL (`contention(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL,
  phone TEXT NOT NULL, score INTEGER NOT NULL)`) via the harness
  `NodeClient` replaces the typed-contention HTTP API; writer row
  shapes keep the Go 32-hex-char ids (`%08x%024x` per
  writer/sequence) and `writer-<n>` names. The Go two-call update
  (name, then phone) is one SQL `UPDATE` setting both columns;
  convergence verdicts hash actual state, so the split is
  unobservable there.
- The table is explicitly flagged `MURMUR SET TABLE ... REPLICATED true`
  on each node (row convergence needs the per-node flag); the Go
  fixture tables replicate by default.
- Convergence compares exact sorted row vectors (which imply equal
  counts and digests); the logged digest is dependency-free FNV-1a
  instead of sha256 hex.
- Restarts alternate crash (`kill_node`) and graceful (`stop_node`)
  legs (Go parity): the harness kills real child processes either
  way. The old in-process port could not distinguish them.
- tc/netem impairment degrades to logical peer flaps in all runs (the Go
  fallback when `CAP_NET_ADMIN` is absent): per-edge `remove_peer` holds
  cycling the latency -> loss -> both phase names, then re-add. There is
  no tc probe because ephemeral child-process ports have no stable
  shaping target.
- Clock skew is always skipped and reported as degraded: server
  children share the host clock, so no node can run under libfaketime
  (Go skips the same way without libfaketime or with a static daemon).
- Key rotation is `MURMUR ROTATE KEY '<previous>' '<new>'` with a fresh
  `/dev/urandom` key; the Rust product has no key IDs, so the Go active
  `ApplicationKeyID` assertion maps to staying online plus later
  restarts unlocking with the new key (the harness stored key is updated
  via `Cluster::set_node_key`, mirroring Go `rewriteKeyConfig`).
- Snapshot trust is configured up front (every node trusts every node)
  instead of via the Go fixture policy; without this overlay no leg
  could request a snapshot.
- The Go `snapshots_received >= 1` leg maps to an event-driven proof per
  offline window off the log-path `applied_transactions` counter
  (`MURMUR SHOW REPLICATION`; no snapshot counter exists over the
  wire): with writers still running, the victim is polled for up to
  240 s until the post-rejoin sample shows the bulk arrived off the log
  path (row-set drift below the outage's acked write count while the
  log-path applied delta since the rejoin stays below it too — a pure
  log catch-up would have to apply every missed transaction). The
  outage outlasts retention plus the offline pin, so survivors GC the
  victim's ranges and only a snapshot can deliver the bulk. The
  victim's own restart plus a donor build plus the chunk transfer can
  take ~45 s under chaos (spool rescans dominate), and a stale donor
  winning the race forces a second cycle, so the poll allows two full
  cycles rather than sampling once at a fixed delay.
- GC counters come from the wire: `MURMUR SHOW RETENTION`
  `collected_transactions` plus `MURMUR SHOW REPLICATION` `last_error`.
  No GC-runs counter exists over the wire, so a run is proven by
  `collected` advancing across samples (durable, restart-proof
  high-water history). No GC-failures counter exists either, and the
  engine multiplexes every subsystem error (including
  peer-disconnect noise from restarts/flaps) into one `last_error`
  latch — so this port fails only on GC-substring matches
  (`retention`/`collect`/`gc_`/`prune`), the faithful mapping of Go's
  GC-specific `spedsql_gc_failures_total` under chaos.
- Spool pressure comes from `MURMUR SHOW METRICS SPOOL` (`disk_bytes`);
  mesh membership from `MURMUR SHOW PEERS` (connected rows per node).
- Writers, the status sampler, and the disk cycler run as goroutines
  sharing the cluster through one mutex; the director runs on the
  test goroutine. Every cluster touch locks, and each write re-checks
  its target's alive flag under the same lock, so lifecycle
  (stop/start) and reads never race (this replaces the old shared
  `Database` handle table, which has no multi-process equivalent).
- The markdown report is written to `runtime/endurance-chaos/`
  (retained under `tests-live/failures/` on FAIL via `Cluster::finish`)
  instead of next to the test, so passing runs leave the repo clean.
- One table, one bounded working set; no file traffic or schema
  migration during chaos, same as Go.

## Compile check

```sh
  go test -count=1 ./tests-live/endurance-chaos/
```
