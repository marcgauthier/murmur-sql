# txchunk-resume (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/txchunk-resume/`
(`txchunk_resume_test.go`: `TestChunkedTransactionResumesAfterReceiverSIGKILL`).

## What it proves

A single transaction larger than the replication frame travels as ~64 KiB
transaction chunks across three `murmur-server` processes. The receiving
node is stopped mid-transfer and restarted; the retry must complete with
all-or-nothing visibility, the applied watermark must never advance before
completion, and all digests must converge without any snapshot transfer.

## Procedure

1. Positive control: node 1 commits one multi-chunk transaction; all three
   nodes converge on it with equal digests.
2. Resume trials (up to `MURMUR_TXCHUNK_ATTEMPTS`): settle the quiet
   baseline (victim acked everything), partition the victim, commit a fresh
   big transaction on the sender, prove the survivors converge while the
   victim sees none of it, heal, stop the victim once the trial transfer to
   it starts flowing, restart it, and watch the retry.
3. Final: exact row counts and identical ordered digests everywhere.

## Env knobs

- `MURMUR_TXCHUNK_ROWS` (default 1200): rows per big transaction.
- `MURMUR_TXCHUNK_VALUE_BYTES` (default 9000): payload bytes per row.
- `MURMUR_TXCHUNK_ATTEMPTS` (default 3): resume trials before failing
  loudly (a trial is discarded when the stop lands too late, exactly as
  in Go).
- Harness knobs apply: `MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
  `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
  `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`, `MURMUR_SERVER_BIN`.

## Pass criteria

- Base and every trial transaction exceed 8 MiB (fail loudly otherwise).
- Victim shows 0 trial rows while partitioned.
- During the retry the sender always shows the full trial and the victim
  shows none or all of it (never a partial transaction).
- The sender's view of the victim's applied watermark for the sender
  origin advances to cover the trial, and the first advance precedes full
  trial visibility by at most 10 s.
- The victim applies (>= 1 `applied_transactions`) after the restart; a
  trial with zero post-restart apply is discarded as too-late and retried
  with fresh rows.
- Final exact row counts plus identical ordered digests on all nodes, with
  the transfer proven to use the log/chunk path rather than snapshots.

## Deviations from the Go original

- Multi-process cluster: one `murmur-server` child per node, driven over
  the PostgreSQL wire (SQL + `MURMUR ...` commands), instead of `testnode`
  daemon processes over HTTPS+mTLS (per design). Process stop/start
  replaces SIGKILL; `mesh()` re-dials after each restart because the node
  rebinds fresh ports. Every node runs
  `MURMUR SET TABLE records REPLICATED true` (per-node config) before
  any writes.
- Plain SQL (`records(id TEXT, name TEXT, phone TEXT)`) replaces the
  managed typed-records HTTP API (`InsertMany` / prefix count / digest);
  one `BEGIN`..`COMMIT` block with N inserts over the wire is the big
  transaction.
- Single-unit shipping (`capture_interval = "1h"`, `batch_bytes` 16 MiB):
  the 1 s background capture tick would stream the unflushed trial as
  incremental captures and the default 4 MiB batch unit would sign/apply
  a 10 MiB session as three units, so the trial would converge
  incrementally and the all-or-nothing resume contract could never
  hold. With the stretched tick each trial ships on its flush as one
  ~10 MiB signed transaction (still chunked over the wire past the
  1 MiB engine threshold) and applies atomically.
- The mid-transfer trigger is staged-gauge-plus-head-start instead of
  victim inbound frame bytes >= 256 KiB: the Rust product exposes no byte
  or push counters over the wire. After the heal the test stops the victim
  as soon as its `staged_transactions` gauge (via
  `MURMUR SHOW REPLICATION`) goes nonzero while it shows no trial rows,
  or 1.5 s after the heal, whichever comes first; the multi-megabyte
  trial takes seconds to transfer, so the stop lands mid-transfer, and
  the post-restart apply check validates each trial (a victim that
  converged first discards the attempt, exactly as in Go).
- The absolute ack baseline is a 2 s-settled `MURMUR SHOW RETENTION`
  watermark instead of an absolute `local_seq`: the Rust origin sequence
  and the local commit cursor live in different numbering spaces, while
  `acknowledged[origin]` is exactly Go's `Have[origin]`. The
  sender-advanced check (`seq_tx > seq_pre`) uses the `committed` column
  of `MURMUR SHOW REPLICATION` in one numbering space.
- Snapshot exclusion is structural plus counter-proven: the engine only
  sends `SnapshotRequest` to explicitly trusted snapshot sources and this
  cluster never configures any (so no snapshot could be taken anywhere),
  while post-restart `applied_transactions` advancement proves the trial
  arrived and applied as a log transaction (snapshot application bypasses
  that counter). `received_transactions` and `chunk_need_sent/served`
  have no wire equivalent (`MURMUR SHOW METRICS` only covers
  SQLite/Spool), so only the cumulative applied counter is asserted and
  the chunk diagnostics are dropped. There are no snapshot sent/received
  counters in the Rust product.
- The Rust engine chunks transactions at 1 MiB (Go: 8 MiB frame); the
  > 8 MiB assertion and the defaults (~10.8 MiB) are kept from Go, which
  trips Rust chunking with wide margin.
- `wait_converged` checks cheap row counts first and reads full
  multi-megabyte digests only once the counts match (same pass criteria,
  cheaper polling).

## Compile check (only gate; never run here)

```sh
  go test -count=1 ./tests-live/txchunk-resume/
```
