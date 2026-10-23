# txchunk-resume (Go port)

Rust source: `/home/marc/MURMUR/tests-live/txchunk-resume/`
(`txchunk_resume.rs`).

## What it proves

A single transaction larger than the replication frame travels as ~64 KiB
transaction chunks. The receiving node is stopped mid-transfer and
restarted; the retry must complete with all-or-nothing visibility, the
applied watermark must never advance before completion, and all digests
must converge without any snapshot transfer.

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
  loudly (a trial is discarded when the stop lands too late).

## Pass criteria (same as Rust)

- Base and every trial transaction exceed 8 MiB (fail loudly otherwise).
- Victim shows 0 trial rows while partitioned.
- During the retry the sender always shows the full trial and the victim
  shows none or all of it (never a partial transaction).
- The sender's view of the victim's applied watermark for the sender
  origin advances to cover the trial, and the first advance precedes full
  trial visibility by at most 10 s.
- The victim stages (>= 1 `ReceivedTransactions`) and applies (>= 1
  `AppliedTransactions`) after the restart; a trial with zero
  post-restart receipt is discarded as too-late and retried with fresh
  rows.
- Final exact row counts plus identical ordered digests on all nodes, with
  the transfer proven to use the log/chunk path rather than snapshots.

## Deviations from the Rust original

- In-process multi-`Database` cluster, same as Rust.
- Plain SQL (`records(id TEXT, name TEXT, phone TEXT)`); one
  `Database.Transaction` with N inserts is the big transaction.
- `Close()` + reopen replaces SIGKILL: signals cannot target one
  in-process node without freezing the driver. Chunk manifests and chunk
  keys are durable in the backend, so close-mid-transfer still exercises
  resume-after-restart-mid-receipt.
- The mid-transfer trigger is push-start (sender + witness
  `PushedTransactions` advancement observed while the victim shows 0
  trial rows) instead of victim inbound frame bytes: the product exposes
  no byte counters. Survivor convergence before the heal proves every
  pre-heal push already landed, so any post-heal push advancement is
  victim-bound trial flow.
- The absolute ack baseline is a 5 s-settled `RetentionStatus()`
  watermark instead of an absolute `local_seq`: the origin sequence
  and the local commit cursor live in different numbering spaces, while
  `Acknowledged[origin]` is exactly the `Have[origin]` watermark. The
  sender-advanced check (`seqTx > seqPre`) uses the commit cursor in one
  numbering space.
- Snapshot exclusion is structural plus counter-proven: the engine only
  sends snapshot requests to explicitly trusted snapshot sources and
  harness clusters never configure any (so no snapshot could be taken
  anywhere), while post-restart `received` + `applied` advancement proves
  the trial arrived and applied as a log transaction (snapshot
  application bypasses both counters). `ChunkNeedSent/Served` are logged
  as resume-path diagnostics.
- The engine chunks transactions at 1 MiB; the > 8 MiB assertion and the
  defaults (~10.8 MiB) are kept, which trips chunking with wide margin.
- `waitConverged` checks cheap row counts first and reads full
  multi-megabyte digests only once the counts match (same pass criteria,
  cheaper polling).
