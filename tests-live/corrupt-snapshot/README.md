# corrupt-snapshot (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/corrupt-snapshot/`
(`README.md` + `corrupt_snapshot_test.go` + `attacker.go`:
`TestStaleNodeDiscardsCorruptSnapshotThenConverges`).

## What it proves

A stale node rejoins while its only available snapshot source is hostile.
The attacker induces a snapshot pull with an unsolicited history-unavailable
error, then serves a structurally valid manifest plus one intact and one
bit-flipped chunk. The receiver must reject the corrupt chunk with nothing
partially published (row count, digest, commit cursor, and replication
counters untouched), discard the staged transfer, then converge via the
honest snapshot path with bit-identical digests.

## Pass criteria

- 3 nodes mesh explicitly; node3 writes 5 `stale-*` rows; all converge (60 s).
- Node3 is isolated and stopped; node1 writes 60 `fresh-*` rows; survivors
  converge on 65 rows (30 s each).
- After the member-deadline wait, each survivor prunes >= 40 transactions
  and no longer pins node3 (proves the rejoin must use the snapshot path,
  since only the newest 10 per origin survive).
- Node3 restarts peerless; the attacker (a raw QUIC client speaking the
  replication wire protocol) dials it, sends `Error{code: 3}`, and node3
  requests a snapshot (30 s). Premise: exactly 1 connected peer.
- The forged manifest is accepted (node3 asks for its chunks, 15 s); the
  intact chunk stages and the bit-flipped chunk is rejected: node3 records
  a `snapshot chunk` authentication error (30 s) and never sends
  `SnapshotApplied` (3 s drain).
- After a 2 s settle: node3 still holds exactly the 5 pre-stop rows with an
  identical digest, commit cursor, and log-path counters.
- The staged corrupt transfer expires (20 s forged lifetime + 10 s margin).
- Honest peers return; all converge on 65 rows with equal digests (120 s)
  while node3's log-path counters stay flat (proves the rows arrived via an
  honest snapshot, which bypasses those counters); the commit cursor never
  regresses and the 5 pre-stop `stale-*` rows survive the merge.

## Env knobs

- `MURMUR_CORRUPT_SNAPSHOT_GC_WAIT_SECONDS` (default 30): member-deadline
  wait before the collection passes. Lower than the Go default (55) because
  this port drives retention with explicit `collect_retention` passes
  instead of waiting out the engine's 60 s collection interval.
- Harness knobs apply: `MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
  `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
  `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.

## Deviations from the Go original

- Multi-process `murmur-server` cluster over PostgreSQL wire SQL and QUIC
  replication instead of `testnode` daemon processes over HTTPS+mTLS.
- Plain SQL (`records(id INTEGER PRIMARY KEY, name TEXT)`) instead of the
  managed typed-records HTTP API; digests are newline-joined names.
- Process stop/start via harness `stop_node`/`start_node`.
- Snapshot trust is configured up front (every node trusts every node; the
  victim additionally trusts the attacker id and origin key) instead of by
  editing the victim's config file before restart. The empty-trust default
  is unchanged: the engine only requests snapshots from explicitly trusted
  sources, so without this overlay neither the attack nor the honest leg
  could run.
- No snapshot metrics exist (`ReplicationMetrics` carries no snapshot
  counters), so the Go counter legs map to behavior: manifest acceptance is
  proven by the victim's `SnapshotNeed`, rejection by its `last_error`
  (`snapshot chunk length or hash`) plus the closed session, non-completion
  by the absent `SnapshotApplied`, and the honest snapshot leg by 60 rows
  arriving with flat log-path counters.
- Per-chunk hashes reject the corrupt chunk at receipt (nothing stages),
  where Go stages both chunks and fails at completion; the forged payload is
  opaque bytes rather than encoded cells for the same reason.
- The forged manifest carries a short (20 s) lifetime: staged-transfer
  expiry is the engine's only discard path for a failed transfer, and an
  unexpired staged transfer would block the honest snapshot (`another
  snapshot is staged`). The honest leg succeeding proves the discard.
- The attacker collects patiently across quiet 500 ms slices (Go returns on
  the first quiet slice); strictly more waiting, same assertions.
- Commit cursor from `MURMUR SHOW REPLICATION` stands in for the Go state
  generation metric.
- Survivor writes flush per row: the product mints one replication log entry
  per flush batch rather than per SQL transaction, so the 60 writes need 60
  flushes to produce enough history for the prune-past-retention leg.

## Compile check (only gate; never run here)

```sh
go test -count=1 ./tests-live/corrupt-snapshot/
```

The raw-QUIC attacker is the shared harness fixture
(`murmur_live_harness::attacker`), which reuses the product
wire-format/crypto implementation instead of forking it.
