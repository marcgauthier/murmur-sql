# encryption (Go port)

Rust source: `/home/marc/MURMUR/tests-live/encryption/`
(`encryption.rs`: `TestTypedEncryptedStoreKeepsRecordsEncryptedAcrossRestart`).

## What it proves

A single node stores one typed row carrying a secret marker, stops,
and its whole node directory is scanned for the marker in plaintext;
the node restarts, a wrong-key open is rejected, and the correct key
unlocks the row again.

## Pass criteria

- `NewCluster` opens cluster `encryption` (1 node) under
  `tests-live/runtime/encryption/`.
- The typed table is migrated, the marker row is inserted and flushed,
  and its count reads back as 1 before the restart.
- After `StopNode`, no regular file under the node dir contains the
  marker bytes.
- `murmur.Open` on the stopped node's data dir with an all-zero key
  fails with exactly `murmur.ErrWrongKey`.
- `StartNode` (same dir + key) succeeds and the marker row count reads
  back as 1.

## Env knobs

- Harness knobs apply: `MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
  `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
  `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.

## Deviations (inherited from Rust)

- Plain SQL (`records(name TEXT NOT NULL PRIMARY KEY)`, `INSERT` /
  `SELECT count(*)`, migrate + flush) instead of the managed
  typed-records HTTP API; the marker is the row key in both.
- Stop/restart is in-process `Close` + reopen via `StopNode` /
  `StartNode`, not daemon `SIGTERM` + re-exec.
- The wrong-key leg is a direct `murmur.Open` asserting
  `murmur.ErrWrongKey`: there is no Remote Unlock endpoint, so there
  is no unlock HTTP status to assert. The zero key mirrors the
  all-zero wrong key.
- The plaintext scan uses byte windows over raw file bytes instead of
  string conversion, so binary spool files are covered too.
