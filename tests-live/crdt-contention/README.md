# crdt-contention (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/crdt-contention/`
(`contention_test.go`,
`TestMultiProcessDisjointAndSharedCellContention`).

## What it proves

Three meshed processes share one logical row (`contention(id, name,
phone, score)`): concurrent edits to separate cells (name on node1,
phone on node2, score on node3) must all survive regardless of arrival
order, and 40 same-cell `name` updates per node must resolve to one
winner, identical on every process, with the full logical row equal
everywhere.

## Pass criteria (same as Go)

- Full `[2, 2, 2]` mesh, then the baseline row converges everywhere.
- Disjoint concurrent cell edits converge to the exact expected row.
- Same-cell contention converges with phone/score undisturbed; the
  winner is non-empty and comes from the concurrent write set; the
  whole logical row is identical on all three processes.

## Env knobs

None beyond the standard harness knobs (`MURMUR_LIVE_RUNTIME`,
`MURMUR_LIVE_FAILURES`, `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
`MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`, `MURMUR_SERVER_BIN`).

## Deviations from the Go original

- Multi-process `murmur-server` children over the PG wire (per design)
  instead of `testnode` daemon processes over HTTPS+mTLS.
- Plain SQL on a replicated table (`contention(id TEXT NOT NULL PRIMARY KEY, ...)`
  with explicit `NOT NULL` constraint per PG wire requirements) instead of
  the typed-contention HTTP API (`TypedContentionInsert`/`Update`/`Read`);
  contention runs as borrowed `tokio::join!` futures in the pilot style.
- Session counts come from `MURMUR SHOW REPLICATION` (`peers` column)
  instead of `/v1/status` (`connected_peers`).

## Verification

```sh
MURMUR_SERVER_BIN=/home/marc/MURMUR-SQL/target/debug/murmur-server \
  CARGO_TARGET_DIR=/home/marc/MURMUR-SQL/target \
  cargo test --manifest-path tests-live/crdt-contention/Cargo.toml -- --nocapture
```
