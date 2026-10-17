# graceful-shutdown (Live SQL test)

Live SQL port of the `graceful-shutdown` scenario.

## What it proves

Phase A stops Node 1 mid-write under concurrent client write load with SIGTERM.
Phase B restarts Node 3 stale (log ranges GC'd via `MURMUR RETENTION COLLECT` after member deadline expiry, forcing snapshot-resync) and interrupts it mid-transfer with a clean stop.
Both restarts are clean and fast, lose zero acknowledged writes, and the cluster reconverges with identical contents across all nodes; post-recovery replication remains fully operational.

## Pass criteria

- 5-row baseline converges across all nodes within 30 s.
- Phase A stops Node 1 with writes in flight under load via SIGTERM; restarts under 60 s; cluster converges with every acknowledged write present.
- 50 survivor rows + 8 fat (~64 KB) rows converge on both survivors while Node 3 is offline.
- Maximum-offline window (5 s) expires, and on-demand `MURMUR RETENTION COLLECT` collects log batches on survivors.
- Node 3 restarts and is interrupted mid-transfer during snapshot resync.
- Node 3 restarts again and all nodes reconverge with all rows intact.
- A `post-recovery` write converges across all 3 nodes.

## How to run

```sh
MURMUR_SERVER_BIN=../../target/debug/murmur-server cargo test --manifest-path tests-live/graceful-shutdown/Cargo.toml -- --nocapture
```
