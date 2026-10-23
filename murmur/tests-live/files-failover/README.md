# files-failover (Go scenario)

New Go scenario (no Rust original): multi-source file fetch failover.

## What it proves

A three-node encrypted mesh (file object key `0x55..`, 4 MiB file cap,
100 ms fetch interval) proves a fetcher fails over across sources. Six
4 KiB files replicate from node 1 to node 2; node 2's stored objects are
then poisoned at rest (same length, flipped ciphertext) and node 3
joins against node 2 only, with node 1 administratively excluded (plain
unlinked nodes would self-connect via membership gossip plus bootstrap
auto-dial). In the corrupt-only phase every node 3 fetch attempt must
fail verification (`object authentication`) with nothing published.
Explicitly re-admitting node 1 lets every fetch complete byte-identical,
and staging drains (poisoned partials discarded, never stranded).

## Pass criteria

- `NewCluster` opens 3 unmeshed nodes under `tests-live/runtime/files-failover/`
  (`ManualPeers`; links are added explicitly per leg).
- Baseline: node 2 fetches all 6 objects; node 3 holds all 6 metadata.
- Corrupt-only phase (5 s): no file becomes available or downloadable
  on node 3; its last fetch error is the verification sentinel; node 2
  cannot download its own poisoned objects.
- After the heal: within 60 s (50 ms poll) every file is available on
  node 3 with matching digest and byte-identical downloads, and staging
  drains to zero.

## Env knobs

None scenario-specific. Harness knobs apply: `MURMUR_LIVE_RUNTIME`,
`MURMUR_LIVE_FAILURES`, `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
`MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.

## Notes

- `files-corrupt-source` proves one poisoned source fails; this scenario
  proves the fetcher survives it by failing over to the honest source.
- Present objects are never re-verified locally, so the poisoned source
  stays poisoned for the whole run by design.
