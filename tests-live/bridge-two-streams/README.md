# bridge-two-streams (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/bridge-two-streams/`
(`two_streams_test.go` + `README.md`).

## What it proves

Two independent single-node Low clusters (distinct DBIDs, CAs, keys,
streams, staging dirs) export managed rows into `stream-a` and `stream-b`;
a two-node High cluster imports stream-a on node0 and stream-b on node1
(per-node bridge receiver configs) and converges both domains' rows over
its mesh (imported rows replicate as auxiliary records). One async test
(`two_low_domains_into_one_high`, from
`TestTwoLowDomainsIntoOneHigh`) proves the Go pass criteria: round 1 lands
5 A-rows + 5 B-rows on both High nodes with exact names; identical
redelivery through a stability window changes nothing (receipt
idempotence); a per-domain Low update flows through to both High nodes,
proving Low-owned provenance; the names are restored; a stream-a-only
round 2 (a-5..a-7) arrives; and a stream-b-only round 3 (b-5, b-6)
delivers exactly once, proving stream-b progress was undisturbed by the
stream-a round.

Pass criteria: both High nodes converge on exactly the 10 round-1 names,
still exactly those 10 after identical redelivery, the 2 renamed probe
values then the restored 10, exactly the 13 round-2 names (`a-0..a-7`,
`b-0..b-4`), and exactly the 15 round-3 names (plus `b-5`, `b-6`).

## Env knobs

Standard harness knobs only (`MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
`MURMUR_LIVE_UNLOCK_STALL_SECONDS`, `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`,
`MURMUR_SERVER_BIN`). Convergence waits are 60 s with 200 ms polls,
matching the Go timeouts.

## Deviations from the Go original

- Multi-process `murmur-server` children over the PG wire (per design)
  instead of `testnode` daemon processes over HTTPS+mTLS.
- Plain SQL via `execute`/`query` replaces the typed HTTP API; there is
  no HTTP server in the Rust product. Row IDs keep the Go `%032x` shape
  as the `TEXT NOT NULL PRIMARY KEY`.
- Bridge capture/process are not exposed over the wire (see USAGE.md);
  the engine drives both automatically in the background on its apply
  tick, and the test plays the operator delivery leg by copying
  completed `.bundle` artifacts from each Low outbox to its High inbox
  (inside every wait poll). Per-call bundle counts are not asserted:
  progress is proven by row convergence instead of `published`/`imported`
  counters.
- `BridgeStatus` progress has no wire API, so the byte-identical progress
  assertion is replaced by behavior: exact B-row content after the
  stream-a-only round, plus the stream-b-only round 3, which can only
  deliver exactly once from an intact cursor.
- `Provenance` has no wire API, so `assertLowOwned` is replaced by
  behavior: a Low update to one row per domain must become visible on
  both High nodes. A High-owned field would suppress the Low update, so
  flow-through proves Low ownership. Probe names are then restored so
  round 2 keeps the exact Go name vectors.
- The duplicate-delivery gate (named in the Go README) is an explicit
  redelivery leg: after round-1 convergence, identical bundle bytes are
  re-copied through a 5 s stability window and both High nodes must still
  show exactly the 10 names.
- Bridge keys are fixed test constants; only public-key derivation
  (stdlib `crypto/ed25519`, X25519 via `golang.org/x/crypto/curve25519`)
  lives in the scenario. Each outbox/inbox dir lives under its
  cluster's runtime dir so it is retained on failure and cleaned on pass.
- `MURMUR SET TABLE ... REPLICATED true` runs on all four nodes: bridge
  capture reads the replication log, which only carries replicated
  tables.
- Three clusters in one test means three `RESULT` lines (one per cluster
  name) and three forensics dumps on failure; cluster setup itself is
  outside the fallible body, so a setup failure panics without artifact
  retention, matching the single-cluster template behavior.

## NOT ported (missing product behavior, documented, no silent skip)

`TestCrossDomainIdentityCollisionFailsClosed` is not ported. The Go product
rejects a cross-domain same-row-ID import with `ErrIdentityCollision`
and leaves first-writer state intact on both High nodes. The Rust product
has no row-identity collision detection (one shadow per source domain,
merged deterministically) and no wire-observable import-rejection signal,
so no failing-closed signal exists to assert. Porting this leg needs a
product change (identity-collision detection on the import path plus an
observable rejection); asserting the current merge behavior would enshrine
a fail-open divergence from the Go gate.

## Compile check (only gate; never run here)

```sh
  go test -count=1 ./tests-live/bridge-two-streams/
```
