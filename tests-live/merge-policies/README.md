# merge-policies (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/merge-policies/`
(`merge_policies_test.go`, `README.md`).

## What it proves

Three in-process nodes share one typed row (`live_merge_policies`, TEXT
primary key, PN_COUNTER / OR_SET / MAX / MIN policy columns); one `#[test]`
fn proves the Go pass criteria:

| Rust test | Go source |
|---|---|
| `disconnected_policies_forward_and_restart` | `TestDisconnectedPoliciesForwardAndRestart` |

Pass criteria: while disconnected, node 0 writes counter +10 / set
{shared, red} / extrema (10, 30), node 1 writes counter +7 / unobserved
remove of "shared" / set {blue} / extrema (20, 15), node 2 writes counter
-2 / set {green} / extrema (5, -7); after nodes 0+1 connect both converge
counter 17; node 0 stops and node 1 forwards to node 2, converging counter
15, set {blue, green, red, shared} (the unobserved remove erased nothing),
peak 20, floor -7 on nodes 1+2; node 2 restarts and recovers counter 15 and
the full projection; a later observed remove of "shared" from node 1
propagates to node 2 ({blue, green, red}, extrema unchanged).

## Env knobs

Standard harness knobs only (`MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
`MURMUR_LIVE_UNLOCK_STALL_SECONDS`, `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`).
Convergence waits are 30 s with 50 ms polls, matching the Go timeouts.

## Deviations from the Go original

- Multi-process `murmur-server` children over the PostgreSQL wire
  protocol instead of `testnode` daemon processes over HTTPS+mTLS (per
  design).
- Policy ops run as `MURMUR POLICY COUNTER_ADD|SET_ADD|SET_REMOVE|MIN|MAX`
  commands over the wire; plain SQL replaces the managed typed HTTP API
  (there is no HTTP server in the Rust product).
- Merge policies are bound at open through the harness
  `ReplicationOptions::merge_policies` overlay.
- `TypedInsertWithID` with one shared row id maps to the same SQL `INSERT`
  of `merge-policy-row` on every node while `manual_peers` keeps them
  disconnected (established offline-row pattern); the TEXT application key
  is the row identity. The table is flagged replicated on each node before
  any row exists.
- OR_SET projections decode from the stored blob (`\x<hex>` over the
  simple protocol, hex-decoded, then postcard-decoded with a test-only
  mirror of the product `Value` enum, same variant order); the harness
  exposes no set-projection read API.
- `UnlockNode` + `WaitNodeReady` after `StartNode` collapse into the harness
  `start_node` (same dir + key respawn under the unlock watchdog); QUIC/mTLS
  and the origin-key trust the Go README mentions are provisioned by the
  harness (`rcgen` CA + per-node certs, mutual `trusted_origins`).
- The node 2 restart rebinds fresh ports, so the 1 <-> 2 edges are
  re-added explicitly after `start_node` (peers still dial the old
  address otherwise), followed by `sync_all` to hurry convergence.

## Compile gate (only gate; never run)

```sh
CARGO_TARGET_DIR=/home/marc/MURMUR-SQL/tests-live/target \
  cargo test --no-run --manifest-path /home/marc/MURMUR-SQL/tests-live/merge-policies/Cargo.toml
```
