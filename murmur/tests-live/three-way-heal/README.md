# three-way-heal (Go port)

Rust source: `/home/marc/MURMUR/tests-live/three-way-heal/`
(`three_way_heal.rs`: `heal_order_does_not_affect_final_digest`).

## What it proves

Four fully meshed nodes split into isolated pairs (0,1) and (2,3),
diverge with deterministic writes (disjoint inserts plus conflicting
updates to the same base rows), then heal link-by-link in a
caller-supplied order. Two different heal orders must reach the
identical final digest: heal order must not affect the converged
state, including LWW conflict winners.

## Pass criteria

- `NewCluster` opens 4 meshed nodes under
  `tests-live/runtime/three-way-heal-a/` (then `-b` for the second
  order).
- 10 baseline inserts on node 0 converge on all 4 nodes within 150 s
  (honest-path control).
- After removing the 4 cross links in both directions, side A (node 1)
  and side B (node 3) each write 10 disjoint inserts plus 10
  conflicting updates to the base rows; each pair converges
  internally on 20 rows within 150 s, with divergent digests (split
  proof).
- Each of the 4 links heals in the prescribed order (default
  `0,1,2,3` then `3,2,1,0`); after every heal, real cross-side
  traffic must appear on the new link within 30 s.
- After the final heal, all 4 nodes converge on exactly 30 rows with
  equal digests within 150 s.
- The final digests of both orders are identical (the assertion); the
  LWW winner on row 1000 is logged for forensics.

## Env knobs

- `MURMUR_THREE_WAY_HEAL_ORDER` (default `0,1,2,3`): first heal order,
  a permutation of the 4 cross-link indexes.
- `MURMUR_THREE_WAY_HEAL_ORDER_B` (default `3,2,1,0`): second heal order.
- Harness knobs apply: `MURMUR_LIVE_RUNTIME`,
  `MURMUR_LIVE_FAILURES`, `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
  `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.

## Deviations from the Rust original

- In-process multi-`Database` cluster (QUIC mesh, ephemeral `127.0.0.1:0`)
  like the Rust port (per design).
- Plain SQL (`records(id INTEGER PRIMARY KEY, name TEXT)`) instead of the
  managed typed-records HTTP API; integer ids 1000+i / 2000+i / 3000+i
  replace the formatted typed UUIDs.
- Digest is FNV-1a 64 (hex) over the canonical `id\0name\0` row
  encoding instead of sha256. The test only asserts digest equality
  across heal orders, which this preserves; rows arrive ordered via
  `ORDER BY id`.
- The two heal orders run as sequential clusters inside one test
  (each with its own runtime dir, forensics, and finish), matching
  the sequential `runHealOrder` calls.
