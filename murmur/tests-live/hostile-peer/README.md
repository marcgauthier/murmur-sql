# hostile-peer (Go port)

Rust source: `/home/marc/MURMUR/tests-live/hostile-peer/`
(`hostile_peer.rs`: `TestHostilePeerAttacksRejected`).

## What it proves

A malicious authenticated peer (CA-issued QUIC cert, untrusted origin
key) fires forged batches, invalid identities, a replayed handshake, a
256 MiB allocation-claim frame, a compressed snapshot bomb, and a
far-future-HLC batch at an honest 2-node mesh. Every attack is
rejected (observed via `Error{code:1}` wire replies, stream death, the
`last_error` latch, and unmoved `received/applied` counters — never
assumed), nothing foreign applies, and the honest pair stays
converged and responsive throughout.

## Pass criteria

- 2 nodes open with full mesh and `max_transaction_bytes = 1 MiB` for
  the oversized leg under `tests-live/runtime/hostile-peer/`.
- Positive control (pre-attack): `honest-1` on node0 and `honest-2`
  on node1 converge both ways within 30 s each.
- Schema-learn dial returns the node's real schema bytes; the control
  dial with those bytes is welcomed with no `SchemaRequest` (proves
  TLS trust, identity, the hand-rolled framer, and the matched-schema
  bytes, so later rejections are caused by the attacks alone); the
  control session tears down (`peers == 1`) before the attacks.
- P1a: on one watched divergent-schema session, a zero-sequence
  batch, a wrong-database batch, a 2 MiB oversized batch, and a
  `Need{from: 0}` each draw `Error{code: 1}` and the session stays
  alive (ping round-trips). Every dial is Welcome-asserted and
  `peers == 2` is observed while the watched session is attached.
- P2: a replayed handshake on the live session draws
  `Error{code: 1, "repeated handshake"}` without killing the session
  (ping round-trips again).
- P1b: a correctly-signed matched-schema batch from the untrusted
  origin kills its session (ping unanswered), latches `untrusted
  transaction origin`, moves neither `received_transactions` nor
  `applied_transactions`, and leaves rows/digests unchanged.
- P3: a 256 MiB allocation-claim frame kills its stream; a fresh
  session handshakes and pings fine (node survives).
- P4: a zstd bomb (4 MiB of `0xFF` in a tiny frame, undecodable as a
  message) kills its stream; a fresh session handshakes and pings
  fine (node survives).
- P-HLC: a far-future-HLC matched-schema batch from the untrusted
  origin is rejected before HLC observation (stream dies, `untrusted`
  latched, counters unmoved); the pair still converges on 2 rows.
- Positive control (post-attack): schema bytes unchanged, counters
  unmoved since the baseline, then `post-attack-1`/`post-attack-2`
  converge both ways (3 then 4 rows) within 30 s each.

## Env knobs

- Harness knobs apply: `MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
  `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
  `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.

## Deviations (inherited from Rust)

- Plain SQL (`records(id INTEGER PRIMARY KEY, name TEXT)`,
  `ORDER BY name`) instead of the managed typed-records HTTP API.
- No Prometheus endpoint exists, so the counters map to product
  observables: origin-unknown / batches-invalid to `Error{code: 1}`
  wire replies plus the `last_error` latch, batches-received to
  `Metrics.ReceivedTransactions` (must not move), sessions-opened to
  per-dial Welcome asserts plus one observed `peers == 2`, and
  schema-epoch to the raw schema bytes served in `Welcome`.
- The engine rejects untrusted origins with an auth error, which also
  terminates the session, so the forged batch runs on its own session
  while the invalid-identity set (all protocol errors) shares the
  surviving watched session.
- The engine does not data-gate divergent-schema peers (pushes are not
  gated on schema agreement), so instead of "no data frames" the port
  asserts every data frame on the watched session carries honest
  origins only.
- The snapshot bomb uses zstd (the product's wire compressor); the
  oversized leg uses a 2 MiB payload against a 1 MiB
  `max_transaction_bytes` overlay instead of the 64 MiB default.
- The `Need{from: 0}` probe stands in for the zero-HLC leg (the
  `Transaction` verifier does not gate on the HLC field).
- Honest nodes may dial the attacker address back on the bootstrap
  tick; those stray dials time out in `handshake timeout` latches
  that can overwrite `last_error`, so the `untrusted` cause is
  captured by a 10 ms background latch recorder, not single samples.
- The raw QUIC attacker is written against quic-go directly (the
  product's QUIC stack): `DialAddr` + `OpenStreamSync` +
  `replication.WriteMessage`/`ReadMessage` framing.
