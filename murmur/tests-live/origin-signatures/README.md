# origin-signatures (Go port)

Rust source: `/home/marc/MURMUR/tests-live/origin-signatures/`
(`tests/origin_signatures.rs`,
`forwarded_origin_survives_offline_origin_and_hostile_relay`), itself
a port of the original Go `origin-signatures` scenario
(`TestForwardedOriginSurvivesOfflineOriginAndHostileRelay`).

## What it proves

Three encrypted nodes share one database id. A commits a transaction,
B receives it, A stops, and C receives A's transaction exclusively
through B. The fixture checks the actual stored origin proof, then a
malicious relay authenticated as B sends modified, fabricated,
unsigned, cross-DBID and chunk-identity attacks at C. Every attack must
be rejected while rows, generation and receipts stay unchanged.
Restart plus signed-duplicate replay check durability and idempotency,
and an untrusted snapshot manifest must be refused.

## Pass criteria (same as Rust)

- C converges on A's row while A is stopped (forwarded through B
  only); B's and C's row sets are equal.
- A's served bytes verify under A's origin key with the origin intact.
- Modified / fabricated / unsigned / cross-DBID batches are rejected;
  the tampered chunk fails before durable staging (availability index
  empty); row set and staged/applied counters never move.
- After a C restart the honest row is still present; replaying the
  signed original is idempotent (no error, session survives, counters
  and rows unchanged).
- A well-formed snapshot manifest from the (untrusted) relay is
  refused; rows and counters unchanged.

## Deviations

None from the Rust port: plain SQL replaces the typed-records HTTP
API; each signature-failure leg uses its own relay session (an auth
failure kills the session); rejection evidence is the wire outcome
plus the `last_error` latch cause; the relay uses a fresh harness-CA
leaf for B's NodeID plus a dedicated attacker signing key; staged /
applied counters stand in for HLC, generation and receipts.
