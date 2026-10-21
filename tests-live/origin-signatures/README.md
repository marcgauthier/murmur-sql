# origin-signatures (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/origin-signatures/`
(`origin_signatures_test.go`:
`TestForwardedOriginSurvivesOfflineOriginAndHostileRelay`).

## What it proves

Three `murmur-server` processes share one database id. A commits a
transaction, B receives it, A stops, and C receives A's transaction
exclusively through B. A hostile relay impersonating B (a raw QUIC
client speaking the replication wire protocol via the shared harness
attacker fixture, which reuses the product implementation) then fetches
A's batches from C, and sends modified, unsigned, cross-DBID,
fabricated, and chunk-identity attacks. Every attack must be rejected
with rows, staged/applied counters, and commit cursor untouched, and a
chunk-availability probe must show nothing staged. Restart checks
durability, a signed-duplicate replay checks idempotency, and an
untrusted snapshot manifest checks donor trust.

## Pass criteria

- C converges on A's row while A is stopped (forwarded through B only);
  B's and C's row sets are equal.
- Counters (`staged_transactions`, `applied_transactions` from
  `MURMUR SHOW REPLICATION`) quiesce on C after B shuts down:
  in-flight honest batches may still apply (verified data is never
  dropped), then `staged` holds at 0, `applied` holds still, and the
  cursor never moves backwards.
- The relay captures A's batches from C over a `Need` session; every
  served batch verifies under A's origin key.
- Each attack (modified, unsigned, cross-DBID, fabricated,
  chunk-identity) is rejected: C records the rejection in `last_error`
  while rows, counters, and commit cursor stay flat; the availability
  probe shows no forged chunks staged.
- After a C restart with A and B stopped, the honest row is still
  present and the fresh-process counters start at zero.
- The signed-duplicate replay draws no error frame and moves nothing
  (rows, counters, cursor).
- The untrusted snapshot manifest (valid B signature, B donor, C's own
  schema) is refused with `last_error` set and nothing moved.
- After B rejoins and re-offers, C's rows, counters, and cursor are
  unchanged (the engine dedups before counting, so `received` stays 0).

## Env knobs

None beyond the shared `MURMUR_LIVE_RUNTIME` / `MURMUR_LIVE_FAILURES` /
`MURMUR_LIVE_UNLOCK_*` harness knobs.

## Deviations from the Go original

- Multi-process `murmur-server` children over the PostgreSQL wire
  protocol instead of `testnode` daemon processes over HTTPS+mTLS (per
  design).
- Plain SQL replaces the typed-records HTTP API; the table is
  explicitly flagged replicated on each node. There is no daemon, no
  HTTPS API, and no Prometheus endpoint.
- Go's HLC, generation, and per-TxID receipt assertions map to the
  staged/applied counters plus the row set (receipts commit atomically
  with the apply; the Rust engine exposes no HLC clock or receipt read
  API). The commit cursor (`committed`) stands in for state generation.
- Go reads the original batch from the stopped origin's store; the port
  fetches it from C over a `Need` session (served-origin-proof capture)
  and verifies it under A's origin key, which additionally proves the
  offline origin's log stays servable through the forwarder.
- Go's `batches_invalid_total` / `origin_signature_invalid_total`
  counters have no Rust equivalent, so rejection is observed via
  `last_error` (changed text containing the check that failed) plus
  flat rows/counters/cursor; per-leg resends tolerate `last_error`
  overwrites from C's redial of stopped B. The unsigned/fabricated legs
  assert an `authentication` rejection rather than the ed25519 message
  text.
- The relay authenticates as B with a fresh cluster-CA leaf for B's id
  (identity is the TLS SAN URI) and B's origin seed; one session per
  leg with a quiet gate between dials (the engine closes duplicate
  inbound sessions for an id with a lingering entry).
- The honest-transport idempotent re-offer (rejoining B re-offers over
  the real mesh) is kept after the relay replay as an extra leg.

## Compile check (only gate; never run here)

```sh
CARGO_TARGET_DIR=/home/marc/MURMUR-SQL/tests-live/target \
  cargo test --no-run --manifest-path /home/marc/MURMUR-SQL/tests-live/origin-signatures/Cargo.toml
```
