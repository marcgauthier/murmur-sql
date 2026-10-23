# hostile-schema (Go port)

Rust source: `/home/marc/MURMUR/tests-live/hostile-schema/`
(`hostile_schema.rs`: `TestHostileSchemaManifestsQuarantined`).

## What it proves

A malicious authenticated peer (CA-issued QUIC cert, untrusted origin
key) serves hostile signed schema manifests at an honest 3-node mesh:
a revision with a corrupted signature, a self-consistent revision from
the untrusted signer, an oversized revision, and a zero-sequence
revision. Every forgery is refused (observed via stream death plus the
`last_error` latch — never assumed) and never adopted: the adoptions
counter stays put, the served schema bytes stay put, honest writes
converge, and digests stay equal.

## Pass criteria

- `NewCluster` opens cluster `hostile-schema` (3 nodes, full mesh,
  `max_transaction_bytes = 1 MiB` for the oversized leg) under
  `tests-live/runtime/hostile-schema/`.
- Positive control (pre-attack): `baseline` on node1 converges on all
  three nodes within 30 s.
- S1: the served tip with two flipped signature bytes kills its
  stream (ping unanswered, no `SchemaAck`), latches `invalid Ed25519
  signature`, moves neither the adoptions counter nor rows, and the
  node itself survives (a fresh session pings fine).
- S2: a correctly-signed divergent revision from the untrusted signer
  kills its stream (ping unanswered, no `SchemaAck`) and latches
  `untrusted schema signer`; never adopted.
- S3: a 2 MiB revision against the 1 MiB cap kills its stream and
  latches `schema envelope identity or size`.
- S4: a zero-sequence revision kills its stream and latches `schema
  envelope identity or size`.
- Quarantine: `schema_adoptions` unmoved since the baseline, served
  schema bytes unchanged, rows unchanged; all 6 attack handshakes
  attached (per-dial Welcome asserts plus one observed `peers == 3`).
- Positive control (post-attack): `post-attack-1`/`post-attack-2` on
  the honest pair and `attacked-node-write` on the attacked node all
  converge (2, 3, then 4 rows) within 30 s each, and the honest pair
  agrees bit-for-bit.

## Env knobs

- Harness knobs apply: `MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
  `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
  `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.

## Deviations (inherited from Rust)

- Plain SQL (`records(id INTEGER PRIMARY KEY, name TEXT)`,
  `ORDER BY name`) instead of the managed typed-records HTTP API.
- No Prometheus endpoint exists, so the counters map to product
  observables: adoptions to `Status().Metrics.SchemaAdoptions` (must
  not move; there is no inbound-manifest counter), conflicts to the
  `last_error` latch needles, and schema-epoch to the raw schema
  bytes served in `Welcome`.
- Bad manifests are rejected with an auth error, which also
  terminates the session, so there is no mismatch reply-plus-gating
  leg: each forgery runs on its own session and refusal is observed
  as stream death plus the latched cause.
- The fatal-path `Error{code: 1}` frame is not asserted on the wire
  (the engine closes the connection right after queueing it, so
  delivery is not guaranteed); the latch recorder is the refusal
  proof instead.
- Honest nodes may dial the attacker address back on
  discovered-membership ticks; those stray dials time out in
  `handshake timeout` latches that can overwrite `last_error`, so
  each refusal cause is captured by a 10 ms background latch
  recorder matched by timestamp, not single samples.
- The oversized leg uses a 2 MiB schema against a 1 MiB
  `max_transaction_bytes` overlay instead of relying on the 64 MiB
  default.
- The raw QUIC attacker is written against quic-go directly (the
  product's QUIC stack): `DialAddr` + `OpenStreamSync` +
  `replication.WriteMessage`/`ReadMessage` framing.
