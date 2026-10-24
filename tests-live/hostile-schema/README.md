# hostile-schema (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/hostile-schema/`
(`README.md` + `attacker.go` + `hostile_schema_test.go`:
`TestHostileSchemaManifestsQuarantined`).

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

- `Cluster::new` opens cluster `hostile-schema` (3 nodes, full mesh,
  `max_transaction_bytes = 1 MiB` for the oversized leg) under
  `tests-live/runtime/hostile-schema/`.
- Positive control (pre-attack): `baseline` on node1 converges on all
  three nodes within 30 s.
- S1: the served tip with two flipped signature bytes kills its
  stream (ping unanswered, no `SchemaAck`), latches `signature
  error`, moves neither the adoptions counter nor rows, and the node
  itself survives (a fresh session pings fine).
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

## Deviations from the Go original

- Plain SQL (`records(id INTEGER PRIMARY KEY, name TEXT)`,
  `ORDER BY name`) instead of the managed typed-records HTTP API.
- No Prometheus endpoint exists, so the Go counters map to product
  observables: `schema_manifests_received` / `schema_adoptions` /
  `schema_merges` to `status().metrics.schema_adoptions` (must not
  move; there is no inbound-manifest counter in the Rust engine),
  `schema_conflicts` to the `last_error` latch needles, and
  `schema_epoch` to the raw schema bytes served in `Welcome`.
- Rust rejects bad manifests with `Error::Auth`, which (unlike Go's
  non-fatal mismatch path) also terminates the session, so there is
  no `ErrSchemaMismatch` reply-plus-gating leg: each forgery runs on
  its own session and refusal is observed as stream death plus the
  latched cause.
- The fatal-path `Error{code: 1}` frame is not asserted on the wire
  (the engine closes the connection right after queueing it, so
  delivery is not guaranteed); the latch recorder is the refusal
  proof instead.
- Honest nodes dial the attacker address back on
  discovered-membership ticks; those stray dials time out in
  `handshake timeout` latches that can overwrite `last_error`, so
  each refusal cause is captured by a 10 ms background latch
  recorder matched by timestamp, not single samples.
- The oversized leg uses a 2 MiB schema against a 1 MiB
  `max_transaction_bytes` overlay instead of relying on the 64 MiB
  default.
- Rewritten to use the async `murmur-live-sql-harness` cluster and `murmur-client` async PostgreSQL-wire client.
- Uses CA-issued certificate from Node 3 (stopped node) to perform authenticated QUIC/TLS handshakes as the malicious peer against the 3 honest nodes (Nodes 0, 1, 2).
- The 10 ms background latch recorder plus direct `MURMUR SHOW REPLICATION` verification captures the refusal latches (`signature error`, `untrusted schema signer`, `schema envelope identity or size`), accounting for potential connection resets or transient error latches.
- All four schema attacks (S1 corrupted signature, S2 untrusted signer, S3 oversized revision, S4 zero-sequence revision), quarantine assertions, and honest node convergence pass against live `murmur-server` processes.

## Run

```sh
MURMUR_SERVER_BIN=/home/marc/MURMUR-SQL/target/debug/murmur-server MURMUR_ENGINE_INTEGRATION=1 ./tests-live/run.sh hostile-schema
```
