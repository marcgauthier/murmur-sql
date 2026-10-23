# scale-mesh (Go port)

Rust source: `/home/marc/MURMUR/tests-live/scale-mesh/`
(`scale_mesh.rs`: `TestScaleMesh`).

## What it proves

Ten encrypted, mutually authenticated nodes mesh with the default
fanout of 4 while two origins write 400 rows (300 on node 1, 100 on
node 8). Smaller suites never exceed the fanout, so this is the only
live proof that peer selection and rotation hold at scale: every node
must converge to identical row sets, and a 2 s sampler asserts no node
ever selects more than 4 replication targets for the whole run.

Pass criteria: 10 meshed nodes open under
`tests-live/runtime/scale-mesh/` with the default replication fanout
(4); node 1 inserts rows `0..300`, node 8 inserts rows `300..400` (one
transaction per row, ids `%032x`, names `w-{i}`); within 300 s (200 ms
poll), `SELECT COUNT(*) FROM records` on every node equals 400; the
PK-ordered `id:name` row vector on every node equals node 1's; every
2 s sample (plus one final sample after convergence) finds at most 4
connected push-mode peers per node; any breach fails the test.

## Env knobs

None scenario-specific. Standard harness knobs apply:
`MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
`MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
`MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.

## Deviations (inherited from Rust)

- In-process multi-`Database` cluster (QUIC mesh, ephemeral
  `127.0.0.1:0`) instead of daemon processes over HTTPS+mTLS.
- Plain SQL (`records(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT
  NULL)`) instead of the managed typed-contention API.
- The fanout sampler counts connected peers with `PushMode` in the
  in-process replication `Status` instead of polling `/v1/status`
  `selected_peers`.
- The sampler is a goroutine with an `atomic.Bool` stop flag and a
  bounded buffered channel (non-blocking send drops on full).
- Canonical row vectors are compared directly instead of comparing
  SHA-256 digests.
