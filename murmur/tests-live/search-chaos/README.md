# search-chaos (new Go scenario, no Rust original)

## What it proves

FTS and vector column-search indexes replicate across nodes and keep
answering identical queries through crash and partition chaos:

- Node 0 is the schema master and performs every schema write;
  followers adopt only the master's manifests.
- Enabling `SetColumnFts` on the master propagates through schema
  adoption: every node builds a ready FTS index and `MATCH` queries agree.
- Enabling `SetColumnVector` on the master propagates the same way and
  KNN queries (`embedding MATCH ? AND k=? ORDER BY distance`) agree.
- After a node crashes while survivors insert rows its indexes never see,
  plus a peering flap mid-write, the restarted node catches up rows and
  index state: every index is ready with no recorded error and every FTS
  and KNN expectation agrees on all nodes.

Query shapes mirror the Go client `fts`/`vectors` suites (temp index
table joined back to the source table on `rowid`).

Schema sync is data-triggered (a peer requests its sender's schema when a
batch arrives with an unknown schema digest, or at session start), so each
flag change is followed by a marker write carrying the new digest.

## Pass criteria

- 3-node cluster opens meshed under `tests-live/runtime/search-chaos/`.
- Baseline rows converge on all nodes before any index exists.
- FTS enabled on node 0 only: all nodes report exactly that index ready;
  `MATCH 'quick'` returns `[1 2]` everywhere.
- Vectors enabled on node 1 only: all nodes report both indexes ready;
  KNN `[0,0] k=3` returns `[1 2 3]` everywhere.
- Post-chaos: all nodes hold all rows, both indexes ready with nil error,
  and every query expectation agrees again.

## Env knobs

None scenario-specific. Harness knobs apply: `MURMUR_LIVE_RUNTIME`,
`MURMUR_LIVE_FAILURES`, `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
`MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`, `MURMUR_TEST_DEADLINE_SCALE`.

## Deviations from the Rust original

None: new scenario. The restarted node gets one `RefreshIndexes` nudge
before the post-chaos index wait; the nudge only hurries the normal tick
rebuild, it must not be required for correctness.

The suite deliberately uses a schema master: in masterless mode a node
can adopt a peer's stale manifest over its own newer schema change,
because manifest sequences are per-signer and cannot order schemas
across signers. The regression is silent and durable (the adoption sends
a storage event), and nothing re-requests afterwards, so a committed
`SetColumnVector` vanished cluster-wide in testing. Single-writer
discipline alone does not close the hole; only the schema master (which
never adopts) does. Masterless stale-adoption safety would need
cross-signer schema versioning and is out of scope for this suite.
