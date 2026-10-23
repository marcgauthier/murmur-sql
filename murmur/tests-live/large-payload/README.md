# large-payload (Go port)

Rust source: `/home/marc/MURMUR/tests-live/large-payload/`
(`large_payload.rs`).

## What it proves

Two encrypted, mutually authenticated nodes form a mesh. Node 1 commits
250 small rows plus one row carrying a 1.5 MiB text value (each in its own
transaction); node 2 must replicate all 251 rows with the payload's exact
length and content.

## Pass criteria (same as Rust)

- `NewCluster` opens 2 meshed nodes under `tests-live/runtime/large-payload/`.
- Node 1 inserts `bulk-0000`..`bulk-0249` plus the `large-payload` row with
  a 1,572,864-byte value (`"murmur-large-payload-"` repeated, `'x'`-padded).
- Within 60 s (50 ms poll), node 2 holds 251 rows and the large row's value
  has exactly 1,572,864 bytes identical to the committed payload.

## Env knobs

None scenario-specific. Harness knobs apply: `MURMUR_LIVE_RUNTIME`,
`MURMUR_LIVE_FAILURES`, `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
`MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.

## Deviations from the Rust original

- In-process multi-`Database` cluster, same as Rust.
- Plain SQL (`records(id TEXT NOT NULL PRIMARY KEY, name TEXT, phone TEXT)`);
  the bulk-row score travels in the `phone` column as text.
- Fixed row ids (`bulk-%04d`, `large-payload-row`).
- Exact byte-equality assertion on the replicated payload instead of a
  digest comparison; strictly stronger and needs no extra dependency.
