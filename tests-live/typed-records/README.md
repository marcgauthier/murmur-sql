# Typed SQL records, keys, transactions, and files

Checks bound text/real/blob/NULL values across replication and restart, remote updates and deletes, statement atomicity and documented transaction-control no-ops, local-only row persistence, populated composite-key replication transitions, immutable/non-null application keys, stable generated keys, streamed encrypted-file replication/download after restart, partitioned counter/set/min/max operations, concurrent additive schema branches, and empty-peer catch-up through a pruned-history snapshot.

Each node is a separate `murmur-server` process. Application operations use
[`murmur-client`](../../clients/rust/README.md) over the PostgreSQL wire protocol;
these tests do not open an embedded database.

Run from the repository root:

```sh
CARGO_TARGET_DIR="$PWD/target" cargo test --manifest-path tests-live/typed-records/Cargo.toml -- --nocapture
```

The process harness builds `murmur-server` and `murmur-admin` with the
`replication` feature, or reuses `MURMUR_SERVER_BIN` and `MURMUR_ADMIN_BIN`
(the admin binary defaults to a sibling of the server). `MURMUR_LIVE_RUNTIME`
selects the parent directory for unique per-test runtime folders. Successful
tests remove their runtime folders; failures retain the config and server logs
at the path printed in the test output.

Application records use SQL and the Rust client. Primary-key generation happens at the managed SQL boundary. The file case enables the configured encrypted file store and verifies exact 150,000-byte content.

The CRDT case configures `replication.merge_policies` and uses `MURMUR POLICY`
through the Rust client. It verifies an exact counter sum, min/max values,
observed set removal, duplicate set additions, and restart durability. Policy
commands currently address one non-null TEXT application-key column; direct
assignment to policy columns must fail. The expected materialized set is built
through public policy operations, without duplicating the engine's encoding.

The snapshot case trusts only the designated donor, prunes older source logs
with `MURMUR RETENTION COLLECT`, and requires exact rows and generated keys
without advancing the receiver's applied-log counter. It then restarts the
receiver to verify the snapshot was persisted.

Bare SQL transaction-control words are compatibility no-ops: `ROLLBACK` cannot
undo an earlier statement. A failed multi-row INSERT must still roll back that
entire statement, including replication and persistence capture.
