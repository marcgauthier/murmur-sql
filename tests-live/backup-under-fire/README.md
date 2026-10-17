# Online backups during SQL transactions

A separate SQL connection commits forty atomic five-row INSERT statements while the driver saves and streams four backups. Restore each archive and require a nondecreasing exact committed prefix of the acknowledged records; no archive may split an atomic statement.

Each node is a separate `murmur-server` process. Application operations use
[`murmur-client`](../../clients/rust/README.md) over the PostgreSQL wire protocol;
these tests do not open an embedded database.

Run from the repository root:

```sh
CARGO_TARGET_DIR="$PWD/target" cargo test --manifest-path tests-live/backup-under-fire/Cargo.toml -- --nocapture
```

The process harness builds `murmur-server` and `murmur-admin` with the
`replication` feature, or reuses `MURMUR_SERVER_BIN` and `MURMUR_ADMIN_BIN`
(the admin binary defaults to a sibling of the server). `MURMUR_LIVE_RUNTIME`
selects the parent directory for unique per-test runtime folders. Successful
tests remove their runtime folders; failures retain the config and server logs
at the path printed in the test output.

The replication transport is disabled for restore validation. Transaction-control words are no-ops; each INSERT statement is atomic. This tests snapshot consistency under concurrent writes, without a throughput or latency SLO.
