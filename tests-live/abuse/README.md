# SQL mesh abuse across four phases

Continuous Rust-client writers exercise a healthy mesh, a two-sided partition, a partial-node outage and a whole-mesh kill/restart storm. Before a server is killed, its writer slots are drained through SQL FLUSH; surviving servers keep accepting writes during the partial outage. The verdict compares every ordered application column on every node against the exact acknowledged-row ledger.

Application operations use the [Rust client](../../clients/rust/README.md)
over PostgreSQL wire connections to separate `murmur-server` processes.
No embedded database is opened by these tests.

Run from the repository root:

```sh
CARGO_TARGET_DIR="$PWD/target" cargo test --manifest-path tests-live/abuse/Cargo.toml -- --nocapture
```

Defaults: MURMUR_ABUSE_NODES=10 (clamped to 3..256), MURMUR_ABUSE_DURATION_SECONDS=600, MURMUR_ABUSE_SETTLE_SECONDS=900, MURMUR_ABUSE_WRITE_INTERVAL_MS=100, MURMUR_ABUSE_SEED=1, and MURMUR_ABUSE_STATUS_SECONDS=15. Positive duration/pacing values are required. Four phases share the requested load duration; readiness and convergence add time. The seed selects a reproducible rotating target schedule. Partitions use live PEER REMOVE/ADD commands with discovery disabled; they do not simulate kernel-level packet loss.

The process harness builds `murmur-server` and `murmur-admin` with replication,
or reuses `MURMUR_SERVER_BIN` and its sibling admin executable
(`MURMUR_ADMIN_BIN` overrides the latter). `MURMUR_LIVE_RUNTIME` selects
the artifact parent. Each test receives a unique runtime directory; passing
tests clean up, while panics retain configuration and server logs.
