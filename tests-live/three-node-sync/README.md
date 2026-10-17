# Concurrent SQL writers

Three real servers each insert 30 disjoint rows using bound Rust-client parameters. The test waits for all 90 ordered names on every server and compares the generated replication keys.

Each node is a separate `murmur-server` process. Application operations use
[`murmur-client`](../../clients/rust/README.md) over the PostgreSQL wire protocol;
these tests do not open an embedded database.

Run from the repository root:

```sh
CARGO_TARGET_DIR="$PWD/target" cargo test --manifest-path tests-live/three-node-sync/Cargo.toml -- --nocapture
```

The process harness builds `murmur-server` and `murmur-admin` with the
`replication` feature, or reuses `MURMUR_SERVER_BIN` and `MURMUR_ADMIN_BIN`
(the admin binary defaults to a sibling of the server). `MURMUR_LIVE_RUNTIME`
selects the parent directory for unique per-test runtime folders. Successful
tests remove their runtime folders; failures retain the config and server logs
at the path printed in the test output.

Three-node convergence includes exact names and identical managed keys; no row-count-only assertion.
