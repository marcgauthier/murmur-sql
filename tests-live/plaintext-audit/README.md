# Plaintext audit of SQL artifacts

Replicate confidential markers between two SQL servers, create a backup, flush and stop both processes, and scan all scenario artifacts for plaintext markers and the database unlock key. Restart both and verify the exact rows.

Each node is a separate `murmur-server` process. Application operations use
[`murmur-client`](../../clients/rust/README.md) over the PostgreSQL wire protocol;
these tests do not open an embedded database.

Run from the repository root:

```sh
CARGO_TARGET_DIR="$PWD/target" cargo test --manifest-path tests-live/plaintext-audit/Cargo.toml -- --nocapture
```

The process harness builds `murmur-server` and `murmur-admin` with the
`replication` feature, or reuses `MURMUR_SERVER_BIN` and `MURMUR_ADMIN_BIN`
(the admin binary defaults to a sibling of the server). `MURMUR_LIVE_RUNTIME`
selects the parent directory for unique per-test runtime folders. Successful
tests remove their runtime folders; failures retain the config and server logs
at the path printed in the test output.

The audit includes logs, storage and backup archives. The scan detects exact byte sequences; replication signing keys and test TLS keys are configuration credentials and are not treated as database unlock keys.
