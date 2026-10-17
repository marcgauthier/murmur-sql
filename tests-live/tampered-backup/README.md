# Corrupt streaming backups fail closed

Create a thirty-row backup and corrupt its header, flip a payload byte, or truncate it. Upload/restore must reject each bad archive and a fresh client must still read the untouched live data. The pristine archive must restore the exact original rows.

Each node is a separate `murmur-server` process. Application operations use
[`murmur-client`](../../clients/rust/README.md) over the PostgreSQL wire protocol;
these tests do not open an embedded database.

Run from the repository root:

```sh
CARGO_TARGET_DIR="$PWD/target" cargo test --manifest-path tests-live/tampered-backup/Cargo.toml -- --nocapture
```

The process harness builds `murmur-server` and `murmur-admin` with the
`replication` feature, or reuses `MURMUR_SERVER_BIN` and `MURMUR_ADMIN_BIN`
(the admin binary defaults to a sibling of the server). `MURMUR_LIVE_RUNTIME`
selects the parent directory for unique per-test runtime folders. Successful
tests remove their runtime folders; failures retain the config and server logs
at the path printed in the test output.

Malformed envelopes may be rejected during upload; accepted uploads must fail restore. This tests the SQL archive API rather than calling embedded backup verification.
