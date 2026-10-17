# SQL storage and backup permissions

Checks product-created data directories and files after initialization, after SQL writes, and after streaming SQL backup/restore and restart. Every audited POSIX path must have zero group/other permission bits. The encrypted key registry must exist, and all 25 control rows must survive restore and restart.

Application operations use the [Rust client](../../clients/rust/README.md)
over PostgreSQL wire connections to separate `murmur-server` processes.
No embedded database is opened by these tests.

Run from the repository root:

```sh
CARGO_TARGET_DIR="$PWD/target" cargo test --manifest-path tests-live/file-permissions/Cargo.toml -- --nocapture
```

Runs on Unix, where POSIX permission bits apply. Windows does not build this POSIX-mode assertion; this test does not validate NTFS ACLs. Operator-created configuration/log directories and fixture credentials are outside the product-data audit. The test requires SQL backup archives to be private too; it never hides a server permission bug by chmodding an archive.

The process harness builds `murmur-server` and `murmur-admin` with replication,
or reuses `MURMUR_SERVER_BIN` and its sibling admin executable
(`MURMUR_ADMIN_BIN` overrides the latter). `MURMUR_LIVE_RUNTIME` selects
the artifact parent. Each test receives a unique runtime directory; passing
tests clean up, while panics retain configuration and server logs.
