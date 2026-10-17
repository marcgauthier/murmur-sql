# SQL mutual TLS

Starts a real SQL server, writes a control row, then restarts it in production mode with TLS and a trusted client CA. The Rust client must reject plaintext, missing client certificates and an untrusted certificate. A trusted certificate still requires the correct password and encryption key. Valid writes must remain available after all rejection probes and after another restart.

Application operations use the [Rust client](../../clients/rust/README.md)
over PostgreSQL wire connections to separate `murmur-server` processes.
No embedded database is opened by these tests.

Run from the repository root:

```sh
CARGO_TARGET_DIR="$PWD/target" cargo test --manifest-path tests-live/api-mtls/Cargo.toml -- --nocapture
```

Production SQL mTLS is tested here. Replication-peer certificate admission is covered separately. The client verifies the server chain and loopback IP name.

The process harness builds `murmur-server` and `murmur-admin` with replication,
or reuses `MURMUR_SERVER_BIN` and its sibling admin executable
(`MURMUR_ADMIN_BIN` overrides the latter). `MURMUR_LIVE_RUNTIME` selects
the artifact parent. Each test receives a unique runtime directory; passing
tests clean up, while panics retain configuration and server logs.
