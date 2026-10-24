# Encrypted SQL persistence

Insert a confidential marker through the Rust SQL client, flush, kill the server, and scan its durable files for plaintext. Restart and require SQLSTATE 28000 for a wrong unlock key while the correct key still reads the original row.

Each node is a separate `murmur-server` process. Application operations use
the `goharness` SQL client over the PostgreSQL wire protocol;
these tests do not open an embedded database.

Run from the repository root:

```sh
MURMUR_ENGINE_INTEGRATION=1 ./tests-live/run.sh encryption
```

The process harness builds `murmur-server` and `murmur-admin` with the
`replication` feature, or reuses `MURMUR_SERVER_BIN` and `MURMUR_ADMIN_BIN`
(the admin binary defaults to a sibling of the server). `MURMUR_LIVE_RUNTIME`
selects the parent directory for unique per-test runtime folders. Successful
tests remove their runtime folders; failures retain the config and server logs
at the path printed in the test output.

The disk scan checks exact plaintext byte sequences. It does not prove general cryptographic security.
