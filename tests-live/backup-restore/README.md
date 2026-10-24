# Streaming SQL backup and restore

Save, list, download, upload, and restore an archive through the Rust client. Verify SQL types, values, generated keys, table replication flags, and durability after restart. A second test rejects an incorrect restore key and a foreign writer identity while preserving live data.

Each node is a separate `murmur-server` process. Application operations use
the `goharness` SQL client over the PostgreSQL wire protocol;
these tests do not open an embedded database.

Run from the repository root:

```sh
MURMUR_ENGINE_INTEGRATION=1 ./tests-live/run.sh backup-restore
```

The process harness builds `murmur-server` and `murmur-admin` with the
`replication` feature, or reuses `MURMUR_SERVER_BIN` and `MURMUR_ADMIN_BIN`
(the admin binary defaults to a sibling of the server). `MURMUR_LIVE_RUNTIME`
selects the parent directory for unique per-test runtime folders. Successful
tests remove their runtime folders; failures retain the config and server logs
at the path printed in the test output.

Backup restore tests run with the replication transport disabled; they still exercise persisted table replication settings. The SQL restore API preserves writer identity and does not expose the embedded clone/identity-rewrite API.
