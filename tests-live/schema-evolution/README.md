# Additive SQL schema evolution

Add a nullable column while one of three servers is offline, write the new column, and restart the offline server. Verify new schema and exact data, persistent replication flags, rejection of replicated table/column drops and renames, and permitted destructive DDL on a local table.

Each node is a separate `murmur-server` process. Application operations use
the `goharness` SQL client over the PostgreSQL wire protocol;
these tests do not open an embedded database.

Run from the repository root:

```sh
MURMUR_ENGINE_INTEGRATION=1 ./tests-live/run.sh schema-evolution
```

The process harness builds `murmur-server` and `murmur-admin` with the
`replication` feature, or reuses `MURMUR_SERVER_BIN` and `MURMUR_ADMIN_BIN`
(the admin binary defaults to a sibling of the server). `MURMUR_LIVE_RUNTIME`
selects the parent directory for unique per-test runtime folders. Successful
tests remove their runtime folders; failures retain the config and server logs
at the path printed in the test output.

This scenario covers additive column adoption. Replicated table and column drops/renames are rejected by design.
