# SQL parameter binding and errors

Insert SQL injection strings, quotes, Unicode, and retired HTTP-route text as bound data. Verify exact replicated values, unchanged application tables, SQLSTATE 23505 for a duplicate key, and a usable connection after a SQL error.

Each node is a separate `murmur-server` process. Application operations use
the `goharness` SQL client over the PostgreSQL wire protocol;
these tests do not open an embedded database.

Run from the repository root:

```sh
MURMUR_ENGINE_INTEGRATION=1 ./tests-live/run.sh sqli-api
```

The process harness builds `murmur-server` and `murmur-admin` with the
`replication` feature, or reuses `MURMUR_SERVER_BIN` and `MURMUR_ADMIN_BIN`
(the admin binary defaults to a sibling of the server). `MURMUR_LIVE_RUNTIME`
selects the parent directory for unique per-test runtime folders. Successful
tests remove their runtime folders; failures retain the config and server logs
at the path printed in the test output.

SQL is now a public interface. The former expectation that SQL HTTP routes return 410 does not apply to the PostgreSQL wire server.
