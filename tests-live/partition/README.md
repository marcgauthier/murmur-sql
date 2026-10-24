# Four-server partition and healing

Start with four connected servers and a baseline row. Stop and reconfigure all servers into isolated pairs with membership discovery disabled, then write on both sides. Verify each pair has exactly its own rows, restore the full topology, and require complete ordered convergence plus a post-heal write.

Each node is a separate `murmur-server` process. Application operations use
the `goharness` SQL client over the PostgreSQL wire protocol;
these tests do not open an embedded database.

Run from the repository root:

```sh
MURMUR_ENGINE_INTEGRATION=1 ./tests-live/run.sh partition
```

The process harness builds `murmur-server` and `murmur-admin` with the
`replication` feature, or reuses `MURMUR_SERVER_BIN` and `MURMUR_ADMIN_BIN`
(the admin binary defaults to a sibling of the server). `MURMUR_LIVE_RUNTIME`
selects the parent directory for unique per-test runtime folders. Successful
tests remove their runtime folders; failures retain the config and server logs
at the path printed in the test output.

Topology changes use config rewrites and process restarts. This does not emulate packet loss on an otherwise running connection.
