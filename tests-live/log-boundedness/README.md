# Bounded SQL replication history

Runs alternating acknowledged SQL writes while aggressive background retention operates. Samples committed and collected transaction cursors and data-directory bytes at the start, midpoint and end. Requires actual collection on both servers, the legacy difference of produced and collected counters at most 512 + produced/4, disk growth at most 8 KiB per row, bounded second-half acceleration, unchanged replication errors, and exact ordered convergence.

Application operations use the [`goharness` SQL client](../goharness/)
over PostgreSQL wire connections to separate `murmur-server` processes.
No embedded database is opened by these tests.

Run from the repository root:

```sh
MURMUR_ENGINE_INTEGRATION=1 ./tests-live/run.sh log-boundedness
```

MURMUR_LOG_BOUNDEDNESS_SECONDS defaults to 75 and accepts a positive integer. At least 100 rows must be acknowledged and the midpoint must be sampled. Replication uses one explicitly established session, membership discovery disabled, one-second retention/collection intervals, and ten minimum retained log records. A replicated control row proves the session is ready before measurement, so bootstrap retry errors cannot contaminate the load window. Uses the committed cursor appended to SHOW REPLICATION and the server collection counter. These count engine batches and logical history transactions respectively, so their difference is a diagnostic estimate under batching, not an authoritative retained-log count. Actual collection, physical disk-growth limits and exact SQL convergence are separate hard assertions.

The process harness builds `murmur-server` and `murmur-admin` with replication,
or reuses `MURMUR_SERVER_BIN` and its sibling admin executable
(`MURMUR_ADMIN_BIN` overrides the latter). `MURMUR_LIVE_RUNTIME` selects
the artifact parent. Each test receives a unique runtime directory; passing
tests clean up, while panics retain configuration and server logs.
