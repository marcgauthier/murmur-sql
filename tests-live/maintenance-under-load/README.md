# maintenance-under-load

Three SQL writers insert into bounded owned ranges and then update names
while background retention runs. Diagnostic counters must prove both retention
passes and actual pruning on every node while writes are active. After stopping
the writers, all four columns must converge exactly, including untouched phone
and score values. A post-maintenance SQL insert must still replicate.

`MURMUR_MAINT_LOAD_SEED` defaults to 120 rows,
`MURMUR_MAINT_LOAD_MAX_INSERTS` to 4,000 per writer, and
`MURMUR_MAINT_LOAD_GC_TIMEOUT` to 150 seconds (seconds or an s/m/h suffix).

All database operations use explicit SQL through the `goharness` SQL client against live
`murmur-server` child processes. Schema setup explicitly enables table
replication; missing settings never implicitly enable it. Peer topology uses
`MURMUR SHOW PEERS`; scheduler, persistence, and replication counters use
`MURMUR SHOW STATUS`. No embedded database handles are linked or opened.

Run from the repository root:

```sh
tests-live/run.sh maintenance-under-load
```

Use `MURMUR_SERVER_BIN` (and optionally `MURMUR_ADMIN_BIN`) to reuse prebuilt
replication-enabled binaries. Success prints `RESULT: PASS`; failure retains
runtime artifacts for inspection. Full-duration runs are required for soak
qualification; a compile check or short smoke run does not establish it.
