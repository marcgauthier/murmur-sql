# rejoin-storm

Two of three server processes are killed after baseline convergence. The
survivor commits and flushes 30 more SQL rows. Both stopped processes restart
concurrently, rejoin the mesh, and must recover every acknowledged row. SQL
status must show received replication traffic and no final replication error.

All database operations use explicit SQL through the `goharness` SQL client against live
`murmur-server` child processes. Schema setup explicitly enables table
replication; missing settings never implicitly enable it. Peer topology uses
`MURMUR SHOW PEERS`; scheduler, persistence, and replication counters use
`MURMUR SHOW STATUS`. No embedded database handles are linked or opened.

Run from the repository root:

```sh
tests-live/run.sh rejoin-storm
```

Use `MURMUR_SERVER_BIN` (and optionally `MURMUR_ADMIN_BIN`) to reuse prebuilt
replication-enabled binaries. Success prints `RESULT: PASS`; failure retains
runtime artifacts for inspection. Full-duration runs are required for soak
qualification; a compile check or short smoke run does not establish it.
