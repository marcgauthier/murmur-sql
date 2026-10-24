# scale-mesh

Ten live servers converge on 400 rows from two origins (300 on node 1,
100 on node 8). A concurrent SQL peer sampler checks that connected push-mode
peers never exceed fanout 4. Every node must hold identical ordered row values;
the convergence deadline is 300 seconds.

All database operations use explicit SQL through the `goharness` SQL client against live
`murmur-server` child processes. Schema setup explicitly enables table
replication; missing settings never implicitly enable it. Peer topology uses
`MURMUR SHOW PEERS`; scheduler, persistence, and replication counters use
`MURMUR SHOW STATUS`. No embedded database handles are linked or opened.

Run from the repository root:

```sh
tests-live/run.sh scale-mesh
```

Use `MURMUR_SERVER_BIN` (and optionally `MURMUR_ADMIN_BIN`) to reuse prebuilt
replication-enabled binaries. Success prints `RESULT: PASS`; failure retains
runtime artifacts for inspection. Full-duration runs are required for soak
qualification; a compile check or short smoke run does not establish it.
