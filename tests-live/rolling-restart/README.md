# rolling-restart

Two tests perform three rolling restarts and six churn restarts respectively.
Acknowledged baseline rows are flushed before each real server restart. Writers
on both surviving nodes continue issuing SQL while the stopped node restarts;
every acknowledged row must converge on all three nodes after each round.

All database operations use explicit SQL through `clients/rust` against live
`murmur-server` child processes. Schema setup explicitly enables table
replication; missing settings never implicitly enable it. Peer topology uses
`MURMUR SHOW PEERS`; scheduler, persistence, and replication counters use
`MURMUR SHOW STATUS`. No embedded database handles are linked or opened.

Run from the repository root:

```sh
tests-live/run.sh rolling-restart
```

Use `MURMUR_SERVER_BIN` (and optionally `MURMUR_ADMIN_BIN`) to reuse prebuilt
replication-enabled binaries. Success prints `RESULT: PASS`; failure retains
runtime artifacts for inspection. Full-duration runs are required for soak
qualification; a compile check or short smoke run does not establish it.
