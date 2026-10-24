# chaos-load

Three SQL writers run while one node is partitioned from the other pair.
The test requires actual divergent row sets before healing while writers are
still active, then exact convergence of all 240 acknowledged rows. A post-heal
write must also replicate successfully. Membership is disabled so it cannot
silently heal the intentional split.

All database operations use explicit SQL through the `goharness` SQL client against live
`murmur-server` child processes. Schema setup explicitly enables table
replication; missing settings never implicitly enable it. Peer topology uses
`MURMUR SHOW PEERS`; scheduler, persistence, and replication counters use
`MURMUR SHOW STATUS`. No embedded database handles are linked or opened.

Run from the repository root:

```sh
tests-live/run.sh chaos-load
```

Use `MURMUR_SERVER_BIN` (and optionally `MURMUR_ADMIN_BIN`) to reuse prebuilt
replication-enabled binaries. Success prints `RESULT: PASS`; failure retains
runtime artifacts for inspection. Full-duration runs are required for soak
qualification; a compile check or short smoke run does not establish it.
