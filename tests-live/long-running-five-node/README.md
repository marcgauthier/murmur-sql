# long-running-five-node

Five SQL writers run while two server processes restart at one-third and
two-thirds of the requested duration. Per-node connection slots keep durable
acknowledgements synchronized with restarts; every acknowledged row is checked
after recovery and final convergence. The test requires at least 10 writes per
node, 100 total, a 2 GiB per-node Spool budget, no storage error, and a
successfully replicated post-soak write.

`MURMUR_FIVE_NODE_DURATION_SECONDS` defaults to 30 (minimum 3),
`MURMUR_FIVE_NODE_INTERVAL_MS` to 50, and
`MURMUR_FIVE_NODE_SETTLE_SECONDS` to 120. The soak runner requests 3,600 seconds.
Short runs require an appropriately short write interval to meet progress gates.

All database operations use explicit SQL through the `goharness` SQL client against live
`murmur-server` child processes. Schema setup explicitly enables table
replication; missing settings never implicitly enable it. Peer topology uses
`MURMUR SHOW PEERS`; scheduler, persistence, and replication counters use
`MURMUR SHOW STATUS`. No embedded database handles are linked or opened.

Run from the repository root:

```sh
tests-live/run.sh long-running-five-node
```

Use `MURMUR_SERVER_BIN` (and optionally `MURMUR_ADMIN_BIN`) to reuse prebuilt
replication-enabled binaries. Success prints `RESULT: PASS`; failure retains
runtime artifacts for inspection. Full-duration runs are required for soak
qualification; a compile check or short smoke run does not establish it.
