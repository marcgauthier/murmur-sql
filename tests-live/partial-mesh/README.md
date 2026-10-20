# partial-mesh

Two tests verify forwarding along a three-node chain with exact peer counts
(1, 2, 1) and no direct endpoint link, and automatic seed-based discovery into a
full mesh. SQL inserts from multiple origins must converge to exact ordered rows.

All database operations use explicit SQL through `clients/rust` against live
`murmur-server` child processes. Schema setup explicitly enables table
replication; missing settings never implicitly enable it. Peer topology uses
`MURMUR SHOW PEERS`; scheduler, persistence, and replication counters use
`MURMUR SHOW STATUS`. No embedded database handles are linked or opened.

Run from the repository root:

```sh
tests-live/run.sh partial-mesh
```

Use `MURMUR_SERVER_BIN` (and optionally `MURMUR_ADMIN_BIN`) to reuse prebuilt
replication-enabled binaries. Success prints `RESULT: PASS`; failure retains
runtime artifacts for inspection. Full-duration runs are required for soak
qualification; a compile check or short smoke run does not establish it.
