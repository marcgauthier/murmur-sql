# swim-discovery

Four server processes discover a full mesh from a single bootstrap seed.
Both tests check SQL peer/member status and exact replicated rows. The second
kills the seed process, writes on survivors, restarts the same data directory,
and requires rediscovery and exact row convergence.

`MURMUR_SWIM_DISCOVERY_SECONDS` defaults to 60 seconds.

All database operations use explicit SQL through `clients/rust` against live
`murmur-server` child processes. Schema setup explicitly enables table
replication; missing settings never implicitly enable it. Peer topology uses
`MURMUR SHOW PEERS`; scheduler, persistence, and replication counters use
`MURMUR SHOW STATUS`. No embedded database handles are linked or opened.

Run from the repository root:

```sh
tests-live/run.sh swim-discovery
```

Use `MURMUR_SERVER_BIN` (and optionally `MURMUR_ADMIN_BIN`) to reuse prebuilt
replication-enabled binaries. Success prints `RESULT: PASS`; failure retains
runtime artifacts for inspection. Full-duration runs are required for soak
qualification; a compile check or short smoke run does not establish it.
