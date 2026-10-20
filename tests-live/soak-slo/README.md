# soak-slo

Three concurrent SQL writers use bounded ranges of 1,000 rows per node,
then update their own rows. The test checks exact final values, at least 60
writes, write p95 at most 5 seconds and maximum at most 20 seconds, staged
transactions at most 5,000, a 512 MiB Spool budget, and no storage error.

`MURMUR_SLO_DURATION_SECONDS` defaults to 5 seconds; the soak runner defaults
to 7,200 seconds. `MURMUR_SLO_SETTLE_SECONDS` defaults to 30 seconds.

All database operations use explicit SQL through `clients/rust` against live
`murmur-server` child processes. Schema setup explicitly enables table
replication; missing settings never implicitly enable it. Peer topology uses
`MURMUR SHOW PEERS`; scheduler, persistence, and replication counters use
`MURMUR SHOW STATUS`. No embedded database handles are linked or opened.

Run from the repository root:

```sh
tests-live/run.sh soak-slo
```

Use `MURMUR_SERVER_BIN` (and optionally `MURMUR_ADMIN_BIN`) to reuse prebuilt
replication-enabled binaries. Success prints `RESULT: PASS`; failure retains
runtime artifacts for inspection. Full-duration runs are required for soak
qualification; a compile check or short smoke run does not establish it.
