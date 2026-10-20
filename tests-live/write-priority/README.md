# write-priority

Concurrent SQL writers on both nodes compete with incoming replication while
reads continue. Each writer has an independent Rust-client connection and uses
atomic 25-row statements over a bounded owned range, updating those rows to
keep state bounded while maintaining engine contention. The test checks
minimum progress, then reads engine scheduler
counters through SQL: at least one second of dual-contention service must be
observed before enforcing the maximum 25% remote-service share. Every
acknowledged row must subsequently converge exactly.

`MURMUR_LIVE_WRITE_PRIORITY_SECONDS` defaults to 12 seconds;
`MURMUR_LIVE_WRITE_PRIORITY_SYNC_TIMEOUT_SECONDS` defaults to 120 seconds.

All database operations use explicit SQL through `clients/rust` against live
`murmur-server` child processes. Schema setup explicitly enables table
replication; missing settings never implicitly enable it. Peer topology uses
`MURMUR SHOW PEERS`; scheduler, persistence, and replication counters use
`MURMUR SHOW STATUS`. No embedded database handles are linked or opened.

Run from the repository root:

```sh
tests-live/run.sh write-priority
```

Use `MURMUR_SERVER_BIN` (and optionally `MURMUR_ADMIN_BIN`) to reuse prebuilt
replication-enabled binaries. Success prints `RESULT: PASS`; failure retains
runtime artifacts for inspection. Full-duration runs are required for soak
qualification; a compile check or short smoke run does not establish it.

Current SQL validation fails the remote-service share assertion. One run
measured 1.057 seconds of remote dual-contention service and zero local
dual-contention service. The assertion is retained; this scenario is not
currently a passing release check, and the cause requires investigation.
