# write-priority

Concurrent SQL writers on both nodes compete with incoming replication while
reads continue. Each writer has an independent Rust-client connection and uses
atomic 25-row statements over a bounded owned range, updating those rows to
keep state bounded while maintaining engine contention. The test checks
minimum progress, then reads engine scheduler counters through SQL: at
least one second of local-plus-remote writer service must be observed,
with replication performing nonzero apply work, before enforcing the
policy that replication takes at most 25% of total writer service
(90/10 shares plus burst slack). Total service — not contested-grant
counts — is the policy signal: replication applies each burst under a
few long-held tickets, so it stays below its share and wins every
contested grant while behind, keeping local dual grants near zero
even under real overlap. Every acknowledged row must subsequently
converge exactly.

`MURMUR_LIVE_WRITE_PRIORITY_SECONDS` defaults to 12 seconds;
`MURMUR_LIVE_WRITE_PRIORITY_SYNC_TIMEOUT_SECONDS` defaults to 120 seconds.

All database operations use explicit SQL through the `goharness` SQL client against live
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

This deviates from the Go original, which counts contested grants:
that proxy needs both classes persistently backlogged, which holds
for microsecond-commit daemons but not for burst-applying replication
(one run showed 0 local vs 94 remote dual grants while total shares
were a fair 95/5). An earlier port asserting on the dual-service
subset failed the same way and was replaced with total service.
