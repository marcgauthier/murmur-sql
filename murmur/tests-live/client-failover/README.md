# client-failover (new Go scenario, no Rust original)

## What it proves

SQL clients fail over across server outages with no acknowledged write
lost, against real `server.Server` + `engine/native` instances:

- Two QUIC-replicated in-process servers sit behind one TCP proxy each; a
  minimal PostgreSQL-wire client (startup, cleartext auth, simple query)
  drives the workload.
- Baseline: rows written through proxy 0 converge to server 1.
- Flap leg: proxy 0 goes down while server 0 is untouched (fresh dials
  must refuse, proving a non-vacuous outage); the client reconnects
  through proxy 1 and every acknowledged id is visible there.
- Restart leg: server 0 shuts down fully (engine closed) and a fresh
  `Server` reopens the same data dir; after the mesh reheals the client
  fails back and the full id set converges on both servers.

Statements that error in flight during an outage are abandoned with their
id range (they may or may not have committed); only acknowledged ids are
asserted. Client-library-specific retry behavior stays with the Go/Python/
Rust client suites; this suite pins the server side (availability across
frontends plus replication durability of acked writes).

## Pass criteria

- Both servers start, unlock, create + opt-in the table, and mesh both
  directions (SHOW PEERS connected).
- 20 baseline ids converge on the standby.
- After the proxy flap, reconnect succeeds and all acked ids are visible
  on the surviving server; 20 more ids commit there.
- After the full restart + failback, all 60 acked ids converge on both
  servers.

## Env knobs

None scenario-specific. Harness knobs apply: `MURMUR_LIVE_RUNTIME`,
`MURMUR_LIVE_FAILURES`, `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
`MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`, `MURMUR_TEST_DEADLINE_SCALE`.

## Deviations from the Rust original

None: new scenario.
