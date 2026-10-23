# tls-floor (Go port)

Rust source: `/home/marc/MURMUR/tests-live/tls-floor/`
(`tls_floor.rs`: `quic_floor_rejects_below_tls13`).

## What it proves

The product has no HTTPS API, so the floor is proven on the QUIC
replication endpoint, whose floor is TLS 1.3: a TLS-1.2-only handshake
must fail, TLS 1.3 succeeds with the negotiated version asserted, and a
2-node mesh converges over it.

## Pass criteria

- Marker converges on both nodes within 60 s.
- TLS 1.3 dial completes handshake + stream open within 10 s / 5 s, and
  the negotiated TLS version reads back as exactly 1.3.
- A TLS-1.2-only dial fails (a successful dial fails the floor as
  violated); dialability itself is proven by the positive leg, so any
  failure is version-caused.
- The rejected handshake attaches no session (mesh peer count intact).

## Env knobs

None scenario-specific. Standard harness knobs apply:
`MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
`MURMUR_LIVE_UNLOCK_STALL_SECONDS`, `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.

## Deviations (inherited from Rust)

- In-process multi-`Database` cluster instead of daemon processes.
- Plain SQL (`live_typed_records(name TEXT NOT NULL PRIMARY KEY)`).
- The Go port asserts the negotiated TLS version directly from the
  QUIC connection state (quic-go exposes it); the Rust port could not
  (quinn exposes no negotiated-version introspection).
