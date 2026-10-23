# dos-client (Go port, QUIC surface only)

Rust source: `/home/marc/MURMUR/tests-live/dos-client/`
(`dos_client.rs`, `TestDoSClientFlood`).

## What it proves

While an attacker sprays garbage UDP and holds valid-credential QUIC
half-opens (stream opened, Hello never sent) against the victim's
replication port, the node keeps serving its legit peer (writes
converge both ways within latency bounds) and attacker state stays
bounded (garbage creates no sessions, half-opens never attach as
replication peers, peer counts recover after teardown).

## Pass criteria (same as Rust, for the ported surface)

- Pre-attack: both directions converge within 30 s; peer counts settle.
- Garbage phase: 5 heartbeat writes converge (7 rows, 30 s); peer
  count unmoved (junk created no server state).
- Flood (`MURMUR_DOS_CLIENT_ATTACK_SECONDS`, default 60;
  `MURMUR_DOS_CLIENT_QUIC_HALFOPEN`, default 24): attacker must hold
  half the attempted half-opens within 30 s or the flood is too weak
  to prove anything; a victim write lands in under 10 s; peers stay
  within baseline + 2; a peer write converges (9 rows) before the
  deadline; the flood holds for the full duration.
- Teardown: peers recover to 1 within 30 s and within baseline + 2.
- Post-attack: one write converges (10 rows) within 15 s with equal
  digests.

## Not ported (no HTTPS target)

- All HTTPS flood vectors (TCP half-opens, partial ClientHellos,
  TLS-idle, slow-loris, certified trickle bodies) and their knobs.
- Phase 2 protocol-floor checks (401 on unauthenticated exec, TLS 1.0
  refusal): QUIC pins TLS 1.3 structurally, and a sub-1.3 QUIC dial is
  inexpressible in quic-go.
- Healthz/legit-query latency legs and goroutine counting (no HTTP
  endpoints, no stacks endpoint); pressure is proven attacker-side via
  held half-opens instead.

## Deviations (inherited from Rust)

- In-process two-`Database` cluster; plain SQL replaces the
  typed-contention HTTP API (`contention(id, name)`).
- Live peer counts (`Status().Peers`) replace the sessions-opened
  counter and `/v1/status` connected-peers reads.
- The QUIC attacker is a real quic-go endpoint in-test, presenting
  the victim's own node certificate from its `tls/` dir with the
  replication ALPN — the stolen-credential model.
- Deterministic xorshift64* replaces `math/rand` (no new dependencies
  for the garbage spray).
