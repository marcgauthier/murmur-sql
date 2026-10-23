# api-mtls (Go port, test-side equivalent)

Rust source: `/home/marc/MURMUR/tests-live/api-mtls/`
(`api_mtls.rs`, `tls_endpoint_requires_mutual_auth`).

## What it proves

The product has no HTTPS API server, so this port proves the TLS policy on
the product's only TLS server — the QUIC replication endpoint, which
verifies client certificates against the cluster CA at handshake:
trusted client certs complete handshakes and open streams, missing or
foreign-CA certs are rejected at handshake, plaintext is refused, and
rejected handshakes attach no sessions.

## Leg mapping (Rust -> Go, unchanged)

- Trusted cert handshake + stream open within 10 s / 5 s.
- Certless and foreign-CA handshakes fail within 10 s each (observed as an
  async close: TLS 1.3 finishes client-side before the server validates the
  certificate flight).
- Zero sessions attach from any rejected handshake or garbage burst.
- Plaintext TCP refused; UDP garbage attaches no session.
- Expired client cert: NOT portable (minting one needs the cluster CA
  key, which the harness never exposes).
- CLI status legs: NOT portable (`murmur-admin` is an offline CLI with
  no API client).

## Env knobs

None scenario-specific. Harness knobs apply: `MURMUR_LIVE_RUNTIME`,
`MURMUR_LIVE_FAILURES`, `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
`MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.

## Deviations from the Rust original

- In-process single-`Database` cluster, same as Rust.
- Attacker endpoints use `quic-go` with `crypto/tls` client configs
  (client chain optional) instead of `quinn` + `rustls`; server
  verification mirrors the product client (roots pinned, server name
  ignored, leaf SAN node id parsed).
- The rogue CA is minted in-test via `harness.GenerateCa`.
