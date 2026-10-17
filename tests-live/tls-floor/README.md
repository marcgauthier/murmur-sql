# tls-floor (Rust port, test-side equivalent)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/tls-floor/`
(`tls_floor_test.go`, `TestAPIFloorRejectsBelowTLS12` +
`TestQUICFloorPinsTLS13`).

## What it proves

Go pins the HTTPS API floor at TLS 1.2 and separately guards the QUIC
floor at TLS 1.3. The server runs as a separate process (`murmur-server`)
exposing both PostgreSQL-wire SQL and a TLS 1.3 QUIC replication endpoint:
a TLS-1.2-only handshake is strictly rejected (RFC 9001 requires TLS 1.3),
while TLS 1.3 succeeds, and a 2-node SQL cluster converges over it.

## Leg mapping (Go -> Rust)

- Mesh positive control (marker over QUIC) is unchanged: 1 row, 60 s.
- TLS 1.1/1.0 API negatives become one TLS-1.2-only QUIC negative (the
  boundary probe): versions pinned to exactly 1.2 so the ClientHello
  genuinely emits it. rustls 0.23 removed TLS 1.0/1.1 entirely, so
  those versions are inexpressible — the 1.2 leg covers the floor.
- Server-side-rejection proof mirrors Go's `isRemoteRejection`: the
  failure must be a peer crypto close (0x100-0x1ff), not a local build
  error and not a bare timeout — else the test fails as unproven
  rather than passing vacuously. (Go's own quic-go stack gates such
  dials locally, which is why Go guards QUIC via constructors; quinn
  builds the config, so the live probe works here.)
- TLS 1.2 API positive becomes a TLS 1.3 QUIC positive (handshake +
  stream open). Go's negotiated-version readback has no analog: quinn
  exposes no negotiated-version introspection, so completing against
  a 1.3-only server is the proof.
- Go's QUIC constructor-guard test maps to the live negative leg (the
  product's TLS constructors are private; the live probe is stronger).

## Pass criteria

- Marker converges on both nodes within 60 s.
- TLS-1.2-only dial initiates but fails within 10 s with a peer crypto
  close (any other outcome — success, local error, timeout — fails).
- TLS 1.3 dial completes handshake + stream open within 10 s / 5 s.
- The rejected handshake attaches no session (mesh peer count intact).

## Compile check (only gate; never run here)

```sh
CARGO_TARGET_DIR=/home/marc/MURMUR/tests-live/target \
  cargo test --no-run --manifest-path /home/marc/MURMUR/tests-live/tls-floor/Cargo.toml
```
