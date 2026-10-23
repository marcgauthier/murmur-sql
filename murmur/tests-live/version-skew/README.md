# version-skew (Go port)

Rust source: `/home/marc/MURMUR/tests-live/version-skew/`
(`tests/version_skew.rs`, `unknown_protocol_version_refused`), itself
a port of the original Go `version-skew` scenario
(`TestUnknownProtocolVersionRefused`).

## What it proves

A node advertising handshake protocol version 99 never connects in
either direction, exchanges no data either way, but stays alive and
keeps serving local reads/writes, while the compatible pair converges
normally throughout.

## Pass criteria (same as Rust)

- 3-node cluster `version-skew` with `manual_peers`; only the
  compatible pair (0, 1) is meshed, node3 stays peerless.
- Positive control: the version-99 fixture dialing node0 with the
  product version is welcomed; the control session tears down before
  the baseline.
- Baseline: 10 `w-*` rows on node0 converge on the pair within 30 s;
  node3 stays empty.
- Over the 10 s skew window (500 ms poll): `peers` stays 1/1/0, node3
  stays empty, and every version-99 probe dial against both honest
  nodes is refused (at least 4 probes, both version gates against both
  nodes, latched in `last_error`).
- Outbound leg (10 s): node0 dials the skewed address; the skewed side
  reads each Hello and closes without a Welcome, at least one honest
  dial is observed, and neither honest node ever exceeds one session.
- Node3 stays writable (3 `skewed-local-*` rows); after the 5 s settle
  node0 still holds exactly the 10 baseline rows and node3 holds
  exactly its 3 local rows.

## Deviations

None from the Rust port: the skewed node is split into a raw QUIC
fixture (same mTLS setup as the engine, version 99 in both directions)
plus the unmeshed third node, the window probes alternate the
frame-header and hello-payload gates, and plain SQL replaces the
typed-records HTTP API.
