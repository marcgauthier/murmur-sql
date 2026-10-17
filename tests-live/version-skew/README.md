# version-skew (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/version-skew/`
(`README.md` + `version_skew_test.go`:
`TestUnknownProtocolVersionRefused`).

## What it proves

A node advertising handshake protocol version 99 never connects in
either direction, exchanges no data either way, but stays alive and
keeps serving local reads/writes, while the compatible pair converges
normally throughout.

## Pass criteria

- `Cluster::new` opens cluster `version-skew` (3 nodes) under
  `tests-live/runtime/version-skew/` with `manual_peers`; only the
  compatible pair (0, 1) is meshed, node3 stays peerless.
- Positive control: the version-99 fixture dialing node0 with the
  product version is welcomed (proves TLS trust, identity, and the
  hand-rolled framer, so later refusals are caused by the version
  alone); the control session tears down before the baseline.
- Baseline: 10 `w-*` rows on node0 converge on the pair within 30 s;
  node3 stays empty.
- Over the 10 s skew window (500 ms poll): `status().peers` stays
  1/1/0, node3 stays empty, and every version-99 probe dial against
  both honest nodes is refused (no Welcome; at least 4 probes, both
  version gates against both nodes). Both nodes latch the frame-header
  refusal (`invalid frame header`) and the hello-payload refusal
  (`incompatible hello`) in `last_error`.
- Outbound leg (10 s): node0 dials the skewed address; the skewed
  side reads each Hello and closes without a Welcome, at least one
  honest dial is observed, and neither honest node ever exceeds one
  session.
- Node3 stays writable (3 `skewed-local-*` rows); after the 5 s
  settle node0 still holds exactly the 10 baseline rows and node3
  holds exactly its 3 local rows.

## Env knobs

- Harness knobs apply: `MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
  `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
  `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.

## Deviations from the Go original

- The Rust product has no `protocol_version_override` /
  `min_protocol_version_override` testing knob (the version gate is
  the hardcoded `version == VERSION` check in `engine.rs` plus the
  frame-header check in `protocol.rs`), so the Go skewed node is
  split in two: a raw QUIC fixture (same `transport::endpoint` +
  cluster-CA cert pattern as the `corrupt-snapshot` attacker) speaks
  version 99 in both directions, and the unmeshed third cluster node
  covers the isolated-but-alive leg (local reads/writes, no data
  either way). No product change was needed.
- The window probes alternate two variants per round: version 99 in
  both the frame header and the Hello payload (a faithful skewed
  peer), and version 99 in the Hello payload only (isolates the
  `engine.rs` payload check past the `protocol.rs` frame check).
- The outbound leg mirrors a real skewed node: it cannot parse our
  frames, so it reads the Hello and closes without a Welcome (the
  dial path has no version field to check — `Welcome` carries none).
- Plain SQL (`records(id INTEGER PRIMARY KEY, name TEXT)`,
  `ORDER BY name`) instead of the managed typed-records HTTP API
  (`TypedInsert`, `TypedNames`).
- Session counts and refusal causes come from `MURMUR SHOW REPLICATION` (`peers` and `last_error`
  columns) instead of embedded handles; peer adding uses `MURMUR PEER ADD`.
- Ran against separate `murmur-server` process nodes via `murmur-live-sql-harness`.

## Compile check (only gate; never run here)

```sh
cargo test --no-run --manifest-path /home/marc/MURMUR/tests-live/version-skew/Cargo.toml
```
