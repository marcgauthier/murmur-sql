# cert-lifecycle (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/cert-lifecycle/`
(`README.md` + `cert_lifecycle_test.go`:
`TestCertExpiryRejectsAndRotationHeals`).

## What it proves

A node whose certificate expires is rejected on fresh replication
handshakes, and rotation restores the mesh: converge a marker (positive
control), swap B to a short-expiry cert for the same NodeID and show the
mesh stays connected pre-expiry, wait past NotAfter and restart B so both
sides must re-handshake (survivor sees no session, `peers` 1->0, an
A-side marker never reaches B during the isolation window), then rotate
to a fresh cert and show full reconvergence plus a bidirectional write.

## Pass criteria

- Phase 0: `marker-init` converges on both nodes; A holds a session to B
  and `peers(A) == 1`.
- Phase 1: after rotating B to a TTL cert, the served leaf is fresh
  rotation-time material (stale-cert guard), A connects to B, and
  `marker-pre-expiry` converges before NotAfter minus 5 s.
- Phase 2: past NotAfter + 2 s, with B restarted on the same expired
  leaf: A reports B not connected, `peers(A) == 0`, B's own view agrees,
  the served leaf is byte-identical to the minted short leaf whose TTL
  has fully elapsed, and during the isolation window `marker-post-expiry`
  never reaches B while A keeps serving all 3 markers with `peers(A) == 0`
  throughout.
- Phase 3: after rotating B to a fresh 24 h cert, the mesh reconnects,
  all 3 markers converge, a B-side `marker-rotated` converges back, and
  both nodes hold equal ordered names.

## Env knobs

- `MURMUR_CERT_LIFECYCLE_TTL_S` (default 25): short-cert lifetime. Values
  below 15 s abort: too little pre-expiry budget (same guard as Go).
- `MURMUR_CERT_LIFECYCLE_WINDOW_S` (default 10): post-expiry isolation
  window.
- Harness knobs apply: `MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
  `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
  `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`, `MURMUR_SERVER_BIN`.

## Deviations from the Go original

- Multi-process `murmur-server` children over the PG wire (per design)
  instead of `testnode` daemon processes over HTTPS+mTLS.
- Plain SQL (`records(id INTEGER PRIMARY KEY, name TEXT NOT NULL)`)
  instead of the managed typed-records HTTP API.
- Session state comes from `MURMUR SHOW PEERS` and `MURMUR SHOW
  REPLICATION` (`peers`) instead of `debug/peers` (`Connected`) and
  `/v1/status` (`connected_peers`).
- No TLS-served-cert fetch: the PG port carries no TLS identity in test
  mode, so expiry is proven against the minted TTL clock (rotation
  instant + TTL) instead of parsing the served leaf's NotAfter. The
  stale-cert and non-vacuity legs read the node's `tls/node.pem`, which
  the harness rewrites from the same material the restarted server
  loads: phase 1 asserts the bytes change at rotation time with a fresh
  mtime, and phase 2 asserts the served bytes are still that same short
  leaf while its TTL has fully elapsed. The rejection assertions
  themselves fail closed: a silently un-swapped cert would stay
  connected and time out the rejection waits.
- Rotation goes through the harness `Cluster::rotate_node_cert` helper
  (stop, issue a fresh leaf for the same NodeID, rewrite `tls/`,
  restart under the unlock watchdog) instead of rewriting `tls/node.crt`
  + `tls/node.key` on disk; every restart is followed by an explicit
  re-mesh (fresh ephemeral ports).
- Phase 1 crashes B (`kill_node`, no graceful QUIC close) instead of
  stopping it, and node ids are pinned with the survivor sorting first.
  Both choices are adversarial on purpose: the survivor keeps a zombie
  session in its tie-break-preferred direction, so the rotation leg
  deterministically exercises zombie replacement instead of flaking on
  close delivery and random id order.
- The isolation-window argument is unchanged: `mesh()` redials
  immediately and the engine keeps dialing, so a steady zero across the
  window proves rejection rather than idleness.

## Compile check (only gate; never run here)

```sh
CARGO_TARGET_DIR=/home/marc/MURMUR-SQL/tests-live/target \
  cargo test --no-run --manifest-path /home/marc/MURMUR-SQL/tests-live/cert-lifecycle/Cargo.toml
```
