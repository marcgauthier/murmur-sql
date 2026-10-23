# cert-lifecycle (Go port)

Rust source: `/home/marc/MURMUR/tests-live/cert-lifecycle/`
(`tests/cert_lifecycle.rs`).

## What it proves

A node whose certificate expires is rejected on fresh replication
handshakes, and rotation restores the mesh: converge a marker (positive
control), swap B to a short-expiry cert for the same NodeID and show the
mesh stays connected pre-expiry, wait past NotAfter and restart B so both
sides must re-handshake (survivor sees no session, `peers` 1->0, an
A-side marker never reaches B during the isolation window), then rotate
to a fresh cert and show full reconvergence plus a bidirectional write.

## Pass criteria (same as Rust)

- Phase 0: `marker-init` converges on both nodes; A holds a session to B
  and `peers(A) == 1`.
- Phase 1: after rotating B to a TTL cert, the served leaf's remaining
  lifetime fits the minted TTL (stale-cert guard), A connects to B, and
  `marker-pre-expiry` converges before NotAfter minus 5 s.
- Phase 2: past NotAfter + 2 s, with B restarted on the same expired
  cert: A reports B not connected, `peers(A) == 0`, B's own view agrees,
  the served leaf parses with NotAfter in the past, and during the
  isolation window `marker-post-expiry` never reaches B while A keeps
  serving all 3 markers with `peers(A) == 0` throughout.
- Phase 3: after rotating B to a fresh 24 h cert, the mesh reconnects,
  all 3 markers converge, a B-side `marker-rotated` converges back, and
  both nodes hold equal ordered names.

## Env knobs (same names as Rust)

- `MURMUR_CERT_LIFECYCLE_TTL_S` (default 25): short-cert lifetime. Values
  below 15 s abort: too little pre-expiry budget (same guard as Rust).
- `MURMUR_CERT_LIFECYCLE_WINDOW_S` (default 10): post-expiry isolation
  window.
- Harness knobs apply: `MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
  `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
  `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.

## Deviations

- Same as the Rust port: in-process multi-`Database` cluster instead of
  daemon processes, plain SQL instead of the typed-records HTTP API,
  session state from the replication status instead of debug endpoints,
  and the served leaf read from the node's `tls/node.der`.
