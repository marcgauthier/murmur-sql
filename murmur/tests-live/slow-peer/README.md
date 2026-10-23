# slow-peer (new Go scenario, no Rust original)

## What it proves

A throttled joiner pulls a snapshot through a slow link while the donor
stays bounded:

- Two nodes mesh directly for a baseline; the joiner stops while the
  donor writes ~1.2 MB past aggressive retention and GC expiry (the
  snapshot-resync forcing pattern: expiry wait plus collection passes).
- After the restart both sides dial through their own UDP throttle
  proxy (token-bucket rate limit, order-preserving): each relay joins
  one dial socket to the other node's listen socket and drops anything
  else. Duplicate arbitration keeps exactly one of the two sessions on
  both sides; both relays throttle, so whichever wins is slow, and
  direct discovery dials lose the same arbitration because the proxied
  sessions establish first.
- During the trickle the donor's spool pending bytes must stay under cap
  (no unbounded slow-peer buffering); relay counters must prove traffic
  actually flowed through the proxy; the joiner must still converge on
  every row via the snapshot path (log growth within the retained tail)
  with equal row digests on both nodes.

## Pass criteria

- 2-node cluster opens under `tests-live/runtime/slow-peer/` with
  retention 1 s / minimum-retained 10 / offline memory 20 s and mutual
  snapshot trust.
- Donor prunes >= 10 transactions before the rejoin (snapshot forced).
- Joiner converges on all rows within 420 s through the throttle.
- Donor pending-bytes peak stays under the cap (default 64 MB).
- Proxy relays a non-zero byte/packet count in the throttled window.
- Final ordered-name digests match on both nodes.

## Env knobs

- `MURMUR_SLOW_PEER_ROWS` (default 1200): bulk row count (~1 KiB each).
- `MURMUR_SLOW_PEER_KBPS` (default 16): throttle rate in KiB/s.
- `MURMUR_SLOW_PEER_MAX_PENDING_MB` (default 64): donor pending cap.
- `MURMUR_SLOW_PEER_GC_WAIT_SECONDS` (default 30): expiry wait.
- Harness knobs apply: `MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
  `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
  `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`, `MURMUR_TEST_DEADLINE_SCALE`.

## Deviations from the Rust original

None: new scenario.
