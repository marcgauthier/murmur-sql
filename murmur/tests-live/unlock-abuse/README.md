# unlock-abuse (Go port)

Rust source: `/home/marc/MURMUR/tests-live/unlock-abuse/`
(`tests/unlock_abuse.rs`, `unlock_abuse`), itself a port of the
original Go `unlock-abuse` scenario (`TestUnlockAbuse`).

## What it proves

Distinct wrong keys fail direct opens of a stopped node's store with
byte-identical generic messages leaking no key bytes, malformed keys
fail cleanly, and the honest reopen serves intact data and rejoins the
mesh.

## Leg mapping (same as Rust)

- Certless-admin legs: NOT portable (no admin endpoint); transport
  auth is covered by the `api-mtls` equivalent instead.
- Wrong-key / unknown-key-id identical 401s become: three distinct
  32-byte wrong keys fail direct open with byte-identical messages.
- No-oracle body checks become: every failure message is non-empty and
  contains neither the tried key's hex nor the real key's hex.
- Unknown-key-id becomes malformed credentials (16- and 64-byte keys):
  cross-identity with the wrong-key message is deliberately NOT
  asserted.
- Honest unlock + audit line becomes: honest reopen serves the intact
  baseline row and rejoins; a post-unlock write converges with equal
  digests.

## Pass criteria

- Baseline converges (1 row, 10 s) with equal digests before any abuse.
- Three wrong-key opens fail with byte-identical messages, no key hex.
- Two malformed-key opens fail with no key hex.
- Honest reopen: baseline intact, mesh rejoined, post-unlock write
  converges (2 rows, 10 s) with equal digests.
