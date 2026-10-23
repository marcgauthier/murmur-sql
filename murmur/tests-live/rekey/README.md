# rekey (Go port)

Rust source: `/home/marc/MURMUR/tests-live/rekey/` (`rekey.rs`:
`typed_storage_key_rekey_survives_restart`,
`rekey_sigkill_restarts_with_new_key`).

## What it proves

A single node rotates its application (master) storage key without
downtime: records written before and after rotation read back after
reopening with the new key, and the retired key is rejected. The second
leg covers SIGKILL recovery: rotation survives an unclean restart and the
reopened store still requires the new key.

## Pass criteria

- `TestTypedStorageKeyRekeySurvivesRestart` (cluster `rekey-restart`,
  1 node): `typed-before-rotate` is inserted, `RotateMasterKey`
  succeeds, `typed-after-rotate` is inserted; after `StopNode`, a direct
  `murmur.Open` with the new key reads both rows back with count 1, and
  opening with the retired key fails with `murmur.ErrWrongKey`.
- `TestRekeySigkillRestartsWithNewKey` (cluster `rekey-sigkill`, 1 node):
  `pre-rotate` is inserted and the key is rotated; a fresh `testnode`
  fixture process opens the rotated store with the new key (ready file
  observed) and is then SIGKILLed; reopening in-process with the new key
  reads `pre-rotate` back with count 1, a `post-restart` write succeeds
  and reads back, and the retired key is rejected with
  `murmur.ErrWrongKey`.

## Env knobs

- Standard harness knobs: `MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
  `MURMUR_LIVE_UNLOCK_STALL_SECONDS`, `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.
- The SIGKILL leg needs the `testnode` fixture binary built (`run.sh`
  builds it and exports `MURMUR_TESTNODE_BIN`; `MURMUR_BIN` also honored).

## Deviations (inherited from Rust)

- Plain SQL (`records(name TEXT NOT NULL PRIMARY KEY)`) instead of the
  managed typed-records API; the record name is the row key in both.
- Rotation is `Database.RotateMasterKey` (synchronous); no cipher
  parameter and no key IDs, so the active-key-ID assertion is dropped
  (new-key-opens / retired-key-rejected carry that proof).
- The new key is fixed (`[0x5a; 32]`) instead of random.
- Restart is in-process close + direct `murmur.Open` with the new key
  (the cluster keeps the pre-rotation key).
- No await-unlock leg: `murmur.Open` takes the key directly (no Remote
  Unlock endpoint).
- The SIGKILL leg kills a `testnode` fixture holding the rotated store
  open rather than the rotating node itself (the fixture has no rotate
  command).
