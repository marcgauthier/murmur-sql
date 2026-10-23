# tampered-backup (Go port)

Rust source: `/home/marc/MURMUR/tests-live/tampered-backup/`
(`tampered_backup.rs`: `tampered_backup`).

## What it proves

A single node seeds 30 rows, stops, and is backed up offline through
the library. Three tampered copies of the backup — a byte flip in a
payload file, a manifest edit, and a payload truncation — must each fail
both `Restore` and `VerifyBackup` with no partial state, while the
pristine backup verifies and restores exactly under a fresh writer
identity (row count plus a canonical digest of the sorted record names).

## Env knobs

Standard harness knobs only (`MURMUR_LIVE_RUNTIME`,
`MURMUR_LIVE_FAILURES`, `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
`MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`). No scenario-specific knobs.

## Pass criteria

- Each tampered leg: restore fails, the restore target directory does not
  exist afterwards, `VerifyBackup` fails, and no `.verify-tmp-*` residue
  remains in the backup's parent dir.
- Positive control: pristine backup verifies, restores under a fresh node
  id, reopens with 30 rows and a digest equal to the pre-backup digest.

## Deviations from the Rust original

- In-process single-`Database` cluster like the Rust port (per design).
- Plain SQL instead of the managed typed-records HTTP API.
- The backup format is a directory (`manifest`/`keys.enc`/
  `segments/`), not a `.tar.gz` archive: the gzip-magic flip becomes a
  segment-byte flip, the `backup-metadata.json` repack becomes a manifest
  byte flip, and the late-payload flip becomes a payload truncation. The
  error-substring assertions ("gzip", "metadata") have no equivalent
  and are replaced by failure assertions (the rejection is logged).
- No restore-intent file exists in the product: `assertTargetUntouched`
  (zero regular files under the target) becomes "the restore target
  directory does not exist", which the product guarantees by removing
  `dst` on verification failure.
- The canonical digest is FNV-1a/64 over the same canonical byte stream
  (sorted `\n`-joined names) instead of SHA-256; only pre/post equality
  is asserted.
- Extra `VerifyBackup` legs: the pristine backup must verify, and each
  tampered backup must fail verification without mutating the source or
  leaving temp residue.
