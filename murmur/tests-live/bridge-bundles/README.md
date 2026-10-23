# bridge-bundles

Fresh scenario (no Rust source): raw sealed bundles cross the Low/High
bridge through the generic bundle API.

## What it proves

A one-node Low mesh and a one-node High mesh (separate DBIDs, CAs, and
keys) move three raw payloads (empty, one byte, 100 KiB) across a
shared staging directory through `Bridge.Export` (Low) plus
`Bridge.Stage` and `Bridge.OpenBundle` (High): bundle files carry
1-based sequence names, staged headers pin the Low database, node,
stream, and sequence, and opened payloads are byte-identical.
Re-staging is idempotent; a tampered copy fails staging and is
quarantined by `Process` while the original stays valid; the exporter
role stages and opens nothing; and a 64 MiB + 1 payload is refused
with `ErrOverloaded` leaving no artifacts.

`Process` applies zero raw bundles: payloads that are not file parts
or `BridgeContent` are held for retry, never applied. That limitation
is pinned deliberately (raw `Export` is transport, not ingestion).

## Env knobs

Standard harness knobs only (`MURMUR_LIVE_RUNTIME`,
`MURMUR_LIVE_FAILURES`, `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
`MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`). No convergence waits: every
leg is synchronous.

## Deviations

- No files, no mesh convergence, no SQL: single-node clusters with
  bridge-only configuration, since the bundle envelope is the whole
  subject.
