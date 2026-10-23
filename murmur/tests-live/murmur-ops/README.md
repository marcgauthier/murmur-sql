# murmur-ops

End-to-end coverage of the `MURMUR` operator commands over the PostgreSQL
wire against two real peered servers.

## What it pins

- **Lock gate**: pre-`UNLOCK` statements fail with `28000`; unknown and
  malformed commands fail with `42601`.
- **SHOW surface**: `STATUS` (JSON), `TABLES REPLICATION`, `COLUMNS
  FTS|VECTOR`, `METRICS` (+`SQLITE`/`SPOOL`), `REPLICATION`, `PEERS`,
  `RETENTION`.
- **SET TABLE**: opt-in converges a write to the peer; opt-out flips the
  flag back.
- **SET COLUMN**: FTS on/off and VECTOR on/off (dimensions + distance)
  round-trip through `SHOW COLUMNS`; bad dimensions, distance, and
  booleans fail with `22023`.
- **Maintenance**: `FLUSH`, `REFRESH INDEXES`, `PEER SYNC`, `RETENTION
  COLLECT`; malformed `PEER ADD/REMOVE/RETIRE` fail with `22023`.
- **POLICY**: `COUNTER_ADD`/`MIN`/`MAX`/`SET_ADD`/`SET_REMOVE` apply
  through configured merge policies and read back over SQL; missing rows
  report `02000`.
- **Disabled surfaces**: `BRIDGE RELEASE` and `FILE LIST` report `55000`
  when unconfigured.
- **Backup/restore**: `BACKUP SAVE/LIST/DOWNLOAD` (with `OFFSET`/`LIMIT`
  paging and name validation) round-trip; downloaded bytes re-uploaded
  via `BACKUP UPLOAD BEGIN/APPEND/FINISH` (plus duplicate-`BEGIN` and
  `ABORT` paths) restore cleanly; `RESTORE FROM` reverts a sentinel row
  written after the backup while keeping older rows.
- **ROTATE KEY**: same-key and wrong-previous validation, rotation, and a
  full server restart after which only the new key unlocks the data dir.

## Run

```sh
go test -count=1 -timeout 600s ./murmur/tests-live/murmur-ops/
```
