# Changelog

All notable user-visible changes to Murmur-SQL. Engine (`../MURMUR`)
changes ship in the engine's own notes.

## [1.0.0] - 2026-10-20

### Changed (operator action may be needed)

- `server.tls.ca_file` outside `server.production` is now rejected at
  config load (`server.tls.ca_file only takes effect with
  server.production`). Previously it was silently ignored while the
  server never verified clients, falsely implying mTLS. Dev TLS (server
  certificate only) omits `ca_file`; mTLS requires production with the
  full bundle.
- `storage.data_dir` must end with a directory name: `.`, `..`, and `/`
  are rejected at config load. Restore swaps the live directory aside
  with rename, which cannot move those paths; failing fast replaces a
  mid-restore failure after the live database is already closed.
- Unknown users and wrong passwords now both complete the password
  round-trip (unknown names verify against a dummy Argon2 hash so
  timing reveals nothing) and fail with PostgreSQL invalid-password
  `28P01` instead of `28000`.

### Fixed

- Re-parsing a prepared statement under the same name and executing it
  without a fresh Describe no longer encodes rows with the previous
  statement's column types. Describe-time type stashes are validated
  against the statement SQL and a schema generation counter (bumped on
  every local DDL); mismatches re-infer instead of silently emitting
  wrong-typed bytes. Covered by a raw-protocol regression test that
  failed pre-fix with an 8-byte INT8 zero where TEXT `x` belongs.
- Backup upload staging, archive packing, and restore unpacking refuse
  to write through a symlink at the destination path: the operation
  fails with `58000` instead of truncating the link target
  (`O_NOFOLLOW` on Linux plus a pre-open check on all platforms;
  upload partials, packed archives, and unpacked entries all land on
  client-influenced names).
- Extended-protocol parameter count is bounded (`MAX_PARAMETERS`), and
  `MURMUR BACKUP DOWNLOAD` limits must be positive and are capped at
  16 MiB per call (`22023` otherwise).
- `max_connections` (default 256) caps concurrent wire connections;
  overflow is refused before the handshake and freed permits admit
  again.
- Log rotation documents and enforces daily-only; one undeletable
  rotated file is skipped with a warning and never fails startup.
- `murmur-admin hash-password` reads the password from stdin without
  `--password`, and `config.toml` table-flag writes are atomic with no
  temp litter on failure.

### Added

- `search-mesh` live scenario: managed FTS/vector/JSON over a
  multi-node mesh with node 0 as schema master; included in the release
  gate (74 scenarios total).
- Regression tests: raw-protocol statement-redefinition test,
  `ca_file_requires_production` and `data_dir_requires_a_final_component`
  config tests, `private_file_creation_refuses_symlinks` backup test,
  `describe_stash_validates_sql_and_generation` unit test, and
  `tests/admin_cli.rs` coverage.
- Live-scenario hardening: `origin-signatures` baselines wait for
  staged/applied counter quiescence instead of asserting exact
  stillness across honest in-flight drain; `gc-balance` scales its
  collection-drain budget with the window's observed slowdown and fails
  fast with a distinct message on a true 30-second collection stall;
  `graceful-shutdown` waits for the killed node's session to drop from
  survivors' peers before asserting retention pruning (lingering
  sessions stay protected past `maximum_offline` by design);
  `hostile-schema` matches the engine's `invalid Ed25519 signature`
  error text; `write-priority` measures total local-vs-remote writer
  service with a documented deviation note.
- `docs/USAGE.md` documents every behavior above with SQLSTATE codes
  (`28P01`, `58000`, `22023`), the `data_dir` restriction, and the
  symlink-refusal rule.

## [0.1.0] - Initial SQL shell

- PostgreSQL-wire server (`murmur-server`) over the embedded Murmur
  engine: SQLite-dialect SQL, per-connection `MURMUR UNLOCK`, `MURMUR`
  admin commands, Argon2 auth, TLS/mTLS with production mode.
- `murmur-admin` CLI, Rust/Go/Python wire clients with runnable
  examples, and the `tests-live/` multi-process scenario suites.
