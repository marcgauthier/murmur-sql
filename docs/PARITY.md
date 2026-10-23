# Port parity checklist

The SQL shell is now connected to the completed MURMUR-GO source. Both production
binaries select the native factory. This integration has not been built or tested;
“ported” below describes code, not verified behavior.

| Area | Ported implementation | Outstanding verification |
| --- | --- | --- |
| Engine dependency | Embedded `murmur/` engine and vendored C dependencies (`third_party/`) | Build/link on supported platforms |
| Configuration | Full settings mapped to engine/replication options | Defaults, invalid limits, credentials, node identities |
| Database lifecycle | Create/open, connection key verification, config flags and callbacks | Wrong-key immutability, restart, legacy flag import |
| SQL and DDL | Public engine transactions/reads/migrations; bound values | SQL behavior, authorizer, durable recovery |
| Query descriptions | Schema scope, aliases, INSERT/comparison/IN/BETWEEN/LIMIT parameter inference, view/search metadata, safe probes | Empty results, dynamic expressions, schema changes, drivers |
| Administration | Diagnostics, table flags, search settings, peer/retention/policy/bridge operations | Row contents, ordering, error codes, disabled capabilities |
| File objects | Engine-backed encrypted upload/download with owned cursors and budgets | Expiry, disconnect, concurrent use, staging, key rotation |
| Backup/restore | Engine checkpoints, Rust-compatible archives, key-verified restore, trial open and rollback | Crash/failure paths, snapshots, live replication restart |
| Key rotation | Verify previous key, engine rewrap, update accepted key, flush | Failed flush, restart, session behavior |
| Offline admin | Native status/inspect/verify/backup/restore/keys and structured output | JSON/human output, identity reassignment, corrupt artifacts |
| Wire/auth/logging/services | Existing Go shell plus affinity coercions and storage-parent permissions | Regression and elevated deployment tests |
| Clients and live scenarios | Retained clients and Go server launchers; obsolete engine-missing skip removed | Full integration gate |

MURMUR-GO now has QUIC transport. Earlier TLS/TCP incompatibility notes no longer
apply. Go↔Rust peering is unverified; protocol resemblance is not a compatibility
claim.

The engine packages (`murmur`, `spool`, `replication`, `compression`, `internal`, and `third_party`) are now self-contained within this repository. The SQL shell calls the public `murmur` APIs and owns configuration, transfer handles/archives, authentication, logs, and deployment.

Known implementation limits/differences:

- Engine SQL operations expose no interrupt handle; cancellation is checked
  before admission and between result rows, not inside a long SQLite step.
- Result sets are buffered with a 64 MiB shell budget. Portals support bounded
  paging; the Rust reference ignored Execute's row limit.
- Descriptions infer result affinity from public schema/query metadata because
  the engine does not expose SQLite declaration/origin metadata on prepared
  statements. Unknown expressions use safe probes and TEXT fallback.
- Replication table flag updates use YAML AST node replacement, preserving comments and structure.
- Linux deployment uses a systemd simple service, with storage-parent directory
  access for first creation and live restore. Elevated platform acceptance and
  Windows path-alias/ACL hardening remain unverified.
