# Usage documentation maintenance

Documentation layout: every Markdown file other than `AGENTS.md` and `README.md`
(`USAGE.md`, `DESIGN.md`, `SCALABILITY.md`, `TESTING.md`, `TODO.md`, `VENDORS.md`)
lives in the `./docs` folder. Only those two stay at the repository root.
Keep relative links valid when editing or moving them.

Keep `./docs/USAGE.md` accurate after code changes. Any change to public APIs, SQL
behavior, Cargo features, configuration options or defaults, CLI commands,
replication, persistence, search, or operational restrictions must update the
corresponding examples and restrictions in `docs/USAGE.md` in the same change.
Document new user-visible features with an example and state their limitations.
Check examples against the current implementation and compile Rust examples
with the features they require when changing those examples. For internal-only
changes, review whether documented behavior changes; do not invent documentation
changes when behavior is unchanged. Keep README guidance consistent with docs/USAGE.md
when modifying overlapping documentation.

# Go client maintenance

`./clients/go` is a pure-Go `database/sql` client over the PostgreSQL wire
protocol; there is no embedded binding, cgo, or native library. Go tests and
examples build `murmur-server` via cargo (or reuse `MURMUR_SERVER_BIN`) and
talk to a scratch server through `internal/servertest`. After a wire-visible
server change (SQL behavior, MURMUR commands, error codes, auth, TLS), update
the Go client, its README, examples, and docs/USAGE.md in the same change.
Keep the Go example catalog and `clients/go/examples/coverage.json` aligned
with the client feature table. New capabilities need a runnable example with
result checks and documented setup/restrictions. Run the Go suite, `go vet`,
and the race detector; preserve Linux, macOS, and Windows CI. Keep `docs/VENDORS.md`
synchronized with the root Rust dependencies and the Go module requirements.
Avoid duplicating Rust conflict, crypto, wire-format, or persistence
implementations in Go.

# Architecture guidance

The design is an SQLITE in memory database that has background functions that provide data persistance with compression and encryption and data replication via masterless peer-to-peer. Keep this in mind when developping, it's SQLITE in memory (with some limitation) first and the replication and persistance is done in the background.


Keep the SQLite crate lightweight and focused on local database operations.
Replication and conflict resolution belong in a separate background task;
they must not add blocking work or heavyweight dependencies to the SQLite
operation path. Keep the boundary between SQLite and that task small.


Use exactly one in-memory SQLite database. Application writes must execute only
application SQL, transaction control, and SQLite session capture. Do not add CRDT
metadata tables, generated triggers, or metadata SQL to that database.

Spool is the authoritative store for conflict versions and tombstones. Background
workers read and update those Spool records directly; do not maintain a separate
Rust metadata database or a second SQLite replica. Transient batch buffers and
Spool's existing key/location index are permitted.

Assign a batch's writing time when its changeset is collected from SQLite, before
queueing it. Preserve that timestamp across worker delays and storage retries.

Maintain a file docs/VENDORS.md that list all of the external package/crate that are dependencies.  I want to see the name of the company and author and country of orgin for the company if there is one otherwise the author. 
Keep Spool point reads small: default to 64 KiB independently compressed and
authenticated blocks, with oversized individual records isolated. Large atomic
commits must span blocks. Keep a byte-bounded hot block cache inside Spool, with
configurable capacity, eviction and decoded-buffer wiping. Cache data is disposable
and must never become a separate authoritative metadata store.

Table replication is controlled by `MURMUR SET TABLE <name> REPLICATED
true|false` and stored in `config.toml` under `[replication.tables]`, keyed by
stable table IDs. The config file is authoritative: the server overlays it onto
the in-memory schema at unlock and rewrites the file atomically on every SET
command. Legacy maps stored at `00/schema/replication` are imported into the
config file once and never written back. Missing entries mean disabled.
Missing settings never enable tables implicitly during unlock or recovery.
Preserve settings through migrations and recovery. Filter row data, tombstones and table
CRDT records in background outgoing and incoming paths; keep local persistence
intact. Do not check these settings on ordinary application SQL writes.
Drops and renames of tables and columns are rejected for replicated tables
and allowed for local-only ones; the gate lives in `schema::inspect` (both
statement scanning and the id diff) so engine and wire paths share it. An
unmapped rename mints a new table identity that stays local-only until
explicitly enabled.

Local tables retain their application SQLite primary key. Enabling replication
rebuilds even populated tables with a read-only `id_murmur_primary_key` UUIDv5
SQLite primary key and a unique application-key constraint. Require supplied,
non-null immutable application keys while replicated. Disabling restores the
application primary key. Keep generation at the managed SQL boundary; never add
triggers or metadata SQL to application writes. Preserve transition records and
replicated tombstones in Spool across mode changes.

Replication retention runs in background workers and persists peer ACKs, peer
retirement, persistence-time ages, snapshot coverage and duplicate floors in
Spool. Enforce both minimum age and minimum log count, protect unretired peers
within the offline window and pending High/Low exports. Publish a durable
snapshot boundary before atomically deleting logs/receipts and advancing floors.
Keep CRDT tombstones. Recheck retained heads on the serialized Spool worker so
cleanup cannot overwrite newer appends. Never add retention SQL to SQLite.
