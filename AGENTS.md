# Usage documentation maintenance

Documentation layout: every Markdown file other than `AGENTS.md`, `README.md`, and `VENDORS.md`
(`USAGE.md`, `DESIGN.md`, `SCALABILITY.md`, `TESTING.md`, `TODO.md`)
lives in the `./docs` folder. Those three stay at the repository root.
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
and the race detector; preserve Linux, macOS, and Windows CI. Keep `VENDORS.md`
synchronized with the root Rust dependencies and the Go module requirements.
Avoid duplicating Rust conflict, crypto, wire-format, or persistence
implementations in Go.

# Architecture guidance

This repository is a thin PostgreSQL-wire shell over the embedded Murmur
engine (the `murmur` crate in the sibling `../MURMUR` checkout). All storage,
replication, conflict-resolution, and search-index behavior lives in that
engine; engine changes belong in `../MURMUR`, never duplicated here. This repo
owns only `src/server` (wire protocol, auth, `MURMUR ...` commands), `src/config.rs`
(`config.toml`), `src/logging.rs`, the two binaries, `clients/`, and `tests-live/`.

Replication and settings live in `config.toml`, which is authoritative. Table
replication is controlled by `MURMUR SET TABLE <name> REPLICATED true|false`
and stored under `[replication.tables]`, keyed by stable table IDs. The server
overlays the file onto the in-memory schema at unlock and rewrites the file
atomically on every SET command. Missing entries mean disabled; missing
settings never enable tables implicitly during unlock or recovery.

Engine-side flag changes (SET TABLE, schema adoption) flow back through the
`TableFlagsSink` callback, which persists the full map to `config.toml` and
mirrors the running snapshot. The engine never touches the file directly; keep
that boundary: the shell owns the file, the engine owns the data.

Keep the shell a projection, not a second implementation. Do not duplicate
engine conflict, crypto, wire-format, schema, or persistence logic in
`src/server`, the binaries, the clients, or the harnesses; add a `murmur::`
API (or `murmur::host::` helper) in `../MURMUR` instead. Wire-visible behavior
changes need a `tests/` pgwire test and a `docs/USAGE.md` example in the same
change.

Maintain a root VENDORS.md listing only direct external package/crate dependencies required to build or run the server and clients. Exclude test-only, benchmark-only, and transitive dependencies.  I want to see the name of the company and author and country of orgin for the company if there is one otherwise the author.
