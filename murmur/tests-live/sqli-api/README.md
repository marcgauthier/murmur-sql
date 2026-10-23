# sqli-api (Go port)

Rust source: `/home/marc/MURMUR/tests-live/sqli-api/`
(`tests/sqli_api.rs`, `sql_application_routes_removed`), itself a port
of the original Go `sqli-api` scenario
(`TestSQLApplicationRoutesRemoved`).

## What it proves

Untrusted SQL text is never executed: SQL payloads replicate as inert
byte-exact data with the schema untouched, and retired-route request
bytes sprayed at the replication port die on arrival, while the managed
typed path stays writable and replicates.

## Leg mapping (same as Rust)

- Literal retired-route request bytes (full POST requests + JSON
  bodies) sent as UDP datagrams at the replication port attach no
  session and change no rows or schema.
- The same payloads plus DDL attempts (`DROP TABLE`, `CREATE TABLE`,
  stacked `DELETE`) inserted through the managed path land byte-exact
  on every node with `sqlite_master` snapshots identical before/after
  on both nodes.
- `before-sql-probe` / `after-sql-probe` writes with per-node counts
  prove the managed path stays writable and replicating.

## Pass criteria

- Pre-probe write converges (20 s); schema baselined both nodes.
- 6 payloads each count exactly 1 everywhere (20 s each); schemas
  identical after.
- Retired-route byte spray: peer counts unmoved, schemas unmoved, row
  set converged.
- Post-probe write converges with per-node counts exact.
