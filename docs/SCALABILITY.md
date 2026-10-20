# Scalability

Murmur-SQL is a thin PostgreSQL-wire shell over the embedded Murmur engine
(`github.com/marcgauthier/murmur`). Engine, replication, and Spool scalability work — merge batching,
snapshot streaming, retention bounds, and the benchmark harnesses — lives in
that repository; see its `docs/SCALABILITY.md` for implementation status,
benchmark evidence, and acceptance gates.

What this shell adds on top of the engine:

- One `murmur-server` process serves many concurrent PostgreSQL-wire
  connections against a single embedded engine. Application writes serialize
  through the engine's writer; tune `[engine]` (`capture_interval`,
  `queue_bytes`, `transaction_bytes`) and `[engine.scheduler]` shares in
  `config.toml` for the write/read mix (see [USAGE.md](USAGE.md#engine-and-enginescheduler)).
- Replication fan-out, anti-entropy, and history retention are engine
  background work configured under `[replication]`; the shell only projects
  its state (`MURMUR SHOW PEERS|REPLICATION|RETENTION`) and persists table
  flags. Per-table replication stays off unless explicitly enabled, so
  local-only tables cost no replication bandwidth.
- TLS/mTLS, authentication, and per-connection unlock add per-connection
  overhead on top of engine cost; server-level load testing is not yet done.
  The 500-node objective from the engine program remains not validated end
  to end through the wire shell.

Live multi-process validation runs `tests-live/run.sh gate` (see
[TESTING.md](TESTING.md#live-multi-process-tests-tests-live)); soak and stress
tiers cover sustained load, restarts, and partitions.
