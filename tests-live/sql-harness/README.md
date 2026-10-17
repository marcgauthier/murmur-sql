# SQL process harness

This standalone package launches real `murmur-server` processes and returns
`murmur-client` connections. It has no dependency on the Murmur engine,
replication implementation, or Spool. It provides per-node SQL/QUIC ports,
shared test certificate authority and trusted origin configuration, unique
runtime folders, process kill/restart, configuration edits while stopped,
and exact SQL convergence polling.

`Cluster::new(ClusterOptions { name, num_nodes, ..Default::default() }).await`
starts a mesh. `cluster.node(i)` is a Rust client; `cluster.connect(i).await`
creates an additional authenticated and unlocked SQL connection.
`cluster.pid(i)` exposes a process ID for scoped signal scenarios; the harness
keeps ownership and reaps an already exited server when stopped.
After editing a stopped node's TLS configuration, `set_client_tls(i, tls)`
sets the Rust client TLS credentials used by its next start/connect.
`cluster.stop_node(i)` kills and reaps that server, and
`cluster.start_node(i).await` reopens the same persistent directory and config.
Flush through the client before a stop when a scenario requires durable writes.

`files` enables encrypted object storage. `local_only` disables the replication
transport; table replication settings are still tested through SQL.
`replication_toml` adds keys inside `[replication]`; `extra_toml` adds top-level
configuration sections. `set_peers` edits bootstrap addresses and disables
membership discovery. Stop all affected processes before changing topology so
existing transport sessions cannot leak writes between partitions.

`MURMUR_SERVER_BIN` reuses a prebuilt server. `MURMUR_ADMIN_BIN` overrides its
sibling admin binary. Without these overrides, the harness builds both with
`--features replication` once per test executable. `CARGO_TARGET_DIR` is honored.
`MURMUR_LIVE_RUNTIME` overrides the default `tests-live/runtime` artifact root.
Each test gets a unique directory, so parallel tests do not remove each other's
state. Passing tests clean up; panics retain server logs and configs.

The harness uses local test credentials and loopback listeners. Failure
artifacts contain test private keys and should be treated as test data.
