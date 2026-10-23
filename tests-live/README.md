> Go-port status: launchers build the native Go server. The latest engine integration is untested. `MURMUR_ENGINE_INTEGRATION=1` opts into live scenarios; no live tests have been run for this integration. See [verification status](../docs/TESTING.md).

# MURMUR Rust live multi-process tests (`tests-live/`)

Port of the Go live suites in `/media/marc/2TB/Archives/MURMUR/tests-live/`
(one dir per scenario: `README.md` + `*_test.go`; runner `run.sh`; shared
`harness/`; overview `README.md`) to this repo. Port order and per-scenario
summaries live in `/home/marc/MURMUR/TESTS-LIVE-TODO.md` (73 scenarios).

## 1. Topology decision (read first)

The Go suites drive **daemon processes** (`testnode agent --config ...`) over
an **HTTPS + mTLS admin/data API**. The Rust product is a **PostgreSQL-wire
server** (`murmur-server --config ...`): SQL plus `MURMUR ...` admin commands
over one Postgres-compatible port, with replication as a background QUIC mesh
between servers.

Therefore Rust ports are **multi-process integration tests**:

- A "node" is a `murmur-server` child process on its own directory
  (`runtime/<scenario>/nodeN/` with `config.toml`, `tls/`, `data/`,
  `logs/`, `backups/`, plus a per-test unique suffix under the SQL harness).
- The harness builds the server once per process (`--features replication`,
  or reuse `MURMUR_SERVER_BIN`), picks free SQL ports, hashes the
  `murmur-admin` password, provisions per-node `config.toml` files with
  `rcgen` certs (SAN URI `replicateddb://node/<hex>`), meshes replication
  via `bootstrap` + runtime `MURMUR PEER ADD`, and pins
  `storage.database_id`/`node_id` plus the unlock key per node.
- Stop/restart/kill act on the child directly (`stop_node`, `start_node`,
  `kill_node`); unix-only `suspend_node`/`resume_node` (SIGSTOP/SIGCONT)
  cover pause legs on the classic harness. No fixture binary is needed.
- All 74 scenario packages use `clients/rust` against live `murmur-server`
  processes. Application data, DDL, settings, diagnostics, backups, and files
  are exercised with explicit SQL statements, including `MURMUR ...` commands.
- Two fixture packages remain: `harness/` wraps a Rust client connection with
  process/fault management and replayable-write retry; `sql-harness/` returns
  the Rust client directly. Neither links the embedded database or opens data
  directories. The attack fixture reuses the engine's replication protocol
  crate only to send hostile QUIC traffic; honest database operations use SQL.
- Scheduler, persistence, and replication-counter assertions read the existing
  engine snapshot through `MURMUR SHOW STATUS`. The shell projects that snapshot
  without implementing engine behavior. `MURMUR SHOW PEERS` verifies topology.
- The root `live_sql_architecture` test prevents embedded dependencies and
  transaction closures from returning, requires both harnesses to use the Rust
  client, and checks that the runner lists every scenario.

## 2. Exact layout

All paths absolute. `tests-live/` is NOT a cargo workspace and NOT part of the
root package: every scenario (and each harness) is a standalone package so it
compile-checks alone without touching product manifests.

```text
/home/marc/MURMUR-SQL/tests-live/
  README.md                    # this file
  run.sh                       # sequential runner (all/gate/soak/stress/single; port agents NEVER run it)
  .gitignore                   # runtime/ failures/ target/
  harness/                     # classic package `murmur-live-harness` (TODO item 1)
    Cargo.toml                 # lib only; registry deps (tokio, rcgen, ...) + murmur-client path dep + murmur-replication path dep (attacker fixture only)
    src/lib.rs                 # re-exports: tokio_postgres, replication (product wire impl), Cluster, ClusterOptions, Error, env_seconds, wait_until(_async)
    src/cluster.rs             # Cluster / Node multi-process management + NodeClient wire wrapper
    src/certs.rs               # rcgen CA + per-node certs (SAN URI replicateddb://node/<hex>)
    src/attacker.rs            # raw-QUIC hostile-peer fixture reusing the product wire impl (corrupt-snapshot, origin-signatures)
    src/unlock.rs              # connect + MURMUR UNLOCK with stall/overall env watchdog
    src/forensics.rs           # dump_forensics via SHOW REPLICATION/PEERS/METRICS (never fails the test)
    src/artifacts.rs           # runtime/ lifecycle, failures/<ts>-<scenario>/ retention, RESULT line
    src/ports.rs               # free SQL-port picking (replication binds :0)
  sql-harness/                 # process package `murmur-live-sql-harness` (murmur-client connections)
    Cargo.toml                 # lib only; murmur-client path dep (../../clients/rust) + tokio/rcgen/ed25519-dalek/rand/toml_edit
    src/lib.rs                 # Cluster/ClusterOptions/Node over murmur-server children; node(i) -> &Client
    README.md                  # options, lifecycle, env knobs (MURMUR_SERVER_BIN/MURMUR_ADMIN_BIN/MURMUR_LIVE_RUNTIME)
  <scenario>/                  # one dir per TODO checkbox, e.g. three-node-sync/
    Cargo.toml                 # package murmur-live-<scenario>; ONE harness path dep via the alias convention below
    tests/<scenario_snake>.rs  # async #[tokio::test] fns (env-gated ones early-return SKIP)
    README.md                  # port notes: Go source, env knobs, pass criteria, deviations
  runtime/                     # created at run time, gitignored
  failures/                    # created at run time, gitignored
```

Alias convention: both harnesses are imported as `murmur_live_harness` so
test bodies keep `use murmur_live_harness::{Cluster, ClusterOptions};`
whichever fixture they use:

```toml
# classic harness scenario
murmur-live-harness = { path = "../harness" }
# SQL-harness scenario (same import path in tests/)
murmur-live-harness = { package = "murmur-live-sql-harness", path = "../sql-harness" }
```

SQL-harness scenarios may also add `murmur-client = { path = "../../clients/rust" }`
for direct client types. Classic-harness scenarios reach product wire types
via `harness::replication`, never a direct `murmur-replication` dep.

## 3. Go harness capability map (Go -> Rust)

| Go (`tests-live/harness/`, `run.sh`) | Rust equivalent | Status |
|---|---|---|
| `Cluster`/`Node`, `StartNode`/`StopNode` (`agent --config`, SIGTERM + SIGKILL 5s, PID sweep) | `harness::Cluster`: one `murmur-server` child per node; `stop_node` = SIGTERM + SIGKILL fallback, `kill_node` = immediate kill, `start_node` = respawn same dir+key+identity on fresh ports; `run.sh` PID sweep scoped to its runtime dir | harness prereq |
| Port-claim registry (`${TMPDIR}/spedsql-portclaims`, 4h TTL) | **Not needed**: SQL ports are picked free per (re)start, replication binds `127.0.0.1:0` (discovered via `MURMUR SHOW REPLICATION`) | dropped by design |
| Cert provisioning (`transport.GenerateCA`, `IssueNode`, `tls/` per node) | `harness::certs` via `rcgen` (CA + SAN-URI leaves, PEM files); keep `tls/` files per node for forensics parity | harness prereq |
| Await-unlock + `/v1/admin/open-progress` polling (`MURMUR_LIVE_UNLOCK_STALL_SECONDS=120`, `...TIMEOUT_SECONDS=1800`, stall-vs-slow watchdog) | `harness::unlock`: TCP connect + `MURMUR UNLOCK` with same two env knobs driving a redial-progress watchdog (any error change resets the stall timer; one UNLOCK runs to completion: no intra-open progress API exists over the wire) | harness prereq |
| `Cluster.DumpForensics` (`/v1/status`, `/v1/debug/peers`, `/metrics`, `/v1/debug/stacks`, 10s bound each, `forensics-<reason>-*` files) | `cluster.dump_forensics(reason).await`: `MURMUR SHOW REPLICATION/PEERS/METRICS` per node (10 s bound each) -> truncated test log + full per-node files; every fetch infallible | harness prereq |
| Failure artifacts (`runtime/<scenario>/` -> `failures/<ts>-<scenario>/` on fail, else removed; `RESULT: PASS/FAIL` line) | `harness::artifacts`: identical convention under `tests-live/runtime/` and `tests-live/failures/` | harness prereq |
| Fixture `testnode` binary (HTTPS daemon, `MURMUR_BIN` override, `.tags` freshness rebuild) | **Dropped**: crash/signal legs act on the server children directly (`kill_node`, `suspend_node`/`resume_node`, `node_pid`); `MURMUR_SERVER_BIN` reuses a prebuilt `--features replication` server, else the harness builds once per process | dropped by design |
| `run.sh` (per-scenario `go test -timeout`, `all`/`gate`/`soak`/`stress` sets, runtime-dir daemon sweep trap, `MURMUR_LIVE_RUNTIME` isolation) | `tests-live/run.sh`: reuses `MURMUR_SERVER_BIN` or builds server bins, sequential per-scenario `cargo test --manifest-path`, same set names, server-PID sweep scoped to its runtime dir | harness prereq |

## 4. Shared harness API sketch

```rust
// murmur-live-harness (tests-live/harness/src/lib.rs)
pub use murmur_client;
pub use murmur_client::tokio_postgres; // wire row/parameter types only

pub enum Dissemination { Gossip, Plumtree }
pub enum MergePolicy { Lww, Counter, Set, Min, Max }

#[derive(Default)]
pub struct ReplicationOptions {
    pub retention: Option<Duration>, pub retention_interval: Option<Duration>,
    pub minimum_retained: Option<u64>, pub fanout: Option<usize>,
    pub dissemination: Option<Dissemination>,
    pub max_transaction_bytes: Option<usize>, pub membership: Option<bool>,
    pub maximum_offline: Option<Duration>,
    pub merge_policies: Option<BTreeMap<(String, String), MergePolicy>>,
}

#[derive(Default)]
pub struct ClusterOptions {
    pub name: String,                       // scenario name; runtime/<name>/
    pub num_nodes: usize,                   // default 3
    pub database_id: Option<[u8; 16]>,      // default: random per cluster
    pub node_ids: Vec<[u8; 16]>,            // default: random per node
    pub key: [u8; 32],                      // default: fixed test key
    pub replication: Option<ReplicationOptions>,
    pub sqlite: Option<SqliteOptions>,      // [engine] budgets
    pub spool_by_node: HashMap<usize, SpoolOptions>,
    pub manual_peers: bool,                 // skip auto full-mesh; test calls add_peer
    pub bootstrap_seeds: Vec<usize>,        // seed indices get bootstrap addrs
    pub allowed_peers: HashMap<usize, BTreeSet<[u8; 16]>>,
    pub allowed_networks: HashMap<usize, Vec<IpNet>>,
    pub trusted_snapshot_sources: HashMap<usize, BTreeSet<[u8; 16]>>,
    pub extra_trusted_origins: HashMap<usize, BTreeMap<[u8; 16], [u8; 32]>>,
    pub dissemination_by_node: HashMap<usize, Dissemination>,
    pub files: Option<FilesOptions>,        // [replication.files] per node
    pub bridge: BridgeOptions,              // [replication.bridge] per node
    pub node_cert_ttl: HashMap<usize, Duration>,
    pub extra_toml: String,                 // complete sections for every node
    pub extra_toml_by_node: HashMap<usize, String>,
}

pub struct Node {
    pub index: usize, pub label: String,    // node1..nodeN
    pub dir: PathBuf, pub data_dir: PathBuf,
    pub node_id: [u8; 16],
    pub repl_addr: SocketAddr,              // discovered; changes across restarts
    pub sql_addr: SocketAddr,               // picked free; changes across restarts
}

pub struct NodeClient { /* one unlocked wire connection */ }
impl NodeClient {
    pub async fn execute(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> murmur_client::Result<u64>;
    pub async fn query(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> murmur_client::Result<Vec<Row>>;
    pub async fn simple(&self, sql: &str) -> murmur_client::Result<Vec<Vec<String>>>; // NULL -> ""
    pub fn inner(&self) -> &tokio_postgres::Client;
}

impl Cluster {
    pub async fn new(opts: ClusterOptions) -> Result<Self, harness::Error>;
    pub async fn add_peer(&self, from: usize, target: usize) -> Result<()>;
    pub async fn add_peer_addr(&self, from: usize, addr: SocketAddr) -> Result<()>;
    pub async fn remove_peer(&self, from: usize, target: usize) -> Result<()>;
    pub async fn retire_peer(&self, from: usize, target: usize) -> Result<()>;
    pub async fn sync_all(&self) -> Result<()>;
    pub async fn mesh(&self) -> Result<()>;
    pub async fn stop_node(&mut self, idx: usize);                     // SIGTERM + SIGKILL fallback, keep dir+key
    pub async fn kill_node(&mut self, idx: usize);                     // immediate kill (crash legs)
    pub async fn suspend_node(&self, idx: usize) -> Result<()>;        // unix SIGSTOP
    pub async fn resume_node(&self, idx: usize) -> Result<()>;         // unix SIGCONT
    pub async fn start_node(&mut self, idx: usize) -> Result<()>;      // respawn same dir+key (+unlock watchdog)
    pub async fn start_nodes_parallel(&mut self, idxs: &[usize]) -> Result<()>;
    pub fn node(&self, idx: usize) -> &NodeClient;                     // panics if stopped (test bug)
    pub fn node_info(&self, idx: usize) -> &Node;
    pub fn node_pid(&self, idx: usize) -> Option<u32>;
    pub async fn ensure_migrated(&self, idx: usize, sql: &str) -> Result<()>;
    pub fn set_spool_options(&mut self, idx: usize, spool: Option<SpoolOptions>) -> Result<()>;
    pub fn set_bridge_config(&mut self, idx: usize, config: Option<BridgeNodeConfig>) -> Result<()>;
    pub fn set_node_key(&mut self, idx: usize, key: [u8; 32]) -> Result<()>;
    pub async fn rotate_node_cert(&mut self, idx: usize, ttl: Option<Duration>) -> Result<()>;
    pub async fn restart_node_with_identity(&mut self, idx: usize, new: [u8; 16]) -> Result<()>;
    pub fn issue_cert(&self, node_id: &[u8; 16]) -> Result<certs::Issued>; // PEM+DER leaf from the cluster CA
    pub fn node_origin_seed(&self, idx: usize) -> [u8; 32];           // origin signing seed (hostile-relay legs)
    pub fn ca_pem(&self) -> String;
    pub fn add_trust_root(&mut self, root_pem: &[u8]);
    pub async fn dump_forensics(&self, reason: &str);                  // infallible by contract
    pub async fn finish(self, failed: bool);                           // kill children, retain-or-clean + RESULT line
}

pub mod certs   { pub fn authority() -> Ca; /* rcgen; issue(&self, node_id, ttl) -> PEM+DER */ }
pub mod attacker { pub struct Attacker; /* new / impersonate / dial; Session send/collect_until/close; forge_corrupt_snapshot */ }
pub mod unlock  { pub async fn connect_with_watchdog(addr, user, pw, key_hex) -> Result<NodeClient>; }
pub mod ports   { pub fn free_port() -> io::Result<u16>; /* SQL listeners only */ }
```

Env knobs (same names as Go): `MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
`MURMUR_LIVE_UNLOCK_STALL_SECONDS` (120), `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS` (1800),
plus `MURMUR_SERVER_BIN` (prebuilt server) and `CARGO_TARGET_DIR` (shared build dir).

## 5. Per-scenario file template

`tests-live/<scenario>/Cargo.toml`:

```toml
[package]
name = "murmur-live-<scenario>"   # e.g. murmur-live-three-node-sync
version = "0.1.0"
edition = "2021"

[dependencies]
murmur-live-harness = { path = "../harness" }

[dev-dependencies]
tokio = { version = "1", features = ["macros", "rt-multi-thread", "time"] }
```

No other harness path dep: classic scenarios use `path = "../harness"`,
SQL-harness scenarios use the alias from section 2. Need a crates.io
helper? Prefer what the harness re-exports (`harness::tokio_postgres` on
classic, `murmur_client::{Client, Config}` on the SQL harness); otherwise
add the registry dep to the scenario only.

`tests-live/<scenario>/tests/<scenario_snake>.rs`:

```rust
use murmur_live_harness::{Cluster, ClusterOptions};

async fn run_body(cluster: &Cluster) -> Result<(), String> {
    // ... port body: SQL, add_peer/remove_peer, stop/start, waits with deadlines ...
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn <scenario_snake>() {
    let cluster = Cluster::new(ClusterOptions {
        name: "<scenario>".into(),
        num_nodes: 3,
        ..Default::default()
    })
    .await
    .expect("cluster setup");
    let result = run_body(&cluster).await;
    let failed = result.is_err();
    if let Err(e) = &result {
        eprintln!("scenario failed: {e}");
    }
    if failed {
        cluster.dump_forensics("<short-reason>").await;
    }
    cluster.finish(failed).await;
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}
```

Return `Result` from the body instead of panicking mid-flight so every exit
path still reaches `dump_forensics`/`finish` (which kill the children).
`finish` also runs from `Drop` on panic paths, and `run.sh` sweeps
stragglers scoped to its runtime dir.

Env-gated scenarios (`clock-skew`, `diskfull-live`, `impaired-network`) start
the test body with a capability probe and early `return` + `eprintln!("SKIP ...")`
when absent (same skip semantics as Go).

`tests-live/<scenario>/README.md`: Go source dir, what it proves, env knobs,
pass criteria, and any deviation from the Go original (e.g. SQL instead of
typed HTTP API, PG-wire TLS instead of HTTPS legs).

Replicated tables need `MURMUR SET TABLE <name> REPLICATED true` on EACH
node (the flag is per-node config). Restarts preserve `[replication.tables]`
automatically; fresh ports require a re-mesh afterwards.

## 6. Compile gate and runtime validation

Per-scenario compile gate (port agents): run exactly this, with the
scenario's absolute manifest path, from anywhere:

```sh
CARGO_TARGET_DIR=/home/marc/MURMUR-SQL/tests-live/target \
  cargo test --no-run --manifest-path /home/marc/MURMUR-SQL/tests-live/<scenario>/Cargo.toml
```

- `CARGO_TARGET_DIR` is mandatory: without it every scenario duplicates the
  full dependency build (~1.6 GB each; this filled the disk). One shared dir,
  still one scenario checked at a time.
- `--manifest-path` scoping is what makes the scenario check ALONE (its crate
  + its ONE harness (`../harness` or `../sql-harness` via the alias) +
  registry deps, nothing else under `tests-live/`).
- Do NOT run `cargo test` (without `--no-run`), `cargo run`, or any binary
  under `target/`; do NOT use `--workspace`; do NOT run `tests-live/run.sh`.
- A green `cargo test --no-run` is the ONLY gate for flipping the TODO checkbox.
- The `flap-partition` pilot is the exception: it runs to green in place
  (`cargo test` with the same `CARGO_TARGET_DIR`) to prove the harness.

Runtime validation (CI/operators, never port agents) is `run.sh`, which
builds `murmur-server` + `murmur-admin` with `--features replication`,
exports `MURMUR_SERVER_BIN`, and runs scenarios sequentially with
per-scenario `timeout(1)` caps:

```sh
tests-live/run.sh <scenario>  # one scenario by directory name
tests-live/run.sh all         # ALL_SCENARIOS in order
tests-live/run.sh gate        # GATE_SCENARIOS release-gate subset
tests-live/run.sh soak        # files-soak + soak-slo + long-running-five-node
tests-live/run.sh stress      # rolling-restart, partial-mesh, crash-recovery,
                              # partition, snapshot-resync x MURMUR_STRESS_COUNT (default 5)
```

Harness-only checks (no scenario): `cargo check --manifest-path
/home/marc/MURMUR-SQL/tests-live/harness/Cargo.toml --all-targets` and the
same for `tests-live/sql-harness/Cargo.toml`.

## 7. Rules for port agents (binding)

1. Never run tests or binaries (except the `flap-partition` pilot). Single-package compile check only (section 6).
2. Sequential single writer: one scenario in flight at a time per checkout.
3. Flip ONLY your own `TESTS-LIVE-TODO.md` checkbox, and only on a clean
   `cargo test --no-run` for your scenario.
4. Never touch product code (`src/`, the `murmur` engine in `../MURMUR`,
   root `Cargo.toml`/`Cargo.lock`) or any other scenario's directory/checkbox.
5. Never fix a missing product API by forking harness code into your scenario;
   shared needs go to the harness prerequisite, product gaps go in your report.
6. If file-edit tools reject absolute paths outside the workspace, use bash
   heredocs (`cat > /abs/path <<'EOF'`) for all writes.
7. Keep the Go scenario's pass criteria; document every deviation in the
   scenario `README.md` (SQL-vs-typed-API, PG-wire-vs-HTTPS, child restart, ...).

## 8. Portability assessment (against current product APIs)

All scenarios drive `murmur-server` children over the PG wire (SQL +
`MURMUR PEER/SHOW/RETENTION/POLICY/FILE/BACKUP` commands) and Unix signals
where noted. Crash (`kill_node`), restart (`stop_node`/`start_node`),
suspend (`suspend_node`, unix), and parallel-restart (`start_nodes_parallel`)
legs work directly on the children with no fixture binary.

Portable now: three-node-sync, partition, chaos-load,
flap-partition, three-way-heal, tail-repair, partial-mesh, swim-discovery,
plumtree-live (`Dissemination::Plumtree`; mixed modes fail closed),
scale-mesh (fanout), long-running-five-node, txchunk-resume
(`max_transaction_bytes` + snapshot resume), corrupt-snapshot
(`trusted_snapshot_sources`), large-payload, crdt-contention, typed-records
(`MURMUR POLICY` + subscriptions), typed-bridge, bridge-two-streams,
merge-policies, delete-pruning, views, schema-evolution,
migration-concurrency, migration-crash, clock-skew (env-gated),
crash-recovery, crash-loop, rejoin-storm, rolling-restart, pause-resume
(`suspend_node`), churn-retirement (`retire_peer`), snapshot-resync,
open-progress (connect watchdog; no intra-open phases over the wire),
compression, gc-balance, log-boundedness, maintenance-under-load,
loadshare, write-priority, soak-slo, cert-lifecycle (rotation =
`rotate_node_cert`; expiry enforced by the QUIC verifiers), allow-nodes
(`allowed_peers`), addrpolicy (`allowed_networks`), dbid-isolation,
version-skew, hostile-peer, hostile-schema (signed manifests), encryption
(`WrongKey` on unlock), plaintext-audit, rekey (`MURMUR ROTATE`),
file-permissions (0600/0700 in data/backup), backup-restore,
backup-under-fire, tampered-backup, downgrade-guard, files-bridge,
files-soak, files-crash, files-corrupt-source, highlow (bridge
export/stage/process/release), origin-signatures (`trusted_origins`),
abuse, endurance-chaos, resource-exhaustion, diskfull-live (env-gated),
impaired-network (env-gated), graceful-shutdown (`stop_node` losslessness;
SIGTERM is the default stop path), unlock-abuse (`MURMUR UNLOCK` key
auth), tls-floor (PG-wire TLS versions), api-mtls (PG-wire mTLS;
production mode), dos-client (PG-wire + QUIC flood legs), search-mesh
(managed FTS/vector/JSON over the mesh; node 0 is schema master).

Needs product-interface decisions (HTTP legs have no Rust equivalent; port
the wire-level intent instead):

- `sqli-api` — no HTTP routes exist, so no 410s to assert; the PG wire
  rejects non-SQL input at parse time instead.
- `overload-budgets` — HTTP 1 MiB body-cap leg has no equivalent; the value
  and concurrency/p99 legs port to wire/server budgets.

## 9. Harness public API (for scenario authors)

Crate `murmur-live-harness` (`harness/`; single path dep for scenarios).
Re-exports `tokio_postgres` so scenarios need no other path deps; use the
registry only for extra helpers (plus the `tokio` dev-dep for `#[tokio::test]`).

```rust
use murmur_live_harness::{Cluster, ClusterOptions, ReplicationOptions, Dissemination, MergePolicy};
use murmur_live_harness::tokio_postgres::types::ToSql; // param refs
// identity/admission overlays use std HashMap/BTreeSet + ipnet::IpNet
// (the harness depends on ipnet; scenarios add it only if naming the type).
```

- `Cluster::new(ClusterOptions { name, num_nodes: 3, database_id?,
  node_ids?, key: [9;32], replication?, sqlite?, spool_by_node?,
  manual_peers, bootstrap_seeds?, allowed_peers?, allowed_networks?,
  trusted_snapshot_sources?, extra_trusted_origins?,
  dissemination_by_node?, files?, bridge?, node_cert_ttl?, extra_toml?,
  extra_toml_by_node? }).await`:
  wipes + rebuilds `runtime/<name>/`, provisions a test CA + per-node
  certs (`tls/` per node), writes per-node `config.toml` files (free SQL
  ports, `:0` replication binds, shared database id, distinct node ids,
  mutual `trusted_origins`), starts nodes under the unlock watchdog (seed
  indices first, others get seed `bootstrap` addrs), then full-meshes
  unless `manual_peers`.
- `cluster.node(i) -> &NodeClient` (panics if stopped: a test bug) with
  `execute`/`query`/`simple`/`inner`; `node_info(i) -> &Node`
  (label/dir/data_dir/node_id/repl_addr/sql_addr, also while stopped),
  `node_id(i)`, `node_origin_pubkey(i)`, `node_pid(i)`, `len()`, `name()`,
  `runtime_dir()`, `database_id()`.
- `add_peer(from, target).await`, `add_peer_addr(from, SocketAddr).await`
  (dial an explicit address, e.g. through a throttle proxy),
  `remove_peer(from, target).await`, `retire_peer(from, target).await`,
  `sync_all().await`, `mesh().await` (redial every ordered pair of
  *running* nodes; use after heals and restarts), `stop_node(i).await`,
  `kill_node(i).await` (crash legs), `suspend_node(i).await` /
  `resume_node(i).await` (unix pause legs), `start_node(i).await`
  (respawn same dir+key; rebinds fresh ports and preserves
  `[replication.tables]`, so re-mesh afterwards),
  `start_nodes_parallel(idxs).await` (barrier restarts),
  `set_spool_options(i, Option<SpoolOptions>)` (stored; next start
  applies it), `set_bridge_config(i, Option<BridgeNodeConfig>)`
  (stored; next start applies it), `set_node_key(i, key)` (stored;
  next start unlocks with it),
  `rotate_node_cert(i, ttl?).await` (reissue the leaf for the same
  NodeID, rewrite `tls/`, restart; re-mesh afterwards),
  `restart_node_with_identity(i, new_id).await` (fresh NodeID + origin
  key + leaf; the data dir must already carry the fresh identity;
  re-mesh afterwards),
  `ca_pem()` + `add_trust_root(pem)` (cross-trust legs: exchange CA
  PEMs, restart all nodes, re-mesh), `ca_der()`, `issue_cert(node_id)`
  (PEM leaf for an external identity).
- `ensure_migrated(i, sql).await` (fixture DDL with converged-schema skip
  for nodes past 0), `dump_forensics(reason).await` (infallible; call
  inside the failure branch), `finish(failed).await` (kills + reaps
  children, retains-or-cleans runtime, prints `RESULT: PASS/FAIL`).
- `murmur_live_harness::wait_until(timeout, poll, cond)` and
  `wait_until_async(timeout, poll, cond)` for convergence waits;
  `env_seconds(name, fallback)` for duration knobs; `hex(bytes)` for ids;
  `Error`/`Result` for harness + wire errors (match
  `tokio_postgres::Error::code()` for SQLSTATEs).
- `certs::Ca::{generate, issue(node_id, ttl?), ca_pem, ca_der,
  write_tls_dir}` for custom-cert legs.
- `unlock::connect_with_watchdog(addr, user, password, key_hex)` for
  connections outside `Cluster` (same stall/overall env knobs).
- `artifacts::{runtime_dir, failures_dir, prepare_runtime_dir, utc_stamp}`.
- `ports::{free_addr, free_port}` (best-effort free-port picking for SQL
  listeners; replication binds `:0` directly).

No workspace wiring: the root `Cargo.toml` is untouched; each package checks alone via
`cargo test --no-run --manifest-path
/home/marc/MURMUR-SQL/tests-live/<scenario>/Cargo.toml` (each harness itself:
`cargo check --manifest-path
/home/marc/MURMUR-SQL/tests-live/harness/Cargo.toml --all-targets`, same for
`tests-live/sql-harness/Cargo.toml`). SQL-harness API details
(`ClusterOptions`, `node`/`connect`, `stop/start_node`, `set_peers`,
`assert_rows`) live in `tests-live/sql-harness/README.md`; `MURMUR ...`
command syntax lives in `docs/USAGE.md#murmur-commands` and the
`murmur-client` helper table in `clients/rust/README.md`.

## SQL migration validation

Compile every scenario independently with one shared `CARGO_TARGET_DIR`; use
`cargo test --no-run --manifest-path tests-live/<scenario>/Cargo.toml`.
Runtime checks must use `tests-live/run.sh <scenario>` (or `all`, `gate`,
`soak`, `stress`). Missing scenario manifests are errors rather than skips.
The runner accepts `MURMUR_SERVER_BIN` and `MURMUR_ADMIN_BIN` for prebuilt
replication-enabled binaries. A compile sweep does not replace runtime fault
checks; shortened soak runs do not establish long-duration stability.

The downgrade guard's future-format corruption fixture is generated by a separate
engine-owned ignored unit-test executable. `run.sh downgrade-guard` builds it
before the scenario, or reuses `MURMUR_FORMAT_FIXTURE_BIN`. The live scenario
itself links no engine/Spool implementation and verifies refusal and recovery
over SQL. This requires the matching fixture test in the sibling engine checkout.
