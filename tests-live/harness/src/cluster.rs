//! Multi-process cluster: one `murmur-server` child per node, driven over the
//! PostgreSQL wire protocol.
//!
//! A "node" is a child process on its own directory
//! (`runtime/<scenario>/nodeN/`) with its own `config.toml`, TLS material
//! (`tls/`), and data dir. Stop, restart, kill, and (on unix) suspend legs
//! act on the child directly, so no fixture binary is needed. SQL runs
//! through [`NodeClient`]; replication topology through `MURMUR PEER ...`
//! commands; unlock through `MURMUR UNLOCK KEY '<64-hex>'`.
//!
//! Restarts pick a fresh SQL port and re-render `config.toml` from the
//! stored inputs, preserving the server-owned `[replication.tables]` section
//! (see `MURMUR SET TABLE`). The replication socket binds `:0`; its address
//! is discovered via `MURMUR SHOW REPLICATION`, so re-mesh (`add_peer` /
//! [`Cluster::mesh`]) after every restart.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use crate::{Error, Result};

pub type NodeId = [u8; 16];

const PG_USER: &str = "admin";
const PG_PASSWORD: &str = "test-password";
const START_TIMEOUT: Duration = Duration::from_secs(20);

/// Decode 64 lowercase hex chars into 32 bytes (origin seeds round-trip
/// through `signing_hex`; `None` on any malformed input).
fn decode_hex_32(hex: &str) -> Option<[u8; 32]> {
    if hex.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[2 * i..2 * i + 2], 16).ok()?;
    }
    Some(out)
}

/// `(server, admin)` binaries: `MURMUR_SERVER_BIN` or a cargo build once per process.
fn binaries() -> &'static (PathBuf, PathBuf) {
    static BINS: OnceLock<(PathBuf, PathBuf)> = OnceLock::new();
    BINS.get_or_init(|| {
        if let Ok(server) = std::env::var("MURMUR_SERVER_BIN") {
            if !server.is_empty() {
                let server = PathBuf::from(server);
                let admin = server.with_file_name(exe_name("murmur-admin"));
                return (server, admin);
            }
        }
        let root = crate::artifacts::tests_live_dir().join("..");
        let status = Command::new("cargo")
            .args(["build", "--features", "replication", "--bin", "murmur-server", "--bin", "murmur-admin"])
            .current_dir(&root)
            .status()
            .expect("run cargo");
        assert!(status.success(), "cargo build of murmur-server failed");
        let target = std::env::var("CARGO_TARGET_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| root.join("target"))
            .join("debug");
        (target.join(exe_name("murmur-server")), target.join(exe_name("murmur-admin")))
    })
}

fn exe_name(base: &str) -> String {
    if cfg!(windows) {
        format!("{base}.exe")
    } else {
        base.to_string()
    }
}

fn password_hash() -> &'static str {
    static HASH: OnceLock<String> = OnceLock::new();
    HASH.get_or_init(|| {
        let out = Command::new(&binaries().1)
            .args(["hash-password", "--password", PG_PASSWORD])
            .output()
            .expect("murmur-admin hash-password");
        assert!(out.status.success(), "hash-password failed");
        let hash = String::from_utf8(out.stdout).unwrap().trim().to_string();
        assert!(hash.starts_with("$argon2id$"), "unexpected hash {hash}");
        hash
    })
    .as_str()
}

/// Merge-policy overlay values (config `replication.merge_policies` strings).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergePolicy {
    Lww,
    Counter,
    Set,
    Min,
    Max,
}

impl MergePolicy {
    fn toml(self) -> &'static str {
        match self {
            MergePolicy::Lww => "lww",
            MergePolicy::Counter => "counter",
            MergePolicy::Set => "set",
            MergePolicy::Min => "min",
            MergePolicy::Max => "max",
        }
    }
}

/// Dissemination modes (config `replication.dissemination`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dissemination {
    Gossip,
    Plumtree,
}

impl Dissemination {
    fn toml(self) -> &'static str {
        match self {
            Dissemination::Gossip => "gossip",
            Dissemination::Plumtree => "plumtree",
        }
    }
}

/// Overlays rendered into every node's `[replication]` section.
#[derive(Debug, Clone, Default)]
pub struct ReplicationOptions {
    pub retention: Option<Duration>,
    /// Collection cadence (`retention_interval`, default 60 s).
    /// GC-balance legs set this to seconds so background passes run inside
    /// the measurement window instead of starving it.
    pub retention_interval: Option<Duration>,
    pub minimum_retained: Option<u64>,
    pub fanout: Option<usize>,
    pub dissemination: Option<Dissemination>,
    pub max_transaction_bytes: Option<usize>,
    /// Send/apply batching unit (`batch_bytes`, default 4 MiB). Sessions
    /// larger than this ship as multiple signed transactions; txchunk legs
    /// raise it so a multi-MiB trial ships and applies as one unit.
    pub batch_bytes: Option<usize>,
    /// Gossip membership on/off (default true). `Some(false)` holds a static
    /// topology (no discovery dialing); transaction push still runs over the
    /// peer sessions.
    pub membership: Option<bool>,
    /// Offline-peer memory for retention floors (default 7 days). Peers
    /// unseen longer than this stop pinning history, so survivors can GC
    /// ranges a stale node still needs (snapshot legs).
    pub maximum_offline: Option<Duration>,
    /// Merge-policy overlays: (table, column) -> policy, rendered into every
    /// node's config before start. Policies bind at open, so CRDT legs
    /// (typed-records, merge-policies) must set this; `None`/empty leaves
    /// LWW everywhere.
    pub merge_policies: Option<BTreeMap<(String, String), MergePolicy>>,
}

/// Encrypted file/object store for every node (per-node dir under the node).
#[derive(Debug, Clone)]
pub struct FilesOptions {
    pub key: [u8; 32],
    pub fetch_interval: Option<Duration>,
    pub max_file_bytes: Option<u64>,
}

impl Default for FilesOptions {
    fn default() -> Self {
        Self {
            key: [61; 32],
            fetch_interval: None,
            max_file_bytes: None,
        }
    }
}

/// Commit-path budget overlays rendered into every node's `[engine]` section.
#[derive(Debug, Clone, Default)]
pub struct SqliteOptions {
    pub transaction_bytes: Option<usize>,
    pub queue_bytes: Option<usize>,
    pub capture_bytes: Option<usize>,
    /// Background session-capture cadence (`capture_interval`, default
    /// 1 s). Explicit flushes capture regardless; txchunk legs stretch
    /// this so a multi-MiB trial ships as one capture on its flush.
    pub capture_interval: Option<Duration>,
}

/// Per-node spool storage overrides, keyed by node index (Go `SpoolByNode`
/// parity; `resource-compaction` legs size victim segments down).
#[derive(Debug, Clone, Default)]
pub struct SpoolOptions {
    /// Spool segment rotation size (`segment_bytes`, default 256 MiB).
    pub segment_bytes: Option<usize>,
}

/// One node's `[replication.bridge]` section: a Low exporter or High
/// receiver. Paths are used as given (scenarios share drop directories
/// between Low exporters and High receivers); the harness does not rewrite
/// them. Unset optionals take server defaults.
#[derive(Debug, Clone, Default)]
pub struct BridgeNodeConfig {
    /// `low-exporter` or `high-receiver`.
    pub role: String,
    /// Database id the bridge serves, 32 hex chars.
    pub domain: String,
    pub trusted_domains: Vec<String>,
    /// Node id (32 hex) to Ed25519 public key (64 hex).
    pub trusted_origins: BTreeMap<String, String>,
    pub directory: String,
    pub stream: String,
    /// X25519 recipient public key, 64 hex chars.
    pub recipient: String,
    pub receiver_secret_hex: Option<String>,
    pub signer_hex: Option<String>,
    pub trusted_signers: BTreeMap<String, String>,
    pub max_bundle_bytes: Option<usize>,
    pub max_payload_bytes: Option<usize>,
    pub staging_bytes: Option<usize>,
}

/// Per-node bridge configs, keyed by node index.
#[derive(Debug, Default)]
pub struct BridgeOptions {
    pub nodes: HashMap<usize, BridgeNodeConfig>,
}

/// Cluster construction options.
#[derive(Debug)]
pub struct ClusterOptions {
    /// Scenario name; owns `runtime/<name>/`.
    pub name: String,
    /// Node count. Default 3.
    pub num_nodes: usize,
    /// Shared database id. Default: random per cluster.
    pub database_id: Option<[u8; 16]>,
    /// Node ids. Default: random per node (must be `num_nodes` long if set).
    pub node_ids: Vec<[u8; 16]>,
    /// Unlock key for every node. Default: fixed test key `[9; 32]`
    /// (hex-encoded for `MURMUR UNLOCK`, so used raw).
    pub key: [u8; 32],
    /// Replication overlays for every node.
    pub replication: Option<ReplicationOptions>,
    /// Commit-path budget overlays for every node.
    pub sqlite: Option<SqliteOptions>,
    /// Per-node spool storage overrides.
    pub spool_by_node: HashMap<usize, SpoolOptions>,
    /// Skip the automatic full mesh; the test calls [`Cluster::add_peer`].
    pub manual_peers: bool,
    /// Seed indices: started first, and every other node dials them via
    /// `bootstrap` addrs (SWIM/seed-discovery legs). Empty disables seeding.
    pub bootstrap_seeds: Vec<usize>,
    /// Per-node identity allow-lists (`allowed_peers`).
    pub allowed_peers: HashMap<usize, BTreeSet<[u8; 16]>>,
    /// Per-node address-policy overlays (`allowed_networks`).
    pub allowed_networks: HashMap<usize, Vec<ipnet::IpNet>>,
    /// Per-node snapshot-donor trust (`trusted_snapshot_sources`).
    /// Empty by default: nodes request snapshots from nobody. Snapshot
    /// scenarios list explicit donor ids here.
    pub trusted_snapshot_sources: HashMap<usize, BTreeSet<[u8; 16]>>,
    /// Per-node extra entries merged into `trusted_origins` (e.g. an
    /// attacker signing key for hostile-source legs).
    pub extra_trusted_origins: HashMap<usize, BTreeMap<[u8; 16], [u8; 32]>>,
    /// Per-node dissemination-mode overrides (mixed-mode legs; wins over
    /// the global `replication.dissemination` for the listed nodes).
    pub dissemination_by_node: HashMap<usize, Dissemination>,
    /// File/object store for every node.
    pub files: Option<FilesOptions>,
    /// Bridge configs for individual nodes.
    pub bridge: BridgeOptions,
    /// Per-node leaf-cert TTL overrides (default validity is effectively
    /// unbounded; `cert-lifecycle` uses short TTLs for expiry legs).
    pub node_cert_ttl: HashMap<usize, Duration>,
    /// Raw TOML appended to every node's config (complete sections only;
    /// must not reopen `[replication]`).
    pub extra_toml: String,
    /// Per-node raw TOML appended after [`ClusterOptions::extra_toml`].
    pub extra_toml_by_node: HashMap<usize, String>,
}

impl Default for ClusterOptions {
    fn default() -> Self {
        Self {
            name: String::new(),
            num_nodes: 3,
            database_id: None,
            node_ids: Vec::new(),
            key: [9; 32],
            replication: None,
            sqlite: None,
            spool_by_node: HashMap::new(),
            manual_peers: false,
            bootstrap_seeds: Vec::new(),
            allowed_peers: HashMap::new(),
            allowed_networks: HashMap::new(),
            trusted_snapshot_sources: HashMap::new(),
            extra_trusted_origins: HashMap::new(),
            dissemination_by_node: HashMap::new(),
            files: None,
            bridge: BridgeOptions::default(),
            node_cert_ttl: HashMap::new(),
            extra_toml: String::new(),
            extra_toml_by_node: HashMap::new(),
        }
    }
}

/// One cluster node: dirs, identity, and addresses.
#[derive(Debug, Clone)]
pub struct Node {
    pub index: usize,
    /// `node1` .. `nodeN`.
    pub label: String,
    /// `runtime/<scenario>/nodeN/` (config, `tls/`, forensics files).
    pub dir: PathBuf,
    /// Database storage dir (`<dir>/data/`).
    pub data_dir: PathBuf,
    pub node_id: [u8; 16],
    /// Bound replication address (ephemeral; changes across restarts).
    pub repl_addr: SocketAddr,
    /// SQL listen address (picked free; changes across restarts).
    pub sql_addr: SocketAddr,
}

/// One unlocked wire connection to a node. Share by reference; concurrent
/// queries multiplex over the connection.
#[derive(Clone)]
pub struct NodeClient {
    client: std::sync::Arc<murmur_client::Client>,
}

impl NodeClient {
    pub(crate) fn new(client: murmur_client::Client) -> Self {
        Self { client: std::sync::Arc::new(client) }
    }

    /// The underlying driver client for application SQL.
    pub fn inner(&self) -> &murmur_client::tokio_postgres::Client {
        self.client.inner()
    }

    /// Run a statement with `$1`, `$2`, ... parameters.
    pub async fn execute(
        &self,
        sql: &str,
        params: &[&(dyn murmur_client::tokio_postgres::types::ToSql + Sync)],
    ) -> murmur_client::Result<u64> {
        self.client.execute(sql, params).await
    }

    /// Run a statement, retrying replayable `40001` pressure errors with
    /// backoff until `timeout` (the documented contract: back off and
    /// retry; e.g. a write immediately after a budget rejection can trip
    /// momentary queue pressure). Non-`40001` errors return immediately.
    pub async fn execute_retry(
        &self,
        sql: &str,
        params: &[&(dyn murmur_client::tokio_postgres::types::ToSql + Sync)],
        timeout: Duration,
    ) -> murmur_client::Result<u64> {
        let end = tokio::time::Instant::now() + timeout;
        loop {
            match self.execute(sql, params).await {
                Ok(n) => return Ok(n),
                Err(e) => {
                    let retryable = e.code() == Some("40001");
                    if !retryable || tokio::time::Instant::now() >= end {
                        return Err(e);
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }

    /// Run a query with `$1`, `$2`, ... parameters.
    pub async fn query(
        &self,
        sql: &str,
        params: &[&(dyn murmur_client::tokio_postgres::types::ToSql + Sync)],
    ) -> murmur_client::Result<Vec<murmur_client::tokio_postgres::Row>> {
        self.client.query(sql, params).await
    }

    /// Run a parameterless command through the simple protocol; every value
    /// is returned as text (`NULL` becomes an empty string).
    pub async fn simple(
        &self,
        sql: &str,
    ) -> murmur_client::Result<Vec<Vec<String>>> {
        self.client.simple(sql).await
    }


}

/// Per-node config inputs, stored for restarts. `config.toml` is re-rendered
/// from these on every (re)start; the server-owned `[replication.tables]`
/// section is preserved across renders.
#[derive(Debug, Clone)]
struct NodeInputs {
    key: [u8; 32],
    database_id: [u8; 16],
    node_id: [u8; 16],
    signing_hex: String,
    trusted_origins: BTreeMap<NodeId, [u8; 32]>,
    chain_file: PathBuf,
    key_file: PathBuf,
    ca_file: PathBuf,
    bootstrap: Vec<SocketAddr>,
    retention: Option<Duration>,
    retention_interval: Option<Duration>,
    minimum_retained: Option<u64>,
    fanout: Option<usize>,
    dissemination: Option<Dissemination>,
    max_transaction_bytes: Option<usize>,
    batch_bytes: Option<usize>,
    membership: Option<bool>,
    maximum_offline: Option<Duration>,
    merge_policies: BTreeMap<(String, String), MergePolicy>,
    allowed_peers: BTreeSet<NodeId>,
    allowed_networks: Vec<ipnet::IpNet>,
    trusted_snapshot_sources: BTreeSet<NodeId>,
    files: Option<FilesOptions>,
    bridge: Option<BridgeNodeConfig>,
    sqlite: Option<SqliteOptions>,
    spool: Option<SpoolOptions>,
    extra_toml: String,
}

struct NodeState {
    node: Node,
    config_path: PathBuf,
    child: Option<Child>,
    client: Option<NodeClient>,
    inputs: NodeInputs,
}

fn toml_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn toml_escape(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

fn fmt_duration(duration: Duration) -> String {
    let ms = duration.as_millis();
    for (mult, suffix) in [(86_400_000u128, "d"), (3_600_000, "h"), (60_000, "m"), (1_000, "s")] {
        if ms > 0 && ms % mult == 0 {
            return format!("\"{}{suffix}\"", ms / mult);
        }
    }
    format!("\"{ms}ms\"")
}

fn hex_list(ids: &BTreeSet<NodeId>) -> String {
    ids.iter().map(|id| format!("\"{}\"", crate::hex(id))).collect::<Vec<_>>().join(", ")
}

fn render_config(
    inputs: &NodeInputs,
    node_dir: &Path,
    sql_port: u16,
    password_hash: &str,
    tables_section: &str,
) -> String {
    let mut cfg = format!(
        "[server]\nbind = \"127.0.0.1:{sql_port}\"\nproduction = false\n\n\
         [storage]\ndata_dir = \"{}\"\ndatabase_id = \"{}\"\nnode_id = \"{}\"\n\n\
         [[auth.users]]\nname = \"admin\"\npassword_hash = \"{}\"\n\n\
         [logging]\ndir = \"{}\"\n\n[backups]\ndir = \"{}\"\n\n",
        toml_path(&node_dir.join("data")),
        crate::hex(&inputs.database_id),
        crate::hex(&inputs.node_id),
        password_hash,
        toml_path(&node_dir.join("logs")),
        toml_path(&node_dir.join("backups")),
    );
    cfg.push_str("[replication]\nenabled = true\nbind = \"127.0.0.1:0\"\n");
    if inputs.bootstrap.is_empty() {
        cfg.push_str("bootstrap = []\n");
    } else {
        let addrs = inputs.bootstrap.iter().map(|a| format!("\"{a}\"")).collect::<Vec<_>>().join(", ");
        cfg.push_str(&format!("bootstrap = [{addrs}]\n"));
    }
    cfg.push_str(&format!(
        "cert_chain_file = \"{}\"\nprivate_key_file = \"{}\"\nca_file = \"{}\"\nsigning_key_hex = \"{}\"\n",
        toml_path(&inputs.chain_file),
        toml_path(&inputs.key_file),
        toml_path(&inputs.ca_file),
        inputs.signing_hex,
    ));
    let origins = inputs
        .trusted_origins
        .iter()
        .map(|(node, key)| format!("\"{}\" = \"{}\"", crate::hex(node), crate::hex(key)))
        .collect::<Vec<_>>()
        .join(", ");
    cfg.push_str(&format!("trusted_origins = {{ {origins} }}\n"));
    if !inputs.allowed_peers.is_empty() {
        cfg.push_str(&format!("allowed_peers = [{}]\n", hex_list(&inputs.allowed_peers)));
    }
    if !inputs.allowed_networks.is_empty() {
        let nets = inputs.allowed_networks.iter().map(|n| format!("\"{n}\"")).collect::<Vec<_>>().join(", ");
        cfg.push_str(&format!("allowed_networks = [{nets}]\n"));
    }
    if !inputs.trusted_snapshot_sources.is_empty() {
        cfg.push_str(&format!(
            "trusted_snapshot_sources = [{}]\n",
            hex_list(&inputs.trusted_snapshot_sources)
        ));
    }
    if let Some(v) = inputs.membership {
        cfg.push_str(&format!("membership = {v}\n"));
    }
    if let Some(d) = inputs.dissemination {
        cfg.push_str(&format!("dissemination = \"{}\"\n", d.toml()));
    }
    if let Some(v) = inputs.fanout {
        cfg.push_str(&format!("fanout = {v}\n"));
    }
    if let Some(v) = inputs.max_transaction_bytes {
        cfg.push_str(&format!("max_transaction_bytes = {v}\n"));
    }
    if let Some(v) = inputs.batch_bytes {
        cfg.push_str(&format!("batch_bytes = {v}\n"));
    }
    if let Some(v) = inputs.retention {
        cfg.push_str(&format!("retention = {}\n", fmt_duration(v)));
    }
    if let Some(v) = inputs.retention_interval {
        cfg.push_str(&format!("retention_interval = {}\n", fmt_duration(v)));
    }
    if let Some(v) = inputs.minimum_retained {
        cfg.push_str(&format!("minimum_retained = {v}\n"));
    }
    if let Some(v) = inputs.maximum_offline {
        cfg.push_str(&format!("maximum_offline = {}\n", fmt_duration(v)));
    }
    if !inputs.merge_policies.is_empty() {
        let policies = inputs
            .merge_policies
            .iter()
            .map(|((table, column), policy)| {
                format!("\"{}.{}\" = \"{}\"", toml_escape(table), toml_escape(column), policy.toml())
            })
            .collect::<Vec<_>>()
            .join(", ");
        cfg.push_str(&format!("merge_policies = {{ {policies} }}\n"));
    }
    cfg.push('\n');
    if let Some(files) = &inputs.files {
        cfg.push_str(&format!(
            "[replication.files]\ndirectory = \"{}\"\nkey_hex = \"{}\"\n",
            toml_path(&node_dir.join("objects")),
            crate::hex(&files.key),
        ));
        if let Some(v) = files.fetch_interval {
            cfg.push_str(&format!("fetch_interval = {}\n", fmt_duration(v)));
        }
        if let Some(v) = files.max_file_bytes {
            cfg.push_str(&format!("max_file_bytes = {v}\n"));
        }
        cfg.push('\n');
    }
    if let Some(bridge) = &inputs.bridge {
        cfg.push_str(&format!(
            "[replication.bridge]\nrole = \"{}\"\ndomain = \"{}\"\ndirectory = \"{}\"\nstream = \"{}\"\nrecipient = \"{}\"\n",
            toml_escape(&bridge.role),
            toml_escape(&bridge.domain),
            toml_escape(&bridge.directory),
            toml_escape(&bridge.stream),
            toml_escape(&bridge.recipient),
        ));
        if !bridge.trusted_domains.is_empty() {
            let domains = bridge.trusted_domains.iter().map(|d| format!("\"{}\"", toml_escape(d))).collect::<Vec<_>>().join(", ");
            cfg.push_str(&format!("trusted_domains = [{domains}]\n"));
        }
        if !bridge.trusted_origins.is_empty() {
            let origins = bridge
                .trusted_origins
                .iter()
                .map(|(node, key)| format!("\"{}\" = \"{}\"", toml_escape(node), toml_escape(key)))
                .collect::<Vec<_>>()
                .join(", ");
            cfg.push_str(&format!("trusted_origins = {{ {origins} }}\n"));
        }
        if let Some(v) = &bridge.receiver_secret_hex {
            cfg.push_str(&format!("receiver_secret_hex = \"{}\"\n", toml_escape(v)));
        }
        if let Some(v) = &bridge.signer_hex {
            cfg.push_str(&format!("signer_hex = \"{}\"\n", toml_escape(v)));
        }
        if !bridge.trusted_signers.is_empty() {
            let signers = bridge
                .trusted_signers
                .iter()
                .map(|(node, key)| format!("\"{}\" = \"{}\"", toml_escape(node), toml_escape(key)))
                .collect::<Vec<_>>()
                .join(", ");
            cfg.push_str(&format!("trusted_signers = {{ {signers} }}\n"));
        }
        if let Some(v) = bridge.max_bundle_bytes {
            cfg.push_str(&format!("max_bundle_bytes = {v}\n"));
        }
        if let Some(v) = bridge.max_payload_bytes {
            cfg.push_str(&format!("max_payload_bytes = {v}\n"));
        }
        if let Some(v) = bridge.staging_bytes {
            cfg.push_str(&format!("staging_bytes = {v}\n"));
        }
        cfg.push('\n');
    }
    let mut engine = String::new();
    if let Some(sqlite) = &inputs.sqlite {
        if let Some(v) = sqlite.transaction_bytes {
            engine.push_str(&format!("transaction_bytes = {v}\n"));
        }
        if let Some(v) = sqlite.queue_bytes {
            engine.push_str(&format!("queue_bytes = {v}\n"));
        }
        if let Some(v) = sqlite.capture_bytes {
            engine.push_str(&format!("capture_bytes = {v}\n"));
        }
        if let Some(v) = sqlite.capture_interval {
            engine.push_str(&format!("capture_interval = {}\n", fmt_duration(v)));
        }
    }
    if let Some(spool) = &inputs.spool {
        if let Some(v) = spool.segment_bytes {
            engine.push_str(&format!("segment_bytes = {v}\n"));
        }
    }
    if !engine.is_empty() {
        cfg.push_str("[engine]\n");
        cfg.push_str(&engine);
        cfg.push('\n');
    }
    if !inputs.extra_toml.trim().is_empty() {
        cfg.push_str(inputs.extra_toml.trim());
        cfg.push_str("\n\n");
    }
    if !tables_section.trim().is_empty() {
        cfg.push_str(tables_section.trim());
        cfg.push('\n');
    }
    cfg
}

/// Extract the server-owned `[replication.tables]` block from an existing
/// config file so restarts keep `MURMUR SET TABLE` flags.
fn preserved_tables_section(config_path: &Path) -> String {
    let Ok(text) = std::fs::read_to_string(config_path) else {
        return String::new();
    };
    let mut out = Vec::new();
    let mut active = false;
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('[') {
            if active {
                break;
            }
            active = trimmed == "[replication.tables]";
        }
        if active {
            out.push(line);
        }
    }
    if !out.is_empty() {
        return out.join("\n") + "\n";
    }
    let mut in_replication = false;
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('[') {
            in_replication = trimmed == "[replication]";
        } else if in_replication && trimmed.starts_with("tables =") {
            if let Some(content) = trimmed.strip_prefix("tables =") {
                let inner = content.trim().trim_start_matches('{').trim_end_matches('}').trim();
                if !inner.is_empty() {
                    let mut s = String::from("[replication.tables]\n");
                    for pair in inner.split(',') {
                        let pair = pair.trim();
                        if let Some((k, v)) = pair.split_once('=') {
                            let k = k.trim().trim_matches('"');
                            let v = v.trim();
                            s.push_str(&format!("\"{k}\" = {v}\n"));
                        }
                    }
                    return s;
                }
            }
        }
    }
    String::new()
}

/// Tail of the newest server log file for early-exit diagnostics (best effort).
fn server_log_tail(node_dir: &Path) -> String {
    let logs = node_dir.join("logs");
    let Ok(entries) = std::fs::read_dir(&logs) else {
        return String::new();
    };
    let mut newest: Option<(std::time::SystemTime, PathBuf)> = None;
    for entry in entries.flatten() {
        let path = entry.path();
        let modified = entry.metadata().and_then(|m| m.modified()).unwrap_or(std::time::SystemTime::UNIX_EPOCH);
        if newest.as_ref().is_none_or(|(best, _)| modified > *best) {
            newest = Some((modified, path));
        }
    }
    let Some((_, path)) = newest else {
        return String::new();
    };
    let Ok(body) = std::fs::read(&path) else {
        return String::new();
    };
    let tail = body.iter().rev().take(2048).copied().collect::<Vec<_>>().into_iter().rev().collect::<Vec<_>>();
    format!("\n--- {} tail ---\n{}", path.display(), String::from_utf8_lossy(&tail))
}

/// Write the node config, spawn the server, wait for the SQL port, unlock,
/// and discover the bound replication address.
async fn start_process(
    label: &str,
    node_dir: &Path,
    config_path: &Path,
    inputs: &NodeInputs,
    password_hash: &str,
) -> Result<(Child, NodeClient, SocketAddr, SocketAddr)> {
    let sql_port = crate::ports::free_port()
        .map_err(|e| Error::Setup(format!("{label}: no free SQL port: {e}")))?;
    let tables = preserved_tables_section(config_path);
    let config = render_config(inputs, node_dir, sql_port, password_hash, &tables);
    std::fs::write(config_path, config)?;
    let mut child = Command::new(&binaries().0)
        .arg("--config")
        .arg(config_path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    let sql_addr: SocketAddr = format!("127.0.0.1:{sql_port}").parse().unwrap();
    let deadline = Instant::now() + START_TIMEOUT;
    loop {
        match tokio::net::TcpStream::connect(&sql_addr).await {
            Ok(stream) => {
                drop(stream);
                break;
            }
            Err(_) => {
                if let Ok(Some(status)) = child.try_wait() {
                    return Err(Error::Setup(format!(
                        "{label}: server exited early: {status}{}",
                        server_log_tail(node_dir)
                    )));
                }
                if Instant::now() > deadline {
                    let _ = child.kill();
                    return Err(Error::Setup(format!("{label}: server on port {sql_port} did not start")));
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
    let key_hex = crate::hex(&inputs.key);
    let client =
        crate::unlock::connect_with_watchdog(sql_addr, PG_USER, PG_PASSWORD, &key_hex).await?;
    let repl_addr = replication_addr(&client, label).await?;
    Ok((child, client, sql_addr, repl_addr))
}

/// Bound replication address, waiting briefly for the endpoint to come up.
async fn replication_addr(client: &NodeClient, label: &str) -> Result<SocketAddr> {
    let end = Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(rows) = client.simple("MURMUR SHOW REPLICATION").await {
            if let Some(row) = rows.first() {
                if row.len() >= 2 && !row[1].is_empty() {
                    if let Ok(addr) = row[1].parse() {
                        return Ok(addr);
                    }
                }
            }
        }
        if Instant::now() >= end {
            return Err(Error::Setup(format!("{label}: replication endpoint never bound")));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// SIGKILL (portable `Child::kill`) + reap with a bounded wait.
async fn reap(mut child: Child, label: &str) {
    let _ = child.kill();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match child.try_wait() {
            Ok(Some(_)) => return,
            Ok(None) => {
                if Instant::now() >= deadline {
                    eprintln!("{label}: child did not exit after kill");
                    return;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(e) => {
                eprintln!("{label}: child wait failed: {e}");
                return;
            }
        }
    }
}

/// A multi-process cluster: one `murmur-server` child per node.
pub struct Cluster {
    name: String,
    runtime_dir: PathBuf,
    database_id: [u8; 16],
    states: Vec<NodeState>,
    /// Cluster CA, kept so scenarios can issue certs for external
    /// identities (e.g. a hostile peer speaking the wire protocol).
    ca: crate::certs::Ca,
}

impl Drop for Cluster {
    /// Best-effort child cleanup for panic paths that skip [`Cluster::finish`].
    /// `run.sh` sweeps stragglers scoped to the runtime dir as a backstop.
    fn drop(&mut self) {
        for st in &mut self.states {
            st.client.take();
            if let Some(child) = st.child.as_mut() {
                let _ = child.kill();
            }
        }
        let deadline = Instant::now() + Duration::from_secs(2);
        for st in &mut self.states {
            while st.child.as_mut().is_some_and(|c| c.try_wait().ok().flatten().is_none())
                && Instant::now() < deadline
            {
                std::thread::sleep(Duration::from_millis(50));
            }
        }
    }
}

impl Cluster {
    /// Build node dirs + certs, start every node (seeds first), mesh unless
    /// `manual_peers`. Wipes any previous `runtime/<name>/` first.
    pub async fn new(mut opts: ClusterOptions) -> Result<Self> {
        if opts.num_nodes == 0 {
            return Err(Error::Setup("num_nodes must be >= 1".into()));
        }
        if opts.name.is_empty() {
            return Err(Error::Setup("ClusterOptions::name must be set".into()));
        }
        let node_ids: Vec<[u8; 16]> = if opts.node_ids.is_empty() {
            (0..opts.num_nodes).map(|_| rand::random()).collect()
        } else {
            if opts.node_ids.len() != opts.num_nodes {
                return Err(Error::Setup(format!(
                    "node_ids has {} entries for {} nodes",
                    opts.node_ids.len(),
                    opts.num_nodes
                )));
            }
            std::mem::take(&mut opts.node_ids)
        };
        let distinct: HashSet<[u8; 16]> = node_ids.iter().copied().collect();
        if distinct.len() != node_ids.len() {
            return Err(Error::Setup("node ids must be distinct".into()));
        }
        for &s in &opts.bootstrap_seeds {
            if s >= opts.num_nodes {
                return Err(Error::Setup(format!("bootstrap seed {s} out of range")));
            }
        }

        let runtime_dir = crate::artifacts::prepare_runtime_dir(&opts.name)?;
        let database_id: [u8; 16] = opts.database_id.unwrap_or_else(rand::random);

        // Signing keys + trust: every node trusts every node's origin key.
        let seeds: Vec<[u8; 32]> = (0..opts.num_nodes).map(|_| rand::random()).collect();
        let mut trusted_base: BTreeMap<NodeId, [u8; 32]> = BTreeMap::new();
        for (id, seed) in node_ids.iter().zip(seeds.iter()) {
            let public = ed25519_dalek::SigningKey::from_bytes(seed).verifying_key().to_bytes();
            trusted_base.insert(*id, public);
        }

        let ca = crate::certs::Ca::generate()?;
        let hash = password_hash().to_string();

        // Start order: seeds first so their ephemeral addrs can seed the rest.
        let mut order: Vec<usize> = opts.bootstrap_seeds.clone();
        order.sort_unstable();
        order.dedup();
        for i in 0..opts.num_nodes {
            if !order.contains(&i) {
                order.push(i);
            }
        }

        let mut states: Vec<Option<NodeState>> = (0..opts.num_nodes).map(|_| None).collect();
        let mut seed_addrs: Vec<SocketAddr> = Vec::new();
        for &i in &order {
            let label = format!("node{}", i + 1);
            let dir = runtime_dir.join(&label);
            std::fs::create_dir_all(&dir)?;
            let data_dir = dir.join("data");

            let issued = ca.issue(&node_ids[i], opts.node_cert_ttl.get(&i).copied())?;
            let tls = ca.write_tls_dir(&issued, &dir)?;

            let mut trusted = trusted_base.clone();
            if let Some(extra) = opts.extra_trusted_origins.get(&i) {
                trusted.extend(extra.iter().map(|(k, v)| (*k, *v)));
            }
            let node_extra = opts.extra_toml_by_node.get(&i).cloned().unwrap_or_default();
            let inputs = NodeInputs {
                key: opts.key,
                database_id,
                node_id: node_ids[i],
                signing_hex: crate::hex(&seeds[i]),
                trusted_origins: trusted,
                chain_file: tls.chain,
                key_file: tls.key,
                ca_file: tls.ca,
                bootstrap: if !seed_addrs.is_empty() && !opts.bootstrap_seeds.contains(&i) {
                    seed_addrs.clone()
                } else {
                    Vec::new()
                },
                retention: opts.replication.as_ref().and_then(|r| r.retention),
                retention_interval: opts.replication.as_ref().and_then(|r| r.retention_interval),
                minimum_retained: opts.replication.as_ref().and_then(|r| r.minimum_retained),
                fanout: opts.replication.as_ref().and_then(|r| r.fanout),
                dissemination: opts
                    .dissemination_by_node
                    .get(&i)
                    .copied()
                    .or_else(|| opts.replication.as_ref().and_then(|r| r.dissemination)),
                max_transaction_bytes: opts.replication.as_ref().and_then(|r| r.max_transaction_bytes),
                batch_bytes: opts.replication.as_ref().and_then(|r| r.batch_bytes),
                membership: opts.replication.as_ref().and_then(|r| r.membership),
                maximum_offline: opts.replication.as_ref().and_then(|r| r.maximum_offline),
                merge_policies: opts
                    .replication
                    .as_ref()
                    .and_then(|r| r.merge_policies.clone())
                    .unwrap_or_default(),
                allowed_peers: opts.allowed_peers.get(&i).cloned().unwrap_or_default(),
                allowed_networks: opts.allowed_networks.get(&i).cloned().unwrap_or_default(),
                trusted_snapshot_sources: opts.trusted_snapshot_sources.get(&i).cloned().unwrap_or_default(),
                files: opts.files.clone(),
                bridge: opts.bridge.nodes.remove(&i),
                sqlite: opts.sqlite.clone(),
                spool: opts.spool_by_node.get(&i).cloned(),
                extra_toml: [opts.extra_toml.as_str(), node_extra.as_str()].join("\n"),
            };
            let config_path = dir.join("config.toml");
            let (child, client, sql_addr, repl_addr) =
                start_process(&label, &dir, &config_path, &inputs, &hash).await?;
            if opts.bootstrap_seeds.contains(&i) {
                seed_addrs.push(repl_addr);
            }
            states[i] = Some(NodeState {
                node: Node {
                    index: i,
                    label,
                    dir,
                    data_dir,
                    node_id: node_ids[i],
                    repl_addr,
                    sql_addr,
                },
                config_path,
                child: Some(child),
                client: Some(client),
                inputs,
            });
        }

        let cluster = Self {
            name: opts.name.clone(),
            runtime_dir,
            database_id,
            states: states.into_iter().map(|s| s.unwrap()).collect(),
            ca,
        };
        if !opts.manual_peers {
            cluster.mesh().await?;
        }
        Ok(cluster)
    }

    /// Scenario name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// `runtime/<scenario>/` root.
    pub fn runtime_dir(&self) -> &PathBuf {
        &self.runtime_dir
    }

    /// Shared database id.
    pub fn database_id(&self) -> [u8; 16] {
        self.database_id
    }

    /// Issue a leaf certificate for an external identity (e.g. a hostile
    /// fixture peer) from the cluster CA.
    pub fn issue_cert(&self, node_id: &[u8; 16]) -> Result<crate::certs::Issued> {
        Ok(self.ca.issue(node_id, None)?)
    }

    /// DER bytes of the cluster CA certificate.
    pub fn ca_der(&self) -> Vec<u8> {
        self.ca.ca_der()
    }

    /// PEM of the cluster CA certificate, for cross-trust legs: hand it to
    /// a peer cluster's [`Cluster::add_trust_root`].
    pub fn ca_pem(&self) -> String {
        self.ca.ca_pem()
    }

    /// Append an external CA root (PEM bytes) to every node's trust file
    /// (deduped). Applies to disk state only: stop and
    /// [`Cluster::start_node`] every node afterwards (then re-mesh).
    pub fn add_trust_root(&mut self, root_pem: &[u8]) {
        for st in &mut self.states {
            let path = st.inputs.ca_file.clone();
            let current = std::fs::read(&path).unwrap_or_default();
            if current.windows(root_pem.len().max(1)).any(|w| w == root_pem) || root_pem.is_empty() {
                continue;
            }
            let mut next = current;
            if !next.ends_with(b"\n") {
                next.push(b'\n');
            }
            next.extend_from_slice(root_pem);
            if !next.ends_with(b"\n") {
                next.push(b'\n');
            }
            if let Err(e) = std::fs::write(&path, &next) {
                eprintln!("{}: trust-root append failed: {e}", st.node.label);
            }
        }
    }

    /// Node count.
    pub fn len(&self) -> usize {
        self.states.len()
    }

    pub fn is_empty(&self) -> bool {
        self.states.is_empty()
    }

    /// Node id (available while stopped too).
    pub fn node_id(&self, idx: usize) -> [u8; 16] {
        self.states
            .get(idx)
            .unwrap_or_else(|| panic!("unknown node index {idx}"))
            .node
            .node_id
    }

    /// Origin signing public key for node `idx` (ed25519 verifying-key bytes).
    /// Available while stopped too.
    pub fn node_origin_pubkey(&self, idx: usize) -> [u8; 32] {
        let st = self
            .states
            .get(idx)
            .unwrap_or_else(|| panic!("unknown node index {idx}"));
        let id = st.node.node_id;
        st.inputs.trusted_origins.get(&id).copied().unwrap_or_else(|| panic!("node{} lost its origin key", idx + 1))
    }

    /// Origin signing seed of one node (decoded from its stored config
    /// input). Hostile-relay legs use it to craft signature attacks that
    /// isolate one check: a valid signature under the wrong key (fabricated
    /// origin) or under the right key over the wrong database (cross-DBID).
    pub fn node_origin_seed(&self, idx: usize) -> [u8; 32] {
        let st = self
            .states
            .get(idx)
            .unwrap_or_else(|| panic!("unknown node index {idx}"));
        decode_hex_32(&st.inputs.signing_hex)
            .unwrap_or_else(|| panic!("node{} lost its origin seed", idx + 1))
    }

    /// Node metadata (dirs, addrs; available while stopped too).
    pub fn node_info(&self, idx: usize) -> &Node {
        &self
            .states
            .get(idx)
            .unwrap_or_else(|| panic!("unknown node index {idx}"))
            .node
    }

    /// Open wire connection. Panics if the node is stopped (a test bug).
    pub fn node(&self, idx: usize) -> &NodeClient {
        let st = self
            .states
            .get(idx)
            .unwrap_or_else(|| panic!("unknown node index {idx}"));
        st.client
            .as_ref()
            .unwrap_or_else(|| panic!("node{} is stopped", idx + 1))
    }

    /// Open an independent Rust-client SQL connection for a concurrent worker.
    pub async fn connect(&self, idx: usize) -> Result<NodeClient> {
        let st = self.states.get(idx).ok_or(Error::UnknownNode(idx))?;
        crate::unlock::connect_with_watchdog(st.node.sql_addr, PG_USER, PG_PASSWORD, &crate::hex(&st.inputs.key)).await
    }

    /// Child PID, or `None` while stopped. Panics on unknown index.
    pub fn node_pid(&self, idx: usize) -> Option<u32> {
        self.states
            .get(idx)
            .unwrap_or_else(|| panic!("unknown node index {idx}"))
            .child
            .as_ref()
            .map(|c| c.id())
    }

    /// Run fixture DDL on one node during setup. Fast meshes converge the
    /// schema from an earlier node before a later node's identical statement
    /// runs; that "already exists" is the converged fixture, not a failure.
    /// Node 0 stays strict (nothing has replicated yet, so pre-existing
    /// tables there mean a dirty runtime dir); later nodes skip with a log
    /// line.
    pub async fn ensure_migrated(&self, idx: usize, sql: &str) -> Result<()> {
        match self.node(idx).simple(sql).await {
            Ok(_) => {
                self.node(idx).simple("MURMUR FLUSH").await?;
                Ok(())
            }
            Err(e) if idx > 0 && crate::db_message(&e).contains("already exists") => {
                eprintln!(
                    "node{}: fixture schema already converged, skipping identical migrate",
                    idx + 1
                );
                Ok(())
            }
            Err(e) => Err(Error::Setup(format!(
                "node{} fixture migrate failed: {e}",
                idx + 1
            ))),
        }
    }

    /// Dial `target` from `from` (both must be running; target addr is its
    /// current ephemeral bind). Clears any earlier exclusion for the target.
    pub async fn add_peer(&self, from: usize, target: usize) -> Result<()> {
        let addr = self.states.get(target).ok_or(Error::UnknownNode(target))?.node.repl_addr;
        self.add_peer_addr(from, addr).await
    }

    /// Dial an explicit address from `from` (`from` must be running).
    /// Proxy legs use this to route a session through a throttle instead
    /// of the target's bound address.
    pub async fn add_peer_addr(&self, from: usize, addr: SocketAddr) -> Result<()> {
        let src = self.states.get(from).ok_or(Error::UnknownNode(from))?;
        let client = src.client.as_ref().ok_or(Error::NodeStopped(from))?;
        client.simple(&format!("MURMUR PEER ADD '{addr}'")).await?;
        Ok(())
    }

    /// Drop `target` from `from`'s peer set (`from` must be running).
    /// Reversible with [`Cluster::add_peer`]; retention is kept.
    pub async fn remove_peer(&self, from: usize, target: usize) -> Result<()> {
        let id = self.states.get(target).ok_or(Error::UnknownNode(target))?.node.node_id;
        let src = self.states.get(from).ok_or(Error::UnknownNode(from))?;
        let client = src.client.as_ref().ok_or(Error::NodeStopped(from))?;
        client.simple(&format!("MURMUR PEER REMOVE '{}'", crate::hex(&id))).await?;
        Ok(())
    }

    /// Permanently exclude `target` from `from` and release its retention
    /// obligation. There is no un-retire.
    pub async fn retire_peer(&self, from: usize, target: usize) -> Result<()> {
        let id = self.states.get(target).ok_or(Error::UnknownNode(target))?.node.node_id;
        let src = self.states.get(from).ok_or(Error::UnknownNode(from))?;
        let client = src.client.as_ref().ok_or(Error::NodeStopped(from))?;
        client.simple(&format!("MURMUR PEER RETIRE '{}'", crate::hex(&id))).await?;
        Ok(())
    }

    /// `MURMUR PEER SYNC` on every *running* node (heal legs converge faster
    /// than waiting for the anti-entropy tick).
    pub async fn sync_all(&self) -> Result<()> {
        for st in &self.states {
            if let Some(client) = st.client.as_ref() {
                client.simple("MURMUR PEER SYNC").await?;
            }
        }
        Ok(())
    }

    /// Dial every ordered pair of *running* nodes (heal legs after restart).
    pub async fn mesh(&self) -> Result<()> {
        for i in 0..self.states.len() {
            if self.states[i].client.is_none() {
                continue;
            }
            for j in 0..self.states.len() {
                if i != j && self.states[j].client.is_some() {
                    self.add_peer(i, j).await?;
                }
            }
        }
        Ok(())
    }

    /// Gracefully stop node `idx` (SIGTERM on unix, kill elsewhere, with a
    /// SIGKILL fallback); dirs, key, and identity are kept for restart.
    /// Unknown index panics (a test bug).
    pub async fn stop_node(&mut self, idx: usize) {
        let st = self
            .states
            .get_mut(idx)
            .unwrap_or_else(|| panic!("unknown node index {idx}"));
        st.client.take();
        let Some(mut child) = st.child.take() else {
            return;
        };
        #[cfg(unix)]
        {
            // Signal delivery can race exec; a failed kill falls through to reap.
            unsafe {
                libc::kill(child.id() as i32, libc::SIGTERM);
            }
            let deadline = Instant::now() + Duration::from_secs(10);
            loop {
                match child.try_wait() {
                    Ok(Some(_)) => return,
                    Ok(None) if Instant::now() < deadline => {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    _ => break,
                }
            }
        }
        reap(child, &st.node.label).await;
    }

    /// Crash node `idx` without a graceful shutdown (crash legs): immediate
    /// kill + reap, keeping dirs, key, and identity for restart. Unknown
    /// index panics (a test bug).
    pub async fn kill_node(&mut self, idx: usize) {
        let st = self
            .states
            .get_mut(idx)
            .unwrap_or_else(|| panic!("unknown node index {idx}"));
        st.client.take();
        if let Some(child) = st.child.take() {
            reap(child, &st.node.label).await;
        }
    }

    /// Suspend node `idx` with SIGSTOP (unix only; pause legs). The node
    /// stays "running" from the harness view: sockets are kept, queries
    /// block until [`Cluster::resume_node`].
    pub async fn suspend_node(&self, idx: usize) -> Result<()> {
        #[cfg(unix)]
        {
            let pid = self.node_pid(idx).ok_or(Error::NodeStopped(idx))?;
            if unsafe { libc::kill(pid as i32, libc::SIGSTOP) } != 0 {
                return Err(Error::Setup(format!("node{}: SIGSTOP failed", idx + 1)));
            }
            Ok(())
        }
        #[cfg(not(unix))]
        {
            let _ = idx;
            Err(Error::Setup("node suspend needs unix signals".into()))
        }
    }

    /// Resume a node suspended with [`Cluster::suspend_node`] (unix only).
    pub async fn resume_node(&self, idx: usize) -> Result<()> {
        #[cfg(unix)]
        {
            let pid = self.node_pid(idx).ok_or(Error::NodeStopped(idx))?;
            if unsafe { libc::kill(pid as i32, libc::SIGCONT) } != 0 {
                return Err(Error::Setup(format!("node{}: SIGCONT failed", idx + 1)));
            }
            Ok(())
        }
        #[cfg(not(unix))]
        {
            let _ = idx;
            Err(Error::Setup("node resume needs unix signals".into()))
        }
    }

    /// Restart stopped node `idx` (same dir + key + identity) under the
    /// unlock watchdog. The node rebinds fresh SQL and replication ports:
    /// re-mesh (`add_peer`/`mesh`) afterwards, since peers still dial the
    /// old replication address. The `[replication.tables]` section carries
    /// over, so `MURMUR SET TABLE` flags survive.
    pub async fn start_node(&mut self, idx: usize) -> Result<()> {
        let st = self.states.get(idx).ok_or(Error::UnknownNode(idx))?;
        if st.client.is_some() {
            return Err(Error::Setup(format!("node{} is already running", idx + 1)));
        }
        let hash = password_hash().to_string();
        let st = self.states.get_mut(idx).ok_or(Error::UnknownNode(idx))?;
        let (child, client, sql_addr, repl_addr) =
            start_process(&st.node.label.clone(), &st.node.dir.clone(), &st.config_path, &st.inputs, &hash).await?;
        st.node.sql_addr = sql_addr;
        st.node.repl_addr = repl_addr;
        st.child = Some(child);
        st.client = Some(client);
        Ok(())
    }

    /// Restart every listed node concurrently, joining all restarts (the
    /// barrier) before returning. Sequential restarts skew a storm by
    /// minutes when opens take seconds each; concurrent restarts share the
    /// wall clock so every node rejoins from the same backlog.
    pub async fn start_nodes_parallel(&mut self, idxs: &[usize]) -> Result<()> {
        for &idx in idxs {
            let st = self.states.get(idx).ok_or(Error::UnknownNode(idx))?;
            if st.client.is_some() {
                return Err(Error::Setup(format!("node{} is already running", idx + 1)));
            }
        }
        let hash = password_hash().to_string();
        let mut tasks = tokio::task::JoinSet::new();
        for &idx in idxs {
            let st = &self.states[idx];
            let label = st.node.label.clone();
            let dir = st.node.dir.clone();
            let config_path = st.config_path.clone();
            let inputs = st.inputs.clone();
            let hash = hash.clone();
            tasks.spawn(async move {
                start_process(&label, &dir, &config_path, &inputs, &hash)
                    .await
                    .map(|(child, client, sql_addr, repl_addr)| (idx, child, client, sql_addr, repl_addr))
            });
        }
        let mut started = Vec::with_capacity(idxs.len());
        while let Some(joined) = tasks.join_next().await {
            match joined {
                Ok(Ok(done)) => started.push(done),
                Ok(Err(e)) => return Err(e),
                Err(e) => return Err(Error::Setup(format!("parallel restart panicked: {e}"))),
            }
        }
        for (idx, child, client, sql_addr, repl_addr) in started {
            let st = self.states.get_mut(idx).ok_or(Error::UnknownNode(idx))?;
            st.node.sql_addr = sql_addr;
            st.node.repl_addr = repl_addr;
            st.child = Some(child);
            st.client = Some(client);
        }
        Ok(())
    }

    /// Replace node `idx`'s stored bridge config. The node must be stopped
    /// (see [`Cluster::stop_node`]); the new config takes effect on the next
    /// [`Cluster::start_node`]. `None` removes the bridge.
    pub fn set_bridge_config(&mut self, idx: usize, config: Option<BridgeNodeConfig>) -> Result<()> {
        let st = self.states.get_mut(idx).ok_or(Error::UnknownNode(idx))?;
        if st.client.is_some() {
            return Err(Error::Setup(format!("node{} is running; stop it first", idx + 1)));
        }
        st.inputs.bridge = config;
        Ok(())
    }

    /// Replace node `idx`'s stored spool options (`None` restores production
    /// storage behavior). The node must be stopped; the new options take
    /// effect on the next [`Cluster::start_node`].
    pub fn set_spool_options(&mut self, idx: usize, spool: Option<SpoolOptions>) -> Result<()> {
        let st = self.states.get_mut(idx).ok_or(Error::UnknownNode(idx))?;
        if st.client.is_some() {
            return Err(Error::Setup(format!("node{} is running; stop it first", idx + 1)));
        }
        st.inputs.spool = spool;
        Ok(())
    }

    /// Replace node `idx`'s stored unlock key. Takes effect on the next
    /// [`Cluster::start_node`]; online rotation (`MURMUR ROTATE`) on a
    /// running node should record the new key here immediately so later
    /// restarts unlock with it instead of the retired key.
    pub fn set_node_key(&mut self, idx: usize, key: [u8; 32]) -> Result<()> {
        let st = self.states.get_mut(idx).ok_or(Error::UnknownNode(idx))?;
        st.inputs.key = key;
        Ok(())
    }

    /// Rotate node `idx` to a freshly issued leaf certificate for the SAME
    /// NodeID, then restart it (cert rotation; `cert-lifecycle`). The node
    /// must be stopped (see [`Cluster::stop_node`]); `ttl` overrides the
    /// default leaf validity (short TTLs for expiry legs, `None` for a
    /// fresh long-lived cert). The node's `tls/` dir is rewritten from the
    /// new material, so file NotAfter always matches the served cert. Like
    /// [`Cluster::start_node`], the node rebinds fresh ports: re-mesh
    /// afterwards.
    pub async fn rotate_node_cert(&mut self, idx: usize, ttl: Option<Duration>) -> Result<()> {
        {
            let st = self.states.get(idx).ok_or(Error::UnknownNode(idx))?;
            if st.client.is_some() {
                return Err(Error::Setup(format!("node{} is running; stop it first", idx + 1)));
            }
        }
        let (node_id, dir) = {
            let st = self.states.get(idx).ok_or(Error::UnknownNode(idx))?;
            (st.node.node_id, st.node.dir.clone())
        };
        let issued = self.ca.issue(&node_id, ttl)?;
        self.ca.write_tls_dir(&issued, &dir)?;
        self.start_node(idx).await
    }

    /// Restart stopped node `idx` under a FRESH node identity (clone legs).
    /// The node must be stopped (see [`Cluster::stop_node`]) and its data
    /// dir must already carry the fresh identity (restored with a new node
    /// id). Issues a fresh leaf certificate for the new id (rewriting
    /// `tls/`), mints a fresh origin signing key, and registers the new id
    /// in the node's own `trusted_origins` (old entries are kept so
    /// pre-restore origins still verify). Peers are NOT told the fresh
    /// origin key, so the restarted node must not originate new
    /// transactions until every peer restarts with it. Like
    /// [`Cluster::start_node`], the node rebinds fresh ports: re-mesh
    /// afterwards.
    pub async fn restart_node_with_identity(&mut self, idx: usize, new_node_id: [u8; 16]) -> Result<()> {
        {
            let st = self.states.get(idx).ok_or(Error::UnknownNode(idx))?;
            if st.client.is_some() {
                return Err(Error::Setup(format!("node{} is running; stop it first", idx + 1)));
            }
        }
        if self.states.iter().any(|s| s.node.node_id == new_node_id) {
            return Err(Error::Setup("fresh node id collides with a cluster member".into()));
        }
        let dir = self.states[idx].node.dir.clone();
        let issued = self.ca.issue(&new_node_id, None)?;
        let tls = self.ca.write_tls_dir(&issued, &dir)?;
        let seed: [u8; 32] = rand::random();
        let public = ed25519_dalek::SigningKey::from_bytes(&seed).verifying_key().to_bytes();
        let st = self.states.get_mut(idx).ok_or(Error::UnknownNode(idx))?;
        st.node.node_id = new_node_id;
        st.inputs.node_id = new_node_id;
        st.inputs.chain_file = tls.chain;
        st.inputs.key_file = tls.key;
        st.inputs.ca_file = tls.ca;
        st.inputs.signing_hex = crate::hex(&seed);
        st.inputs.trusted_origins.insert(new_node_id, public);
        self.start_node(idx).await
    }

    /// Forensics for every node (running or stopped). Infallible by contract.
    pub async fn dump_forensics(&self, reason: &str) {
        for st in &self.states {
            crate::forensics::dump_node_forensics(
                &st.node.label,
                &st.node.dir,
                st.client.as_ref(),
                reason,
            )
            .await;
        }
    }

    /// Kill all children (reaping them), then retain-or-clean the runtime dir
    /// + `RESULT` line.
    pub async fn finish(mut self, failed: bool) {
        for st in &mut self.states {
            st.client.take();
            if let Some(child) = st.child.take() {
                reap(child, &st.node.label).await;
            }
        }
        crate::artifacts::finish(&self.name, &self.runtime_dir, failed);
    }
}
