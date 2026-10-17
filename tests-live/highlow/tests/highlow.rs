//! Rust port of the Go `highlow` live scenarios.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/highlow/`
//! (`highlow_test.go`, `ownership_reorder_test.go`, `README.md`). Real Low
//! (exporter) and High (importer) domains move logical writes across a
//! directory drop: disconnected transfer with one-way role gating, gap
//! delivery with duplicate recovery, two-receiver convergence,
//! forgery/corruption/misnaming rejection without partial apply, outbox/inbox
//! restart resumption, schema-hold survival across restart with release after
//! a local High migration (no DDL crosses), signer rotation plus outage
//! catch-up, and High-owned-field reorder convergence across two meshed High
//! peers with opposite import/override orders.
//!
//! Low and High are separate `murmur-server` clusters driven over the
//! PostgreSQL wire protocol. Plain SQL replaces the Go typed HTTP API (the
//! Rust product has no HTTP server); `contacts()` is the `name -> score`
//! visible-state map (Go: `contacts()` + `TypedCounterValue`, with plain
//! LWW `score` increments standing in for the sequential single-writer
//! `TypedCounterAdd` legs, which converge identically). `BridgeExport`/
//! `BridgeImport` have no wire equivalent: the replication engine drives
//! `capture()`/`process()` automatically every apply tick (~1 s), so the
//! test only polls file and row state.
//!
//! Directory-drop routing (deviation): Go shares one staging dir because its
//! daemons only export/import on HTTP trigger. The Rust replication engine
//! also drives `capture()`/`process()` in the background on every apply tick
//! (`engine.rs`), so Low and High use SEPARATE staging dirs and the test
//! ferries `*.bundle` files one way via [`Ferry`]; only what the test ferries
//! is visible to High, which keeps gap/withhold/forgery/outage legs
//! deterministic. Bundle filenames (`<nodehex>-<seq:020>.bundle`) sort in
//! export order, so the middle-bundle withhold mirrors Go exactly.
//!
//! Count/progress mapping (deviation): per-call `captured`/`published`/
//! `received`/`imported` counts and `BridgeStatus` progress (`Applied`,
//! `Missing`, `Holds`, `pending_events`) have no public Rust equivalent and
//! are racy under background driving, so every such assertion is proven
//! behaviorally instead: gap holds by the exact visible row set after a
//! background-settle sleep, duplicate redelivery by state stability plus
//! the bundle staying out of quarantine, restart resume by Low's bundle
//! file set being unchanged and High's rows holding exactly, and outage
//! catch-up by the exact converged counter (a lost or doubled event would
//! show a different total). `Bridge::process()` quarantines undecryptable
//! artifacts into `staging/quarantine/` and returns `Ok` (Go: import
//! error), so forgery legs assert quarantine placement + empty High + no
//! recorded progress (the genuine bundle still applies afterwards). High/Low
//! field provenance has no public API (spool keys are internal), so
//! ownership is proven behaviorally per the `bridge-two-streams` port
//! convention: High-owned by a Low update arriving yet staying suppressed.
//!
//! Dropped legs (deviation): the API-boundary role-gating asserts
//! (`process()` on the exporter / `capture()` on the importer returning
//! zero), the unreadable-staging-file `Err` leg (background import errors
//! are invisible over the wire), and the explicit `Bridge::release`
//! convergence leg (no wire equivalent) have no observable form and are
//! omitted; directionality is still proven end to end (Low never imports:
//! no High bundle is ever ferried back, and High state only changes via
//! ferried Low bundles).
//!
//! Signer rotation (deviation): Go overlaps two trusted signer keys for one
//! source node during rotation. Rust pins a single signer per source node
//! (`trusted_signers`), so overlap is inexpressible: round 1 is imported
//! BEFORE the rotation (config edit plus restart on both sides, Go
//! `rewriteNodeConfig` parity via `Cluster::set_bridge_config`), round 2
//! imports after, and a retired-signer replay then fails closed into
//! quarantine while a current-key duplicate still stages cleanly.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::Duration;

use murmur_live_harness::{
    artifacts, hex, BridgeNodeConfig, BridgeOptions, Cluster, ClusterOptions,
};

/// Bridge stream (Go: `field-records`).
const STREAM: &str = "field-records";
/// Contacts table (Go: `liveContactsSchema`).
const TABLE: &str = "contacts";
const BASE_DDL: &str =
    "CREATE TABLE contacts(id TEXT NOT NULL PRIMARY KEY, name TEXT, score INTEGER)";
const EMAIL_DDL: &str = "CREATE TABLE contacts(id TEXT NOT NULL PRIMARY KEY, name TEXT, score INTEGER, email TEXT)";
const ADD_EMAIL: &str = "ALTER TABLE contacts ADD COLUMN email TEXT";
/// Convergence deadline (Go: 30 s waits).
const WAIT: Duration = Duration::from_secs(30);
/// Convergence poll interval (Go: 100 ms polls).
const POLL: Duration = Duration::from_millis(50);
/// Background-settle sleep for negative legs (engine capture/process run
/// every ~1 s apply tick; this covers several ticks).
const SETTLE: Duration = Duration::from_secs(3);
/// Export-quiescence observation: the Low bundle set must hold still this
/// long (covers >2 apply ticks) before a drain returns.
const DRAIN_QUIET: Duration = Duration::from_millis(2500);
/// Export-quiescence poll interval and overall bound.
const DRAIN_POLL: Duration = Duration::from_millis(250);

/// Independent bridge domains. Fixed test vectors shared by every test in
/// this file (clusters never interact across tests, like the
/// `tests/bridge_files.rs` fixed-key convention).
const LOW_DBID: [u8; 16] = [0xD0; 16];
const HIGH_DBID: [u8; 16] = [0xD1; 16];
const ROGUE_DBID: [u8; 16] = [0xD2; 16];
const LOW_NODE: [u8; 16] = [0x10; 16];
const HIGH_NODE_0: [u8; 16] = [0x11; 16];
const HIGH_NODE_1: [u8; 16] = [0x12; 16];
const ROGUE_NODE: [u8; 16] = [0x13; 16];
/// Low bundle-signer seeds (ed25519; High pins the verifying keys).
const LOW_SIGNER_A: [u8; 32] = [0x21; 32];
const LOW_SIGNER_B: [u8; 32] = [0x23; 32];
const ROGUE_SIGNER: [u8; 32] = [0x24; 32];
/// Receiver secrets (X25519; Low encrypts to the derived recipients).
const HIGH_SECRET: [u8; 32] = [0x22; 32];
const OTHER_SECRET: [u8; 32] = [0x25; 32];
const ROGUE_SECRET: [u8; 32] = [0x26; 32];

/// ed25519 verifying key for a bundle-signer seed (product
/// `protocol::public_key` parity).
fn signer_public(seed: &[u8; 32]) -> [u8; 32] {
    ed25519_dalek::SigningKey::from_bytes(seed).verifying_key().to_bytes()
}

/// X25519 recipient public key for a receiver secret (product
/// `bridge::recipient_public` parity).
fn recipient_public(secret: &[u8; 32]) -> [u8; 32] {
    x25519_dalek::PublicKey::from(&x25519_dalek::StaticSecret::from(*secret)).to_bytes()
}

/// Contact insert (Go: `writeContact`).
async fn insert_contact(cluster: &Cluster, idx: usize, id: &str, name: &str, score: i64) -> Result<(), String> {
    let changed = cluster
        .node(idx)
        .execute(
            "INSERT INTO contacts(id, name, score) VALUES($1, $2, $3)",
            &[&id, &name, &score],
        )
        .await
        .map_err(|e| format!("contact insert failed: {e}"))?;
    if changed != 1 {
        return Err(format!("contact insert hit {changed} rows"));
    }
    cluster
        .node(idx)
        .flush()
        .await
        .map_err(|e| format!("flush after contact insert: {e}"))?;
    Ok(())
}

/// Contact rename (Go: `Cluster.TypedRename`).
async fn rename(cluster: &Cluster, idx: usize, id: &str, name: &str) -> Result<(), String> {
    let changed = cluster
        .node(idx)
        .execute("UPDATE contacts SET name = $1 WHERE id = $2", &[&name, &id])
        .await
        .map_err(|e| format!("contact rename failed: {e}"))?;
    if changed != 1 {
        return Err(format!("rename hit {changed} rows"));
    }
    cluster
        .node(idx)
        .flush()
        .await
        .map_err(|e| format!("flush after contact rename: {e}"))?;
    Ok(())
}

/// Known-field email update (Go: `Cluster.TypedSetNote`).
async fn set_email(cluster: &Cluster, idx: usize, id: &str, email: &str) -> Result<(), String> {
    let changed = cluster
        .node(idx)
        .execute("UPDATE contacts SET email = $1 WHERE id = $2", &[&email, &id])
        .await
        .map_err(|e| format!("contact email update failed: {e}"))?;
    if changed != 1 {
        return Err(format!("email update hit {changed} rows"));
    }
    cluster
        .node(idx)
        .flush()
        .await
        .map_err(|e| format!("flush after email update: {e}"))?;
    Ok(())
}

/// Sequential score increment (Go: `Cluster.TypedCounterAdd`; single-writer
/// legs converge identically under LWW).
///
/// The delta is inlined as a literal (injection-safe: formatted `i64`):
/// the server reports expression-position parameters as type `Unknown`
/// and the driver refuses to serialize non-string Rust types for them
/// (`error serializing parameter 0`), so only `&str` params may sit in
/// positions the server cannot infer from a column.
async fn add_score(cluster: &Cluster, idx: usize, id: &str, delta: i64) -> Result<(), String> {
    let changed = cluster
        .node(idx)
        .execute(
            &format!("UPDATE contacts SET score = score + {delta} WHERE id = $1"),
            &[&id],
        )
        .await
        .map_err(|e| format!("score update failed: {e}"))?;
    if changed != 1 {
        return Err(format!("score update hit {changed} rows"));
    }
    cluster
        .node(idx)
        .flush()
        .await
        .map_err(|e| format!("flush after score update: {e}"))?;
    Ok(())
}

/// Visible contacts as `name -> score` (Go: `contacts()`).
async fn contacts(cluster: &Cluster, idx: usize) -> Result<BTreeMap<String, i64>, String> {
    let rows = cluster
        .node(idx)
        .query("SELECT name, score FROM contacts", &[])
        .await
        .map_err(|e| format!("contacts read failed: {e}"))?;
    let mut out = BTreeMap::new();
    for row in &rows {
        let name: String = row
            .try_get(0)
            .map_err(|e| format!("decode contact name: {e}"))?;
        let score: i64 = row
            .try_get(1)
            .map_err(|e| format!("decode contact score: {e}"))?;
        out.insert(name, score);
    }
    Ok(out)
}

/// Email field read (Go: `Cluster.TypedNote`); `None` when the column is
/// absent (pre-migration High) or the value is NULL.
async fn email_of(cluster: &Cluster, idx: usize, id: &str) -> Option<String> {
    let rows = cluster
        .node(idx)
        .query("SELECT email FROM contacts WHERE id = $1", &[&id])
        .await
        .ok()?;
    rows.first()?.try_get::<_, Option<String>>(0).ok().flatten()
}

/// Whether `table` currently has `column` (probed via a zero-row select;
/// PRAGMA has no guaranteed wire form).
async fn has_column(cluster: &Cluster, idx: usize, table: &str, column: &str) -> bool {
    cluster
        .node(idx)
        .query(&format!("SELECT {column} FROM {table} LIMIT 0"), &[])
        .await
        .is_ok()
}

/// Fixture DDL on one node plus the replicated flag (row convergence and
/// bridge capture need the per-table flag on each node).
async fn migrate(cluster: &Cluster, idx: usize, ddl: &str, label: &str) -> Result<(), String> {
    cluster
        .ensure_migrated(idx, ddl)
        .await
        .map_err(|e| format!("{label} migrate failed: {e}"))?;
    cluster
        .node(idx)
        .simple(&format!("MURMUR SET TABLE {TABLE} REPLICATED true"))
        .await
        .map_err(|e| format!("{label} SET TABLE REPLICATED: {e}"))?;
    cluster
        .node(idx)
        .flush()
        .await
        .map_err(|e| format!("{label} flush after migrate: {e}"))?;
    Ok(())
}

/// Wait until Low's export quiesces: the bundle-file set must hold still
/// for [`DRAIN_QUIET`] (the background capture drives every ~1 s apply
/// tick). Sound: capture cursors are durable and only the test writes,
/// so a still set means quiescent.
async fn drain_low(ferry: &Ferry) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + WAIT;
    let mut last = bundle_files(&ferry.low)?;
    let mut still_since = tokio::time::Instant::now();
    loop {
        tokio::time::sleep(DRAIN_POLL).await;
        let now = bundle_files(&ferry.low)?;
        if now == last {
            if still_since.elapsed() >= DRAIN_QUIET {
                return Ok(());
            }
        } else {
            last = now;
            still_since = tokio::time::Instant::now();
        }
        if tokio::time::Instant::now() >= deadline {
            return Err("low bridge did not become idle".into());
        }
    }
}

/// Sorted `*.bundle` names directly under `dir` (Go: `stagingNames`).
fn bundle_files(dir: &Path) -> Result<Vec<String>, String> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir).map_err(|e| format!("read dir {}: {e}", dir.display()))? {
        let name = entry
            .map_err(|e| format!("staging entry: {e}"))?
            .file_name()
            .to_string_lossy()
            .into_owned();
        if name.ends_with(".bundle") {
            out.push(name);
        }
    }
    out.sort();
    Ok(out)
}

/// Remove every `*.bundle` file directly under `dir`.
fn clear_bundles(dir: &Path) -> Result<(), String> {
    for name in bundle_files(dir)? {
        std::fs::remove_file(dir.join(&name))
            .map_err(|e| format!("clear {}: {e}", dir.join(&name).display()))?;
    }
    Ok(())
}

/// Sorted names under `staging/quarantine/` (empty when no dir yet).
fn quarantine_files(staging: &Path) -> Vec<String> {
    let dir = staging.join("quarantine");
    if !dir.is_dir() {
        return Vec::new();
    }
    let mut out = Vec::new();
    if let Ok(rd) = std::fs::read_dir(&dir) {
        for entry in rd.flatten() {
            out.push(entry.file_name().to_string_lossy().into_owned());
        }
    }
    out.sort();
    out
}

/// Remove every file under `staging/quarantine/`.
fn clear_quarantine(staging: &Path) -> Result<(), String> {
    for name in quarantine_files(staging) {
        std::fs::remove_file(staging.join("quarantine").join(&name))
            .map_err(|e| format!("clear quarantine file: {e}"))?;
    }
    Ok(())
}

/// One-way directory drop: Low exports into `low`, the test ferries
/// `*.bundle` files into `high`. Ferries are tracked so `ferry_all` only
/// moves new bundles; targeted withholds use [`Ferry::ferry_names`].
struct Ferry {
    low: PathBuf,
    high: PathBuf,
    done: HashSet<String>,
}

impl Ferry {
    fn new(low: PathBuf, high: PathBuf) -> Self {
        Self { low, high, done: HashSet::new() }
    }

    /// Bundles exported since the last ferry, in export order.
    fn new_bundles(&self) -> Result<Vec<String>, String> {
        Ok(bundle_files(&self.low)?
            .into_iter()
            .filter(|n| !self.done.contains(n))
            .collect())
    }

    /// Ferry every new bundle to High.
    fn ferry_all(&mut self) -> Result<(), String> {
        let new = self.new_bundles()?;
        self.ferry_names(&new)
    }

    /// Ferry exactly `names` (already-exported bundles) to High.
    fn ferry_names(&mut self, names: &[String]) -> Result<(), String> {
        for name in names {
            std::fs::copy(self.low.join(name), self.high.join(name))
                .map_err(|e| format!("ferry {name}: {e}"))?;
            self.done.insert(name.clone());
        }
        Ok(())
    }
}

/// Single-node Low exporter (Go: `newLowCluster`).
async fn open_exporter(
    name: &str,
    dbid: [u8; 16],
    node: [u8; 16],
    staging: PathBuf,
    signer: [u8; 32],
    recipient: [u8; 32],
) -> Result<Cluster, murmur_live_harness::Error> {
    Cluster::new(ClusterOptions {
        name: name.into(),
        num_nodes: 1,
        database_id: Some(dbid),
        node_ids: vec![node],
        manual_peers: true,
        bridge: BridgeOptions {
            nodes: HashMap::from([(
                0,
                BridgeNodeConfig {
                    role: "low-exporter".into(),
                    domain: hex(&dbid),
                    directory: staging.to_string_lossy().replace('\\', "/"),
                    stream: STREAM.into(),
                    recipient: hex(&recipient),
                    signer_hex: Some(hex(&signer)),
                    ..Default::default()
                },
            )]),
        },
        ..Default::default()
    })
    .await
}

/// High importer cluster (Go: `newHighCluster`); one staging dir per node.
/// Always `manual_peers`: the test meshes explicitly after migrating.
async fn open_high(
    name: &str,
    dbid: [u8; 16],
    nodes: Vec<[u8; 16]>,
    stagings: Vec<PathBuf>,
    secret: [u8; 32],
    domains: BTreeSet<[u8; 16]>,
    signers: BTreeMap<[u8; 16], [u8; 32]>,
    origins: BTreeMap<[u8; 16], [u8; 32]>,
) -> Result<Cluster, murmur_live_harness::Error> {
    assert_eq!(nodes.len(), stagings.len(), "nodes/stagings length mismatch");
    let receivers = stagings
        .into_iter()
        .enumerate()
        .map(|(i, staging)| {
            (
                i,
                BridgeNodeConfig {
                    role: "high-receiver".into(),
                    domain: hex(&dbid),
                    trusted_domains: domains.iter().map(|id| hex(id)).collect(),
                    trusted_origins: origins.iter().map(|(n, k)| (hex(n), hex(k))).collect(),
                    directory: staging.to_string_lossy().replace('\\', "/"),
                    stream: STREAM.into(),
                    recipient: hex(&recipient_public(&secret)),
                    receiver_secret_hex: Some(hex(&secret)),
                    trusted_signers: signers.iter().map(|(n, k)| (hex(n), hex(k))).collect(),
                    ..Default::default()
                },
            )
        })
        .collect();
    Cluster::new(ClusterOptions {
        name: name.into(),
        num_nodes: nodes.len(),
        database_id: Some(dbid),
        node_ids: nodes,
        manual_peers: true,
        bridge: BridgeOptions { nodes: receivers },
        ..Default::default()
    })
    .await
}

/// Receiver trust pinning Low's bundle signer and origin key (the seeds
/// themselves stay inside the harness).
fn low_signers(signer: &[u8; 32]) -> BTreeMap<[u8; 16], [u8; 32]> {
    BTreeMap::from([(LOW_NODE, signer_public(signer))])
}

fn low_origins(low: &Cluster) -> BTreeMap<[u8; 16], [u8; 32]> {
    BTreeMap::from([(LOW_NODE, low.node_origin_pubkey(0))])
}

/// Wait until High node `idx` shows exactly `want` (Go: contact waits plus
/// `assertContactsEqual`). The engine's background capture/process drive
/// the transfer; the ferry stays explicit (withhold legs must not
/// auto-ferry).
async fn wait_high(
    high: &Cluster,
    idx: usize,
    want: &BTreeMap<String, i64>,
) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        if contacts(high, idx).await? == *want {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "high node{idx} contacts = {:?}, want {want:?}",
                contacts(high, idx).await?
            ));
        }
        tokio::time::sleep(POLL).await;
    }
}

/// Wait until every listed High node shows exactly `want` over the mesh
/// (no bridge traffic remains; plain convergence polling).
async fn wait_meshed(
    high: &Cluster,
    idxs: &[usize],
    want: &BTreeMap<String, i64>,
    what: &str,
) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        let mut all = true;
        for &i in idxs {
            if contacts(high, i).await? != *want {
                all = false;
                break;
            }
        }
        if all {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            let mut states = Vec::new();
            for &i in idxs {
                states.push(contacts(high, i).await?);
            }
            return Err(format!("{what}: high nodes did not converge on {want:?}: {states:?}"));
        }
        tokio::time::sleep(POLL).await;
    }
}

/// Settle sleep for negative legs: background capture/process run every
/// ~1 s apply tick, so a still-unchanged state after [`SETTLE`] proves
/// the hold without explicit pumps.
async fn settle() {
    tokio::time::sleep(SETTLE).await;
}

/// Disconnected transfer with one-way role gating (Go:
/// `TestHighLowDisconnectedTransferLive`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn disconnected_transfer_with_one_way_role_gating() {
    let staging_low = artifacts::runtime_dir("highlow-transfer-low").join("staging");
    let staging_high = artifacts::runtime_dir("highlow-transfer-high").join("staging");
    let low = open_exporter(
        "highlow-transfer-low",
        LOW_DBID,
        LOW_NODE,
        staging_low.clone(),
        LOW_SIGNER_A,
        recipient_public(&HIGH_SECRET),
    )
    .await
    .expect("low cluster setup");
    let high = open_high(
        "highlow-transfer-high",
        HIGH_DBID,
        vec![HIGH_NODE_0],
        vec![staging_high.clone()],
        HIGH_SECRET,
        BTreeSet::from([LOW_DBID]),
        low_signers(&LOW_SIGNER_A),
        low_origins(&low),
    )
    .await
    .expect("high cluster setup");
    migrate(&low, 0, BASE_DDL, "low").await.expect("low migrate");
    migrate(&high, 0, BASE_DDL, "high").await.expect("high migrate");
    let mut ferry = Ferry::new(staging_low, staging_high);

    let result: Result<(), String> = async {
        if low.database_id() == high.database_id() {
            return Err("Low and High share a DBID; domains must differ".into());
        }
        // One-way role gating is structural (no wire equivalent for the
        // old API-boundary asserts): nothing is ever ferried back, so
        // High state can only change via ferried Low bundles, and Low
        // never sees High bytes at all.
        insert_contact(&low, 0, "ann-id", "ann", 1).await?;
        drain_low(&ferry).await?;
        if ferry.new_bundles()?.is_empty() {
            return Err("low exported no bundles for the contact".into());
        }
        ferry.ferry_all()?;
        let want = BTreeMap::from([("ann".to_owned(), 1)]);
        wait_high(&high, 0, &want).await?;
        if contacts(&low, 0).await? != contacts(&high, 0).await? {
            return Err("low/high contact states diverged".into());
        }
        Ok(())
    }
    .await;
    let failed = result.is_err();
    if let Err(e) = &result {
        eprintln!("scenario failed: {e}");
    }
    if failed {
        low.dump_forensics("highlow-transfer-low").await;
        high.dump_forensics("highlow-transfer-high").await;
    }
    low.finish(failed).await;
    high.finish(failed).await;
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}

/// Gap delivery with duplicate recovery (Go:
/// `TestHighLowDuplicateReorderRecoveryLive`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gap_delivery_with_duplicate_recovery() {
    let staging_low = artifacts::runtime_dir("highlow-gap-low").join("staging");
    let staging_high = artifacts::runtime_dir("highlow-gap-high").join("staging");
    let low = open_exporter(
        "highlow-gap-low",
        LOW_DBID,
        LOW_NODE,
        staging_low.clone(),
        LOW_SIGNER_A,
        recipient_public(&HIGH_SECRET),
    )
    .await
    .expect("low cluster setup");
    let high = open_high(
        "highlow-gap-high",
        HIGH_DBID,
        vec![HIGH_NODE_0],
        vec![staging_high.clone()],
        HIGH_SECRET,
        BTreeSet::from([LOW_DBID]),
        low_signers(&LOW_SIGNER_A),
        low_origins(&low),
    )
    .await
    .expect("high cluster setup");
    migrate(&low, 0, BASE_DDL, "low").await.expect("low migrate");
    migrate(&high, 0, BASE_DDL, "high").await.expect("high migrate");
    let mut ferry = Ferry::new(staging_low, staging_high);

    let result: Result<(), String> = async {
        // Baseline: drain the migration bundles so the three contact
        // rounds below are exactly three new bundles.
        drain_low(&ferry).await?;
        ferry.ferry_all()?;
        settle().await;
        if !contacts(&high, 0).await?.is_empty() {
            return Err("high imported baseline bundles as contacts".into());
        }

        insert_contact(&low, 0, "ann-id", "ann", 0).await?;
        drain_low(&ferry).await?;
        insert_contact(&low, 0, "bob-id", "bob", 1).await?;
        drain_low(&ferry).await?;
        insert_contact(&low, 0, "cid-id", "cid", 2).await?;
        drain_low(&ferry).await?;
        let new = ferry.new_bundles()?;
        if new.len() != 3 {
            return Err(format!("expected 3 contact bundles, staged {new:?}"));
        }

        // Withhold the middle bundle (Go: rename aside): the importer
        // applies the first and records a gap instead of skipping ahead.
        // The gap bundle can never apply (sequence check), so a settle
        // sleep proves the hold.
        ferry.ferry_names(&[new[0].clone(), new[2].clone()])?;
        settle().await;
        if contacts(&high, 0).await? != BTreeMap::from([("ann".to_owned(), 0)]) {
            return Err(format!(
                "gap leg applied the wrong set: {:?}",
                contacts(&high, 0).await?
            ));
        }

        // Restoring the bundle fills the gap; all three converge.
        ferry.ferry_names(&[new[1].clone()])?;
        let want = BTreeMap::from([
            ("ann".to_owned(), 0),
            ("bob".to_owned(), 1),
            ("cid".to_owned(), 2),
        ]);
        wait_high(&high, 0, &want).await?;

        // Duplicate redelivery is a no-op (Go: imported == 0): re-copy an
        // applied bundle and prove the visible state is unchanged and the
        // bundle is not quarantined.
        std::fs::copy(ferry.low.join(&new[0]), ferry.high.join(&new[0]))
            .map_err(|e| format!("duplicate redelivery: {e}"))?;
        settle().await;
        if contacts(&high, 0).await? != want {
            return Err("duplicate redelivery changed state".into());
        }
        if !ferry.high.join(&new[0]).exists() {
            return Err("duplicate of an applied bundle was quarantined".into());
        }
        if !quarantine_files(&ferry.high).is_empty() {
            return Err(format!(
                "duplicate redelivery quarantined: {:?}",
                quarantine_files(&ferry.high)
            ));
        }
        if contacts(&low, 0).await? != contacts(&high, 0).await? {
            return Err("low/high contact states diverged".into());
        }
        Ok(())
    }
    .await;
    let failed = result.is_err();
    if let Err(e) = &result {
        eprintln!("scenario failed: {e}");
    }
    if failed {
        low.dump_forensics("highlow-gap-low").await;
        high.dump_forensics("highlow-gap-high").await;
    }
    low.finish(failed).await;
    high.finish(failed).await;
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}

/// Two receivers converge (Go: `TestHighLowTwoReceiversConvergeLive`): each
/// receiver imports the same staging independently; the High mesh then
/// converges both to identical state.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_receivers_converge() {
    let staging_low = artifacts::runtime_dir("highlow-2r-low").join("staging");
    let staging_high = artifacts::runtime_dir("highlow-2r-high").join("staging");
    let low = open_exporter(
        "highlow-2r-low",
        LOW_DBID,
        LOW_NODE,
        staging_low.clone(),
        LOW_SIGNER_A,
        recipient_public(&HIGH_SECRET),
    )
    .await
    .expect("low cluster setup");
    let high = open_high(
        "highlow-2r-high",
        HIGH_DBID,
        vec![HIGH_NODE_0, HIGH_NODE_1],
        vec![staging_high.clone(), staging_high.clone()],
        HIGH_SECRET,
        BTreeSet::from([LOW_DBID]),
        low_signers(&LOW_SIGNER_A),
        low_origins(&low),
    )
    .await
    .expect("high cluster setup");
    migrate(&low, 0, BASE_DDL, "low").await.expect("low migrate");
    migrate(&high, 0, BASE_DDL, "high-0").await.expect("high-0 migrate");
    migrate(&high, 1, BASE_DDL, "high-1").await.expect("high-1 migrate");
    high.mesh().await.expect("high mesh");
    let mut ferry = Ferry::new(staging_low, staging_high);

    let result: Result<(), String> = async {
        insert_contact(&low, 0, "ann-id", "ann", 1).await?;
        drain_low(&ferry).await?;
        ferry.ferry_all()?;
        let want = BTreeMap::from([("ann".to_owned(), 1)]);
        wait_high(&high, 0, &want).await?;
        wait_high(&high, 1, &want).await?;
        if contacts(&high, 0).await? != contacts(&high, 1).await? {
            return Err("receivers diverged".into());
        }
        if contacts(&low, 0).await? != contacts(&high, 0).await? {
            return Err("low/high contact states diverged".into());
        }
        Ok(())
    }
    .await;
    let failed = result.is_err();
    if let Err(e) = &result {
        eprintln!("scenario failed: {e}");
    }
    if failed {
        low.dump_forensics("highlow-2r-low").await;
        high.dump_forensics("highlow-2r-high").await;
    }
    low.finish(failed).await;
    high.finish(failed).await;
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}

/// Forgeries rejected without partial apply or recorded progress (Go:
/// `TestHighLowForgeriesRejectedLive`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn forgeries_rejected_without_partial_apply() {
    let staging_low = artifacts::runtime_dir("highlow-forge-low").join("staging");
    let staging_high = artifacts::runtime_dir("highlow-forge-high").join("staging");
    let staging_rogue = artifacts::runtime_dir("highlow-forge-rogue").join("staging");
    let staging_other = artifacts::runtime_dir("highlow-forge-other").join("staging");
    let low = open_exporter(
        "highlow-forge-low",
        LOW_DBID,
        LOW_NODE,
        staging_low.clone(),
        LOW_SIGNER_A,
        recipient_public(&HIGH_SECRET),
    )
    .await
    .expect("low cluster setup");
    let high = open_high(
        "highlow-forge-high",
        HIGH_DBID,
        vec![HIGH_NODE_0],
        vec![staging_high.clone()],
        HIGH_SECRET,
        BTreeSet::from([LOW_DBID]),
        low_signers(&LOW_SIGNER_A),
        low_origins(&low),
    )
    .await
    .expect("high cluster setup");
    // Rogue domain: its own signer and recipient, exporting one bundle.
    let rogue = open_exporter(
        "highlow-forge-rogue",
        ROGUE_DBID,
        ROGUE_NODE,
        staging_rogue.clone(),
        ROGUE_SIGNER,
        recipient_public(&ROGUE_SECRET),
    )
    .await
    .expect("rogue cluster setup");
    // Wrong-recipient domain: main signer and identity, unknown recipient,
    // so the bundle reaches the recipient check (a distinct domain or node
    // would fail earlier at the domain/signer checks).
    let other = open_exporter(
        "highlow-forge-other",
        LOW_DBID,
        LOW_NODE,
        staging_other.clone(),
        LOW_SIGNER_A,
        recipient_public(&OTHER_SECRET),
    )
    .await
    .expect("other cluster setup");
    migrate(&low, 0, BASE_DDL, "low").await.expect("low migrate");
    migrate(&high, 0, BASE_DDL, "high").await.expect("high migrate");
    migrate(&rogue, 0, BASE_DDL, "rogue").await.expect("rogue migrate");
    migrate(&other, 0, BASE_DDL, "other").await.expect("other migrate");
    let mut ferry = Ferry::new(staging_low, staging_high);
    let rogue_ferry = Ferry::new(staging_rogue, ferry.high.clone());
    let other_ferry = Ferry::new(staging_other, ferry.high.clone());

    let result: Result<(), String> = async {
        // Baseline the migration bundles, then stage exactly one genuine
        // contact bundle and clear it: each hostile artifact below is
        // presented alone.
        drain_low(&ferry).await?;
        ferry.ferry_all()?;
        settle().await;
        clear_bundles(&ferry.high)?;
        insert_contact(&low, 0, "ann-id", "ann", 1).await?;
        drain_low(&ferry).await?;
        ferry.ferry_all()?;
        let genuine = bundle_files(&ferry.high)?;
        if genuine.len() != 1 {
            return Err(format!("want 1 genuine bundle, staged {genuine:?}"));
        }
        let good_raw = std::fs::read(ferry.high.join(&genuine[0]))
            .map_err(|e| format!("read genuine bundle: {e}"))?;
        clear_bundles(&ferry.high)?;

        drain_low(&rogue_ferry).await?;
        insert_contact(&rogue, 0, "mallory-id", "mallory", 9).await?;
        drain_low(&rogue_ferry).await?;
        let rogue_files = bundle_files(&rogue_ferry.low)?;
        let rogue_raw = std::fs::read(
            rogue_ferry.low.join(rogue_files.last().ok_or("no rogue bundle")?),
        )
        .map_err(|e| format!("read rogue bundle: {e}"))?;
        drain_low(&other_ferry).await?;
        insert_contact(&other, 0, "carol-id", "carol", 3).await?;
        drain_low(&other_ferry).await?;
        let other_files = bundle_files(&other_ferry.low)?;
        let other_raw = std::fs::read(
            other_ferry.low.join(other_files.last().ok_or("no other bundle")?),
        )
        .map_err(|e| format!("read wrong-recipient bundle: {e}"))?;

        let mut tampered = good_raw.clone();
        let tail = tampered.len() - 1;
        tampered[tail] ^= 0xFF;
        let mut oversized = good_raw.clone();
        oversized.extend(vec![0u8; 17 << 20]);
        let hostile = [
            ("rogue.bundle", rogue_raw),
            ("sealed.bundle", other_raw),
            ("evil.bundle", tampered),
            ("big.bundle", oversized),
        ];
        for (name, raw) in &hostile {
            std::fs::write(ferry.high.join(name), raw)
                .map_err(|e| format!("stage hostile artifact: {e}"))?;
            // Fail-closed quarantine (deviation: Go's import returns an
            // error; Rust quarantines and returns Ok, driven here by the
            // background importer).
            settle().await;
            if !contacts(&high, 0).await?.is_empty() {
                return Err(format!(
                    "hostile artifact {name} partially applied: {:?}",
                    contacts(&high, 0).await?
                ));
            }
            if quarantine_files(&ferry.high) != vec![name.to_string()] {
                return Err(format!(
                    "hostile artifact {name} quarantine: {:?}",
                    quarantine_files(&ferry.high)
                ));
            }
            std::fs::remove_file(ferry.high.join("quarantine").join(name))
                .map_err(|e| format!("clear quarantine: {e}"))?;
        }

        // Wrong extensions are not bridge artifacts at all: ignored in
        // place, never quarantined, never applied.
        for name in ["wrongname.txt", ".hidden"] {
            std::fs::write(ferry.high.join(name), &good_raw)
                .map_err(|e| format!("stage misnamed artifact: {e}"))?;
            settle().await;
            if !contacts(&high, 0).await?.is_empty() {
                return Err(format!(
                    "misnamed artifact {name} applied: {:?}",
                    contacts(&high, 0).await?
                ));
            }
            if !quarantine_files(&ferry.high).is_empty() {
                return Err(format!("misnamed artifact {name} quarantined"));
            }
            if !ferry.high.join(name).exists() {
                return Err(format!("ignored artifact {name} was moved"));
            }
            std::fs::remove_file(ferry.high.join(name))
                .map_err(|e| format!("clear misnamed: {e}"))?;
        }

        // No unreadable-staging-file leg: the background importer swallows
        // the error with no wire-visible signal (the old in-process port
        // asserted the direct `process()` Err).

        // The importer still accepts the genuine bundle afterwards, which
        // also proves no hostile artifact recorded progress (a skipped
        // sequence would stay skipped).
        std::fs::write(ferry.high.join(&genuine[0]), &good_raw)
            .map_err(|e| format!("restore genuine: {e}"))?;
        let want = BTreeMap::from([("ann".to_owned(), 1)]);
        wait_high(&high, 0, &want).await?;
        if contacts(&low, 0).await? != contacts(&high, 0).await? {
            return Err("low/high contact states diverged".into());
        }
        Ok(())
    }
    .await;
    let failed = result.is_err();
    if let Err(e) = &result {
        eprintln!("scenario failed: {e}");
    }
    if failed {
        low.dump_forensics("highlow-forge-low").await;
        high.dump_forensics("highlow-forge-high").await;
        rogue.dump_forensics("highlow-forge-rogue").await;
        other.dump_forensics("highlow-forge-other").await;
    }
    low.finish(failed).await;
    high.finish(failed).await;
    rogue.finish(failed).await;
    other.finish(failed).await;
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}

/// Restart resumption on both sides (Go: `TestHighLowRestartResumeLive`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_resumes_without_recapture_or_reapply() {
    let staging_low = artifacts::runtime_dir("highlow-restart-low").join("staging");
    let staging_high = artifacts::runtime_dir("highlow-restart-high").join("staging");
    let mut low = open_exporter(
        "highlow-restart-low",
        LOW_DBID,
        LOW_NODE,
        staging_low.clone(),
        LOW_SIGNER_A,
        recipient_public(&HIGH_SECRET),
    )
    .await
    .expect("low cluster setup");
    let mut high = open_high(
        "highlow-restart-high",
        HIGH_DBID,
        vec![HIGH_NODE_0],
        vec![staging_high.clone()],
        HIGH_SECRET,
        BTreeSet::from([LOW_DBID]),
        low_signers(&LOW_SIGNER_A),
        low_origins(&low),
    )
    .await
    .expect("high cluster setup");
    migrate(&low, 0, BASE_DDL, "low").await.expect("low migrate");
    migrate(&high, 0, BASE_DDL, "high").await.expect("high migrate");
    let ferry = Ferry::new(staging_low, staging_high);

    let result = run_restart_body(&mut low, &mut high, ferry).await;
    let failed = result.is_err();
    if let Err(e) = &result {
        eprintln!("scenario failed: {e}");
    }
    if failed {
        low.dump_forensics("highlow-restart-low").await;
        high.dump_forensics("highlow-restart-high").await;
    }
    low.finish(failed).await;
    high.finish(failed).await;
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}

async fn run_restart_body(
    low: &mut Cluster,
    high: &mut Cluster,
    mut ferry: Ferry,
) -> Result<(), String> {
    insert_contact(low, 0, "ann-id", "ann", 1).await?;
    insert_contact(low, 0, "bob-id", "bob", 2).await?;
    drain_low(&ferry).await?;
    ferry.ferry_all()?;
    let want = BTreeMap::from([("ann".to_owned(), 1), ("bob".to_owned(), 2)]);
    wait_high(high, 0, &want).await?;

    // Restarting the exporter loses no capture resume: nothing
    // re-captures (Go: captured == 0), proven by the Low bundle file
    // set being unchanged across the restart.
    let before = bundle_files(&ferry.low)?;
    low.stop_node(0).await;
    low.start_node(0).await.map_err(|e| format!("low restart: {e}"))?;
    drain_low(&ferry).await?;
    if bundle_files(&ferry.low)? != before {
        return Err("low re-exported bundles across restart".into());
    }

    // Restarting the importer loses no inbox/cursor resume: the
    // redelivery is a no-op (Go: received == imported == 0) and state
    // holds both contacts exactly once.
    high.stop_node(0).await;
    high.start_node(0).await.map_err(|e| format!("high restart: {e}"))?;
    settle().await;
    if contacts(high, 0).await? != want {
        return Err("high state changed across restart".into());
    }
    if contacts(low, 0).await? != contacts(high, 0).await? {
        return Err("low/high contact states diverged".into());
    }
    Ok(())
}

/// Schema-hold survival with local-migration release (Go:
/// `TestHighLowSchemaHoldMigrationLive`): the unknown column holds the
/// bundles (nothing applies, no DDL crosses), the hold survives restart,
/// and a local High migration releases the held bundles.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schema_hold_survives_restart_and_releases_on_local_migration() {
    let staging_low = artifacts::runtime_dir("highlow-schema-low").join("staging");
    let staging_high = artifacts::runtime_dir("highlow-schema-high").join("staging");
    let low = open_exporter(
        "highlow-schema-low",
        LOW_DBID,
        LOW_NODE,
        staging_low.clone(),
        LOW_SIGNER_A,
        recipient_public(&HIGH_SECRET),
    )
    .await
    .expect("low cluster setup");
    let mut high = open_high(
        "highlow-schema-high",
        HIGH_DBID,
        vec![HIGH_NODE_0],
        vec![staging_high.clone()],
        HIGH_SECRET,
        BTreeSet::from([LOW_DBID]),
        low_signers(&LOW_SIGNER_A),
        low_origins(&low),
    )
    .await
    .expect("high cluster setup");
    migrate(&low, 0, EMAIL_DDL, "low").await.expect("low migrate");
    migrate(&high, 0, BASE_DDL, "high").await.expect("high migrate");
    let mut ferry = Ferry::new(staging_low, staging_high);

    let result = run_schema_body(&low, &mut high, &mut ferry).await;
    let failed = result.is_err();
    if let Err(e) = &result {
        eprintln!("scenario failed: {e}");
    }
    if failed {
        low.dump_forensics("highlow-schema-low").await;
        high.dump_forensics("highlow-schema-high").await;
    }
    low.finish(failed).await;
    high.finish(failed).await;
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}

async fn run_schema_body(
    low: &Cluster,
    high: &mut Cluster,
    ferry: &mut Ferry,
) -> Result<(), String> {
    insert_contact(low, 0, "ann-id", "ann", 1).await?;
    set_email(low, 0, "ann-id", "a@x").await?;
    drain_low(ferry).await?;
    ferry.ferry_all()?;
    // The unknown column holds the bundles: nothing applies (Rust
    // traps the hold internally and returns Ok, so there is no
    // waiting-schema error to assert like Go).
    settle().await;
    if !contacts(high, 0).await?.is_empty() {
        return Err(format!("held bundle applied: {:?}", contacts(high, 0).await?));
    }
    if has_column(high, 0, TABLE, "email").await {
        return Err("DDL crossed the bridge".into());
    }
    // The hold survives restart and still applies nothing.
    high.stop_node(0).await;
    high.start_node(0).await.map_err(|e| format!("high restart: {e}"))?;
    settle().await;
    if !contacts(high, 0).await?.is_empty() {
        return Err(format!(
            "held bundle applied after restart: {:?}",
            contacts(high, 0).await?
        ));
    }
    if has_column(high, 0, TABLE, "email").await {
        return Err("DDL crossed the bridge".into());
    }
    // The local migration releases the bundles; the held email arrives.
    // (Local DDL only: the replicated flag set at first migrate persists
    // in the server-owned config section.)
    high.ensure_migrated(0, ADD_EMAIL)
        .await
        .map_err(|e| format!("high ADD_EMAIL migrate failed: {e}"))?;
    high.node(0).flush().await.map_err(|e| format!("high flush after ADD_EMAIL: {e}"))?;
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        if email_of(high, 0, "ann-id").await.as_deref() == Some("a@x") {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "held email never arrived after local migration: {:?}",
                email_of(high, 0, "ann-id").await
            ));
        }
        tokio::time::sleep(POLL).await;
    }
    if contacts(high, 0).await? != BTreeMap::from([("ann".to_owned(), 1)]) {
        return Err("released bundle applied the wrong set".into());
    }
    Ok(())
}

/// Signer rotation plus outage catch-up (Go:
/// `TestHighLowKeyRotationAndOutageResumeLive`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn signer_rotation_and_outage_catch_up() {
    let staging_low = artifacts::runtime_dir("highlow-rotate-low").join("staging");
    let staging_high = artifacts::runtime_dir("highlow-rotate-high").join("staging");
    let mut low = open_exporter(
        "highlow-rotate-low",
        LOW_DBID,
        LOW_NODE,
        staging_low.clone(),
        LOW_SIGNER_A,
        recipient_public(&HIGH_SECRET),
    )
    .await
    .expect("low cluster setup");
    let mut high = open_high(
        "highlow-rotate-high",
        HIGH_DBID,
        vec![HIGH_NODE_0],
        vec![staging_high.clone()],
        HIGH_SECRET,
        BTreeSet::from([LOW_DBID]),
        low_signers(&LOW_SIGNER_A),
        low_origins(&low),
    )
    .await
    .expect("high cluster setup");
    migrate(&low, 0, BASE_DDL, "low").await.expect("low migrate");
    migrate(&high, 0, BASE_DDL, "high").await.expect("high migrate");
    let mut ferry = Ferry::new(staging_low, staging_high);
    let low_origin = low.node_origin_pubkey(0);

    let result = run_rotation_body(&mut low, &mut high, &mut ferry, low_origin).await;
    let failed = result.is_err();
    if let Err(e) = &result {
        eprintln!("scenario failed: {e}");
    }
    if failed {
        low.dump_forensics("highlow-rotate-low").await;
        high.dump_forensics("highlow-rotate-high").await;
    }
    low.finish(failed).await;
    high.finish(failed).await;
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}

async fn run_rotation_body(
    low: &mut Cluster,
    high: &mut Cluster,
    ferry: &mut Ferry,
    low_origin: [u8; 32],
) -> Result<(), String> {
    drain_low(ferry).await?;
    ferry.ferry_all()?;
    settle().await;

    // Round 1 under the predecessor key, imported before rotation
    // (Rust pins a single signer per source node, so Go's overlap
    // trust is inexpressible; see the module docs).
    insert_contact(low, 0, "ann-id", "ann", 1).await?;
    drain_low(ferry).await?;
    let round1 = ferry.new_bundles()?;
    ferry.ferry_names(&round1)?;
    wait_high(high, 0, &BTreeMap::from([("ann".to_owned(), 1)])).await?;

    // Rotate: the successor signs from here on (config edit plus
    // restart on both sides, Go rewriteNodeConfig parity).
    low.stop_node(0).await;
    low.set_bridge_config(
        0,
        Some(BridgeNodeConfig {
            role: "low-exporter".into(),
            domain: hex(&LOW_DBID),
            directory: ferry.low.to_string_lossy().replace('\\', "/"),
            stream: STREAM.into(),
            recipient: hex(&recipient_public(&HIGH_SECRET)),
            signer_hex: Some(hex(&LOW_SIGNER_B)),
            ..Default::default()
        }),
    )
    .map_err(|e| format!("low signer rotation: {e}"))?;
    low.start_node(0).await.map_err(|e| format!("low restart after rotation: {e}"))?;
    high.stop_node(0).await;
    high.set_bridge_config(
        0,
        Some(BridgeNodeConfig {
            role: "high-receiver".into(),
            domain: hex(&HIGH_DBID),
            trusted_domains: vec![hex(&LOW_DBID)],
            trusted_origins: BTreeMap::from([(hex(&LOW_NODE), hex(&low_origin))]),
            directory: ferry.high.to_string_lossy().replace('\\', "/"),
            stream: STREAM.into(),
            recipient: hex(&recipient_public(&HIGH_SECRET)),
            receiver_secret_hex: Some(hex(&HIGH_SECRET)),
            trusted_signers: BTreeMap::from([(hex(&LOW_NODE), hex(&signer_public(&LOW_SIGNER_B)))]),
            ..Default::default()
        }),
    )
    .map_err(|e| format!("high signer rotation: {e}"))?;
    high.start_node(0).await.map_err(|e| format!("high restart after rotation: {e}"))?;

    // Round 2 under the successor key imports on top of round 1.
    insert_contact(low, 0, "bob-id", "bob", 2).await?;
    drain_low(ferry).await?;
    let round2 = ferry.new_bundles()?;
    ferry.ferry_names(&round2)?;
    let both = BTreeMap::from([("ann".to_owned(), 1), ("bob".to_owned(), 2)]);
    wait_high(high, 0, &both).await?;

    // Retire the predecessor: clear the applied bundles, then prove a
    // duplicate of a current bundle still stages cleanly while a
    // retired-signer replay fails closed without disturbing state.
    clear_bundles(&ferry.high)?;
    clear_quarantine(&ferry.high)?;
    std::fs::copy(ferry.low.join(&round2[0]), ferry.high.join(&round2[0]))
        .map_err(|e| format!("duplicate round-2 bundle: {e}"))?;
    settle().await;
    if !ferry.high.join(&round2[0]).exists() {
        return Err("duplicate of a current bundle was quarantined".into());
    }
    if !quarantine_files(&ferry.high).is_empty() {
        return Err(format!(
            "current duplicate quarantined: {:?}",
            quarantine_files(&ferry.high)
        ));
    }
    if contacts(high, 0).await? != both {
        return Err("current duplicate changed state".into());
    }
    std::fs::remove_file(ferry.high.join(&round2[0]))
        .map_err(|e| format!("clear duplicate: {e}"))?;
    let retired =
        std::fs::read(ferry.low.join(&round1[0])).map_err(|e| format!("read round-1 bundle: {e}"))?;
    std::fs::write(ferry.high.join("replay.bundle"), &retired)
        .map_err(|e| format!("stage replay: {e}"))?;
    settle().await;
    if quarantine_files(&ferry.high) != vec!["replay.bundle".to_owned()] {
        return Err(format!(
            "retired-signer replay not quarantined: {:?}",
            quarantine_files(&ferry.high)
        ));
    }
    if contacts(high, 0).await? != both {
        return Err("retired-signer replay disturbed state".into());
    }
    std::fs::remove_file(ferry.high.join("quarantine").join("replay.bundle"))
        .map_err(|e| format!("clear quarantine: {e}"))?;

    // Prolonged outage: events queue durably on Low (capture-only; the
    // bundles stay in the Low dir), survive a Low restart, then catch
    // up completely. There is no pending-events counter API, so the
    // exact converged total (backlog == 4) proves all five events
    // arrived exactly once.
    insert_contact(low, 0, "backlog-id", "backlog", 0).await?;
    for _ in 0..4 {
        add_score(low, 0, "backlog-id", 1).await?;
    }
    drain_low(ferry).await?;
    low.stop_node(0).await;
    low.start_node(0).await.map_err(|e| format!("low restart during outage: {e}"))?;
    if contacts(high, 0).await?.contains_key("backlog") {
        return Err(format!("outage leaked to high: {:?}", contacts(high, 0).await?));
    }
    ferry.ferry_all()?;
    let caught_up = BTreeMap::from([
        ("ann".to_owned(), 1),
        ("bob".to_owned(), 2),
        ("backlog".to_owned(), 4),
    ]);
    wait_high(high, 0, &caught_up).await?;
    if contacts(low, 0).await? != contacts(high, 0).await? {
        return Err("low/high contact states diverged".into());
    }
    Ok(())
}

/// High-owned-field reorder convergence across two meshed High peers (Go:
/// `TestHighLowOwnershipReorderAcrossPeersLive`): peer A imports bundle 1,
/// takes a High override, then imports bundle 2 (override-then-import);
/// peer B imports both bundles before the mesh delivers the override
/// (import-then-override). Both converge on the High bytes with High-owned
/// provenance. (Adversarial HLC ordering stays pinned at the DB layer by
/// `TestBridgeShadowReorderConvergence`, as in Go. The explicit-release
/// convergence leg has no wire equivalent and is omitted.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn high_owned_field_reorder_converges_across_peers() {
    let staging_low = artifacts::runtime_dir("highlow-own-low").join("staging");
    let staging_high = artifacts::runtime_dir("highlow-own-high").join("staging");
    let low = open_exporter(
        "highlow-own-low",
        LOW_DBID,
        LOW_NODE,
        staging_low.clone(),
        LOW_SIGNER_A,
        recipient_public(&HIGH_SECRET),
    )
    .await
    .expect("low cluster setup");
    let high = open_high(
        "highlow-own-high",
        HIGH_DBID,
        vec![HIGH_NODE_0, HIGH_NODE_1],
        vec![staging_high.clone(), staging_high.clone()],
        HIGH_SECRET,
        BTreeSet::from([LOW_DBID]),
        low_signers(&LOW_SIGNER_A),
        low_origins(&low),
    )
    .await
    .expect("high cluster setup");
    migrate(&low, 0, BASE_DDL, "low").await.expect("low migrate");
    migrate(&high, 0, BASE_DDL, "high-0").await.expect("high-0 migrate");
    migrate(&high, 1, BASE_DDL, "high-1").await.expect("high-1 migrate");
    let mut ferry = Ferry::new(staging_low, staging_high);

    let result: Result<(), String> = async {
        drain_low(&ferry).await?;
        ferry.ferry_all()?;
        settle().await;

        insert_contact(&low, 0, "row-0", "low-1", 1).await?;
        drain_low(&ferry).await?;
        ferry.ferry_all()?;
        // Peer A: override-then-import. The wait for bundle 1 is the
        // ordering gate: the override lands strictly before bundle 2
        // exists, so no background pump can collapse the order.
        wait_high(&high, 0, &BTreeMap::from([("low-1".to_owned(), 1)])).await?;
        rename(&high, 0, "row-0", "high").await?;
        if contacts(&high, 0).await? != BTreeMap::from([("high".to_owned(), 1)]) {
            return Err("A override not visible locally".into());
        }
        rename(&low, 0, "row-0", "low-2").await?;
        drain_low(&ferry).await?;
        ferry.ferry_all()?;
        wait_high(&high, 0, &BTreeMap::from([("high".to_owned(), 1)])).await?;

        // Peer B: import-then-override (the override arrives later over
        // the mesh). B has no owner record yet (no mesh), so both bundles
        // show the Low value.
        wait_high(&high, 1, &BTreeMap::from([("low-2".to_owned(), 1)])).await?;

        // Mesh the High peers only after both orders are established
        // locally; the mesh is the only override-delivery path.
        high.add_peer(0, 1).await.map_err(|e| format!("mesh a->b: {e}"))?;
        high.add_peer(1, 0).await.map_err(|e| format!("mesh b->a: {e}"))?;
        let owned = BTreeMap::from([("high".to_owned(), 1)]);
        wait_meshed(&high, &[0, 1], &owned, "ownership reorder").await?;
        if contacts(&high, 0).await? != contacts(&high, 1).await? {
            return Err("peers diverged after mesh".into());
        }

        // High-owned provenance (behavioral): drain the bridge to idle so
        // the Low update has provably arrived, then prove it stayed
        // suppressed on both peers.
        rename(&low, 0, "row-0", "low-3").await?;
        drain_low(&ferry).await?;
        ferry.ferry_all()?;
        settle().await;
        // Settle again after a second drain: the ferry above moved every
        // exported bundle, so the background importer has seen them all.
        drain_low(&ferry).await?;
        ferry.ferry_all()?;
        settle().await;
        if contacts(&high, 0).await? != owned {
            return Err("low-3 leaked through A's ownership".into());
        }
        if contacts(&high, 1).await? != owned {
            return Err("low-3 leaked through B's ownership".into());
        }
        Ok(())
    }
    .await;
    let failed = result.is_err();
    if let Err(e) = &result {
        eprintln!("scenario failed: {e}");
    }
    if failed {
        low.dump_forensics("highlow-own-low").await;
        high.dump_forensics("highlow-own-high").await;
    }
    low.finish(failed).await;
    high.finish(failed).await;
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}
