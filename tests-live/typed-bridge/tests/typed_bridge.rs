//! Rust port of the Go `typed-bridge` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/typed-bridge/`
//! (`typed_bridge_test.go`; the Go dir has no README.md). Two single-node
//! `murmur-server` clusters form the Low (exporter) and High (importer)
//! bridge domains sharing one staging directory: a typed row inserted on Low
//! is exported, imported through High's typed durable path
//! (`high_low::import`), overridden by a High write (automatic ownership via
//! `promote`), shielded from a Low update, released over the wire
//! (`MURMUR BRIDGE RELEASE`), and updated by Low again; the imported
//! result's survival across a High restart is proven last.
//!
//! Plain SQL replaces the Go typed HTTP API (the Rust product has no HTTP
//! server); `TypedRename` maps to an `UPDATE` of the LWW `title` field so the
//! primary-key row identity is preserved (ownership only covers non-PK cells).
//! The replication engine pumps bridge capture/process on its 1 s background
//! tick, so export/import progress is proven by row convergence instead of
//! manual pumps. See the scenario README.md for the full leg mapping.

use std::collections::HashMap;
use std::time::Duration;

use murmur_live_harness::{
    artifacts, hex, BridgeNodeConfig, BridgeOptions, Cluster, ClusterOptions,
};

/// Low (exporter) cluster name; owns `runtime/typed-bridge-low/`, which also
/// holds the shared staging dir so artifacts are retained/cleaned with it.
const LOW_NAME: &str = "typed-bridge-low";
/// High (importer) cluster name; owns `runtime/typed-bridge-high/`.
const HIGH_NAME: &str = "typed-bridge-high";
/// Bridge stream (Go: `typed-stream`).
const STREAM: &str = "typed-stream";
/// Typed table (Go: `live_typed_records`). `title` is the LWW rename target.
const TABLE: &str = "live_typed_records";
const DDL: &str =
    "CREATE TABLE live_typed_records(name TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL)";
/// Row identity, fixed for the whole scenario (Go renames one row by ID).
const ROW: &str = "bridge-native-record";
/// Convergence deadline (Go: 30 s waits).
const WAIT: Duration = Duration::from_secs(30);
/// Convergence poll interval (Go: 50 ms polls).
const POLL: Duration = Duration::from_millis(50);
/// Export proof deadline: a new bundle file must appear in the staging dir
/// after a Low write (the engine capture tick runs every 1 s).
const EXPORT_TIMEOUT: Duration = Duration::from_secs(30);
/// Settle after the export proof before asserting suppression: the High-side
/// import tick runs every 1 s, so this covers several pump cycles.
const SUPPRESSION_SETTLE: Duration = Duration::from_secs(10);

/// Independent bridge domains (Go: two separate clusters).
const LOW_DBID: [u8; 16] = [0xb0; 16];
const HIGH_DBID: [u8; 16] = [0xb1; 16];
const LOW_NODE: [u8; 16] = [0x10; 16];
const HIGH_NODE: [u8; 16] = [0x11; 16];
/// Low bundle-signer seed (ed25519; High pins the verifying key).
const LOW_SIGNER: [u8; 32] = [0x21; 32];
/// High receiver secret (X25519; Low encrypts to the derived recipient).
const HIGH_SECRET: [u8; 32] = [0x22; 32];

/// ed25519 verifying key for a signer seed (mirrors
/// `murmur_replication::protocol::public_key`).
fn ed25519_public(seed: &[u8; 32]) -> [u8; 32] {
    ed25519_dalek::SigningKey::from_bytes(seed).verifying_key().to_bytes()
}

/// X25519 recipient public key for a receiver secret (mirrors
/// `murmur_replication::bridge::recipient_public`).
fn x25519_public(secret: [u8; 32]) -> [u8; 32] {
    x25519_dalek::PublicKey::from(&x25519_dalek::StaticSecret::from(secret)).to_bytes()
}

/// Typed insert on Low (Go: `Cluster.TypedInsertWithID`).
async fn insert_row(low: &Cluster) -> Result<(), String> {
    low.node(0)
        .execute(
            "INSERT INTO live_typed_records(name, title) VALUES($1, $2)",
            &[&ROW, &ROW],
        )
        .await
        .map_err(|e| format!("low typed insert failed: {e}"))?;
    low.node(0)
        .simple("MURMUR FLUSH")
        .await
        .map_err(|e| format!("low flush after typed insert failed: {e}"))?;
    Ok(())
}

/// Known-field update (Go: `Cluster.TypedRename`, applied to the LWW `title`
/// field so the primary-key row identity is preserved).
async fn update_title(cluster: &Cluster, label: &str, title: &str) -> Result<(), String> {
    cluster
        .node(0)
        .execute(
            "UPDATE live_typed_records SET title = $1 WHERE name = $2",
            &[&title, &ROW],
        )
        .await
        .map_err(|e| format!("{label} title update failed: {e}"))?;
    cluster
        .node(0)
        .simple("MURMUR FLUSH")
        .await
        .map_err(|e| format!("{label} flush after title update failed: {e}"))?;
    Ok(())
}

/// Release High ownership of one field (Go: `Cluster.BridgeRelease`).
/// Retries while the engine reports busy (`released` = false). Callers
/// pass the column in mixed case to cover case-insensitive resolution.
async fn release_title(high: &Cluster, column: &str) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        let rows = high
            .node(0)
            .simple(&format!("MURMUR BRIDGE RELEASE {TABLE} '{ROW}' COLUMN {column}"))
            .await
            .map_err(|e| format!("high bridge release failed: {e}"))?;
        if rows.first().is_some_and(|r| r.first().is_some_and(|v| v == "true")) {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err("high bridge release never applied (engine busy)".into());
        }
        tokio::time::sleep(POLL).await;
    }
}

/// Row count by title on High (Go: `Cluster.TypedCount`).
async fn title_count(high: &Cluster, title: &str) -> Result<i64, String> {
    let rows = high
        .node(0)
        .query(
            "SELECT count(*) FROM live_typed_records WHERE title = $1",
            &[&title],
        )
        .await
        .map_err(|e| format!("high title count failed: {e}"))?;
    rows.first()
        .ok_or_else(|| "high title count returned no rows".to_string())?
        .try_get(0)
        .map_err(|e| format!("high decode title count: {e}"))
}

/// Wait until High shows `want` rows with `title` (the engine's 1 s tick
/// pumps export on Low and import on High; progress is proven by row
/// convergence).
async fn wait_title(high: &Cluster, title: &str, want: i64) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        if title_count(high, title).await? == want {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "high title count for {title:?} = {}, want {want}",
                title_count(high, title).await?
            ));
        }
        tokio::time::sleep(POLL).await;
    }
}

/// Bundle files currently in the staging dir.
fn bundles(staging: &std::path::Path) -> Vec<std::path::PathBuf> {
    let Ok(entries) = std::fs::read_dir(staging) else {
        return Vec::new();
    };
    entries
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|e| e == "bundle"))
        .collect()
}

/// Wait until the staging dir holds more than `before` bundle files: proves
/// Low exported the latest write (the engine capture tick runs every 1 s).
async fn wait_exported(staging: &std::path::Path, before: usize) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + EXPORT_TIMEOUT;
    loop {
        let count = bundles(staging).len();
        if count > before {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "staging dir still holds {count} bundle files after {EXPORT_TIMEOUT:?} (had {before}); low never exported"
            ));
        }
        tokio::time::sleep(POLL).await;
    }
}

async fn run_body(
    low: &mut Cluster,
    high: &mut Cluster,
    staging: &std::path::Path,
) -> Result<(), String> {
    for (label, cluster) in [("low", &*low), ("high", &*high)] {
        cluster
            .ensure_migrated(0, DDL)
            .await
            .map_err(|e| format!("{label} migrate failed: {e}"))?;
        cluster
            .node(0)
            .simple(&format!("MURMUR SET TABLE {TABLE} REPLICATED true"))
            .await
            .map_err(|e| format!("{label} SET TABLE REPLICATED: {e}"))?;
        cluster
            .node(0)
            .simple("MURMUR FLUSH")
            .await
            .map_err(|e| format!("{label} flush after SET TABLE failed: {e}"))?;
    }

    // Export + import the native record through the typed durable path.
    insert_row(low).await?;
    wait_title(high, ROW, 1).await?;
    // High typed field override claims ownership of `title`.
    update_title(high, "high", "high-owned-record").await?;
    if title_count(high, "high-owned-record").await? != 1 {
        return Err("high override not visible locally".into());
    }
    // Low update while the field is High-owned: exported (bundle proof)
    // and imported, but suppressed by High ownership.
    let before = bundles(staging).len();
    update_title(low, "low", "low-update-before-release").await?;
    wait_exported(staging, before).await?;
    // The import tick runs every 1 s; settle so the (suppressed) import
    // has provably had time to arrive before asserting it stayed out.
    tokio::time::sleep(SUPPRESSION_SETTLE).await;
    if title_count(high, "high-owned-record").await? != 1 {
        return Err("high-owned value lost to low update".into());
    }
    if title_count(high, "low-update-before-release").await? != 0 {
        return Err("low update leaked through high ownership".into());
    }
    // Release High ownership of `title`: the next Low update flows
    // through and replaces the High-owned value.
    release_title(high, "TITLE").await?;
    update_title(low, "low", "low-update-after-release").await?;
    wait_title(high, "low-update-after-release", 1).await?;
    if title_count(high, "high-owned-record").await? != 0 {
        return Err("high-owned value survived release".into());
    }
    // The imported result survives a High-node restart.
    high.stop_node(0).await;
    high.start_node(0)
        .await
        .map_err(|e| format!("high restart: {e}"))?;
    if title_count(high, "low-update-after-release").await? != 1 {
        return Err("imported row lost across high restart".into());
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn typed_bridge_imports_into_native_rime() {
    let staging = artifacts::runtime_dir(LOW_NAME).join("staging");
    let recipient = hex(&x25519_public(HIGH_SECRET));
    let low = Cluster::new(ClusterOptions {
        name: LOW_NAME.into(),
        num_nodes: 1,
        database_id: Some(LOW_DBID),
        node_ids: vec![LOW_NODE],
        manual_peers: true,
        bridge: BridgeOptions {
            nodes: HashMap::from([(
                0,
                BridgeNodeConfig {
                    role: "low-exporter".into(),
                    domain: hex(&LOW_DBID),
                    directory: staging.to_string_lossy().into_owned(),
                    stream: STREAM.into(),
                    recipient: recipient.clone(),
                    signer_hex: Some(hex(&LOW_SIGNER)),
                    ..Default::default()
                },
            )]),
        },
        ..Default::default()
    })
    .await
    .expect("low cluster setup");
    // High pins Low's origin key so imported transactions verify, and
    // Low's bundle-signer key so bundles verify; the seeds themselves
    // stay inside the test/harness.
    let low_origin = low.node_origin_pubkey(0);
    let mut high = Cluster::new(ClusterOptions {
        name: HIGH_NAME.into(),
        num_nodes: 1,
        database_id: Some(HIGH_DBID),
        node_ids: vec![HIGH_NODE],
        manual_peers: true,
        bridge: BridgeOptions {
            nodes: HashMap::from([(
                0,
                BridgeNodeConfig {
                    role: "high-receiver".into(),
                    domain: hex(&HIGH_DBID),
                    trusted_domains: vec![hex(&LOW_DBID)],
                    trusted_origins: [(hex(&LOW_NODE), hex(&low_origin))].into(),
                    directory: staging.to_string_lossy().into_owned(),
                    stream: STREAM.into(),
                    recipient,
                    receiver_secret_hex: Some(hex(&HIGH_SECRET)),
                    trusted_signers: [(hex(&LOW_NODE), hex(&ed25519_public(&LOW_SIGNER)))]
                        .into(),
                    ..Default::default()
                },
            )]),
        },
        ..Default::default()
    })
    .await
    .expect("high cluster setup");
    let mut low = low;
    let result = run_body(&mut low, &mut high, &staging).await;
    let failed = result.is_err();
    if let Err(e) = &result {
        eprintln!("scenario failed: {e}");
    }
    if failed {
        low.dump_forensics("typed-bridge-low").await;
        high.dump_forensics("typed-bridge-high").await;
    }
    low.finish(failed).await;
    high.finish(failed).await;
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}
