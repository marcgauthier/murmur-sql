//! Rust port of the Go `bridge-two-streams` live scenario (main test).
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/bridge-two-streams/`
//! (`two_streams_test.go` + `README.md`). Two independent single-node Low
//! domains (distinct DBIDs, CAs, keys, streams, staging dirs) export managed
//! rows; a two-node High cluster imports stream-a on node0 and stream-b on
//! node1, then converges both domains' rows over its mesh. A stream-a-only
//! second round must arrive without disturbing stream-b progress.
//!
//! The engine drives bridge capture/process automatically in the background;
//! the test plays the operator delivery leg by copying completed `.bundle`
//! artifacts from each Low outbox to its High inbox. Plain SQL replaces the
//! Go typed HTTP API (the Rust product has no HTTP server); progress is
//! proven by row convergence (see the scenario README.md).
//!
//! NOT ported: `TestCrossDomainIdentityCollisionFailsClosed`. The Go product
//! rejects a cross-domain same-row-ID import with `ErrIdentityCollision`
//! ("collides") and leaves first-writer state intact; the Rust product has no
//! row-identity collision detection (one shadow per source domain, merged
//! deterministically) and no wire-observable import-rejection signal.
//! Asserting the current merge behavior would enshrine a fail-open
//! divergence from the Go gate. See the scenario README.md.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use murmur_live_harness::tokio_postgres::types::ToSql;
use murmur_live_harness::{
    artifacts, hex, BridgeNodeConfig, BridgeOptions, Cluster, ClusterOptions,
};

/// Low-A (exporter) cluster name; owns `runtime/bridge-two-streams-low-a/`,
/// which also holds its outbox so artifacts are retained/cleaned with it.
const LOW_A_NAME: &str = "bridge-two-streams-low-a";
/// Low-B (exporter) cluster name; owns `runtime/bridge-two-streams-low-b/`.
const LOW_B_NAME: &str = "bridge-two-streams-low-b";
/// High (importer) cluster name; owns `runtime/bridge-two-streams-high/`.
const HIGH_NAME: &str = "bridge-two-streams-high";
/// Bridge streams (Go: `stream-a`, `stream-b`).
const STREAM_A: &str = "stream-a";
const STREAM_B: &str = "stream-b";
/// Convergence deadline (Go: 60 s waits).
const WAIT: Duration = Duration::from_secs(60);
/// Convergence poll interval (Go: 200 ms polls).
const POLL: Duration = Duration::from_millis(200);
/// Redelivery stability window: identical bytes are re-copied while the
/// converged set must stay unchanged (receipt idempotence).
const IDEMPOTENCE_WINDOW: Duration = Duration::from_secs(5);

/// Independent bridge domains (Go: distinct DBIDs, CAs, keys per domain;
/// each harness cluster already provisions its own CA).
const LOW_A_DBID: [u8; 16] = [0xA0; 16];
const LOW_B_DBID: [u8; 16] = [0xB0; 16];
const HIGH_DBID: [u8; 16] = [0xC0; 16];
const LOW_A_NODE: [u8; 16] = [0xA1; 16];
const LOW_B_NODE: [u8; 16] = [0xB1; 16];
const HIGH_NODE_0: [u8; 16] = [0xC1; 16];
const HIGH_NODE_1: [u8; 16] = [0xC2; 16];
const LOW_A_KEY: [u8; 32] = [0xA2; 32];
const LOW_B_KEY: [u8; 32] = [0xB2; 32];
const HIGH_KEY: [u8; 32] = [0xC3; 32];
/// Low bundle-signer seeds (ed25519; each High importer pins its domain key).
const LOW_A_SIGNER: [u8; 32] = [0x21; 32];
const LOW_B_SIGNER: [u8; 32] = [0x22; 32];
/// High receiver secrets (X25519; one per stream, like the Go key files).
const HIGH_SECRET_A: [u8; 32] = [0x31; 32];
const HIGH_SECRET_B: [u8; 32] = [0x32; 32];

fn ed25519_public(seed: &[u8; 32]) -> [u8; 32] {
    ed25519_dalek::SigningKey::from_bytes(seed).verifying_key().to_bytes()
}

fn x25519_public(secret: &[u8; 32]) -> [u8; 32] {
    x25519_dalek::PublicKey::from(&x25519_dalek::StaticSecret::from(*secret)).to_bytes()
}

/// Go row identity: `%032x` of a per-domain base plus the index.
fn row_id(base: u32, i: u32) -> String {
    format!("{:032x}", base + i)
}

/// Contention insert (Go: `TypedContentionInsert`).
async fn insert_row(cluster: &Cluster, id: &str, name: &str) -> Result<(), String> {
    let params = [&id as &(dyn ToSql + Sync), &name as _];
    cluster
        .node(0)
        .execute("INSERT INTO live_typed_contention(id, name) VALUES($1, $2)", &params)
        .await
        .map_err(|e| format!("insert {id:?}: {e}"))?;
    cluster.node(0).simple("MURMUR FLUSH").await.map_err(|e| format!("flush after {id:?}: {e}"))?;
    Ok(())
}

/// Contention rename (Go provenance probes use the `Name` field).
async fn update_name(cluster: &Cluster, id: &str, name: &str) -> Result<(), String> {
    let params = [&name as &(dyn ToSql + Sync), &id as _];
    cluster
        .node(0)
        .execute("UPDATE live_typed_contention SET name = $1 WHERE id = $2", &params)
        .await
        .map_err(|e| format!("update {id:?}: {e}"))?;
    cluster.node(0).simple("MURMUR FLUSH").await.map_err(|e| format!("flush after update {id:?}: {e}"))?;
    Ok(())
}

/// Sorted visible names on one High node (Go: `TypedContentionRows` +
/// digest over names).
async fn sorted_names(high: &Cluster, idx: usize) -> Result<Vec<String>, String> {
    let rows = high
        .node(idx)
        .query("SELECT name FROM live_typed_contention ORDER BY name", &[])
        .await
        .map_err(|e| format!("high-{idx} SELECT names: {e}"))?;
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        out.push(
            row.try_get::<_, String>(0)
                .map_err(|e| format!("high-{idx} decode name: {e}"))?,
        );
    }
    Ok(out)
}

/// Operator delivery leg: copy completed `.bundle` artifacts from a Low
/// outbox to its High inbox (the engine captures/exports and stages/applies
/// automatically in the background). Returns bundles copied.
fn deliver(outbox: &Path, inbox: &Path) -> Result<usize, String> {
    let entries = match std::fs::read_dir(outbox) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(format!("read outbox {}: {e}", outbox.display())),
    };
    std::fs::create_dir_all(inbox).map_err(|e| format!("create inbox {}: {e}", inbox.display()))?;
    let mut copied = 0;
    for entry in entries.flatten() {
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().into_owned();
        if !name.ends_with(".bundle") || name.starts_with('.') {
            continue;
        }
        let dest = inbox.join(&name);
        let fresh = match (std::fs::metadata(&path), std::fs::metadata(&dest)) {
            (Ok(src), Ok(dst)) => src.len() != dst.len(),
            (Ok(_), Err(_)) => true,
            _ => false,
        };
        if fresh {
            std::fs::copy(&path, &dest)
                .map_err(|e| format!("deliver {}: {e}", path.display()))?;
            copied += 1;
        }
    }
    Ok(copied)
}

struct Lanes {
    outbox_a: PathBuf,
    inbox_a: PathBuf,
    outbox_b: PathBuf,
    inbox_b: PathBuf,
}

fn deliver_all(lanes: &Lanes) -> Result<(), String> {
    deliver(&lanes.outbox_a, &lanes.inbox_a)?;
    deliver(&lanes.outbox_b, &lanes.inbox_b)?;
    Ok(())
}

/// Wait until both High nodes show exactly `want` (Go: `waitHighConverged`
/// + `assertNames`, with the delivery leg inside the poll).
async fn wait_high_names(
    lanes: &Lanes,
    high: &Cluster,
    want: &[String],
) -> Result<(), String> {
    let deadline = Instant::now() + WAIT;
    loop {
        deliver_all(lanes)?;
        if sorted_names(high, 0).await? == *want && sorted_names(high, 1).await? == *want {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "high nodes did not converge on {} rows: node0={:?} node1={:?}",
                want.len(),
                sorted_names(high, 0).await?,
                sorted_names(high, 1).await?
            ));
        }
        tokio::time::sleep(POLL).await;
    }
}

fn exporter(domain: [u8; 16], directory: PathBuf, stream: &str, signer: [u8; 32], recipient: [u8; 32]) -> BridgeNodeConfig {
    BridgeNodeConfig {
        role: "low-exporter".to_string(),
        domain: hex(&domain),
        directory: directory.to_string_lossy().into_owned(),
        stream: stream.to_string(),
        recipient: hex(&recipient),
        signer_hex: Some(hex(&signer)),
        ..Default::default()
    }
}

#[allow(clippy::too_many_arguments)]
fn receiver(
    domain: [u8; 16],
    directory: PathBuf,
    stream: &str,
    secret: [u8; 32],
    trusted_domain: [u8; 16],
    low_node: [u8; 16],
    signer_pub: [u8; 32],
    origin_pub: [u8; 32],
) -> BridgeNodeConfig {
    BridgeNodeConfig {
        role: "high-receiver".to_string(),
        domain: hex(&domain),
        trusted_domains: vec![hex(&trusted_domain)],
        trusted_origins: BTreeMap::from([(hex(&low_node), hex(&origin_pub))]),
        directory: directory.to_string_lossy().into_owned(),
        stream: stream.to_string(),
        recipient: hex(&x25519_public(&secret)),
        receiver_secret_hex: Some(hex(&secret)),
        trusted_signers: BTreeMap::from([(hex(&low_node), hex(&signer_pub))]),
        ..Default::default()
    }
}

async fn run_body(lanes: &Lanes, low_a: &Cluster, low_b: &Cluster, high: &Cluster) -> Result<(), String> {
    // Identical DDL on all three domains, then replicate it (bridge capture
    // reads the replication log, which only carries replicated tables).
    const DDL: &str =
        "CREATE TABLE live_typed_contention(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL)";
    for (label, db, idx) in [("low-a", low_a, 0), ("low-b", low_b, 0), ("high-0", high, 0), ("high-1", high, 1)] {
        db.ensure_migrated(idx, DDL)
            .await
            .map_err(|e| format!("{label} migrate failed: {e}"))?;
        db.node(idx)
            .simple("MURMUR SET TABLE live_typed_contention REPLICATED true")
            .await
            .map_err(|e| format!("{label} SET TABLE REPLICATED: {e}"))?;
        db.node(idx).simple("MURMUR FLUSH").await.map_err(|e| format!("{label} flush after SET TABLE: {e}"))?;
    }

    // Round 1: disjoint rows from both domains land on both High nodes.
    for i in 0..5 {
        insert_row(low_a, &row_id(0xA000, i), &format!("a-{i}")).await?;
        insert_row(low_b, &row_id(0xB000, i), &format!("b-{i}")).await?;
    }
    let mut round1: Vec<String> = (0..5)
        .map(|i| format!("a-{i}"))
        .chain((0..5).map(|i| format!("b-{i}")))
        .collect();
    round1.sort();
    wait_high_names(lanes, high, &round1).await?;

    // Duplicate delivery: re-copy identical bytes through a stability
    // window; the converged set must stay unchanged (receipt idempotence:
    // redelivery applies nothing twice).
    let window_end = Instant::now() + IDEMPOTENCE_WINDOW;
    while Instant::now() < window_end {
        deliver_all(lanes)?;
        tokio::time::sleep(POLL).await;
    }
    if sorted_names(high, 0).await? != round1 {
        return Err("high-0 changed after identical redelivery".into());
    }
    if sorted_names(high, 1).await? != round1 {
        return Err("high-1 changed after identical redelivery".into());
    }

    // Low-owned provenance (behavioral): a Low update to one row per
    // domain must flow through to both High nodes (a High-owned field
    // would suppress it); then restore the Go row names.
    let probe_a = row_id(0xA000, 0);
    let probe_b = row_id(0xB000, 0);
    update_name(low_a, &probe_a, "a-0-prov").await?;
    update_name(low_b, &probe_b, "b-0-prov").await?;
    let mut proved: Vec<String> = round1
        .iter()
        .map(|n| match n.as_str() {
            "a-0" => "a-0-prov".to_owned(),
            "b-0" => "b-0-prov".to_owned(),
            other => other.to_owned(),
        })
        .collect();
    proved.sort();
    wait_high_names(lanes, high, &proved).await?;
    update_name(low_a, &probe_a, "a-0").await?;
    update_name(low_b, &probe_b, "b-0").await?;
    wait_high_names(lanes, high, &round1).await?;

    // Round 2: stream-a only. New A rows arrive on both High nodes.
    for i in 5..8 {
        insert_row(low_a, &row_id(0xA000, i), &format!("a-{i}")).await?;
    }
    let mut round2: Vec<String> = (0..8)
        .map(|i| format!("a-{i}"))
        .chain((0..5).map(|i| format!("b-{i}")))
        .collect();
    round2.sort();
    wait_high_names(lanes, high, &round2).await?;

    // Stream-b progress isolation (behavioral): a stream-b-only round must
    // deliver exactly once from the undisturbed cursor (no loss from a
    // skipped sequence, no duplication).
    for i in 5..7 {
        insert_row(low_b, &row_id(0xB000, i), &format!("b-{i}")).await?;
    }
    let mut round3: Vec<String> = (0..8)
        .map(|i| format!("a-{i}"))
        .chain((0..7).map(|i| format!("b-{i}")))
        .collect();
    round3.sort();
    wait_high_names(lanes, high, &round3).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_low_domains_into_one_high() {
    let lanes = Lanes {
        outbox_a: artifacts::runtime_dir(LOW_A_NAME).join("outbox-a"),
        inbox_a: artifacts::runtime_dir(HIGH_NAME).join("inbox-a"),
        outbox_b: artifacts::runtime_dir(LOW_B_NAME).join("outbox-b"),
        inbox_b: artifacts::runtime_dir(HIGH_NAME).join("inbox-b"),
    };
    let low_a = Cluster::new(ClusterOptions {
        name: LOW_A_NAME.into(),
        num_nodes: 1,
        database_id: Some(LOW_A_DBID),
        node_ids: vec![LOW_A_NODE],
        key: LOW_A_KEY,
        manual_peers: true,
        bridge: BridgeOptions {
            nodes: HashMap::from([(
                0,
                exporter(
                    LOW_A_DBID,
                    lanes.outbox_a.clone(),
                    STREAM_A,
                    LOW_A_SIGNER,
                    x25519_public(&HIGH_SECRET_A),
                ),
            )]),
        },
        ..Default::default()
    })
    .await
    .expect("low-a cluster setup");
    let low_b = Cluster::new(ClusterOptions {
        name: LOW_B_NAME.into(),
        num_nodes: 1,
        database_id: Some(LOW_B_DBID),
        node_ids: vec![LOW_B_NODE],
        key: LOW_B_KEY,
        manual_peers: true,
        bridge: BridgeOptions {
            nodes: HashMap::from([(
                0,
                exporter(
                    LOW_B_DBID,
                    lanes.outbox_b.clone(),
                    STREAM_B,
                    LOW_B_SIGNER,
                    x25519_public(&HIGH_SECRET_B),
                ),
            )]),
        },
        ..Default::default()
    })
    .await
    .expect("low-b cluster setup");
    // Each High importer pins its own domain's origin key so imported
    // transactions verify.
    let low_a_origin = low_a.node_origin_pubkey(0);
    let low_b_origin = low_b.node_origin_pubkey(0);
    let high = Cluster::new(ClusterOptions {
        name: HIGH_NAME.into(),
        num_nodes: 2,
        database_id: Some(HIGH_DBID),
        node_ids: vec![HIGH_NODE_0, HIGH_NODE_1],
        key: HIGH_KEY,
        bridge: BridgeOptions {
            nodes: HashMap::from([
                (
                    0,
                    receiver(
                        HIGH_DBID,
                        lanes.inbox_a.clone(),
                        STREAM_A,
                        HIGH_SECRET_A,
                        LOW_A_DBID,
                        LOW_A_NODE,
                        ed25519_public(&LOW_A_SIGNER),
                        low_a_origin,
                    ),
                ),
                (
                    1,
                    receiver(
                        HIGH_DBID,
                        lanes.inbox_b.clone(),
                        STREAM_B,
                        HIGH_SECRET_B,
                        LOW_B_DBID,
                        LOW_B_NODE,
                        ed25519_public(&LOW_B_SIGNER),
                        low_b_origin,
                    ),
                ),
            ]),
        },
        ..Default::default()
    })
    .await
    .expect("high cluster setup");

    let result = run_body(&lanes, &low_a, &low_b, &high).await;
    let failed = result.is_err();
    if let Err(e) = &result {
        eprintln!("scenario failed: {e}");
    }
    if failed {
        low_a.dump_forensics("bridge-two-streams-low-a").await;
        low_b.dump_forensics("bridge-two-streams-low-b").await;
        high.dump_forensics("bridge-two-streams-high").await;
    }
    low_a.finish(failed).await;
    low_b.finish(failed).await;
    high.finish(failed).await;
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}
