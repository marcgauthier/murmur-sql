//! Rust port of the Go `corrupt-snapshot` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/corrupt-snapshot/`
//! (`corrupt_snapshot_test.go` + `attacker.go`). A stale node rejoins while
//! its only available snapshot source is hostile: the attacker induces a
//! snapshot pull with an unsolicited history-unavailable error, then serves
//! a structurally valid manifest plus one intact and one bit-flipped chunk.
//! The receiver must reject the corrupt chunk with nothing partially
//! published (row count, digest, and commit cursor untouched), discard the
//! staged transfer, then converge via the honest snapshot path with
//! bit-identical digests. Plain SQL replaces the Go typed-records HTTP API,
//! multi-process `murmur-server` processes replace embedded `Database`, and
//! rejection is observed via the replication `last_error` plus wire behavior.
//! The raw-QUIC attacker is the shared harness fixture
//! (`murmur_live_harness::attacker`), which reuses the product wire
//! implementation; this scenario reaches product types only through the
//! harness re-export.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::{Duration, Instant};

use murmur_live_harness::attacker::{forge_corrupt_snapshot, Attacker};
use murmur_live_harness::replication::protocol::{self, Message};
use murmur_live_harness::replication::retention;
use murmur_live_harness::tokio_postgres::types::ToSql;
use murmur_live_harness::{env_seconds, hex, Cluster, ClusterOptions, ReplicationOptions};

/// Role index: node3 goes stale, then rejoins (Go: `stale = 2`).
const STALE: usize = 2;
/// Attacker identity: fixed test id, asserted distinct from the node ids.
const ATTACKER_ID: [u8; 16] = [0xA7; 16];
/// Attacker origin signing key (the victim trusts its public half, mirroring
/// the Go test authorizing the hostile source).
const ATTACKER_SEED: [u8; 32] = [0x5E; 32];
/// Forged snapshot payload: two 64 KiB wire chunks (Go serves two chunks).
const FORGED_BYTES: usize = 70_000;
/// Forged manifest lifetime (attacker-side): short so the victim's expiry
/// reclamation clears the staged corrupt transfer before the honest leg.
const FORGED_LIFETIME: Duration = Duration::from_secs(20);
/// Reclaim margin past the forged expiry (the engine's maintenance tick runs
/// about every second).
const RECLAIM_MARGIN: Duration = Duration::from_secs(10);

/// Log retention (Go: `MinLogRetentionMs = 1000`).
const RETENTION: Duration = Duration::from_secs(1);
/// Offline-peer memory (Go: `MaxOfflineLogRetentionMs = 20000`).
const MAXIMUM_OFFLINE: Duration = Duration::from_secs(20);
/// Newest transactions per origin the survivors must keep (Go: `MinRetainedBatches = 10`).
const MINIMUM_RETAINED: u64 = 10;
/// Minimum pruned transactions per survivor before the attack: 60 survivor
/// writes minus the 10 retained, with margin. Proves the stale node's needed
/// ranges are genuinely unrecoverable from logs, so the rejoin must use the
/// snapshot path.
const MIN_COLLECTED: u64 = 40;

/// Baseline convergence deadline (Go: 60 s).
const BASELINE_TIMEOUT: Duration = Duration::from_secs(60);
/// Survivor row-count deadline (Go: 30 s).
const ROW_TIMEOUT: Duration = Duration::from_secs(30);
/// Convergence poll interval (Go: 200 ms).
const CONVERGE_POLL: Duration = Duration::from_millis(200);
/// GC collection-loop deadline past the expiry wait.
const COLLECT_TIMEOUT: Duration = Duration::from_secs(60);
/// Snapshot-pull inducement deadline (Go: 30 s).
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
/// Forged-chunk request deadline (Go: manifests/chunks received, 15 s).
const NEED_TIMEOUT: Duration = Duration::from_secs(15);
/// Corrupt-chunk rejection deadline (Go: apply failures, 30 s).
const FAIL_TIMEOUT: Duration = Duration::from_secs(30);
/// Post-attack drain: no `SnapshotApplied` may arrive (Go: 3 s).
const DRAIN_TIMEOUT: Duration = Duration::from_secs(3);
/// Post-attack settle before asserting nothing was published (Go: 2 s).
const SETTLE: Duration = Duration::from_secs(2);
/// Honest-resync convergence deadline (Go: 120 s).
const HONEST_TIMEOUT: Duration = Duration::from_secs(120);

/// Canonical ordered names on one node (Go: `Cluster.TypedNames`).
async fn names(cluster: &Cluster, idx: usize) -> Vec<String> {
    let rows = cluster
        .node(idx)
        .query("SELECT name FROM records ORDER BY name", &[])
        .await
        .expect("read names");
    rows.into_iter().map(|r| r.get(0)).collect()
}

/// Canonical digest of a name set (Go: sorted names joined with newlines).
fn digest(names: &[String]) -> String {
    names.join("\n")
}

/// Row count on one node (cheap pre-check before full name reads).
async fn row_count(cluster: &Cluster, idx: usize) -> usize {
    let rows = cluster
        .node(idx)
        .query("SELECT COUNT(*) FROM records", &[])
        .await
        .expect("row count");
    let n: i64 = rows[0].get(0);
    n as usize
}

/// One single-row transaction (Go: `TypedInsert`).
async fn insert(cluster: &Cluster, node: usize, id: i64, name: &str) {
    let params: [&(dyn ToSql + Sync); 2] = [&id, &name];
    cluster
        .node(node)
        .execute("INSERT INTO records(id, name) VALUES($1, $2)", &params)
        .await
        .unwrap_or_else(|e| panic!("node {node} insert failed: {e}"));
}

/// Wait until one node holds exactly `want` rows (Go: `waitRowCount`).
async fn wait_row_count(cluster: &Cluster, idx: usize, want: usize, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        if row_count(cluster, idx).await == want {
            return;
        }
        if Instant::now() >= deadline {
            panic!(
                "node {idx} record count = {}, want {want} within {timeout:?}",
                row_count(cluster, idx).await,
            );
        }
        tokio::time::sleep(CONVERGE_POLL).await;
    }
}

/// Wait until every node holds exactly `want` rows with equal digests
/// (Go: `waitConverged`).
async fn wait_converged(cluster: &Cluster, want: usize, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        let mut counts = Vec::new();
        for i in 0..cluster.len() {
            counts.push(row_count(cluster, i).await);
        }
        if counts.iter().all(|&n| n == want) {
            let mut first = String::new();
            let mut equal = true;
            for idx in 0..cluster.len() {
                let d = digest(&names(cluster, idx).await);
                if idx == 0 {
                    first = d;
                } else if d != first {
                    equal = false;
                    break;
                }
            }
            if equal {
                return;
            }
        }
        if Instant::now() >= deadline {
            for i in 0..cluster.len() {
                let node_names = names(cluster, i).await;
                eprintln!(
                    "node {i} at timeout: count={} digest={}",
                    node_names.len(),
                    digest(&node_names)
                );
            }
            panic!("nodes did not converge on {want} rows with equal digests within {timeout:?}");
        }
        tokio::time::sleep(CONVERGE_POLL).await;
    }
}

/// Connected replication sessions on one node (Go: `spedsql_connected_peers`).
async fn connected_peers(cluster: &Cluster, idx: usize) -> usize {
    let rows = cluster
        .node(idx)
        .simple("MURMUR SHOW REPLICATION")
        .await
        .expect("SHOW REPLICATION");
    rows.first()
        .and_then(|r| r.get(2))
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(0)
}

/// Latest replication error on one node (Go: failure counters, which have no
/// Rust equivalent; the corrupt-chunk rejection lands here).
async fn last_error(cluster: &Cluster, idx: usize) -> Option<String> {
    let rows = cluster
        .node(idx)
        .simple("MURMUR SHOW REPLICATION")
        .await
        .expect("SHOW REPLICATION");
    rows.first()
        .and_then(|r| r.get(6))
        .filter(|s| !s.is_empty())
        .cloned()
}

/// Local commit cursor (Go: `spedsql_state_generation` advance check).
async fn committed(cluster: &Cluster, idx: usize) -> u64 {
    let rows = cluster
        .node(idx)
        .simple("MURMUR SHOW REPLICATION")
        .await
        .expect("SHOW REPLICATION");
    rows.first()
        .and_then(|r| r.get(7))
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0)
}

/// Log-path activity counters for one node: `(staged_transactions,
/// applied_transactions)`. Snapshot application bypasses both counters, so a
/// flat pair across the honest resync proves the rows arrived via snapshot.
async fn log_counters(cluster: &Cluster, idx: usize) -> (u64, u64) {
    let rows = cluster
        .node(idx)
        .simple("MURMUR SHOW REPLICATION")
        .await
        .expect("SHOW REPLICATION");
    let r = rows.first().expect("replication status row");
    let staged: u64 = r.get(4).and_then(|s| s.parse().ok()).unwrap_or(0);
    let applied: u64 = r.get(5).and_then(|s| s.parse().ok()).unwrap_or(0);
    (staged, applied)
}

/// Run one bounded retention pass on one node; returns transactions pruned.
async fn collect_history(cluster: &Cluster, idx: usize) -> u64 {
    let rows = cluster
        .node(idx)
        .simple("MURMUR RETENTION COLLECT")
        .await
        .unwrap_or_else(|e| panic!("node {idx} collect retention: {e}"));
    rows.first()
        .and_then(|r| r.get(0))
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0)
}

/// Total transactions ever pruned on one node.
async fn collected_total(cluster: &Cluster, idx: usize) -> u64 {
    let rows = cluster
        .node(idx)
        .simple("MURMUR SHOW RETENTION")
        .await
        .expect("SHOW RETENTION");
    rows.first()
        .and_then(|r| r.get(5))
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0)
}

/// Read last seen millis for peer with node_id `victim_hex` from node `idx`.
async fn last_seen_millis(cluster: &Cluster, idx: usize, victim_hex: &str) -> Option<u64> {
    let rows = cluster
        .node(idx)
        .simple("MURMUR SHOW RETENTION")
        .await
        .expect("SHOW RETENTION");
    for r in rows {
        if r.get(0).map(|s| s.as_str()) == Some(victim_hex) {
            return r.get(1).and_then(|s| s.parse::<u64>().ok());
        }
    }
    None
}

async fn run_body(cluster: &mut Cluster) -> Result<(), String> {
    // Phase 0: create replicated table on all nodes.
    for i in 0..cluster.len() {
        cluster
            .node(i)
            .simple("CREATE TABLE records(id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)")
            .await
            .map_err(|e| format!("migrate records table node {i}: {e}"))?;
        cluster
            .node(i)
            .simple("MURMUR SET TABLE records REPLICATED true")
            .await
            .map_err(|e| format!("set replicated node {i}: {e}"))?;
        cluster
            .node(i)
            .flush()
            .await
            .map_err(|e| format!("flush schema node {i}: {e}"))?;
    }

    // Phase 1: build the honest mesh explicitly and converge a baseline.
    for (from, target) in [(0, 1), (1, 0), (0, 2), (2, 0), (1, 2), (2, 1)] {
        cluster
            .add_peer(from, target)
            .await
            .map_err(|e| format!("AddPeer({from},{target}): {e}"))?;
    }
    for i in 0..5 {
        insert(cluster, STALE, i + 1, &format!("stale-{i}")).await;
    }
    cluster
        .node(STALE)
        .flush()
        .await
        .map_err(|e| format!("flush baseline: {e}"))?;
    wait_converged(cluster, 5, BASELINE_TIMEOUT).await;
    let pre_stop_digest = digest(&names(cluster, STALE).await);

    // Phase 2: isolate node3, stop it, and let the survivors write past
    // retention so the rejoin genuinely needs the snapshot path.
    for (from, target) in [(2, 0), (2, 1), (0, 2), (1, 2)] {
        cluster
            .remove_peer(from, target)
            .await
            .map_err(|e| format!("RemovePeer({from},{target}): {e}"))?;
    }
    cluster.stop_node(STALE).await;

    // Flush every write: the product mints one replication log entry per
    // flush batch, not per SQL transaction, so a single trailing flush
    // would leave fewer entries than `minimum_retained` and nothing
    // would ever become eligible for pruning.
    for i in 0..60 {
        insert(cluster, 0, 100 + i, &format!("fresh-{i}")).await;
        cluster
            .node(0)
            .flush()
            .await
            .map_err(|e| format!("flush survivor write: {e}"))?;
    }
    wait_row_count(cluster, 0, 65, ROW_TIMEOUT).await;
    wait_row_count(cluster, 1, 65, ROW_TIMEOUT).await;

    // Member-deadline expiry, then explicit collection passes: the stale
    // node's retention pin must lapse before its needed ranges become
    // genuinely unrecoverable from logs.
    eprintln!("waiting for member-deadline expiry and collection");
    tokio::time::sleep(env_seconds(
        "MURMUR_CORRUPT_SNAPSHOT_GC_WAIT_SECONDS",
        30,
    ))
    .await;

    let collect_deadline = Instant::now() + COLLECT_TIMEOUT;
    loop {
        let totals = [collected_total(cluster, 0).await, collected_total(cluster, 1).await];
        if totals.iter().all(|&n| n >= MIN_COLLECTED) {
            eprintln!("survivors pruned {totals:?} transactions past retention");
            break;
        }
        if Instant::now() >= collect_deadline {
            return Err(format!(
                "survivors collected only {totals:?} transactions, want >= {MIN_COLLECTED} each (snapshot path not forced)"
            ));
        }
        collect_history(cluster, 0).await;
        collect_history(cluster, 1).await;
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    // Prove the deadline leg explicitly: the victim is now older than
    // every survivor's offline memory.
    let victim_id_hex = hex(&cluster.node_id(STALE));
    for i in [0, 1] {
        let last_seen = last_seen_millis(cluster, i, &victim_id_hex)
            .await
            .ok_or_else(|| format!("node {i} has no retention record for victim {victim_id_hex}"))?;
        let age = retention::now_millis().saturating_sub(last_seen);
        if age <= MAXIMUM_OFFLINE.as_millis() as u64 {
            return Err(format!(
                "node {i} still pins the victim (last seen {age} ms ago, offline max {:?})",
                MAXIMUM_OFFLINE
            ));
        }
    }

    // Phase 3: node3 restarts peerless; the attacker's inbound session is
    // its only source. The attacker induces a snapshot pull, then serves
    // bit-flipped chunks.
    cluster
        .start_node(STALE)
        .await
        .map_err(|e| format!("stale node restart: {e}"))?;
    cluster
        .node(STALE)
        .simple("MURMUR SET TABLE records REPLICATED true")
        .await
        .map_err(|e| format!("re-set replicated on stale node: {e}"))?;
    cluster
        .node(STALE)
        .flush()
        .await
        .map_err(|e| format!("flush schema on stale node: {e}"))?;
    let victim_addr = cluster.node_info(STALE).repl_addr;
    let victim_id = cluster.node_id(STALE);
    let dbid = cluster.database_id();
    let gen_before_attack = committed(cluster, STALE).await;
    let log_before_attack = log_counters(cluster, STALE).await;

    // Run the attacker dialogue against the victim (shared harness
    // fixture; product wire implementation, not a fork).
    let mut attacker = Attacker::new(cluster, ATTACKER_ID, ATTACKER_SEED)
        .map_err(|e| format!("attacker identity: {e}"))?;
    attacker.config_mut().snapshot_transfer_lifetime = FORGED_LIFETIME;
    let mut evil = attacker
        .dial(victim_addr, dbid, victim_id)
        .await
        .map_err(|e| format!("attacker dial: {e}"))?;

    // Prove the "only available source" premise: exactly one peer.
    let poll_deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if connected_peers(cluster, STALE).await == 1 {
            break;
        }
        if Instant::now() >= poll_deadline {
            return Err(format!(
                "stale node has {} connected peers during the attack, want 1",
                connected_peers(cluster, STALE).await
            ));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Induce the pull: an unsolicited history-unavailable error makes
    // the node request a snapshot from this peer over the live session.
    evil.send(&Message::Error {
        code: 3,
        message: "ranges unavailable".into(),
    })
    .await
    .map_err(|e| format!("attacker induce pull: {e}"))?;
    let reqs = evil
        .collect_until(REQUEST_TIMEOUT, |m| matches!(m, Message::SnapshotRequest))
        .await;
    if !reqs.iter().any(|m| matches!(m, Message::SnapshotRequest)) {
        return Err("stale node never requested a snapshot from the attacker".into());
    }

    let forged = forge_corrupt_snapshot(
        attacker.config(),
        ATTACKER_ID,
        dbid,
        victim_id,
        evil.welcome_schema(),
        FORGED_BYTES,
    )
    .map_err(|e| format!("forge corrupt snapshot: {e}"))?;
    evil.send(&Message::SnapshotManifest(forged.manifest.clone()))
        .await
        .map_err(|e| format!("attacker serve manifest: {e}"))?;

    // The manifest passed structural checks: the node asks for its chunks.
    let needs = evil
        .collect_until(NEED_TIMEOUT, |m| matches!(m, Message::SnapshotNeed { .. }))
        .await;
    if !needs.iter().any(|m| {
        matches!(m, Message::SnapshotNeed { transfer, .. } if *transfer == forged.transfer)
    }) {
        return Err("victim never asked for the forged chunks (manifest not accepted)".into());
    }

    evil.send(&Message::SnapshotChunk {
        transfer: forged.transfer,
        index: 0,
        data: forged.chunk0,
    })
    .await
    .map_err(|e| format!("attacker serve chunk0: {e}"))?;
    evil.send(&Message::SnapshotChunk {
        transfer: forged.transfer,
        index: 1,
        data: forged.chunk1_bad,
    })
    .await
    .map_err(|e| format!("attacker serve chunk1: {e}"))?;

    // The corrupt transfer must never complete: drain while asserting
    // no SnapshotApplied arrives.
    let tail = evil.collect_until(DRAIN_TIMEOUT, |_| false).await;
    if tail.iter().any(|m| matches!(m, Message::SnapshotApplied { .. })) {
        return Err("victim applied the corrupt snapshot".into());
    }
    evil.close();

    // The receiver must detect the digest mismatch and fail the chunk...
    let fail_deadline = Instant::now() + FAIL_TIMEOUT;
    loop {
        if let Some(err) = last_error(cluster, STALE).await {
            if err.contains("snapshot chunk") {
                eprintln!("victim rejected the corrupt chunk: {err}");
                break;
            }
        }
        if Instant::now() >= fail_deadline {
            return Err("victim never recorded the corrupt-chunk rejection".into());
        }
        tokio::time::sleep(CONVERGE_POLL).await;
    }

    // ...while no snapshot completed and nothing was partially published.
    tokio::time::sleep(SETTLE).await;
    if row_count(cluster, STALE).await != 5 {
        return Err(format!(
            "stale node row count moved on corrupt transfer: {} != 5",
            row_count(cluster, STALE).await
        ));
    }
    if digest(&names(cluster, STALE).await) != pre_stop_digest {
        return Err("stale node digest changed on corrupt transfer (partial publication)".into());
    }
    if committed(cluster, STALE).await != gen_before_attack {
        return Err("commit cursor moved on corrupt transfer (partial publication)".into());
    }
    if log_counters(cluster, STALE).await != log_before_attack {
        return Err("replication counters moved on corrupt transfer (partial publication)".into());
    }

    // The staged corrupt transfer is discarded on expiry (the only
    // staged-transfer discard path); the honest leg below proves it.
    tokio::time::sleep(FORGED_LIFETIME + RECLAIM_MARGIN).await;

    // Phase 4: honest peers return; the node retries from them and converges.
    for (from, target) in [(2, 0), (2, 1), (0, 2), (1, 2)] {
        cluster
            .add_peer(from, target)
            .await
            .map_err(|e| format!("honest AddPeer({from},{target}): {e}"))?;
    }
    let _ = cluster.sync_all().await;
    let log_pre_heal = log_counters(cluster, STALE).await;
    wait_converged(cluster, 65, HONEST_TIMEOUT).await;

    // The rejoin completed via an honest snapshot, not the corrupt one
    // and not the logs: 60 rows arrived while the log-path counters
    // stayed flat (snapshot application bypasses both).
    if log_counters(cluster, STALE).await != log_pre_heal {
        return Err("honest resync arrived over the log path, not a snapshot".into());
    }
    if committed(cluster, STALE).await < gen_before_attack {
        return Err("commit cursor regressed (non-monotonic publication)".into());
    }
    let stale_rows = names(cluster, STALE)
        .await
        .iter()
        .filter(|name| name.starts_with("stale-"))
        .count();
    if stale_rows != 5 {
        return Err(format!("stale rows after honest resync = {stale_rows}, want 5"));
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn stale_node_discards_corrupt_snapshot_then_converges() {
    // Fixed node ids (asserted distinct from the attacker id) so the
    // snapshot-trust overlays can be built before the cluster opens.
    let node_ids = vec![[0x10; 16], [0x20; 16], [0x30; 16]];
    assert!(
        !node_ids.contains(&ATTACKER_ID),
        "attacker id collides with a node id"
    );

    // Every node trusts every node as a snapshot donor (Go harness all-node
    // fixture policy); the victim additionally authorizes the hostile source
    // (Go: the test appends the attacker to the victim's config file).
    let all_nodes: BTreeSet<[u8; 16]> = node_ids.iter().copied().collect();
    let mut trusted_snapshot_sources: HashMap<usize, BTreeSet<[u8; 16]>> = HashMap::new();
    for i in 0..node_ids.len() {
        trusted_snapshot_sources.insert(i, all_nodes.clone());
    }
    let mut victim_sources = all_nodes.clone();
    victim_sources.insert(ATTACKER_ID);
    trusted_snapshot_sources.insert(STALE, victim_sources);
    let mut extra_trusted_origins: HashMap<usize, BTreeMap<[u8; 16], [u8; 32]>> =
        HashMap::new();
    extra_trusted_origins.insert(
        STALE,
        BTreeMap::from([(ATTACKER_ID, protocol::public_key(&ATTACKER_SEED))]),
    );

    let mut cluster = Cluster::new(ClusterOptions {
        name: "corrupt-snapshot".into(),
        num_nodes: 3,
        node_ids,
        // Manual peers so the stale node restarts truly peerless: the
        // attacker becomes its only snapshot source by construction.
        manual_peers: true,
        replication: Some(ReplicationOptions {
            retention: Some(RETENTION),
            minimum_retained: Some(MINIMUM_RETAINED),
            maximum_offline: Some(MAXIMUM_OFFLINE),
            ..Default::default()
        }),
        trusted_snapshot_sources,
        extra_trusted_origins,
        ..Default::default()
    })
    .await
    .expect("cluster setup");

    let result = run_body(&mut cluster).await;
    let failed = result.is_err();
    if let Err(e) = &result {
        eprintln!("scenario failed: {e}");
    }
    if failed {
        cluster.dump_forensics("corrupt-snapshot-no-convergence").await;
    }
    cluster.finish(failed).await;
    assert!(
        !failed,
        "scenario failed; artifacts retained under tests-live/failures/"
    );
}
