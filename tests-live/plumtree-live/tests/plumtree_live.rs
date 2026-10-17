//! Rust port of the Go `plumtree-live` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/plumtree-live/`
//! (`plumtree_live_test.go`). `plumtree_mesh_converges` requires end-to-end
//! convergence of a three-node all-Plumtree mesh; `mixed_mode_refuses_gossip_peer`
//! requires a gossip-mode node meshed with Plumtree peers to stay isolated in
//! both directions while handshake refusals are recorded and the Plumtree pair
//! converges. Plain SQL replaces the Go typed-records HTTP API, and nodes are
//! `murmur-server` child processes driven over the PostgreSQL wire (see the
//! scenario README.md for the deviations).

use std::collections::HashMap;
use std::time::Duration;

use murmur_live_harness::{wait_until_async, Cluster, ClusterOptions, Dissemination, ReplicationOptions};

/// Rows inserted by the origin node (Go: 10 per test).
const ROWS: usize = 10;
/// All-Plumtree convergence deadline (Go: 60 s).
const CONVERGE_TIMEOUT: Duration = Duration::from_secs(60);
/// Mixed-mode pair-convergence deadline (Go: 30 s).
const MIXED_TIMEOUT: Duration = Duration::from_secs(30);
/// Convergence poll interval (Go: 200 ms).
const POLL_INTERVAL: Duration = Duration::from_millis(200);
/// Leak window after mixed-mode convergence (Go: 5 s).
const LEAK_WINDOW: Duration = Duration::from_secs(5);

/// Canonical ordered names on one node (Go: `Cluster.TypedNames`).
async fn names(cluster: &Cluster, idx: usize) -> Result<Vec<String>, String> {
    let rows = cluster
        .node(idx)
        .query("SELECT name FROM records ORDER BY name", &[])
        .await
        .map_err(|e| format!("node{} SELECT names: {e}", idx + 1))?;
    rows.iter()
        .map(|row| {
            row.try_get::<_, String>(0)
                .map_err(|e| format!("node{} decode name: {e}", idx + 1))
        })
        .collect()
}

/// Same `records` table on every node (Go: TypedRecords fixture tables),
/// flagged replicated on each node (row convergence needs the per-node flag).
async fn migrate_all(cluster: &Cluster) -> Result<(), String> {
    for i in 0..cluster.len() {
        cluster
            .ensure_migrated(i, "CREATE TABLE records(id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
            .await
            .map_err(|e| format!("migrate node{}: {e}", i + 1))?;
    }
    for i in 0..cluster.len() {
        cluster
            .node(i)
            .simple("MURMUR SET TABLE records REPLICATED true")
            .await
            .map_err(|e| format!("node{} SET TABLE REPLICATED: {e}", i + 1))?;
        cluster
            .node(i)
            .flush()
            .await
            .map_err(|e| format!("node{} flush after SET TABLE: {e}", i + 1))?;
    }
    Ok(())
}

async fn insert_names(cluster: &Cluster, idx: usize, prefix: &str, count: usize) -> Result<(), String> {
    for row in 0..count {
        let name = format!("{prefix}-{row}");
        let id = row as i64;
        cluster
            .node(idx)
            .execute("INSERT INTO records(id, name) VALUES($1, $2)", &[&id, &name])
            .await
            .map_err(|e| format!("node{} insert '{name}': {e}", idx + 1))?;
    }
    cluster
        .node(idx)
        .flush()
        .await
        .map_err(|e| format!("node{} flush after inserts: {e}", idx + 1))?;
    Ok(())
}

/// One `MURMUR SHOW REPLICATION` row: (peers, last_error).
async fn replication_summary(cluster: &Cluster, idx: usize) -> Result<(u64, String), String> {
    let rows = cluster
        .node(idx)
        .simple("MURMUR SHOW REPLICATION")
        .await
        .map_err(|e| format!("node{} SHOW REPLICATION: {e}", idx + 1))?;
    let row = rows
        .first()
        .ok_or_else(|| format!("node{} SHOW REPLICATION returned no rows", idx + 1))?;
    if row.len() < 7 {
        return Err(format!("node{} SHOW REPLICATION has {} columns, want >= 7", idx + 1, row.len()));
    }
    let peers = row[2]
        .parse::<u64>()
        .map_err(|e| format!("node{} peers {:?}: {e}", idx + 1, row[2]))?;
    Ok((peers, row[6].clone()))
}

async fn run_converges(cluster: &Cluster) -> Result<(), String> {
    migrate_all(cluster).await?;
    insert_names(cluster, 0, "plum", ROWS).await?;

    let mut want = Vec::with_capacity(ROWS);
    for row in 0..ROWS {
        want.push(format!("plum-{row}"));
    }
    let converged = wait_until_async(CONVERGE_TIMEOUT, POLL_INTERVAL, || async {
        for i in 0..cluster.len() {
            match names(cluster, i).await {
                Ok(got) if got == want => {}
                _ => return false,
            }
        }
        true
    })
    .await;
    if !converged {
        return Err(format!(
            "three Plumtree nodes did not converge on {ROWS} rows within {CONVERGE_TIMEOUT:?}"
        ));
    }
    eprintln!("Three Plumtree nodes converged on {ROWS} rows");
    Ok(())
}

async fn run_mixed(cluster: &Cluster) -> Result<(), String> {
    migrate_all(cluster).await?;
    insert_names(cluster, 0, "mixed-plum", ROWS).await?;
    insert_names(cluster, 2, "gossip-only", 1).await?;

    // The Plumtree pair converges; the gossip node stays isolated.
    let pair_converged = wait_until_async(MIXED_TIMEOUT, POLL_INTERVAL, || async {
        let n0 = names(cluster, 0).await.map(|n| n.len()).unwrap_or(usize::MAX);
        let n1 = names(cluster, 1).await.map(|n| n.len()).unwrap_or(usize::MAX);
        n0 == ROWS && n1 == ROWS
    })
    .await;
    if !pair_converged {
        return Err(format!(
            "Plumtree pair did not converge on {ROWS} rows within {MIXED_TIMEOUT:?}"
        ));
    }
    tokio::time::sleep(LEAK_WINDOW).await; // let any leak arrive
    for (i, want) in [ROWS, ROWS, 1].into_iter().enumerate() {
        let got = names(cluster, i).await?;
        if got.len() != want {
            return Err(format!(
                "node{} count = {}, want {want} (mode isolation broken?)",
                i + 1,
                got.len()
            ));
        }
    }

    // Refusal evidence (Go: `spedsql_repl_handshake_capability_refusals_total >= 1`).
    // The engine fails closed with `incompatible hello/welcome`, latched in the
    // `last_error` column of `MURMUR SHOW REPLICATION` on both dial and accept sides.
    let mut refusals = 0;
    for i in 0..cluster.len() {
        let (peers, last_error) = replication_summary(cluster, i).await?;
        if last_error.contains("incompatible") {
            refusals += 1;
        }
        eprintln!("node{}: peers={peers} last_error={last_error:?}", i + 1);
    }
    if refusals < 1 {
        return Err("no handshake incompatibility refusals recorded (negotiation silent?)".into());
    }
    eprintln!("mode refusals recorded on {refusals} node(s)");

    // Session-level isolation: the gossip node never holds a session,
    // while the converged Plumtree pair holds exactly each other.
    let (gossip_peers, _) = replication_summary(cluster, 2).await?;
    if gossip_peers != 0 {
        return Err(format!(
            "gossip node holds {gossip_peers} sessions, want 0 (mode isolation broken?)"
        ));
    }
    for i in 0..2 {
        let (peers, _) = replication_summary(cluster, i).await?;
        if peers != 1 {
            return Err(format!(
                "plumtree node{i} holds {peers} sessions, want 1 (peer of the gossip node?)"
            ));
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn plumtree_mesh_converges() {
    let cluster = Cluster::new(ClusterOptions {
        name: "plumtree-live".into(),
        num_nodes: 3,
        replication: Some(ReplicationOptions {
            dissemination: Some(Dissemination::Plumtree),
            ..Default::default()
        }),
        ..Default::default()
    })
    .await
    .expect("cluster setup");
    assert_eq!(cluster.len(), 3, "this scenario is written for 3 nodes");
    let result = run_converges(&cluster).await;
    let failed = result.is_err();
    if let Err(e) = &result {
        eprintln!("scenario failed: {e}");
    }
    if failed {
        cluster.dump_forensics("no-convergence").await;
    }
    cluster.finish(failed).await;
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mixed_mode_refuses_gossip_peer() {
    let mut dissemination_by_node = HashMap::new();
    dissemination_by_node.insert(2, Dissemination::Gossip);
    let cluster = Cluster::new(ClusterOptions {
        name: "plumtree-mixed".into(),
        num_nodes: 3,
        replication: Some(ReplicationOptions {
            dissemination: Some(Dissemination::Plumtree),
            ..Default::default()
        }),
        dissemination_by_node,
        ..Default::default()
    })
    .await
    .expect("cluster setup");
    assert_eq!(cluster.len(), 3, "this scenario is written for 3 nodes");
    let result = run_mixed(&cluster).await;
    let failed = result.is_err();
    if let Err(e) = &result {
        eprintln!("scenario failed: {e}");
    }
    if failed {
        cluster.dump_forensics("mode-isolation").await;
    }
    cluster.finish(failed).await;
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}
