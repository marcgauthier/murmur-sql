//! Rust port of the Go `churn-retirement` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/churn-retirement/`
//! (`churn_retirement_test.go`). Three meshed `murmur-server` processes;
//! node3 is excluded on the survivors: sessions drop, its writes stop
//! arriving, the exclusion survives a survivor restart with no rediscovery
//! re-mesh, and re-adding heals replication. Retirement (permanent, with
//! retention release) is proven after the heal: the Rust product splits
//! Go's single `RemovePeer` into reversible exclusion (`PEER REMOVE`,
//! which keeps retention) and permanent retirement (`PEER RETIRE`, which
//! releases it). Plain SQL replaces the Go typed-records HTTP API (see the
//! scenario README.md).

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use murmur_live_harness::tokio_postgres::types::ToSql;
use murmur_live_harness::{hex, Cluster, ClusterOptions};

/// Excluded node (Go: node3).
const EXCLUDED: usize = 2;
/// Convergence/session deadlines (Go: 60 s for saturated parallel boxes).
const WAIT_TIMEOUT: Duration = Duration::from_secs(60);
/// Poll interval (Go: 200 ms).
const POLL: Duration = Duration::from_millis(200);
/// Isolation hold after the excluded node's write (Go: 5 s).
const ISOLATION_HOLD: Duration = Duration::from_secs(5);
/// Post-restart rediscovery watch (Go: 5 s).
const REMESH_WATCH: Duration = Duration::from_secs(5);

/// Sorted names on one node (Go: `TypedNames`).
async fn names(cluster: &Cluster, idx: usize) -> Result<Vec<String>, String> {
    let rows = cluster
        .node(idx)
        .query("SELECT name FROM records", &[])
        .await
        .map_err(|e| format!("node{} SELECT names: {e}", idx + 1))?;
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        out.push(
            row.try_get::<_, String>(0)
                .map_err(|e| format!("node{} decode name: {e}", idx + 1))?,
        );
    }
    out.sort();
    Ok(out)
}

/// Insert one row plus a durability barrier (Go: `TypedInsert`).
async fn insert(cluster: &Cluster, node: usize, id: i64, name: &str) -> Result<(), String> {
    let params = [&id as &(dyn ToSql + Sync), &name as _];
    cluster
        .node(node)
        .execute("INSERT INTO records(id, name) VALUES($1, $2)", &params)
        .await
        .map_err(|e| format!("node{} insert '{name}': {e}", node + 1))?;
    cluster
        .node(node)
        .flush()
        .await
        .map_err(|e| format!("node{} flush after '{name}': {e}", node + 1))?;
    Ok(())
}

/// Wait until every node holds exactly `want` rows with equal contents (Go:
/// `waitConverged`).
async fn wait_converged(cluster: &Cluster, want: usize, timeout: Duration) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    loop {
        let mut first: Option<Vec<String>> = None;
        let mut ok = true;
        for i in 0..cluster.len() {
            let node_names = names(cluster, i).await?;
            if node_names.len() != want {
                ok = false;
                break;
            }
            match &first {
                None => first = Some(node_names),
                Some(want_names) if *want_names == node_names => {}
                _ => {
                    ok = false;
                    break;
                }
            }
        }
        if ok {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!("nodes did not converge on {want} rows within {timeout:?}"));
        }
        tokio::time::sleep(POLL).await;
    }
}

/// Whether `from` currently holds an open session to `target` (Go:
/// `spedsql_peer_connected{peer=...}`; here `MURMUR SHOW PEERS`).
async fn peer_connected(cluster: &Cluster, from: usize, target: usize) -> Result<bool, String> {
    let want = hex(&cluster.node_id(target));
    let rows = cluster
        .node(from)
        .simple("MURMUR SHOW PEERS")
        .await
        .map_err(|e| format!("node{} SHOW PEERS: {e}", from + 1))?;
    Ok(rows.iter().any(|row| {
        row.first().is_some_and(|id| id.eq_ignore_ascii_case(&want))
            && row.get(2).is_some_and(|c| c == "true")
    }))
}

/// Open sessions on one node (Go: `spedsql_peer_count`; only rows with
/// `connected=true` count).
async fn connected_count(cluster: &Cluster, idx: usize) -> Result<usize, String> {
    let rows = cluster
        .node(idx)
        .simple("MURMUR SHOW PEERS")
        .await
        .map_err(|e| format!("node{} SHOW PEERS: {e}", idx + 1))?;
    Ok(rows.iter().filter(|row| row.get(2).is_some_and(|c| c == "true")).count())
}

/// Distinct non-retired peers pinning retention on one node (Go:
/// `spedsql_gating_members`; here `MURMUR SHOW RETENTION`).
async fn gating_members(cluster: &Cluster, idx: usize) -> Result<BTreeSet<String>, String> {
    let rows = cluster
        .node(idx)
        .simple("MURMUR SHOW RETENTION")
        .await
        .map_err(|e| format!("node{} SHOW RETENTION: {e}", idx + 1))?;
    Ok(rows
        .iter()
        .filter(|row| row.get(2).is_none_or(|retired| retired != "true"))
        .filter_map(|row| row.first().cloned())
        .collect())
}

/// Whether `target`'s retention record on `from` is marked retired.
async fn peer_retired(cluster: &Cluster, from: usize, target: usize) -> Result<bool, String> {
    let want = hex(&cluster.node_id(target));
    let rows = cluster
        .node(from)
        .simple("MURMUR SHOW RETENTION")
        .await
        .map_err(|e| format!("node{} SHOW RETENTION: {e}", from + 1))?;
    Ok(rows.iter().any(|row| {
        row.first().is_some_and(|id| id.eq_ignore_ascii_case(&want))
            && row.get(2).is_some_and(|retired| retired == "true")
    }))
}

/// Wait until sessions to node3 drop on both survivors (Go:
/// `waitCondition`).
async fn wait_sessions_drop(cluster: &Cluster, timeout: Duration) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    loop {
        if !peer_connected(cluster, 0, EXCLUDED).await?
            && !peer_connected(cluster, 1, EXCLUDED).await?
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!("sessions to node3 did not drop within {timeout:?}"));
        }
        tokio::time::sleep(POLL).await;
    }
}

async fn run_body(cluster: &mut Cluster) -> Result<(), String> {
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

    for i in 0..5 {
        insert(cluster, 0, i + 1, &format!("a-{i}")).await?;
    }
    wait_converged(cluster, 5, WAIT_TIMEOUT).await?;
    let gating_before = gating_members(cluster, 0).await?;

    // Exclude node3 on both survivors.
    cluster
        .remove_peer(0, EXCLUDED)
        .await
        .map_err(|e| format!("remove peer 0->2: {e}"))?;
    cluster
        .remove_peer(1, EXCLUDED)
        .await
        .map_err(|e| format!("remove peer 1->2: {e}"))?;
    // Sessions to node3 drop on both survivors.
    wait_sessions_drop(cluster, WAIT_TIMEOUT).await?;
    for i in [0, 1] {
        let count = connected_count(cluster, i).await?;
        if count != 1 {
            return Err(format!("node{} peers = {count}, want 1 after exclusion", i + 1));
        }
    }
    // Node3's writes no longer arrive on survivors.
    insert(cluster, EXCLUDED, 100, "isolated").await?;
    tokio::time::sleep(ISOLATION_HOLD).await;
    for i in [0, 1] {
        let survivor = names(cluster, i).await?;
        if survivor.len() != 5 {
            return Err(format!(
                "node{} names after isolation = {survivor:?}; want five survivors",
                i + 1
            ));
        }
    }

    // Exclusion survives a survivor restart (no rediscovery re-mesh): the
    // restarted node re-meshes only its fellow survivor (fresh ephemeral
    // port), never the excluded node.
    cluster.stop_node(0).await;
    cluster
        .start_node(0)
        .await
        .map_err(|e| format!("restart node1: {e}"))?;
    cluster
        .add_peer(0, 1)
        .await
        .map_err(|e| format!("re-mesh 0->1: {e}"))?;
    cluster
        .add_peer(1, 0)
        .await
        .map_err(|e| format!("re-mesh 1->0: {e}"))?;
    tokio::time::sleep(REMESH_WATCH).await;
    if peer_connected(cluster, 0, EXCLUDED).await? {
        return Err("node1 re-meshed excluded node3 after restart".into());
    }
    let count = connected_count(cluster, 0).await?;
    if count != 1 {
        return Err(format!("node1 peers after restart = {count}, want 1 (exclusion lost?)"));
    }

    // Re-adding heals replication (removal was not destructive).
    for (from, target) in [(0, 2), (2, 0), (1, 2), (2, 1)] {
        cluster
            .add_peer(from, target)
            .await
            .map_err(|e| format!("re-add peer {from}->{target}: {e}"))?;
    }
    wait_converged(cluster, 6, WAIT_TIMEOUT).await?;

    // The retired peer releases its GC retention pin. This leg runs after
    // the heal (not immediately after exclusion like Go): Rust splits Go's
    // single RemovePeer into reversible exclusion and permanent
    // retirement, and retiring before the re-add would make the heal leg
    // impossible. Every assertion is preserved; only the order changes.
    cluster
        .retire_peer(0, EXCLUDED)
        .await
        .map_err(|e| format!("retire peer 0->2: {e}"))?;
    cluster
        .retire_peer(1, EXCLUDED)
        .await
        .map_err(|e| format!("retire peer 1->2: {e}"))?;
    for i in [0, 1] {
        if !peer_retired(cluster, i, EXCLUDED).await? {
            return Err(format!("node{} shows no retired retention record for node3", i + 1));
        }
        let gating = gating_members(cluster, i).await?;
        if gating.len() >= gating_before.len() {
            return Err(format!(
                "node{} gating members = {} , want below baseline {}",
                i + 1,
                gating.len(),
                gating_before.len()
            ));
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exclude_persists_and_releases_retention() {
    // Pinned adversarial id order (restarted node1 sorts last): both
    // survivors' pre-restart sessions to it are then in their
    // tie-break-preferred direction, so the restart/re-mesh legs exercise
    // zombie replacement deterministically instead of flaking on random ids.
    let mut cluster = Cluster::new(ClusterOptions {
        name: "churn-retirement".into(),
        num_nodes: 3,
        node_ids: vec![[0x03; 16], [0x01; 16], [0x02; 16]],
        ..Default::default()
    })
    .await
    .expect("cluster setup");
    assert_eq!(cluster.len(), 3, "this scenario is written for 3 nodes");
    let result = run_body(&mut cluster).await;
    let failed = result.is_err();
    if let Err(e) = &result {
        eprintln!("scenario failed: {e}");
    }
    if failed {
        cluster.dump_forensics("churn-retirement-no-convergence").await;
    }
    cluster.finish(failed).await;
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}
