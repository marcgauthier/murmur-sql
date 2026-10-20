//! Rust port of the Go `crdt-contention` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/crdt-contention/`
//! (`contention_test.go`). Three meshed `murmur-server` processes share one
//! logical row: concurrent edits to separate cells must all survive, and
//! same-cell contention must resolve to one application-visible winner on
//! every process. Plain SQL replaces the Go typed-contention HTTP API (see
//! the scenario README.md).

use std::time::{Duration, Instant};

use murmur_live_harness::tokio_postgres::types::ToSql;
use murmur_live_harness::{Cluster, ClusterOptions};

/// Contended row id (Go: fixed UUID-shaped id).
const ROW_ID: &str = "03141592-6000-4000-8000-000000000000";
/// Same-cell updates per node (Go: `updatesPerNode = 40`).
const UPDATES_PER_NODE: usize = 40;
/// State/poll interval (Go: 50 ms).
const POLL: Duration = Duration::from_millis(50);
/// Mesh deadline (Go: 20 s).
const PEER_TIMEOUT: Duration = Duration::from_secs(20);
/// Baseline/disjoint-state deadlines (Go: 20 s).
const STATE_TIMEOUT: Duration = Duration::from_secs(20);
/// Same-cell convergence deadline (Go: 30 s).
const CONVERGE_TIMEOUT: Duration = Duration::from_secs(30);

/// One logical row (Go: `TypedContentionRow`).
#[derive(Debug, Clone, PartialEq, Eq)]
struct Row {
    name: String,
    phone: String,
    score: i64,
}

/// Read the contended row on one node (Go: `TypedContentionRead`).
async fn read_row(cluster: &Cluster, idx: usize) -> Result<Row, String> {
    let params = [&ROW_ID as &(dyn ToSql + Sync)];
    let rows = cluster
        .node(idx)
        .query("SELECT name, phone, score FROM contention WHERE id = $1", &params)
        .await
        .map_err(|e| format!("node{} read contention row: {e}", idx + 1))?;
    let row = rows
        .first()
        .ok_or_else(|| format!("node{} contention row missing", idx + 1))?;
    Ok(Row {
        name: row
            .try_get::<_, String>(0)
            .map_err(|e| format!("node{} decode name: {e}", idx + 1))?,
        phone: row
            .try_get::<_, String>(1)
            .map_err(|e| format!("node{} decode phone: {e}", idx + 1))?,
        score: row
            .try_get::<_, i64>(2)
            .map_err(|e| format!("node{} decode score: {e}", idx + 1))?,
    })
}

/// Insert the baseline row (Go: `TypedContentionInsert`).
async fn insert_row(
    cluster: &Cluster,
    node: usize,
    name: &str,
    phone: &str,
    score: i64,
) -> Result<(), String> {
    let params = [
        &ROW_ID as &(dyn ToSql + Sync),
        &name as _,
        &phone as _,
        &score as _,
    ];
    cluster
        .node(node)
        .execute(
            "INSERT INTO contention(id, name, phone, score) VALUES($1, $2, $3, $4)",
            &params,
        )
        .await
        .map_err(|e| format!("node{} baseline insert: {e}", node + 1))?;
    cluster
        .node(node)
        .simple("MURMUR FLUSH")
        .await
        .map_err(|e| format!("node{} flush after baseline: {e}", node + 1))?;
    Ok(())
}

/// Update one text cell of the contended row (Go: `TypedContentionUpdate`
/// on `name`/`phone`).
async fn update_text(cluster: &Cluster, node: usize, field: &str, value: &str) -> Result<(), String> {
    assert!(
        field == "name" || field == "phone",
        "unexpected text field {field}"
    );
    let sql = format!("UPDATE contention SET {field} = $1 WHERE id = $2");
    let params = [&value as &(dyn ToSql + Sync), &ROW_ID as _];
    cluster
        .node(node)
        .execute(&sql, &params)
        .await
        .map_err(|e| format!("node{} update {field}={value:?}: {e}", node + 1))?;
    cluster
        .node(node)
        .simple("MURMUR FLUSH")
        .await
        .map_err(|e| format!("node{} flush after {field} update: {e}", node + 1))?;
    Ok(())
}

/// Update the score cell of the contended row (Go: `TypedContentionUpdate`
/// on `score`).
async fn update_score(cluster: &Cluster, node: usize, value: i64) -> Result<(), String> {
    let params = [&value as &(dyn ToSql + Sync), &ROW_ID as _];
    cluster
        .node(node)
        .execute("UPDATE contention SET score = $1 WHERE id = $2", &params)
        .await
        .map_err(|e| format!("node{} update score={value}: {e}", node + 1))?;
    cluster
        .node(node)
        .simple("MURMUR FLUSH")
        .await
        .map_err(|e| format!("node{} flush after score update: {e}", node + 1))?;
    Ok(())
}

/// Connected sessions on one node (Go: `connectedPeers` via `/v1/status`;
/// here the `peers` column of `MURMUR SHOW REPLICATION`).
async fn connected_peers(cluster: &Cluster, idx: usize) -> Result<i64, String> {
    let rows = cluster
        .node(idx)
        .simple("MURMUR SHOW REPLICATION")
        .await
        .map_err(|e| format!("node{} SHOW REPLICATION: {e}", idx + 1))?;
    let peers = rows
        .first()
        .and_then(|row| row.get(2).cloned())
        .unwrap_or_default();
    peers
        .parse::<i64>()
        .map_err(|_| format!("node{} unparsable peers {peers:?}", idx + 1))
}

/// Wait until every node reports `want` sessions (Go: `waitForPeers`).
async fn wait_for_peers(cluster: &Cluster, want: i64, timeout: Duration) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    loop {
        let mut ok = true;
        for i in 0..cluster.len() {
            if connected_peers(cluster, i).await? != want {
                ok = false;
                break;
            }
        }
        if ok {
            return Ok(());
        }
        if Instant::now() >= deadline {
            for i in 0..cluster.len() {
                eprintln!(
                    "node{} connected peers={:?}, want {want}",
                    i + 1,
                    connected_peers(cluster, i).await?
                );
            }
            return Err(format!("peers did not reach {want} on every node within {timeout:?}"));
        }
        tokio::time::sleep(POLL).await;
    }
}

/// Wait until every process shows the exact expected row (Go:
/// `waitForState`).
async fn wait_for_state(
    cluster: &Cluster,
    want_name: &str,
    want_phone: &str,
    want_score: i64,
    timeout: Duration,
) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    let mut last = vec![String::new(); cluster.len()];
    loop {
        let mut ok = true;
        for i in 0..cluster.len() {
            match read_row(cluster, i).await {
                Ok(row) => {
                    last[i] = format!("row={row:?}");
                    if row.name != want_name || row.phone != want_phone || row.score != want_score {
                        ok = false;
                        break;
                    }
                }
                Err(e) => {
                    last[i] = format!("err={e}");
                    ok = false;
                    break;
                }
            }
        }
        if ok {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for {want_name:?}/{want_phone:?}/{want_score} on every process; last={last:?}"
            ));
        }
        tokio::time::sleep(POLL).await;
    }
}

/// Wait until the name cell agrees everywhere while phone/score stay put;
/// returns the winning name (Go: `waitForCellConvergence`).
async fn wait_for_cell_convergence(
    cluster: &Cluster,
    want_phone: &str,
    want_score: i64,
    timeout: Duration,
) -> Result<String, String> {
    let deadline = Instant::now() + timeout;
    let mut last = vec![String::new(); cluster.len()];
    loop {
        let mut winner = String::new();
        let mut converged = true;
        for i in 0..cluster.len() {
            match read_row(cluster, i).await {
                Ok(row) => {
                    last[i] = format!(
                        "name={:?} phone={:?} score={}",
                        row.name, row.phone, row.score
                    );
                    if row.phone != want_phone || row.score != want_score {
                        converged = false;
                    }
                    if i == 0 {
                        winner = row.name;
                    } else if row.name != winner {
                        converged = false;
                    }
                }
                Err(e) => {
                    last[i] = format!("err={e}");
                    converged = false;
                }
            }
        }
        if converged {
            return Ok(winner);
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "same-cell contention did not converge across {} processes; last={last:?}",
                cluster.len()
            ));
        }
        tokio::time::sleep(POLL).await;
    }
}

async fn run_body(cluster: &Cluster) -> Result<(), String> {
    for i in 0..cluster.len() {
        let info = cluster.node_info(i);
        eprintln!(
            "{} pid={:?} dir={} repl={}",
            info.label,
            cluster.node_pid(i),
            info.dir.display(),
            info.repl_addr
        );
    }
    // Same schema on every node (Go: TypedContention fixture table), then
    // replicate it: cell convergence needs the per-table flag on each node.
    for i in 0..cluster.len() {
        cluster
            .ensure_migrated(
                i,
                "CREATE TABLE contention(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL, phone TEXT NOT NULL, score INTEGER NOT NULL)",
            )
            .await
            .map_err(|e| format!("migrate node{}: {e}", i + 1))?;
    }
    for i in 0..cluster.len() {
        cluster
            .node(i)
            .simple("MURMUR SET TABLE contention REPLICATED true")
            .await
            .map_err(|e| format!("node{} SET TABLE REPLICATED: {e}", i + 1))?;
        cluster
            .node(i)
            .simple("MURMUR FLUSH")
            .await
            .map_err(|e| format!("node{} flush after SET TABLE: {e}", i + 1))?;
    }
    wait_for_peers(cluster, 2, PEER_TIMEOUT).await?;

    insert_row(cluster, 0, "baseline", "baseline", 0).await?;
    wait_for_state(cluster, "baseline", "baseline", 0, STATE_TIMEOUT).await?;

    // Concurrent edits to separate cells must all survive regardless of
    // arrival order at other processes.
    let (name_res, phone_res, score_res) = tokio::join!(
        update_text(cluster, 0, "name", "node1-name"),
        update_text(cluster, 1, "phone", "node2-phone"),
        update_score(cluster, 2, 42),
    );
    name_res?;
    phone_res?;
    score_res?;
    wait_for_state(cluster, "node1-name", "node2-phone", 42, STATE_TIMEOUT).await?;

    // All three processes now contend on the same name cell. The winning
    // value is intentionally not preselected; the LWW version must resolve
    // to the same application-visible result on every process.
    let contend = |node: usize| async move {
        for seq in 0..UPDATES_PER_NODE {
            let value = format!("node{}-cell-{seq:03}", node + 1);
            update_text(cluster, node, "name", &value).await?;
        }
        Ok::<(), String>(())
    };
    let (r0, r1, r2) = tokio::join!(contend(0), contend(1), contend(2));
    r0?;
    r1?;
    r2?;

    let winner = wait_for_cell_convergence(cluster, "node2-phone", 42, CONVERGE_TIMEOUT).await?;
    if winner.is_empty() {
        return Err("same-cell contention resolved to an empty name".into());
    }
    if !winner.starts_with("node1-cell-")
        && !winner.starts_with("node2-cell-")
        && !winner.starts_with("node3-cell-")
    {
        return Err(format!("same-cell winner {winner:?} did not come from the concurrent write set"));
    }
    let first = read_row(cluster, 0).await?;
    for i in 1..cluster.len() {
        let other = read_row(cluster, i).await?;
        if other != first {
            return Err(format!("logical row differs: node1={first:?} node{}={other:?}", i + 1));
        }
    }
    eprintln!("three-process cell conflict converged to {winner:?} after {UPDATES_PER_NODE} updates per node");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_process_disjoint_and_shared_cell_contention() {
    let cluster = Cluster::new(ClusterOptions {
        name: "crdt-contention".into(),
        num_nodes: 3,
        ..Default::default()
    })
    .await
    .expect("cluster setup");
    assert_eq!(cluster.len(), 3, "this scenario is written for 3 nodes");
    let result = run_body(&cluster).await;
    let failed = result.is_err();
    if let Err(e) = &result {
        eprintln!("scenario failed: {e}");
    }
    if failed {
        cluster.dump_forensics("crdt-contention-no-convergence").await;
    }
    cluster.finish(failed).await;
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}
