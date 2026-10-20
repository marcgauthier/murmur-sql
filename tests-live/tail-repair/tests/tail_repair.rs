//! Rust port of the Go `tail-repair` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/tail-repair/`
//! (`tail_repair_test.go`). Three `murmur-server` processes mesh; a node
//! partitioned mid-write-burst misses a contiguous range of its peers'
//! origin logs, then heals and must repair the missing tail from peer logs
//! WITHOUT the snapshot path: row counts and ordered digests converge
//! everywhere while the healed node's log-path counter proves the range
//! arrived as transactions, not as a snapshot. Plain SQL replaces the Go
//! typed-records HTTP API, and the Go `/metrics` snapshot counters (absent
//! from the Rust product) are replaced by a structural no-snapshot guarantee
//! plus an `applied_transactions` advancement assertion (see the scenario
//! README.md).

use std::time::Duration;

use murmur_live_harness::{Cluster, ClusterOptions, ReplicationOptions};

/// Convergence poll interval (Go: 100 ms).
const POLL_INTERVAL: Duration = Duration::from_millis(100);
/// Baseline convergence deadline (Go: 30 s).
const BASELINE_TIMEOUT: Duration = Duration::from_secs(30);
/// Connected-peers settle deadlines, split and heal (Go: 15 s / 30 s).
const SPLIT_PEERS_TIMEOUT: Duration = Duration::from_secs(15);
const HEAL_PEERS_TIMEOUT: Duration = Duration::from_secs(30);
/// Isolation-probe reach deadline on the survivor (Go: 15 s).
const PROBE_TIMEOUT: Duration = Duration::from_secs(15);
/// Negative-check settle margin before asserting the isolate is frozen
/// (Go: 2 s sleep).
const SETTLE_MARGIN: Duration = Duration::from_secs(2);
/// Burst convergence deadline on the survivors (Go: 30 s).
const BURST_TIMEOUT: Duration = Duration::from_secs(30);
/// Full convergence deadline after healing (Go: 60 s).
const HEAL_TIMEOUT: Duration = Duration::from_secs(60);
/// Post-heal write convergence deadline (Go: 30 s).
const POST_HEAL_TIMEOUT: Duration = Duration::from_secs(30);

/// Baseline rows on node 0 (Go: `baseline = 10`, ids `i`, `base-i`).
const BASELINE_ROWS: i64 = 10;
/// Isolation-probe row id (Go: 9000, `probe`).
const PROBE_ID: i64 = 9000;
/// Burst row id base (Go: 10000+i, `burst-i`).
const BURST_ID: i64 = 10000;
/// Post-heal row id on the healed node (Go: 20000, `post-heal`).
const POST_HEAL_ID: i64 = 20000;

/// `MURMUR SHOW REPLICATION` column indices (no header over `simple()`).
const REPL_PEERS: usize = 2;
const REPL_APPLIED: usize = 5;

/// One marker row: Go's `TypedContentionRow{ID, Name}`.
type MarkerRow = (i64, String);

/// All marker rows on one node (Go: `Cluster.TypedContentionRows`).
async fn rows(cluster: &Cluster, idx: usize) -> Result<Vec<MarkerRow>, String> {
    let db_rows = cluster
        .node(idx)
        .query("SELECT id, name FROM records", &[])
        .await
        .map_err(|e| format!("node{} SELECT rows: {e}", idx + 1))?;
    db_rows
        .iter()
        .map(|row| {
            let id: i64 = row
                .try_get(0)
                .map_err(|e| format!("node{} decode id: {e}", idx + 1))?;
            let name: String = row
                .try_get(1)
                .map_err(|e| format!("node{} decode name: {e}", idx + 1))?;
            Ok((id, name))
        })
        .collect()
}

/// Canonical digest of a row set (Go: `digestRows`, sorted then `%#v`).
fn digest(mut rows: Vec<MarkerRow>) -> String {
    rows.sort();
    format!("{rows:?}")
}

/// Insert one marker row plus a durability barrier (Go: `insertMarker`).
async fn insert_marker(cluster: &Cluster, node: usize, id: i64, name: &str) -> Result<(), String> {
    cluster
        .node(node)
        .execute("INSERT INTO records(id, name) VALUES($1, $2)", &[&id, &name])
        .await
        .map_err(|e| format!("node{} insert '{name}': {e}", node + 1))?;
    cluster
        .node(node)
        .simple("MURMUR FLUSH")
        .await
        .map_err(|e| format!("node{} flush after '{name}': {e}", node + 1))?;
    Ok(())
}

/// Wait until one node holds exactly `want` rows (Go: `waitRowCount`).
async fn wait_row_count(
    cluster: &Cluster,
    idx: usize,
    want: usize,
    timeout: Duration,
    what: &str,
) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if rows(cluster, idx).await?.len() == want {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "node{} typed row count = {}, want {want} within {timeout:?} ({what})",
                idx + 1,
                rows(cluster, idx).await?.len(),
            ));
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Wait until every node holds exactly `want` rows with equal digests
/// (Go: `waitConverged`).
async fn wait_converged(
    cluster: &Cluster,
    want: usize,
    timeout: Duration,
    what: &str,
) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let mut first = String::new();
        let mut converged = true;
        for (i, idx) in (0..cluster.len()).enumerate() {
            let node_rows = rows(cluster, idx).await?;
            if node_rows.len() != want {
                converged = false;
                break;
            }
            let d = digest(node_rows);
            if i == 0 {
                first = d;
            } else if d != first {
                converged = false;
                break;
            }
        }
        if converged {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            for i in 0..cluster.len() {
                let node_rows = rows(cluster, i).await?;
                eprintln!(
                    "node{} at timeout: count={} digest={}",
                    i + 1,
                    node_rows.len(),
                    digest(node_rows)
                );
            }
            return Err(format!(
                "nodes did not converge on {want} rows with equal digests within {timeout:?} ({what})"
            ));
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// One `MURMUR SHOW REPLICATION` row (single-row output).
async fn show_replication(cluster: &Cluster, idx: usize) -> Result<Vec<String>, String> {
    cluster
        .node(idx)
        .simple("MURMUR SHOW REPLICATION")
        .await
        .map_err(|e| format!("node{} SHOW REPLICATION: {e}", idx + 1))?
        .into_iter()
        .next()
        .ok_or_else(|| format!("node{} SHOW REPLICATION returned no rows", idx + 1))
}

/// Wait until one node reports exactly `want` replication peers
/// (Go: `waitConnectedPeers` over `/v1/status connected_peers`).
async fn wait_connected_peers(
    cluster: &Cluster,
    idx: usize,
    want: usize,
    timeout: Duration,
) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let row = show_replication(cluster, idx).await?;
        let peers: usize = row
            .get(REPL_PEERS)
            .ok_or_else(|| format!("node{} SHOW REPLICATION has no peers column", idx + 1))?
            .parse()
            .map_err(|e| format!("node{} parse peers: {e}", idx + 1))?;
        if peers == want {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "node{} connected peers = {peers}, want {want} within {timeout:?}",
                idx + 1,
            ));
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Cumulative log-applied transactions on one node. Snapshot application
/// bypasses this counter (see README.md), so advancement across the heal
/// proves the missed range arrived as log transactions.
/// (`received_transactions` has no wire equivalent: `MURMUR SHOW METRICS`
/// only covers SQLite/Spool, and `staged_transactions` is a gauge.)
async fn applied_transactions(cluster: &Cluster, idx: usize) -> Result<u64, String> {
    let row = show_replication(cluster, idx).await?;
    row.get(REPL_APPLIED)
        .ok_or_else(|| format!("node{} SHOW REPLICATION has no applied column", idx + 1))?
        .parse::<u64>()
        .map_err(|e| format!("node{} parse applied_transactions: {e}", idx + 1))
}

/// Positive-integer env knob with fallback (Go: `envInt`).
fn env_usize(name: &str, fallback: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(fallback)
}

async fn run_body(cluster: &Cluster, burst_rows: usize) -> Result<(), String> {
    // Same schema on every node (Go: TypedRecords fixture tables), then
    // replicate it: row convergence needs the per-table flag on each node.
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
            .simple("MURMUR FLUSH")
            .await
            .map_err(|e| format!("node{} flush after SET TABLE: {e}", i + 1))?;
    }

    // Baseline: 10 rows on node 0, converged everywhere.
    for i in 0..BASELINE_ROWS {
        insert_marker(cluster, 0, i, &format!("base-{i}")).await?;
    }
    wait_converged(cluster, BASELINE_ROWS as usize, BASELINE_TIMEOUT, "baseline").await?;
    let baseline_digest = digest(rows(cluster, 2).await?);

    // Log-path baseline on the soon-isolated node (Go: snapshot
    // counters that have no Rust equivalent; see README.md).
    let applied_before = applied_transactions(cluster, 2).await?;
    eprintln!("node3 pre-partition applied_transactions={applied_before}");

    // Partition node3 away from both peers in both directions.
    for (from, target) in [(2, 0), (0, 2), (2, 1), (1, 2)] {
        cluster
            .remove_peer(from, target)
            .await
            .map_err(|e| format!("remove peer {from}->{target}: {e}"))?;
    }
    wait_connected_peers(cluster, 2, 0, SPLIT_PEERS_TIMEOUT).await?;

    // Isolation probe: one row on node 0 must reach node 1 but never
    // node3.
    insert_marker(cluster, 0, PROBE_ID, "probe").await?;
    wait_row_count(
        cluster,
        1,
        BASELINE_ROWS as usize + 1,
        PROBE_TIMEOUT,
        "probe on survivor",
    )
    .await?;
    tokio::time::sleep(SETTLE_MARGIN).await; // negative check needs a settle margin
    let isolated = rows(cluster, 2).await?;
    if isolated.len() != BASELINE_ROWS as usize {
        return Err(format!(
            "partition leaked: node3 count = {}, want {BASELINE_ROWS}",
            isolated.len()
        ));
    }
    if digest(isolated) != baseline_digest {
        return Err("partition leaked: node3 digest changed while partitioned".into());
    }

    // Write burst on the survivors while node3 is isolated.
    for i in 0..burst_rows {
        let node = i % 2;
        insert_marker(cluster, node, BURST_ID + i as i64, &format!("burst-{i}")).await?;
    }
    let want = BASELINE_ROWS as usize + 1 + burst_rows;
    wait_row_count(cluster, 0, want, BURST_TIMEOUT, "burst on node1").await?;
    wait_row_count(cluster, 1, want, BURST_TIMEOUT, "burst on node2").await?;
    // Anti-vacuity: node3 must actually be missing the contiguous range.
    let isolated = rows(cluster, 2).await?;
    if isolated.len() != BASELINE_ROWS as usize {
        return Err(format!(
            "node3 count = {}, want {BASELINE_ROWS} (it must miss the burst range)",
            isolated.len()
        ));
    }
    let missed = want - BASELINE_ROWS as usize;
    eprintln!("node3 missed {missed} rows while partitioned");

    // Heal and require full convergence.
    for (from, target) in [(2, 0), (0, 2), (2, 1), (1, 2)] {
        cluster
            .add_peer(from, target)
            .await
            .map_err(|e| format!("add peer {from}->{target}: {e}"))?;
    }
    for i in 0..cluster.len() {
        wait_connected_peers(cluster, i, 2, HEAL_PEERS_TIMEOUT).await?;
    }
    wait_converged(cluster, want, HEAL_TIMEOUT, "post-heal").await?;

    // The healing must have used log range repair, not the snapshot
    // path. The Rust engine only requests/serves/accepts snapshots
    // for explicitly trusted snapshot sources, and this cluster never
    // configures any, so no snapshot could have been taken; the
    // counter advancement below proves the missed range arrived and
    // applied as log transactions (snapshot application bypasses the
    // counter). Diagnostic analog of Go's
    // gaps_detected/needs_sent/batches_received log line. Every burst
    // row is its own flushed transaction, so the missed row count is
    // also the missed transaction count.
    let applied_after = applied_transactions(cluster, 2).await?;
    eprintln!(
        "node3 log repair: applied {applied_before}->{applied_after} (missed {missed} rows)"
    );
    if applied_after.saturating_sub(applied_before) < missed as u64 {
        return Err(format!(
            "node3 applied_transactions advanced by {}, want at least {missed} (healing must apply the missed range from peer logs)",
            applied_after.saturating_sub(applied_before)
        ));
    }

    // Post-heal write on the healed node replicates everywhere.
    insert_marker(cluster, 2, POST_HEAL_ID, "post-heal").await?;
    wait_converged(cluster, want + 1, POST_HEAL_TIMEOUT, "post-heal write").await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partitioned_node_repairs_tail_from_peer_logs() {
    let burst_rows = env_usize("MURMUR_TAIL_REPAIR_BURST_ROWS", 100);
    // Generous retention: the missed range must stay comfortably inside
    // every peer's origin log, so log repair (not snapshot resync) is
    // the only legitimate healing path. Mirrors Go's
    // MinLogRetentionMs=300000 / MinRetainedBatches=10000; Go's
    // MaxOfflineLogRetentionMs has no harness knob and keeps its 7-day
    // default, which is strictly more generous (see README.md).
    let cluster = Cluster::new(ClusterOptions {
        name: "tail-repair".into(),
        num_nodes: 3,
        replication: Some(ReplicationOptions {
            retention: Some(Duration::from_millis(300_000)),
            minimum_retained: Some(10_000),
            ..Default::default()
        }),
        ..Default::default()
    })
    .await
    .expect("cluster setup");
    let result = run_body(&cluster, burst_rows).await;
    let failed = result.is_err();
    if let Err(e) = &result {
        eprintln!("scenario failed: {e}");
    }
    if failed {
        cluster.dump_forensics("tail-repair-no-convergence").await;
    }
    cluster.finish(failed).await;
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}
