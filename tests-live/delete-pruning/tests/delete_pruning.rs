//! Rust port of the Go `delete-pruning` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/delete-pruning/`
//! (`delete_pruning_test.go`). Concurrent deletes, resurrections, and
//! updates on a shared row set converge under aggressive log retention. A
//! retention pass is driven explicitly while tombstone batches are live,
//! and afterwards all nodes agree on contents plus exact row counts. There
//! is no stored-tombstone pruning API in the product (tombstone markers
//! stay in state; only origin-log batches are collected), so this suite
//! proves delete/resurrect convergence plus log collection of the delete
//! batches. Plain SQL replaces the Go typed-contention HTTP API (see the
//! scenario README.md).

use std::time::{Duration, Instant};

use murmur_live_harness::tokio_postgres::types::ToSql;
use murmur_live_harness::{Cluster, ClusterOptions, ReplicationOptions};

/// Log retention (Go: `MinLogRetentionMs = 1000`).
const RETENTION: Duration = Duration::from_secs(1);
/// Offline-peer memory (Go: `MaxOfflineLogRetentionMs = 20000`).
const MAXIMUM_OFFLINE: Duration = Duration::from_secs(20);
/// Newest batches per origin the collectors must keep (Go:
/// `MinRetainedBatches = 10`).
const MINIMUM_RETAINED: u64 = 10;
/// Convergence poll interval (Go: 100 ms).
const POLL: Duration = Duration::from_millis(100);
/// Convergence deadlines (Go: 60 s; post-prune 30 s).
const CONVERGE_TIMEOUT: Duration = Duration::from_secs(60);
const POST_PRUNE_TIMEOUT: Duration = Duration::from_secs(30);
/// Collection-pass deadline (Go: 150 s waiting on the 30 s GC ticker; the
/// Rust port drives `MURMUR RETENTION COLLECT` explicitly instead).
const COLLECT_TIMEOUT: Duration = Duration::from_secs(120);
/// Collection poll interval (Go: 2 s metric polls).
const COLLECT_POLL: Duration = Duration::from_secs(2);
/// Seed flush stride: one replication batch per stride (the product mints
/// one log entry per flush batch, not per row).
const SEED_FLUSH_STRIDE: usize = 25;

/// Positive-integer env knob with fallback (Go: `envInt`).
fn env_int(name: &str, fallback: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(fallback)
}

/// Row identity (Go: `deleteRowID`, UUID-shaped hex).
fn row_id(n: usize) -> String {
    format!("{n:08x}-0000-4000-8000-{n:012x}")
}

/// Insert one row (Go: `TypedContentionInsert`); flush is the caller's
/// choice so seeds can batch while phases mint one batch per op.
async fn insert(cluster: &Cluster, node: usize, id: &str, name: &str) -> Result<(), String> {
    let params = [&id as &(dyn ToSql + Sync), &name as _];
    cluster
        .node(node)
        .execute("INSERT INTO rows(id, name) VALUES($1, $2)", &params)
        .await
        .map_err(|e| format!("node{} insert {id:?}: {e}", node + 1))?;
    Ok(())
}

/// Rename one row (Go: `TypedContentionUpdate` on `name`).
async fn update(cluster: &Cluster, node: usize, id: &str, name: &str) -> Result<(), String> {
    let params = [&name as &(dyn ToSql + Sync), &id as _];
    cluster
        .node(node)
        .execute("UPDATE rows SET name = $1 WHERE id = $2", &params)
        .await
        .map_err(|e| format!("node{} update {id:?}: {e}", node + 1))?;
    Ok(())
}

/// Delete one row (Go: `TypedContentionDelete`).
async fn delete(cluster: &Cluster, node: usize, id: &str) -> Result<(), String> {
    let params = [&id as &(dyn ToSql + Sync)];
    cluster
        .node(node)
        .execute("DELETE FROM rows WHERE id = $1", &params)
        .await
        .map_err(|e| format!("node{} delete {id:?}: {e}", node + 1))?;
    Ok(())
}

async fn flush(cluster: &Cluster, node: usize) -> Result<(), String> {
    cluster
        .node(node)
        .simple("MURMUR FLUSH")
        .await
        .map(|_| ())
        .map_err(|e| format!("node{} flush: {e}", node + 1))
}

/// All (id, name) pairs on one node, ordered by id (Go:
/// `TypedContentionRows`).
async fn all_rows(cluster: &Cluster, idx: usize) -> Result<Vec<(String, String)>, String> {
    let rows = cluster
        .node(idx)
        .query("SELECT id, name FROM rows ORDER BY id", &[])
        .await
        .map_err(|e| format!("node{} SELECT rows: {e}", idx + 1))?;
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let id: String = row
            .try_get(0)
            .map_err(|e| format!("node{} decode id: {e}", idx + 1))?;
        let name: String = row
            .try_get(1)
            .map_err(|e| format!("node{} decode name: {e}", idx + 1))?;
        out.push((id, name));
    }
    Ok(out)
}

/// Canonical digest over ordered rows (Go: JSON marshal of id-sorted rows).
fn digest(rows: &[(String, String)]) -> String {
    rows.iter()
        .map(|(id, name)| format!("{id}\x00{name}"))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Wait until every node holds exactly `want` rows with equal digests (Go:
/// `waitConverged`).
async fn wait_converged(cluster: &Cluster, want: usize, timeout: Duration) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    loop {
        let mut first: Option<String> = None;
        let mut ok = true;
        for i in 0..cluster.len() {
            let rows = all_rows(cluster, i).await?;
            if rows.len() != want {
                ok = false;
                break;
            }
            let node_digest = digest(&rows);
            match &first {
                None => first = Some(node_digest),
                Some(want_digest) if *want_digest == node_digest => {}
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
            for i in 0..cluster.len() {
                eprintln!("node{} at timeout: count={}", i + 1, all_rows(cluster, i).await?.len());
            }
            return Err(format!("nodes did not converge on {want} rows with equal digests within {timeout:?}"));
        }
        tokio::time::sleep(POLL).await;
    }
}

/// Node-global retention counters on one node: `(collected_transactions,
/// last_collection_millis)` from `MURMUR SHOW RETENTION` (Go:
/// `spedsql_gc_log_collected_total` + `spedsql_gc_runs_total`).
async fn retention_counters(cluster: &Cluster, idx: usize) -> Result<(u64, u64), String> {
    let rows = cluster
        .node(idx)
        .simple("MURMUR SHOW RETENTION")
        .await
        .map_err(|e| format!("node{} SHOW RETENTION: {e}", idx + 1))?;
    let Some(first) = rows.first() else {
        return Ok((0, 0));
    };
    let collected = first.get(5).cloned().unwrap_or_default();
    let last = first.get(6).cloned().unwrap_or_default();
    Ok((
        last.parse::<u64>().unwrap_or(0),
        collected.parse::<u64>().unwrap_or(0),
    ))
}

/// Name of one row, or `None` when the row is absent (Go:
/// `TypedContentionRead`, whose 404 becomes an empty result here).
async fn read_name(cluster: &Cluster, idx: usize, id: &str) -> Result<Option<String>, String> {
    let params = [&id as &(dyn ToSql + Sync)];
    let rows = cluster
        .node(idx)
        .query("SELECT name FROM rows WHERE id = $1", &params)
        .await
        .map_err(|e| format!("node{} read {id:?}: {e}", idx + 1))?;
    match rows.first() {
        None => Ok(None),
        Some(row) => Ok(Some(
            row.try_get::<_, String>(0)
                .map_err(|e| format!("node{} decode name: {e}", idx + 1))?,
        )),
    }
}

/// Phase 1a: delete the first third from node 0 (one flush per op so every
/// delete becomes its own collectable log batch).
async fn phase_delete(cluster: &Cluster, node: usize, range: std::ops::Range<usize>) -> Result<(), String> {
    for i in range {
        delete(cluster, node, &row_id(i)).await?;
        flush(cluster, node).await?;
    }
    Ok(())
}

/// Phase 1b: rename the middle third from node 1.
async fn phase_update(cluster: &Cluster, range: std::ops::Range<usize>) -> Result<(), String> {
    for i in range {
        let id = row_id(i);
        update(cluster, 1, &id, &format!("upd-{i}")).await?;
        flush(cluster, 1).await?;
    }
    Ok(())
}

async fn run_body(cluster: &Cluster, keys: usize) -> Result<(), String> {
    let third = keys / 3;
    for i in 0..cluster.len() {
        cluster
            .ensure_migrated(i, "CREATE TABLE rows(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL)")
            .await
            .map_err(|e| format!("migrate node{}: {e}", i + 1))?;
    }
    for i in 0..cluster.len() {
        cluster
            .node(i)
            .simple("MURMUR SET TABLE rows REPLICATED true")
            .await
            .map_err(|e| format!("node{} SET TABLE REPLICATED: {e}", i + 1))?;
        flush(cluster, i).await?;
    }

    // Seed the shared row set on node 0 and converge.
    for i in 0..keys {
        let id = row_id(i);
        insert(cluster, 0, &id, &format!("seed-{i}")).await?;
        if i % SEED_FLUSH_STRIDE == SEED_FLUSH_STRIDE - 1 {
            flush(cluster, 0).await?;
        }
    }
    flush(cluster, 0).await?;
    wait_converged(cluster, keys, CONVERGE_TIMEOUT).await?;

    let (runs_before, collected_before) = retention_counters(cluster, 0).await?;

    // Phase 1, concurrent on disjoint thirds: node 0 deletes the first
    // third, node 1 updates the middle third, node 2 deletes the last
    // third. Only the middle third survives.
    let (d0, u1, d2) = tokio::join!(
        phase_delete(cluster, 0, 0..third),
        phase_update(cluster, third..2 * third),
        phase_delete(cluster, 2, 2 * third..keys),
    );
    d0?;
    u1?;
    d2?;
    // Anti-vacuity: the deletes must actually remove rows everywhere.
    wait_converged(cluster, third, CONVERGE_TIMEOUT).await?;
    let spot = read_name(cluster, 0, &row_id(third)).await?;
    if spot.as_deref() != Some(&format!("upd-{third}")) {
        return Err(format!("middle-third value = {spot:?}; want upd-{third} (concurrent update lost)"));
    }

    // Phase 2: resurrect the first third with new values from node 1.
    for i in 0..third {
        let id = row_id(i);
        insert(cluster, 1, &id, &format!("res-{i}")).await?;
        flush(cluster, 1).await?;
    }
    wait_converged(cluster, 2 * third, CONVERGE_TIMEOUT).await?;

    // Phase 3: delete the middle third from node 0. Survivors: the
    // resurrected first third only.
    for i in third..2 * third {
        delete(cluster, 0, &row_id(i)).await?;
        flush(cluster, 0).await?;
    }
    wait_converged(cluster, third, CONVERGE_TIMEOUT).await?;

    // Drive collection passes over the tombstone batches until the run
    // counter advances and batches are collected (Go polls the 30 s GC
    // ticker's counters; here `MURMUR RETENTION COLLECT` runs a bounded
    // pass on demand).
    let deadline = Instant::now() + COLLECT_TIMEOUT;
    let (mut runs_after, mut collected_after);
    loop {
        cluster
            .node(0)
            .simple("MURMUR RETENTION COLLECT")
            .await
            .map_err(|e| format!("node1 RETENTION COLLECT: {e}"))?;
        (runs_after, collected_after) = retention_counters(cluster, 0).await?;
        if runs_after > runs_before && collected_after > collected_before {
            break;
        }
        if Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(COLLECT_POLL).await;
    }
    eprintln!(
        "retention: runs {runs_before} -> {runs_after}, log batches collected during test: {}",
        collected_after.saturating_sub(collected_before)
    );
    if runs_after <= runs_before {
        return Err(format!(
            "no retention pass ran (last-collection {runs_before} -> {runs_after})"
        ));
    }
    if collected_after <= collected_before {
        return Err(format!(
            "no log batches collected (before={collected_before} after={collected_after}); retention did not prune delete batches"
        ));
    }

    // Identical ordered digests and exact row counts on all nodes.
    for i in 0..cluster.len() {
        let rows = all_rows(cluster, i).await?;
        if rows.len() != third {
            return Err(format!("node{} count = {}, want {third}", i + 1, rows.len()));
        }
    }
    let want_digest = digest(&all_rows(cluster, 0).await?);
    for i in 1..cluster.len() {
        let node_digest = digest(&all_rows(cluster, i).await?);
        if node_digest != want_digest {
            return Err(format!("node{} digest differs from node1", i + 1));
        }
    }
    // Content proof: resurrected rows present with new values, deleted
    // rows absent, on every node.
    for i in 0..cluster.len() {
        let got = read_name(cluster, i, &row_id(0)).await?;
        if got.as_deref() != Some("res-0") {
            return Err(format!("node{} resurrected value = {got:?}, want \"res-0\"", i + 1));
        }
        let absent = read_name(cluster, i, &row_id(third)).await?;
        if absent.is_some() {
            return Err(format!("node{} deleted row still present: {absent:?}", i + 1));
        }
    }

    // A post-prune write replicates everywhere.
    let post_id = row_id(keys + 1);
    insert(cluster, 2, &post_id, "post-prune").await?;
    flush(cluster, 2).await?;
    wait_converged(cluster, third + 1, POST_PRUNE_TIMEOUT).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_deletes_resurrections_and_updates_converge() {
    let keys = env_int("MURMUR_DELETE_PRUNING_KEYS", 300) as usize;
    assert!(keys % 3 == 0, "MURMUR_DELETE_PRUNING_KEYS={keys} must be a multiple of 3");

    let cluster = Cluster::new(ClusterOptions {
        name: "delete-pruning".into(),
        num_nodes: 3,
        replication: Some(ReplicationOptions {
            retention: Some(RETENTION),
            minimum_retained: Some(MINIMUM_RETAINED),
            maximum_offline: Some(MAXIMUM_OFFLINE),
            ..Default::default()
        }),
        ..Default::default()
    })
    .await
    .expect("cluster setup");
    assert_eq!(cluster.len(), 3, "this scenario is written for 3 nodes");
    let result = run_body(&cluster, keys).await;
    let failed = result.is_err();
    if let Err(e) = &result {
        eprintln!("scenario failed: {e}");
    }
    if failed {
        cluster.dump_forensics("delete-pruning-no-convergence").await;
    }
    cluster.finish(failed).await;
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}
