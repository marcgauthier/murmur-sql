//! Rust port of the Go `merge-policies` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/merge-policies/`
//! (`merge_policies_test.go`, `README.md`).
//! `TestDisconnectedPoliciesForwardAndRestart`: three `murmur-server`
//! processes write PN_COUNTER / OR_SET / MAX / MIN policy ops on one shared
//! row while disconnected; nodes 0 and 1 connect and converge the counter;
//! node 0 stops and node 1 forwards the causal operations to node 2; node 2
//! restarts and recovers; finally an observed set remove propagates. The
//! unobserved remove (node 1 removing "shared" before seeing node 0's add)
//! must not erase the add, while the later observed remove must.
//!
//! Policy ops run as `MURMUR POLICY ...` commands over the wire; set
//! projections decode from the stored postcard bytes (see the scenario
//! README.md for the per-leg mapping).

use std::collections::BTreeMap;
use std::time::Duration;

use murmur_live_harness::{wait_until_async, Cluster, ClusterOptions, MergePolicy, ReplicationOptions};

/// Typed table (Go: managed typed record with counter/set/extrema fields).
const TABLE: &str = "live_merge_policies";
/// Base DDL. `counter`/`tags`/`peak`/`floor` are policy columns (see
/// [`merge_policies`]). Set projections start as `x'00'` (postcard empty
/// vec), the `tests/` convention for fresh set cells.
const BASE_DDL: &str = "CREATE TABLE live_merge_policies(name TEXT NOT NULL PRIMARY KEY, counter INTEGER NOT NULL, tags BLOB NOT NULL, peak INTEGER NOT NULL, floor REAL NOT NULL)";
/// Shared row (Go: `merge-policy-row`, one row id seeded on every node).
const ROW: &str = "merge-policy-row";
/// Convergence deadline (Go: 30 s waits).
const WAIT: Duration = Duration::from_secs(30);
/// Convergence poll interval (Go: 50 ms polls).
const POLL: Duration = Duration::from_millis(50);

/// Test-only mirror of the product `Value` enum for OR_SET projection
/// decoding: same variant order, so the postcard representation matches.
#[derive(serde::Deserialize)]
#[allow(dead_code)]
enum WireValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

/// (table, column) -> merge policy for the typed table (Go: PN_COUNTER,
/// OR_SET, numeric MAX/MIN field kinds).
fn merge_policies() -> BTreeMap<(String, String), MergePolicy> {
    BTreeMap::from([
        ((TABLE.into(), "counter".into()), MergePolicy::Counter),
        ((TABLE.into(), "tags".into()), MergePolicy::Set),
        ((TABLE.into(), "peak".into()), MergePolicy::Max),
        ((TABLE.into(), "floor".into()), MergePolicy::Min),
    ])
}

/// Migrate every node and flag the table replicated on each node (row and
/// policy-op convergence needs the per-node flag). Flagging happens before
/// any row exists, so no populated-table rebuild is involved.
async fn migrate_all(cluster: &Cluster) -> Result<(), String> {
    for i in 0..cluster.len() {
        cluster
            .ensure_migrated(i, BASE_DDL)
            .await
            .map_err(|e| format!("node{} migrate failed: {e}", i + 1))?;
    }
    for i in 0..cluster.len() {
        cluster
            .node(i)
            .simple(&format!("MURMUR SET TABLE {TABLE} REPLICATED true"))
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

/// Typed insert on one node (Go: `Cluster.TypedInsertWithID`).
async fn insert_row(cluster: &Cluster, idx: usize, name: &str) -> Result<(), String> {
    cluster
        .node(idx)
        .simple(&format!(
            "INSERT INTO {TABLE}(name, counter, tags, peak, floor) VALUES('{name}', 0, x'00', 0, 0.0)"
        ))
        .await
        .map_err(|e| format!("node{} typed insert failed: {e}", idx + 1))?;
    cluster
        .node(idx)
        .flush()
        .await
        .map_err(|e| format!("node{} flush after typed insert: {e}", idx + 1))?;
    Ok(())
}

/// Single text cell of a single-row SELECT.
async fn select_one(cluster: &Cluster, idx: usize, sql: &str) -> Result<String, String> {
    let rows = cluster
        .node(idx)
        .simple(sql)
        .await
        .map_err(|e| format!("node{} {sql}: {e}", idx + 1))?;
    rows.into_iter()
        .next()
        .and_then(|r| r.into_iter().next())
        .ok_or_else(|| format!("node{} {sql}: no rows", idx + 1))
}

/// PN_COUNTER projection (Go: `Cluster.TypedCounterValue`).
async fn counter_value(cluster: &Cluster, idx: usize, name: &str) -> i64 {
    let sql = format!("SELECT counter FROM {TABLE} WHERE name = '{name}'");
    match select_one(cluster, idx, &sql).await {
        Ok(cell) => cell.parse().unwrap_or(i64::MIN),
        Err(_) => i64::MIN,
    }
}

/// OR_SET projection, sorted (Go: `Cluster.TypedSetValues`). The stored
/// blob renders as `\x<hex>` over the simple protocol; hex-decode it and
/// postcard-decode the value vec.
async fn set_values(cluster: &Cluster, idx: usize, name: &str) -> Vec<String> {
    let sql = format!("SELECT tags FROM {TABLE} WHERE name = '{name}'");
    let cell = match select_one(cluster, idx, &sql).await {
        Ok(cell) => cell,
        Err(_) => return vec!["<read-error>".into()],
    };
    let hex = cell.strip_prefix("\\x").unwrap_or(&cell);
    if hex.len() % 2 != 0 {
        return vec![];
    }
    let mut bytes = Vec::with_capacity(hex.len() / 2);
    for pair in hex.as_bytes().chunks_exact(2) {
        let word = std::str::from_utf8(pair).unwrap_or("");
        match u8::from_str_radix(word, 16) {
            Ok(b) => bytes.push(b),
            Err(_) => return vec![],
        }
    }
    let values: Vec<WireValue> = postcard::from_bytes(&bytes).unwrap_or_default();
    let mut out: Vec<String> = values
        .into_iter()
        .filter_map(|v| match v {
            WireValue::Text(s) => Some(s),
            _ => None,
        })
        .collect();
    out.sort();
    out
}

/// Numeric extrema projection (Go: `Cluster.TypedExtremaValues`).
async fn extrema(cluster: &Cluster, idx: usize, name: &str) -> (i64, f64) {
    let sql = format!("SELECT peak, floor FROM {TABLE} WHERE name = '{name}'");
    let rows = match cluster.node(idx).simple(&sql).await {
        Ok(rows) => rows,
        Err(_) => return (i64::MIN, f64::NAN),
    };
    match rows.first() {
        Some(row) if row.len() >= 2 => (
            row[0].parse().unwrap_or(i64::MIN),
            row[1].parse().unwrap_or(f64::NAN),
        ),
        _ => (i64::MIN, f64::NAN),
    }
}

/// One `MURMUR POLICY ...` op plus a durability barrier.
async fn policy(cluster: &Cluster, idx: usize, op: &str) -> Result<(), String> {
    cluster
        .node(idx)
        .simple(op)
        .await
        .map_err(|e| format!("node{} {op}: {e}", idx + 1))?;
    cluster
        .node(idx)
        .flush()
        .await
        .map_err(|e| format!("node{} flush after {op}: {e}", idx + 1))?;
    Ok(())
}

/// PN_COUNTER op (Go: `Cluster.TypedCounterAdd`).
async fn counter_add(cluster: &Cluster, idx: usize, name: &str, delta: i64) -> Result<(), String> {
    policy(cluster, idx, &format!("MURMUR POLICY COUNTER_ADD {TABLE} counter '{name}' {delta}")).await
}

/// OR_SET add (Go: `Cluster.TypedSetAdd`).
async fn set_add(cluster: &Cluster, idx: usize, name: &str, value: &str) -> Result<(), String> {
    policy(cluster, idx, &format!("MURMUR POLICY SET_ADD {TABLE} tags '{name}' '{value}'")).await
}

/// OR_SET remove (Go: `Cluster.TypedSetRemove`).
async fn set_remove(cluster: &Cluster, idx: usize, name: &str, value: &str) -> Result<(), String> {
    policy(cluster, idx, &format!("MURMUR POLICY SET_REMOVE {TABLE} tags '{name}' '{value}'")).await
}

/// Numeric MIN/MAX op (Go: `Cluster.TypedExtremaUpdate`).
async fn extrema_update(cluster: &Cluster, idx: usize, name: &str, peak: i64, floor: f64) -> Result<(), String> {
    policy(cluster, idx, &format!("MURMUR POLICY MAX {TABLE} peak '{name}' {peak}")).await?;
    policy(cluster, idx, &format!("MURMUR POLICY MIN {TABLE} floor '{name}' {floor}")).await
}

async fn wait_counter(cluster: &Cluster, node: usize, name: &str, want: i64) -> Result<(), String> {
    let converged = wait_until_async(WAIT, POLL, || async {
        counter_value(cluster, node, name).await == want
    })
    .await;
    if !converged {
        return Err(format!(
            "node{} counter for {name:?} = {}, want {want}",
            node + 1,
            counter_value(cluster, node, name).await
        ));
    }
    Ok(())
}

/// Combined set + extrema projection wait (Go: `waitPolicyProjection`).
async fn wait_policy_projection(
    cluster: &Cluster,
    node: usize,
    name: &str,
    want_tags: &[&str],
    want_peak: i64,
    want_floor: f64,
) -> Result<(), String> {
    let want: Vec<String> = want_tags.iter().map(|s| s.to_string()).collect();
    let converged = wait_until_async(WAIT, POLL, || async {
        set_values(cluster, node, name).await == want
            && extrema(cluster, node, name).await == (want_peak, want_floor)
    })
    .await;
    if !converged {
        return Err(format!(
            "node{} projection tags={:?} extrema={:?}; want tags={want:?} extrema=({want_peak},{want_floor})",
            node + 1,
            set_values(cluster, node, name).await,
            extrema(cluster, node, name).await
        ));
    }
    Ok(())
}

async fn run_body(cluster: &mut Cluster) -> Result<(), String> {
    migrate_all(cluster).await?;

    // Every node seeds the same logical row while disconnected (Go:
    // TypedInsertWithID with one shared row id on all nodes).
    for i in 0..cluster.len() {
        insert_row(cluster, i, ROW).await?;
    }

    // Each node makes independent typed CRDT updates while disconnected.
    counter_add(cluster, 0, ROW, 10).await?;
    set_add(cluster, 0, ROW, "shared").await?;
    set_add(cluster, 0, ROW, "red").await?;
    extrema_update(cluster, 0, ROW, 10, 30.0).await?;

    counter_add(cluster, 1, ROW, 7).await?;
    // This remove has not observed node 0's add and must not erase it.
    set_remove(cluster, 1, ROW, "shared").await?;
    set_add(cluster, 1, ROW, "blue").await?;
    extrema_update(cluster, 1, ROW, 20, 15.0).await?;

    counter_add(cluster, 2, ROW, -2).await?;
    set_add(cluster, 2, ROW, "green").await?;
    extrema_update(cluster, 2, ROW, 5, -7.0).await?;

    // Nodes 0 and 1 connect (node 2 stays isolated); their counters sum.
    cluster.add_peer(0, 1).await.map_err(|e| format!("connect 0 -> 1: {e}"))?;
    cluster.add_peer(1, 0).await.map_err(|e| format!("connect 1 -> 0: {e}"))?;
    cluster.sync_all().await.map_err(|e| format!("sync after 0<->1 connect: {e}"))?;
    wait_counter(cluster, 0, ROW, 17).await?;
    wait_counter(cluster, 1, ROW, 17).await?;

    // The original writer stops; node 1 forwards its causal operations to
    // node 2.
    cluster.stop_node(0).await;
    cluster.add_peer(1, 2).await.map_err(|e| format!("connect 1 -> 2: {e}"))?;
    cluster.add_peer(2, 1).await.map_err(|e| format!("connect 2 -> 1: {e}"))?;
    cluster.sync_all().await.map_err(|e| format!("sync after 1<->2 connect: {e}"))?;
    wait_counter(cluster, 1, ROW, 15).await?;
    wait_counter(cluster, 2, ROW, 15).await?;
    wait_policy_projection(cluster, 1, ROW, &["blue", "green", "red", "shared"], 20, -7.0).await?;
    wait_policy_projection(cluster, 2, ROW, &["blue", "green", "red", "shared"], 20, -7.0).await?;

    // Node 2 restarts and recovers the merged projections from durable
    // state. The restart rebinds fresh ports, so the 1 <-> 2 edges are
    // re-added explicitly (peers still dial the old address otherwise).
    cluster.stop_node(2).await;
    cluster.start_node(2).await.map_err(|e| format!("restart node 2: {e}"))?;
    cluster.add_peer(1, 2).await.map_err(|e| format!("reconnect 1 -> 2: {e}"))?;
    cluster.add_peer(2, 1).await.map_err(|e| format!("reconnect 2 -> 1: {e}"))?;
    cluster.sync_all().await.map_err(|e| format!("sync after node 2 restart: {e}"))?;
    wait_counter(cluster, 2, ROW, 15).await?;
    wait_policy_projection(cluster, 2, ROW, &["blue", "green", "red", "shared"], 20, -7.0).await?;

    // A later observed remove must propagate after restart.
    set_remove(cluster, 1, ROW, "shared").await?;
    wait_policy_projection(cluster, 2, ROW, &["blue", "green", "red"], 20, -7.0).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn disconnected_policies_forward_and_restart() {
    let mut cluster = Cluster::new(ClusterOptions {
        name: "merge-policies".into(),
        num_nodes: 3,
        manual_peers: true,
        replication: Some(ReplicationOptions {
            merge_policies: Some(merge_policies()),
            ..Default::default()
        }),
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
        cluster.dump_forensics("merge-policies-no-convergence").await;
    }
    cluster.finish(failed).await;
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}
