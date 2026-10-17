//! Rust port of the Go `snapshot-resync` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/snapshot-resync/`
//! (`README.md` + `snapshot_resync_test.go` + `snapshot_multisource_test.go`).
//! Three `murmur-server` processes mesh with aggressive log retention; a node
//! stops while the survivors write past retention and GC expiry, then
//! restarts with the same durable directory. Its needed log ranges are
//! unrecoverable, so it must rejoin by merging a snapshot without discarding
//! its acknowledged pre-stop state. A second test restarts two stale nodes
//! back-to-back so their snapshot requests overlap on one survivor, and a
//! multi-source case gives the stale node three snapshot sources
//! concurrently. Plain SQL replaces the Go typed-records HTTP API, and the
//! snapshot path is proven by flat log-path counters over the wire (see the
//! scenario README.md for the deviations).

use std::collections::{BTreeSet, HashMap};
use std::time::Duration;

use murmur_live_harness::{env_seconds, hex, Cluster, ClusterOptions, ReplicationOptions};

/// Log retention (Go: `MinLogRetentionMs = 1000`).
const RETENTION: Duration = Duration::from_secs(1);
/// Offline-peer memory (Go: `MaxOfflineLogRetentionMs = 20000`).
const MAXIMUM_OFFLINE: Duration = Duration::from_secs(20);
/// Newest transactions per origin the survivors must keep (Go: `MinRetainedBatches = 10`).
const MINIMUM_RETAINED: u64 = 10;
/// Minimum pruned transactions per survivor before the rejoin: 60 survivor
/// writes minus the 10 retained, with margin. Proves the stale node's needed
/// ranges are genuinely unrecoverable from logs, so the rejoin must use the
/// snapshot path.
const MIN_COLLECTED: u64 = 40;

/// Baseline convergence deadline (Go: 30 s; margin matches the corrupt-snapshot port).
const BASELINE_TIMEOUT: Duration = Duration::from_secs(60);
/// Survivor row-count deadline (Go: 30 s).
const ROW_TIMEOUT: Duration = Duration::from_secs(30);
/// Convergence poll interval (Go: 200 ms).
const CONVERGE_POLL: Duration = Duration::from_millis(200);
/// GC collection-loop deadline past the expiry wait.
const COLLECT_TIMEOUT: Duration = Duration::from_secs(60);
/// Single-stale rejoin convergence deadline (Go: 90 s).
const RESYNC_TIMEOUT: Duration = Duration::from_secs(90);
/// Dual/multi-source rejoin convergence deadline (Go: 120 s).
const RESYNC_TIMEOUT_LONG: Duration = Duration::from_secs(120);

/// `MURMUR SHOW REPLICATION` column indices (no header over `simple()`).
const REPL_APPLIED: usize = 5;
const REPL_PEERS: usize = 2;

/// `MURMUR SHOW RETENTION` column indices (no header over `simple()`).
const RET_NODE: usize = 0;
const RET_LAST_SEEN: usize = 1;
const RET_COLLECTED: usize = 5;

/// Canonical ordered names on one node (Go: `Cluster.TypedContentionRows`).
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

/// Canonical digest of a name set (Go: sorted rows hashed; newline-joined
/// names are equally strict here since every row differs by name).
fn digest(names: &[String]) -> String {
    names.join("\n")
}

/// Row count on one node (cheap pre-check before full name reads).
async fn row_count(cluster: &Cluster, idx: usize) -> Result<usize, String> {
    let rows = cluster
        .node(idx)
        .query("SELECT COUNT(*) FROM records", &[])
        .await
        .map_err(|e| format!("node{} SELECT count: {e}", idx + 1))?;
    let n: i64 = rows
        .first()
        .ok_or_else(|| format!("node{} count returned no rows", idx + 1))?
        .try_get(0)
        .map_err(|e| format!("node{} decode count: {e}", idx + 1))?;
    Ok(n as usize)
}

/// One single-row transaction (Go: `TypedContentionInsert`). No flush: the
/// caller batches (baseline) or flushes per write (survivor legs, where one
/// replication log entry is minted per flush batch).
async fn insert(cluster: &Cluster, node: usize, id: i64, name: &str) -> Result<(), String> {
    cluster
        .node(node)
        .execute("INSERT INTO records(id, name) VALUES($1, $2)", &[&id, &name])
        .await
        .map_err(|e| format!("node{} insert '{name}': {e}", node + 1))?;
    Ok(())
}

async fn flush(cluster: &Cluster, node: usize, what: &str) -> Result<(), String> {
    cluster
        .node(node)
        .flush()
        .await
        .map_err(|e| format!("node{} flush ({what}): {e}", node + 1))?;
    Ok(())
}

/// One multi-row transaction of `count` rows (Go: `TypedContentionInsertMany`;
/// the multi-source leg needs snapshot volume, not round trips).
async fn insert_many(
    cluster: &Cluster,
    node: usize,
    start_id: i64,
    count: i64,
    prefix: &str,
) -> Result<(), String> {
    let client = cluster.node(node).inner();
    client
        .batch_execute("BEGIN")
        .await
        .map_err(|e| format!("node{} batch BEGIN: {e}", node + 1))?;
    for n in start_id..start_id + count {
        let name = format!("{prefix}-{n}");
        client
            .execute("INSERT INTO records(id, name) VALUES($1, $2)", &[&n, &name])
            .await
            .map_err(|e| format!("node{} batch insert '{name}': {e}", node + 1))?;
    }
    client
        .batch_execute("COMMIT")
        .await
        .map_err(|e| format!("node{} batch COMMIT: {e}", node + 1))?;
    Ok(())
}

/// Wait until one node holds exactly `want` rows (Go: `waitRowCount`).
async fn wait_row_count(cluster: &Cluster, idx: usize, want: usize, timeout: Duration) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if row_count(cluster, idx).await? == want {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "node{} record count = {}, want {want} within {timeout:?}",
                idx + 1,
                row_count(cluster, idx).await?,
            ));
        }
        tokio::time::sleep(CONVERGE_POLL).await;
    }
}

/// Wait until every node holds exactly `want` rows with equal digests
/// (Go: `waitConverged`).
async fn wait_converged(cluster: &Cluster, want: usize, timeout: Duration) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let mut counts = Vec::new();
        for i in 0..cluster.len() {
            counts.push(row_count(cluster, i).await?);
        }
        if counts.iter().all(|&n| n == want) {
            let mut first = String::new();
            let mut equal = true;
            for (i, idx) in (0..cluster.len()).enumerate() {
                let d = digest(&names(cluster, idx).await?);
                if i == 0 {
                    first = d;
                } else if d != first {
                    equal = false;
                    break;
                }
            }
            if equal {
                return Ok(());
            }
        }
        if tokio::time::Instant::now() >= deadline {
            for i in 0..cluster.len() {
                let node_names = names(cluster, i).await?;
                eprintln!(
                    "node{} at timeout: count={} digest={}",
                    i + 1,
                    node_names.len(),
                    digest(&node_names)
                );
            }
            return Err(format!(
                "nodes did not converge on {want} rows with equal digests within {timeout:?}"
            ));
        }
        tokio::time::sleep(CONVERGE_POLL).await;
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

/// Cumulative log-applied transactions on one node. Snapshot application
/// bypasses this counter, so flat growth across a resync proves the rows
/// arrived via snapshot. (`received_transactions` has no wire equivalent:
/// `MURMUR SHOW METRICS` only covers SQLite/Spool, and `staged_transactions`
/// is a gauge, not a cumulative counter; see the README.)
async fn applied_transactions(cluster: &Cluster, idx: usize) -> Result<u64, String> {
    let row = show_replication(cluster, idx).await?;
    row.get(REPL_APPLIED)
        .ok_or_else(|| format!("node{} SHOW REPLICATION has no applied column", idx + 1))?
        .parse::<u64>()
        .map_err(|e| format!("node{} parse applied_transactions: {e}", idx + 1))
}

/// Live replication sessions on one node.
async fn connected_peers(cluster: &Cluster, idx: usize) -> Result<usize, String> {
    let row = show_replication(cluster, idx).await?;
    row.get(REPL_PEERS)
        .ok_or_else(|| format!("node{} SHOW REPLICATION has no peers column", idx + 1))?
        .parse::<usize>()
        .map_err(|e| format!("node{} parse peers: {e}", idx + 1))
}

/// Assert the resync used the snapshot path (Go: `spedsql_repl_snapshots_received_total >= 1`,
/// which has no Rust equivalent). The retained tail (`minimum_retained` per
/// origin) legitimately arrives via logs, so the applied growth must stay
/// within the tail and far below the gap; the bulk can only have arrived via
/// snapshot, the only other row path.
async fn assert_snapshot_path(
    cluster: &Cluster,
    idx: usize,
    before: u64,
    gap: usize,
) -> Result<(), String> {
    let after = applied_transactions(cluster, idx).await?;
    let growth = after.saturating_sub(before);
    if growth > 2 * MINIMUM_RETAINED {
        return Err(format!(
            "node{} resync applied {growth} log transactions, above the retained tail (2 x {MINIMUM_RETAINED})",
            idx + 1,
        ));
    }
    if (growth as usize) >= gap {
        return Err(format!(
            "node{} resync arrived over the log path (applied growth {growth}, gap {gap}), not a snapshot (log catch-up would hide a snapshot-path regression)",
            idx + 1,
        ));
    }
    Ok(())
}

/// Run one bounded retention pass on one node; returns transactions pruned.
async fn collect_history(cluster: &Cluster, idx: usize) -> Result<u64, String> {
    let rows = cluster
        .node(idx)
        .simple("MURMUR RETENTION COLLECT")
        .await
        .map_err(|e| format!("node{} RETENTION COLLECT: {e}", idx + 1))?;
    rows.first()
        .and_then(|row| row.first())
        .ok_or_else(|| format!("node{} RETENTION COLLECT returned no rows", idx + 1))?
        .parse::<u64>()
        .map_err(|e| format!("node{} parse collected: {e}", idx + 1))
}

/// `MURMUR SHOW RETENTION` rows on one node.
async fn show_retention(cluster: &Cluster, idx: usize) -> Result<Vec<Vec<String>>, String> {
    cluster
        .node(idx)
        .simple("MURMUR SHOW RETENTION")
        .await
        .map_err(|e| format!("node{} SHOW RETENTION: {e}", idx + 1))
}

/// Total transactions ever pruned on one node (node-global trailing counter).
async fn collected_total(cluster: &Cluster, idx: usize) -> Result<u64, String> {
    let rows = show_retention(cluster, idx).await?;
    match rows.first().and_then(|row| row.get(RET_COLLECTED)) {
        Some(v) if !v.is_empty() => v
            .parse::<u64>()
            .map_err(|e| format!("node{} parse collected_transactions: {e}", idx + 1)),
        // No retention peers yet: nothing could have been pruned.
        _ => Ok(0),
    }
}

/// Count of acknowledged pre-stop rows surviving on one node (Go: `countPrefix(rows, "stale-")`).
async fn stale_rows(cluster: &Cluster, idx: usize) -> Result<usize, String> {
    Ok(names(cluster, idx)
        .await?
        .iter()
        .filter(|name| name.starts_with("stale-"))
        .count())
}

/// Same schema on every node (Go: TypedRecords fixture tables), then
/// replicate it: row convergence needs the per-table flag on each node.
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
        flush(cluster, i, "SET TABLE").await?;
    }
    Ok(())
}

/// Standard snapshot-resync cluster: aggressive retention plus all-node
/// snapshot trust (Go harness fixture policy; the engine only requests
/// snapshots from explicitly trusted sources).
async fn snapshot_cluster(name: &str, node_ids: Vec<[u8; 16]>) -> Result<Cluster, String> {
    let all: BTreeSet<[u8; 16]> = node_ids.iter().copied().collect();
    let mut trusted_snapshot_sources: HashMap<usize, BTreeSet<[u8; 16]>> = HashMap::new();
    for i in 0..node_ids.len() {
        trusted_snapshot_sources.insert(i, all.clone());
    }
    let num_nodes = node_ids.len();
    Cluster::new(ClusterOptions {
        name: name.into(),
        num_nodes,
        node_ids,
        replication: Some(ReplicationOptions {
            retention: Some(RETENTION),
            minimum_retained: Some(MINIMUM_RETAINED),
            maximum_offline: Some(MAXIMUM_OFFLINE),
            ..Default::default()
        }),
        trusted_snapshot_sources,
        ..Default::default()
    })
    .await
    .map_err(|e| format!("cluster setup: {e}"))
}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Member-deadline expiry plus explicit collection passes: every survivor
/// must prune past retention and stop pinning every victim, proving the
/// victims' needed ranges are genuinely unrecoverable from logs (Go: the
/// 55 s expiry-plus-GC-pass sleep).
async fn ensure_snapshot_path(
    cluster: &Cluster,
    survivors: &[usize],
    victims: &[usize],
) -> Result<(), String> {
    eprintln!("waiting for member-deadline expiry and collection");
    tokio::time::sleep(env_seconds("MURMUR_SNAPSHOT_RESYNC_GC_WAIT_SECONDS", 30)).await;
    let collect_deadline = tokio::time::Instant::now() + COLLECT_TIMEOUT;
    loop {
        let mut totals = Vec::new();
        for &i in survivors {
            totals.push(collected_total(cluster, i).await?);
        }
        if totals.iter().all(|&n| n >= MIN_COLLECTED) {
            eprintln!("survivors pruned {totals:?} transactions past retention");
            break;
        }
        if tokio::time::Instant::now() >= collect_deadline {
            return Err(format!(
                "survivors collected only {totals:?} transactions, want >= {MIN_COLLECTED} each (snapshot path not forced)"
            ));
        }
        for &i in survivors {
            collect_history(cluster, i).await?;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    // Prove the deadline leg explicitly: every victim is now older than
    // every survivor's offline memory.
    for &i in survivors {
        let rows = show_retention(cluster, i).await?;
        for &victim in victims {
            let want = hex(&cluster.node_id(victim));
            let seen = rows
                .iter()
                .find(|row| row.get(RET_NODE).is_some_and(|n| n == &want))
                .and_then(|row| row.get(RET_LAST_SEEN))
                .ok_or_else(|| format!("node{} has no retention record for node{}", i + 1, victim + 1))?
                .parse::<u64>()
                .map_err(|e| format!("node{} parse last_seen_millis: {e}", i + 1))?;
            let age = now_millis().saturating_sub(seen);
            if age <= MAXIMUM_OFFLINE.as_millis() as u64 {
                return Err(format!(
                    "node{} still pins node{} (last seen {age} ms ago)",
                    i + 1,
                    victim + 1,
                ));
            }
        }
    }
    Ok(())
}

async fn single_body(cluster: &mut Cluster) -> Result<(), String> {
    // Phase 1: node3 writes acknowledged rows, then all three converge.
    migrate_all(cluster).await?;
    for i in 0..5 {
        insert(cluster, 2, i + 1, &format!("stale-{i}")).await?;
    }
    flush(cluster, 2, "baseline").await?;
    wait_converged(cluster, 5, BASELINE_TIMEOUT).await?;

    // Phase 2: node3 stops; survivors write far past retention.
    // Flush every write: the product mints one replication log entry
    // per flush batch, not per SQL transaction, so a single trailing
    // flush would leave fewer entries than `minimum_retained` and
    // nothing would ever become eligible for pruning.
    cluster.stop_node(2).await;
    for i in 0..60 {
        insert(cluster, 0, 100 + i, &format!("fresh-{i}")).await?;
        flush(cluster, 0, "survivor write").await?;
    }
    wait_row_count(cluster, 0, 65, ROW_TIMEOUT).await?;
    wait_row_count(cluster, 1, 65, ROW_TIMEOUT).await?;
    ensure_snapshot_path(cluster, &[0, 1], &[2]).await?;

    // Phase 3: node3 restarts with the same durable directory and rejoins.
    cluster
        .start_node(2)
        .await
        .map_err(|e| format!("stale node restart: {e}"))?;
    cluster
        .mesh()
        .await
        .map_err(|e| format!("re-mesh after restart: {e}"))?;
    let applied_before = applied_transactions(cluster, 2).await?;
    wait_converged(cluster, 65, RESYNC_TIMEOUT).await?;

    // The rejoin must have used the snapshot path, not log catch-up.
    assert_snapshot_path(cluster, 2, applied_before, 65 - 5).await?;

    // Node3's acknowledged pre-stop state survives the snapshot merge.
    let stale = stale_rows(cluster, 2).await?;
    if stale != 5 {
        return Err(format!("node3 stale rows after resync = {stale}, want 5"));
    }
    Ok(())
}

async fn dual_body(cluster: &mut Cluster) -> Result<(), String> {
    migrate_all(cluster).await?;
    for i in 0..5 {
        insert(cluster, 2, i + 1, &format!("stale-{i}")).await?;
    }
    flush(cluster, 2, "baseline").await?;
    wait_converged(cluster, 5, BASELINE_TIMEOUT).await?;

    // Two nodes stop; the survivor writes far past retention (flushed
    // per write: one replication log entry is minted per flush batch).
    cluster.stop_node(1).await;
    cluster.stop_node(2).await;
    for i in 0..60 {
        insert(cluster, 0, 100 + i, &format!("fresh-{i}")).await?;
        flush(cluster, 0, "survivor write").await?;
    }
    wait_row_count(cluster, 0, 65, ROW_TIMEOUT).await?;
    ensure_snapshot_path(cluster, &[0], &[1, 2]).await?;

    // Restart both stale nodes back-to-back so their snapshot requests
    // overlap on the survivor.
    cluster
        .start_node(1)
        .await
        .map_err(|e| format!("stale node2 restart: {e}"))?;
    cluster
        .start_node(2)
        .await
        .map_err(|e| format!("stale node3 restart: {e}"))?;
    cluster
        .mesh()
        .await
        .map_err(|e| format!("re-mesh after restart: {e}"))?;
    let mut applied_before = Vec::new();
    for i in [1, 2] {
        applied_before.push(applied_transactions(cluster, i).await?);
    }
    wait_converged(cluster, 65, RESYNC_TIMEOUT_LONG).await?;

    for (k, &idx) in [1, 2].iter().enumerate() {
        assert_snapshot_path(cluster, idx, applied_before[k], 65 - 5).await?;
        let stale = stale_rows(cluster, idx).await?;
        if stale != 5 {
            return Err(format!(
                "node{} stale rows after resync = {stale}, want 5",
                idx + 1
            ));
        }
        // The resync diagnostics surface end to end: each rejoined node
        // reports live replication sessions (Go asserts the presence of
        // the progress/deferral series, which have no Rust equivalent).
        let peers = connected_peers(cluster, idx).await?;
        if peers < 1 {
            return Err(format!(
                "node{} reports no connected peers after resync",
                idx + 1
            ));
        }
    }
    Ok(())
}

async fn multisource_body(cluster: &mut Cluster) -> Result<(), String> {
    const VICTIM: usize = 3;
    migrate_all(cluster).await?;
    for i in 0..5 {
        insert(cluster, VICTIM, i + 1, &format!("stale-{i}")).await?;
    }
    flush(cluster, VICTIM, "baseline").await?;
    wait_converged(cluster, 5, BASELINE_TIMEOUT).await?;

    // Victim stops; survivors write thousands of rows so the resync
    // snapshot spans many chunks (a 1-2 chunk snapshot rarely
    // interleaves badly enough to expose source contention).
    cluster.stop_node(VICTIM).await;
    // Batched multi-row transactions: the test needs snapshot volume
    // (many chunks), not round trips. Flush per batch: one replication
    // log entry is minted per flush batch, and retention needs dozens
    // of entries to prune past (a single trailing flush would leave
    // fewer entries than `minimum_retained`).
    const FRESH_ROWS: i64 = 6000;
    const BATCH: i64 = 100;
    let start = tokio::time::Instant::now();
    for base in (0..FRESH_ROWS).step_by(BATCH as usize) {
        insert_many(cluster, 0, 100 + base, BATCH, "fresh").await?;
        flush(cluster, 0, "survivor batch").await?;
    }
    eprintln!("wrote {FRESH_ROWS} survivor rows in {:?}", start.elapsed());
    let want = 5 + FRESH_ROWS as usize;
    for i in 0..3 {
        wait_row_count(cluster, i, want, ROW_TIMEOUT).await?;
    }
    ensure_snapshot_path(cluster, &[0, 1, 2], &[VICTIM]).await?;

    cluster
        .start_node(VICTIM)
        .await
        .map_err(|e| format!("victim restart: {e}"))?;
    cluster
        .mesh()
        .await
        .map_err(|e| format!("re-mesh after restart: {e}"))?;
    let applied_before = applied_transactions(cluster, VICTIM).await?;
    wait_converged(cluster, want, RESYNC_TIMEOUT_LONG).await?;

    // The rejoin used the snapshot path (not log catch-up) under real
    // multi-source contention: three survivors serve concurrently while
    // the transfer completes to convergence. Rival-source suppression
    // has no Rust counter; contention coverage is behavioral.
    assert_snapshot_path(cluster, VICTIM, applied_before, want - 5).await?;
    let stale = stale_rows(cluster, VICTIM).await?;
    if stale != 5 {
        return Err(format!("victim stale rows after resync = {stale}, want 5"));
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_node_rejoins_via_snapshot() {
    let mut cluster = snapshot_cluster("snapshot-resync", vec![[0x10; 16], [0x20; 16], [0x30; 16]])
        .await
        .expect("cluster setup");
    let result = single_body(&mut cluster).await;
    let failed = result.is_err();
    if let Err(e) = &result {
        eprintln!("scenario failed: {e}");
    }
    if failed {
        cluster.dump_forensics("snapshot-resync-no-convergence").await;
    }
    cluster.finish(failed).await;
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dual_stale_nodes_rejoin_via_snapshot() {
    let mut cluster = snapshot_cluster("snapshot-resync-dual", vec![[0x11; 16], [0x21; 16], [0x31; 16]])
        .await
        .expect("cluster setup");
    let result = dual_body(&mut cluster).await;
    let failed = result.is_err();
    if let Err(e) = &result {
        eprintln!("scenario failed: {e}");
    }
    if failed {
        cluster.dump_forensics("snapshot-resync-dual-no-convergence").await;
    }
    cluster.finish(failed).await;
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_node_rejoins_via_single_snapshot_source() {
    let mut cluster = snapshot_cluster(
        "snapshot-multisource",
        vec![[0x12; 16], [0x22; 16], [0x32; 16], [0x42; 16]],
    )
    .await
    .expect("cluster setup");
    let result = multisource_body(&mut cluster).await;
    let failed = result.is_err();
    if let Err(e) = &result {
        eprintln!("scenario failed: {e}");
    }
    if failed {
        cluster.dump_forensics("snapshot-multisource-no-convergence").await;
    }
    cluster.finish(failed).await;
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}
