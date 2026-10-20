//! Rust port of the Go `flap-partition` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/flap-partition/`
//! (`flap_partition_test.go`). Three fully meshed `murmur-server` processes
//! take continuous writes on every node while node3's links flap through
//! split/heal cycles. The first split carries an isolation proof (a marker
//! written on node1 must reach node2 but never node3); no write to a live
//! node may fail, and the final heal must converge every written row with
//! equal contents. Plain SQL replaces the Go typed-records HTTP API (see the
//! scenario README.md).

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::time::Duration;

use murmur_live_harness::{env_seconds, Cluster, ClusterOptions};

/// Convergence poll interval (Go: 200 ms).
const POLL_INTERVAL: Duration = Duration::from_millis(200);
/// Per-writer insert cadence (Go: 50 ms ticker).
const WRITE_INTERVAL: Duration = Duration::from_millis(50);
/// Isolation-probe poll interval (Go: 50 ms).
const ISOLATION_POLL: Duration = Duration::from_millis(50);
/// Baseline / final convergence deadlines (Go: 150 s each).
const CONVERGE_TIMEOUT: Duration = Duration::from_secs(150);
/// Isolation marker propagation deadline (Go: 10 s).
const ISOLATION_TIMEOUT: Duration = Duration::from_secs(10);
/// Post-proof leak watch window (Go: 1 s).
const LEAK_WINDOW: Duration = Duration::from_secs(1);
/// Progress-log cadence inside convergence waits (Go: 10 s).
const PROGRESS_LOG_INTERVAL: Duration = Duration::from_secs(10);
/// Baseline row count written on node 0 before flapping (Go: 6).
const BASELINE_ROWS: i64 = 6;

/// Row-id stride per writer node: ids are `node * ID_STRIDE + seq`, so the
/// three concurrent writers never collide (Go: server-generated typed IDs).
const ID_STRIDE: i64 = 10_000_000;
/// Baseline ids live above every writer range.
const BASELINE_ID_BASE: i64 = 90_000_001;
/// Isolation marker id, outside baseline and writer ranges.
const ISOLATION_MARKER_ID: i64 = 90_001_001;
/// Isolation marker name (Go: `isolation-marker`).
const ISOLATION_MARKER: &str = "isolation-marker";

/// All record names on one node (Go: `Cluster.TypedNames`).
async fn names(cluster: &Cluster, idx: usize) -> Result<Vec<String>, String> {
    let rows = cluster
        .node(idx)
        .query("SELECT name FROM records", &[])
        .await
        .map_err(|e| format!("node{} SELECT names: {e}", idx + 1))?;
    rows.iter()
        .map(|row| {
            row.try_get::<_, String>(0)
                .map_err(|e| format!("node{} decode name: {e}", idx + 1))
        })
        .collect()
}

/// Insert one uniquely-named row plus a durability barrier (Go:
/// `Cluster.TypedInsert` + flush).
async fn insert(cluster: &Cluster, node: usize, id: i64, name: &str) -> Result<(), String> {
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

/// Whether one node currently holds `name` (Go: `hasName`).
async fn has_name(cluster: &Cluster, idx: usize, name: &str) -> Result<bool, String> {
    Ok(names(cluster, idx).await?.iter().any(|got| got == name))
}

/// Isolate (true) or rejoin (false) node index 2 (Go: `setPartition`).
async fn set_partition(cluster: &Cluster, isolate: bool) -> Result<(), String> {
    for peer in [0, 1] {
        if isolate {
            cluster
                .remove_peer(2, peer)
                .await
                .map_err(|e| format!("partition link 2<->{} (isolate=true): {e}", peer + 1))?;
            cluster
                .remove_peer(peer, 2)
                .await
                .map_err(|e| format!("partition link {}<->2 (isolate=true): {e}", peer + 1))?;
        } else {
            cluster
                .add_peer(2, peer)
                .await
                .map_err(|e| format!("partition link 2<->{} (heal): {e}", peer + 1))?;
            cluster
                .add_peer(peer, 2)
                .await
                .map_err(|e| format!("partition link {}<->2 (heal): {e}", peer + 1))?;
        }
    }
    Ok(())
}

/// Write a marker row on node 0 while node 2 is split off and prove the
/// split is real: node 1 must observe it, node 2 must never observe it
/// during the whole window (Go: `proveIsolated`). The marker counts in
/// `written` so the final want stays exact.
async fn prove_isolated(cluster: &Cluster, written: &AtomicI64) -> Result<(), String> {
    insert(cluster, 0, ISOLATION_MARKER_ID, ISOLATION_MARKER)
        .await
        .map_err(|e| format!("isolation marker write: {e}"))?;
    written.fetch_add(1, Ordering::SeqCst);
    // Node 1 must see the marker: proves cross-node traffic flows on the
    // intact side while we watch node 2 for leaks.
    let deadline = tokio::time::Instant::now() + ISOLATION_TIMEOUT;
    loop {
        if has_name(cluster, 1, ISOLATION_MARKER).await? {
            break;
        }
        if has_name(cluster, 2, ISOLATION_MARKER).await? {
            return Err("isolation violated: node3 observed the marker while split (remove_peer ineffective?)".into());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!("node2 never observed the isolation marker within {ISOLATION_TIMEOUT:?}"));
        }
        tokio::time::sleep(ISOLATION_POLL).await;
    }
    // Node 1 has it; give a leak one more second to show on node 2.
    tokio::time::sleep(LEAK_WINDOW).await;
    if has_name(cluster, 2, ISOLATION_MARKER).await? {
        return Err("isolation violated: node3 observed the marker while split (remove_peer ineffective?)".into());
    }
    eprintln!("isolation proven: marker reached node2, never node3");
    Ok(())
}

/// Wait until every node holds exactly `want` rows with equal ordered
/// contents (Go: `waitConverged`).
async fn wait_converged(cluster: &Cluster, want: usize, timeout: Duration) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut last_log = tokio::time::Instant::now();
    loop {
        let mut ok = true;
        let mut first: Vec<String> = Vec::new();
        for i in 0..cluster.len() {
            let mut node_names = names(cluster, i).await?;
            if node_names.len() != want {
                ok = false;
                break;
            }
            node_names.sort();
            if i == 0 {
                first = node_names;
            } else if node_names != first {
                ok = false;
                break;
            }
        }
        if ok {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        if last_log.elapsed() > PROGRESS_LOG_INTERVAL {
            last_log = tokio::time::Instant::now();
            let mut counts = Vec::new();
            for i in 0..cluster.len() {
                counts.push(names(cluster, i).await?.len());
            }
            eprintln!("converge progress: counts={counts:?} want={want}");
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
    for i in 0..cluster.len() {
        let count = names(cluster, i).await?.len();
        eprintln!("node{} at timeout: count={count}", i + 1);
    }
    Err(format!("nodes did not converge on {want} rows with equal contents within {timeout:?}"))
}

/// Positive-integer env knob with fallback (Go: `envInt`).
fn env_int(name: &str, fallback: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(fallback)
}

/// One continuous writer: inserts on `node` every [`WRITE_INTERVAL`] until
/// `stop` is set. Runs as a borrowed future inside `tokio::join!` (no
/// `'static` spawn), so it always observes the stop flag.
async fn writer(
    cluster: &Cluster,
    node: usize,
    written: &AtomicI64,
    failed: &AtomicI64,
    stop: &AtomicBool,
) {
    let mut seq: i64 = 0;
    while !stop.load(Ordering::SeqCst) {
        tokio::time::sleep(WRITE_INTERVAL).await;
        if stop.load(Ordering::SeqCst) {
            break;
        }
        seq += 1;
        let name = format!("n{node}-s{seq}");
        match insert(cluster, node, node as i64 * ID_STRIDE + seq, &name).await {
            Ok(()) => {
                written.fetch_add(1, Ordering::SeqCst);
            }
            Err(e) => {
                eprintln!("writer node{} failed: {e}", node + 1);
                failed.fetch_add(1, Ordering::SeqCst);
            }
        }
    }
}

async fn run_body(
    cluster: &Cluster,
    cycles: usize,
    split: Duration,
    heal_gap: Duration,
) -> Result<(), String> {
    for i in 0..cluster.len() {
        let info = cluster.node_info(i);
        eprintln!("{} dir={} sql={} repl={}", info.label, info.dir.display(), info.sql_addr, info.repl_addr);
    }
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

    // Baseline converges before flapping (honest-path control).
    for i in 0..BASELINE_ROWS {
        insert(cluster, 0, BASELINE_ID_BASE + i, &format!("base-{i}"))
            .await
            .map_err(|e| format!("baseline write: {e}"))?;
    }
    wait_converged(cluster, BASELINE_ROWS as usize, CONVERGE_TIMEOUT).await?;

    // Continuous writers on every node; node3 stays live (running, only
    // unreachable), so its local commits must succeed too. The driver sets
    // `stop` on every exit path so a failed phase cannot hang the join.
    let written = AtomicI64::new(0);
    let failed = AtomicI64::new(0);
    let stop = AtomicBool::new(false);
    let driver = async {
        let result = async {
            for c in 0..cycles {
                set_partition(cluster, true).await?;
                eprintln!("cycle {}/{}: node3 isolated for {split:?}", c + 1, cycles);
                if c == 0 {
                    // Isolation proof (F1): while split, a marker written on
                    // node0 must reach node1 but never node2. Without this,
                    // a no-op remove_peer would pass vacuously. The marker is
                    // counted in written so the final want stays exact.
                    prove_isolated(cluster, &written).await?;
                }
                tokio::time::sleep(split).await;
                set_partition(cluster, false).await?;
                eprintln!("cycle {}/{}: healed, settling {heal_gap:?}", c + 1, cycles);
                tokio::time::sleep(heal_gap).await;
            }
            Ok::<(), String>(())
        }
        .await;
        stop.store(true, Ordering::SeqCst);
        result
    };
    let (_, _, _, driver_result) = tokio::join!(
        writer(cluster, 0, &written, &failed, &stop),
        writer(cluster, 1, &written, &failed, &stop),
        writer(cluster, 2, &written, &failed, &stop),
        driver,
    );
    driver_result?;

    // Final heal (idempotent) then full convergence on everything written.
    set_partition(cluster, false).await?;
    cluster.sync_all().await.map_err(|e| format!("final sync: {e}"))?;
    let want = (BASELINE_ROWS + written.load(Ordering::SeqCst)) as usize;
    eprintln!(
        "flapping done: {cycles} cycles, {want} rows written, {} failed",
        failed.load(Ordering::SeqCst)
    );
    if failed.load(Ordering::SeqCst) != 0 {
        return Err(format!(
            "partition failed {} writes to live nodes, want 0",
            failed.load(Ordering::SeqCst)
        ));
    }
    wait_converged(cluster, want, CONVERGE_TIMEOUT).await?;
    eprintln!("post-flap convergence: {want} rows, identical digests");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flapping_partition_loses_no_writes() {
    let cycles = env_int("MURMUR_FLAP_CYCLES", 5) as usize;
    let split = env_seconds("MURMUR_FLAP_SPLIT_SECONDS", 3);
    let heal_gap = env_seconds("MURMUR_FLAP_HEAL_SECONDS", 2);

    let cluster = Cluster::new(ClusterOptions {
        name: "flap-partition".into(),
        num_nodes: 3,
        ..Default::default()
    })
    .await
    .expect("cluster setup");
    assert_eq!(cluster.len(), 3, "this scenario is written for 3 nodes");
    let result = run_body(&cluster, cycles, split, heal_gap).await;
    let failed = result.is_err();
    if let Err(e) = &result {
        eprintln!("scenario failed: {e}");
    }
    if failed {
        cluster.dump_forensics("flap-partition-no-convergence").await;
    }
    cluster.finish(failed).await;
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}
