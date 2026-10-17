//! Rust port of the Go `crash-recovery` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/crash-recovery/`
//! (`crash_recovery_test.go`). Three meshed `murmur-server` processes take
//! continuous writes (two workers per node) while node2 and then node3 are
//! SIGKILLed mid-write and restarted under load. Every acknowledged
//! application write must survive both restarts with identical digests on
//! all nodes, and a post-recovery write must replicate everywhere. Plain
//! SQL replaces the Go typed-records HTTP API (see the scenario README.md).

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use murmur_live_harness::tokio_postgres::types::ToSql;
use murmur_live_harness::{env_seconds, Cluster, ClusterOptions};

/// Writers per node (Go: 2 workers per node).
const WORKERS_PER_NODE: usize = 2;
/// Writer insert cadence (Go: 50 ms ticker).
const WRITE_INTERVAL: Duration = Duration::from_millis(50);
/// Convergence poll interval (Go: 50-100 ms).
const POLL: Duration = Duration::from_millis(100);
/// Per-insert wire timeout so a kill can never wedge a writer on the lock.
const INSERT_TIMEOUT: Duration = Duration::from_secs(10);
/// Baseline mesh/ack deadlines (Go: 20 s).
const BASELINE_TIMEOUT: Duration = Duration::from_secs(20);
/// In-flight gate before each SIGKILL (Go: 5 s).
const INFLIGHT_TIMEOUT: Duration = Duration::from_secs(5);
/// Rejoin deadlines under load (Go: 90 s).
const REJOIN_TIMEOUT: Duration = Duration::from_secs(90);
/// Row-id stride per (node, worker): ids are
/// `(node * WORKERS_PER_NODE + worker) * ID_STRIDE + seq`, so the six
/// writers never collide (Go: server-generated typed IDs).
const ID_STRIDE: i64 = 10_000_000;
/// Baseline ids live above every writer range.
const BASELINE_ID_BASE: i64 = 90_000_001;
/// Post-recovery marker id.
const POST_RECOVERY_ID: i64 = 90_001_001;

/// Shared writer state (same shape as the `crash-loop` port: one async
/// mutex around the cluster plus per-node `running` flags so writers never
/// touch a stopped node while the driver kills/restarts it).
struct Shared {
    cluster: tokio::sync::Mutex<Cluster>,
    acknowledged: Mutex<HashSet<String>>,
    in_flight: [AtomicI64; 3],
    seqs: [AtomicI64; 3],
    running: [AtomicBool; 3],
    stop: AtomicBool,
}

impl Shared {
    fn new(cluster: Cluster) -> Self {
        Self {
            cluster: tokio::sync::Mutex::new(cluster),
            acknowledged: Mutex::new(HashSet::new()),
            in_flight: [const { AtomicI64::new(0) }; 3],
            seqs: [const { AtomicI64::new(0) }; 3],
            running: [const { AtomicBool::new(true) }; 3],
            stop: AtomicBool::new(false),
        }
    }
}

/// Insert one row plus a durability barrier. Only error-free inserts join
/// the acknowledged set (Go drops refused requests during kill/restart
/// windows and continues with the next unique insert).
async fn insert(shared: &Shared, node: usize, id: i64, name: &str) -> Result<(), String> {
    shared.in_flight[node].fetch_add(1, Ordering::SeqCst);
    let outcome = async {
        let cluster = shared.cluster.lock().await;
        if !shared.running[node].load(Ordering::SeqCst) {
            return Err("node stopped".to_string());
        }
        let tagged = format!("{name}@{id}");
        let params = [&id as &(dyn ToSql + Sync), &tagged as _];
        let write = cluster
            .node(node)
            .execute("INSERT INTO records(id, name) VALUES($1, $2)", &params);
        tokio::time::timeout(INSERT_TIMEOUT, write)
            .await
            .map_err(|_| format!("node{} insert '{name}' timed out", node + 1))?
            .map_err(|e| format!("node{} insert '{name}': {e}", node + 1))?;
        let flush = cluster.node(node).flush();
        tokio::time::timeout(INSERT_TIMEOUT, flush)
            .await
            .map_err(|_| format!("node{} flush after '{name}' timed out", node + 1))?
            .map_err(|e| format!("node{} flush after '{name}': {e}", node + 1))?;
        Ok::<(), String>(())
    }
    .await;
    shared.in_flight[node].fetch_sub(1, Ordering::SeqCst);
    if outcome.is_ok() {
        shared
            .acknowledged
            .lock()
            .unwrap()
            .insert(format!("{name}@{id}"));
    }
    outcome
}

/// One application writer (Go: per-node 50 ms ticker goroutines, two per
/// node).
async fn writer(shared: Arc<Shared>, node: usize, worker: usize) {
    while !shared.stop.load(Ordering::SeqCst) {
        tokio::time::sleep(WRITE_INTERVAL).await;
        if shared.stop.load(Ordering::SeqCst) {
            break;
        }
        if !shared.running[node].load(Ordering::SeqCst) {
            continue;
        }
        let seq = shared.seqs[node].fetch_add(1, Ordering::SeqCst) + 1;
        let name = format!("node{}-worker{worker}-write-{seq:08}", node + 1);
        let id = (node * WORKERS_PER_NODE + worker) as i64 * ID_STRIDE + seq;
        if insert(&shared, node, id, &name).await.is_err() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
}

/// Sorted names on one node (Go: `TypedNames`).
async fn names(shared: &Shared, idx: usize) -> Result<Vec<String>, String> {
    let cluster = shared.cluster.lock().await;
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

/// Canonical table digest (Go: `canonicalTableDigest`, sha256 over sorted
/// names; FNV-1a/64 here: only cross-node equality within one process is
/// asserted).
async fn digest(shared: &Shared, idx: usize) -> Result<String, String> {
    let mut hash: u64 = 0xcbf29ce484222325;
    for name in names(shared, idx).await? {
        for b in name.bytes() {
            hash ^= b as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        hash ^= b'\n' as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    Ok(format!("{hash:016x}"))
}

/// Connected sessions per node, in node order (Go: `connectedPeers` via
/// `/v1/status`; here the `peers` column of `MURMUR SHOW REPLICATION`).
async fn peer_counts(shared: &Shared) -> Result<Vec<i64>, String> {
    let cluster = shared.cluster.lock().await;
    let mut out = Vec::with_capacity(cluster.len());
    for i in 0..cluster.len() {
        let rows = cluster
            .node(i)
            .simple("MURMUR SHOW REPLICATION")
            .await
            .map_err(|e| format!("node{} SHOW REPLICATION: {e}", i + 1))?;
        let peers = rows
            .first()
            .and_then(|row| row.get(2).cloned())
            .unwrap_or_default();
        out.push(
            peers
                .parse::<i64>()
                .map_err(|_| format!("node{} unparsable peers {peers:?}", i + 1))?,
        );
    }
    Ok(out)
}

/// Wait until every node reports its wanted session count (Go:
/// `waitForPeers`).
async fn wait_for_peers(shared: &Shared, want: &[i64], timeout: Duration) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    loop {
        if peer_counts(shared).await? == want {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "peers did not reach {want:?} within {timeout:?} (last: {:?})",
                peer_counts(shared).await?
            ));
        }
        tokio::time::sleep(POLL).await;
    }
}

/// Every acknowledged write must be present on every node (Go:
/// `assertAcknowledgedPresent`).
async fn assert_acknowledged_present(shared: &Shared) -> Result<(), String> {
    let acknowledged = shared.acknowledged.lock().unwrap().clone();
    let len = shared.cluster.lock().await.len();
    for i in 0..len {
        let present: HashSet<String> = names(shared, i).await?.into_iter().collect();
        for name in &acknowledged {
            if !present.contains(name) {
                return Err(format!("node{} lost acknowledged write {name:?}", i + 1));
            }
        }
    }
    Ok(())
}

/// All nodes hold equal digests (Go: `assertConverged`).
async fn assert_converged(shared: &Shared) -> Result<(), String> {
    let len = shared.cluster.lock().await.len();
    let mut first = String::new();
    for i in 0..len {
        let node_digest = digest(shared, i).await?;
        if i == 0 {
            first = node_digest;
        } else if node_digest != first {
            return Err(format!("node{} digest {node_digest} != {first}", i + 1));
        }
    }
    Ok(())
}

/// Wait until every node holds equal counts with equal digests (Go:
/// `waitForConvergence`).
async fn wait_for_convergence(shared: &Shared, timeout: Duration) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    loop {
        let len = shared.cluster.lock().await.len();
        let mut expected: Option<(usize, String)> = None;
        let mut ok = true;
        for i in 0..len {
            let node_names = names(shared, i).await?;
            let node_digest = digest(shared, i).await?;
            match &expected {
                None => expected = Some((node_names.len(), node_digest)),
                Some((count, digest)) if *count == node_names.len() && *digest == node_digest => {}
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
            let len = shared.cluster.lock().await.len();
            for i in 0..len {
                let node_names = names(shared, i).await?;
                let node_digest = digest(shared, i).await?;
                eprintln!(
                    "node{} after recovery rows={} digest={node_digest}",
                    i + 1,
                    node_names.len()
                );
            }
            return Err(format!("three nodes did not converge after recovery within {timeout:?}"));
        }
        tokio::time::sleep(POLL).await;
    }
}

/// SIGKILL one node with a write provably in flight (Go:
/// `killDuringWrites`).
async fn kill_during_writes(shared: &Shared, node: usize) -> Result<(), String> {
    let deadline = Instant::now() + INFLIGHT_TIMEOUT;
    while shared.in_flight[node].load(Ordering::SeqCst) == 0 {
        if Instant::now() >= deadline {
            return Err(format!("node{} had no write in flight before SIGKILL", node + 1));
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    eprintln!(
        "SIGKILL node{} with {} writes in flight",
        node + 1,
        shared.in_flight[node].load(Ordering::SeqCst)
    );
    shared.running[node].store(false, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(150)).await;
    shared.cluster.lock().await.kill_node(node).await;
    Ok(())
}

/// Restart one node and re-mesh it (Go: `restartNode` + redial).
async fn restart_node(shared: &Shared, node: usize) -> Result<(), String> {
    {
        let mut cluster = shared.cluster.lock().await;
        cluster
            .start_node(node)
            .await
            .map_err(|e| format!("restart node{}: {e}", node + 1))?;
        cluster
            .node(node)
            .simple("MURMUR SET TABLE records REPLICATED true")
            .await
            .map_err(|e| format!("re-set replicated node{}: {e}", node + 1))?;
        cluster
            .node(node)
            .flush()
            .await
            .map_err(|e| format!("flush node{}: {e}", node + 1))?;
        cluster
            .mesh()
            .await
            .map_err(|e| format!("re-mesh after node{} restart: {e}", node + 1))?;
        let _ = cluster.sync_all().await;
    }
    shared.running[node].store(true, Ordering::SeqCst);
    Ok(())
}

async fn run_body(shared: Arc<Shared>, write_burst: Duration, settle: Duration) -> Result<(), String> {
    {
        let cluster = shared.cluster.lock().await;
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
    }
    for i in 0..3 {
        let cluster = shared.cluster.lock().await;
        cluster
            .ensure_migrated(i, "CREATE TABLE records(id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
            .await
            .map_err(|e| format!("migrate node{}: {e}", i + 1))?;
    }
    for i in 0..3 {
        let cluster = shared.cluster.lock().await;
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

    for node in 0..3 {
        insert(&shared, node, BASELINE_ID_BASE + node as i64, &format!("baseline-node{}", node + 1))
            .await?;
    }
    wait_for_peers(&shared, &[2, 2, 2], BASELINE_TIMEOUT).await?;
    wait_for_convergence(&shared, BASELINE_TIMEOUT).await?;
    assert_acknowledged_present(&shared).await?;

    let mut writers = Vec::new();
    for node in 0..3 {
        for worker in 0..WORKERS_PER_NODE {
            let shared = Arc::clone(&shared);
            writers.push(tokio::spawn(async move { writer(shared, node, worker).await }));
        }
    }

    let driver = async {
        tokio::time::sleep(write_burst).await;
        kill_during_writes(&shared, 1).await?;
        tokio::time::sleep(write_burst).await;
        restart_node(&shared, 1).await?;
        wait_for_peers(&shared, &[2, 2, 2], REJOIN_TIMEOUT).await?;
        eprintln!("node2 rejoined under continuing application load");

        tokio::time::sleep(write_burst).await;
        kill_during_writes(&shared, 2).await?;
        tokio::time::sleep(write_burst).await;
        restart_node(&shared, 2).await?;
        wait_for_peers(&shared, &[2, 2, 2], REJOIN_TIMEOUT).await?;
        Ok::<(), String>(())
    }
    .await;
    shared.stop.store(true, Ordering::SeqCst);
    for w in writers {
        let _ = w.await;
    }
    driver?;

    wait_for_convergence(&shared, settle).await?;
    assert_acknowledged_present(&shared).await?;
    assert_converged(&shared).await?;
    eprintln!(
        "all acknowledged application writes survived two SIGKILL restarts ({} acknowledged rows)",
        shared.acknowledged.lock().unwrap().len()
    );

    insert(&shared, 0, POST_RECOVERY_ID, "post-recovery-write").await?;
    wait_for_convergence(&shared, settle).await?;
    assert_acknowledged_present(&shared).await?;
    assert_converged(&shared).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn abrupt_process_death_recovers_committed_rows() {
    let write_burst = env_seconds("MURMUR_CRASH_WRITE_SECONDS", 8);
    let settle = env_seconds("MURMUR_CRASH_SETTLE_SECONDS", 90);

    let cluster = Cluster::new(ClusterOptions {
        name: "crash-recovery".into(),
        num_nodes: 3,
        ..Default::default()
    })
    .await
    .expect("cluster setup");
    assert_eq!(cluster.len(), 3, "this scenario is written for 3 nodes");
    let shared = Arc::new(Shared::new(cluster));
    let result = run_body(Arc::clone(&shared), write_burst, settle).await;
    let failed = result.is_err();
    if let Err(e) = &result {
        eprintln!("scenario failed: {e}");
    }
    match Arc::try_unwrap(shared) {
        Ok(shared) => {
            let cluster = shared.cluster.into_inner();
            if failed {
                cluster.dump_forensics("crash-recovery-no-convergence").await;
            }
            cluster.finish(failed).await;
        }
        Err(_) => {
            eprintln!("scenario failed: writer tasks still hold the cluster");
            std::process::exit(1);
        }
    }
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}
