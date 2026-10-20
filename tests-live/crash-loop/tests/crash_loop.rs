//! Rust port of the Go `crash-loop` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/crash-loop/`
//! (`crash_loop_test.go`). One of three meshed `murmur-server` processes is
//! SIGKILLed and restarted N times under continuous writes from every node,
//! with short uptimes between kills. Every write acknowledged before each
//! kill must survive; the looped node must converge to identical contents;
//! it must reach ready within a bound after the final restart (no wedge);
//! and a post-loop write on it must replicate everywhere.
//!
//! Kill placement is proven, not assumed: every non-final round requires a
//! write provably in flight on the victim (mid-write), and the final round
//! kills immediately after a 30-row burst on a survivor, which the remote
//! apply path cannot have flushed yet (mid-replication). Plain SQL replaces
//! the Go typed-records HTTP API (see the scenario README.md).

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use murmur_live_harness::tokio_postgres::types::ToSql;
use murmur_live_harness::{Cluster, ClusterOptions};

/// Crash victim (Go: `victim = 2`, node3).
const VICTIM: usize = 2;
/// Writer insert cadence (Go: 25 ms ticker).
const WRITE_INTERVAL: Duration = Duration::from_millis(25);
/// Convergence poll interval (Go: 100 ms).
const POLL: Duration = Duration::from_millis(100);
/// Per-insert wire timeout so a kill can never wedge a writer on the lock.
const INSERT_TIMEOUT: Duration = Duration::from_secs(10);
/// Positive-control deadlines (Go: 30 s).
const CONTROL_TIMEOUT: Duration = Duration::from_secs(30);
/// In-flight gate for non-final kills (Go: 10 s).
const INFLIGHT_TIMEOUT: Duration = Duration::from_secs(10);
/// Final-round burst size on a survivor (Go: 30).
const BURST_ROWS: i64 = 30;
/// Final-round bound between burst and kill (Go: 500 ms).
const BURST_WINDOW: Duration = Duration::from_millis(500);
/// No-wedge bound for the final restart (Go: 60 s).
const READY_BOUND: Duration = Duration::from_secs(60);
/// Post-loop mesh/convergence deadlines (Go: 90 s).
const HEAL_TIMEOUT: Duration = Duration::from_secs(90);
/// Post-loop write convergence deadline (Go: 60 s).
const POST_TIMEOUT: Duration = Duration::from_secs(60);
/// Row-id stride per writer node: ids are `node * ID_STRIDE + seq`, so the
/// three continuous writers never collide (Go: server-generated typed IDs).
const ID_STRIDE: i64 = 10_000_000;
/// Baseline ids live above every writer range.
const BASELINE_ID_BASE: i64 = 90_000_001;
/// Burst ids live above the baseline range.
const BURST_ID_BASE: i64 = 90_001_001;
/// Post-loop marker id.
const POST_LOOP_ID: i64 = 90_002_001;

/// Positive-integer env knob with fallback (Go: `envInt`).
fn env_int(name: &str, fallback: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(fallback)
}

/// Shared writer state. Writers and the driver share the cluster behind one
/// mutex; the `running` flags keep writers off a stopped node (whose
/// `Cluster::node` would panic) without racing the driver's kill path: the
/// driver clears the victim flag well before locking for the kill, and
/// writers re-check the flag while holding the lock.
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

/// Insert one row plus a durability barrier, recording the acknowledgement.
/// Returns the acknowledgement state: only error-free inserts count (Go
/// drops refused requests during kill/restart windows and continues with
/// the next unique insert).
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
        let flush = cluster.node(node).simple("MURMUR FLUSH");
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

/// One continuous writer (Go: per-node 25 ms ticker goroutine).
async fn writer(shared: Arc<Shared>, node: usize) {
    while !shared.stop.load(Ordering::SeqCst) {
        tokio::time::sleep(WRITE_INTERVAL).await;
        if shared.stop.load(Ordering::SeqCst) {
            break;
        }
        if !shared.running[node].load(Ordering::SeqCst) {
            continue;
        }
        let seq = shared.seqs[node].fetch_add(1, Ordering::SeqCst) + 1;
        let name = format!("n{}-{seq:08}", node + 1);
        // Refusals during kill/restart windows are dropped (Go parity); the
        // next unique insert continues the workload.
        let _ = insert(&shared, node, node as i64 * ID_STRIDE + seq, &name).await;
    }
}

/// Sorted names on one node (Go: `TypedNames` + digest).
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

/// Wait until every node holds equal counts with equal sorted-name digests
/// (Go: `waitForConvergence`).
async fn wait_for_convergence(shared: &Shared, timeout: Duration) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    loop {
        let mut want: Option<Vec<String>> = None;
        let mut ok = true;
        let len = shared.cluster.lock().await.len();
        for i in 0..len {
            let node_names = names(shared, i).await?;
            match &want {
                None => want = Some(node_names),
                Some(first) if *first == node_names => {}
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
                eprintln!("node{} at timeout: count={}", i + 1, node_names.len());
            }
            return Err(format!(
                "nodes did not converge (counts + name digests) within {timeout:?}"
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

async fn run_body(shared: Arc<Shared>, rounds: usize, uptime: Duration) -> Result<(), String> {
    {
        let cluster = shared.cluster.lock().await;
        for i in 0..cluster.len() {
            let info = cluster.node_info(i);
            eprintln!(
                "{} pid={:?} dir={} sql={} repl={}",
                info.label,
                cluster.node_pid(i),
                info.dir.display(),
                info.sql_addr,
                info.repl_addr
            );
        }
    }
    // Same schema on every node, then replicate it (Go: TypedRecords
    // fixture tables; row convergence needs the per-table flag on each).
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
            .simple("MURMUR FLUSH")
            .await
            .map_err(|e| format!("node{} flush after SET TABLE: {e}", i + 1))?;
    }

    // Positive control: honest 3-node convergence before any crash.
    for node in 0..3 {
        insert(&shared, node, BASELINE_ID_BASE + node as i64, &format!("base-node{}", node + 1))
            .await?;
    }
    wait_for_peers(&shared, &[2, 2, 2], CONTROL_TIMEOUT).await?;
    wait_for_convergence(&shared, CONTROL_TIMEOUT).await?;
    eprintln!("positive control: 3-node mesh converged");

    let writers: Vec<tokio::task::JoinHandle<()>> = (0..3)
        .map(|node| {
            let shared = Arc::clone(&shared);
            tokio::spawn(async move { writer(shared, node).await })
        })
        .collect();

    let mut ready_durations = Vec::new();
    let driver = async {
        for round in 1..=rounds {
            tokio::time::sleep(uptime).await;
            if round == rounds {
                // Burst on a survivor, then kill at once: prefer a moment
                // with a request in flight (mid-write), but bound the wait
                // so the burst is still unflushed (mid-replication).
                for i in 0..BURST_ROWS {
                    insert(&shared, 0, BURST_ID_BASE + i, &format!("burst-{i}")).await?;
                }
                let deadline = Instant::now() + BURST_WINDOW;
                while shared.in_flight[VICTIM].load(Ordering::SeqCst) == 0
                    && Instant::now() < deadline
                {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            } else {
                // Every non-final kill requires a request in flight:
                // mid-write proven.
                let deadline = Instant::now() + INFLIGHT_TIMEOUT;
                while shared.in_flight[VICTIM].load(Ordering::SeqCst) == 0 {
                    if Instant::now() >= deadline {
                        return Err(format!(
                            "node{} had no write request in flight before SIGKILL",
                            VICTIM + 1
                        ));
                    }
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
            eprintln!(
                "round {round}/{rounds}: SIGKILL node{} with {} request(s) in flight",
                VICTIM + 1,
                shared.in_flight[VICTIM].load(Ordering::SeqCst)
            );
            // Writers observe the flag (and the re-check under the lock)
            // before the kill lands, so no writer can touch the victim
            // while it is stopped.
            shared.running[VICTIM].store(false, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(150)).await;
            let start = Instant::now();
            {
                let mut cluster = shared.cluster.lock().await;
                cluster.kill_node(VICTIM).await;
                cluster
                    .start_node(VICTIM)
                    .await
                    .map_err(|e| format!("round {round} restart node{}: {e}", VICTIM + 1))?;
                cluster
                    .node(VICTIM)
                    .simple("MURMUR SET TABLE records REPLICATED true")
                    .await
                    .map_err(|e| format!("round {round} re-set replicated: {e}"))?;
                cluster
                    .node(VICTIM)
                    .simple("MURMUR FLUSH")
                    .await
                    .map_err(|e| format!("round {round} flush node{}: {e}", VICTIM + 1))?;
                cluster
                    .mesh()
                    .await
                    .map_err(|e| format!("round {round} re-mesh: {e}"))?;
                let _ = cluster.sync_all().await;
            }
            shared.running[VICTIM].store(true, Ordering::SeqCst);
            ready_durations.push(start.elapsed());
            eprintln!(
                "round {round}/{rounds}: node{} ready in {:?}",
                VICTIM + 1,
                ready_durations.last().unwrap()
            );
        }
        Ok::<(), String>(())
    }
    .await;
    shared.stop.store(true, Ordering::SeqCst);
    for w in writers {
        let _ = w.await;
    }
    driver?;

    // No wedge: the final restart reaches ready within the bound, the mesh
    // reforms, and every acked write is present everywhere.
    if let Some(last) = ready_durations.last() {
        if *last > READY_BOUND {
            return Err(format!("final restart took {last:?} to reach ready, want < {READY_BOUND:?} (wedge)"));
        }
    }
    wait_for_peers(&shared, &[2, 2, 2], HEAL_TIMEOUT).await?;
    eprintln!("mesh reformed after the crash loop");
    let _ = shared.cluster.lock().await.sync_all().await;
    wait_for_convergence(&shared, HEAL_TIMEOUT).await?;
    assert_acknowledged_present(&shared).await?;
    eprintln!(
        "all {} acknowledged writes survived {rounds} SIGKILLs",
        shared.acknowledged.lock().unwrap().len()
    );

    insert(&shared, VICTIM, POST_LOOP_ID, "post-loop-from-victim").await?;
    wait_for_convergence(&shared, POST_TIMEOUT).await?;
    assert_acknowledged_present(&shared).await?;
    eprintln!("post-loop write on the looped node replicated everywhere");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn crash_loop_node_converges_without_loss() {
    let rounds = env_int("MURMUR_CRASH_LOOP_ROUNDS", 5) as usize;
    let uptime = Duration::from_millis(env_int("MURMUR_CRASH_LOOP_UPTIME_MS", 1000));

    let cluster = Cluster::new(ClusterOptions {
        name: "crash-loop".into(),
        num_nodes: 3,
        ..Default::default()
    })
    .await
    .expect("cluster setup");
    assert_eq!(cluster.len(), 3, "this scenario is written for 3 nodes");
    let shared = Arc::new(Shared::new(cluster));
    let result = run_body(Arc::clone(&shared), rounds, uptime).await;
    let failed = result.is_err();
    if let Err(e) = &result {
        eprintln!("scenario failed: {e}");
    }
    // Reclaim the cluster for forensics + finish (all writers joined).
    match Arc::try_unwrap(shared) {
        Ok(shared) => {
            let cluster = shared.cluster.into_inner();
            if failed {
                cluster.dump_forensics("crash-loop-no-convergence").await;
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
