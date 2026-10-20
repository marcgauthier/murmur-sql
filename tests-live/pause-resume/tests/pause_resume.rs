//! Rust port of the Go `pause-resume` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/pause-resume/`
//! (`pause_resume_test.go`). Three `murmur-server` processes mesh while all
//! take writes; node 2 is frozen with SIGSTOP for 15-30 s while the
//! survivors keep writing, then resumes with zero data loss and converging
//! digests. The freeze must be *detected* (member-view dip, stale peer, or
//! dropped session), the frozen member must not be evicted, and the catch-up
//! path must be proven (log-path staging, not a snapshot).
//!
//! Env-gated like Go's platform skip: SKIPs on non-unix platforms, where
//! process suspend needs signals that do not exist.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use murmur_live_harness::{wait_until_async, Cluster, ClusterOptions};

/// Table DDL (Go: typed records; only names matter here).
const TABLE_DDL: &str = "CREATE TABLE live_typed_records(name TEXT NOT NULL PRIMARY KEY)";
/// Frozen node (Go: node2).
const FROZEN: usize = 1;
/// Freeze-window knob (Go: `MURMUR_PAUSE_RESUME_STOP_SECONDS`, default 20,
/// 15 s floor).
const STOP_ENV: &str = "MURMUR_PAUSE_RESUME_STOP_SECONDS";
const DEFAULT_STOP_SECS: u64 = 20;
const MIN_STOP_SECS: u64 = 15;
/// Baseline rows (Go: 5).
const BASELINE_ROWS: usize = 5;
/// Writer cadence (Go: 25 ms ticker on every node).
const WRITE_TICK: Duration = Duration::from_millis(25);
/// In-flight drain before freezing (Go: 300 ms).
const DRAIN: Duration = Duration::from_millis(300);
/// Pre-roll before the freeze (Go: 2 s).
const PRE_ROLL: Duration = Duration::from_secs(2);
/// Mesh waits (Go: 60 s peers + membership, 30 s baseline converge).
const MESH_TIMEOUT: Duration = Duration::from_secs(60);
const BASELINE_TIMEOUT: Duration = Duration::from_secs(30);
/// Freeze-detection sample cadence (Go: 1 s).
const SAMPLE_TICK: Duration = Duration::from_secs(1);
/// Post-resume convergence bound (Go: 180 s).
const CONVERGE_TIMEOUT: Duration = Duration::from_secs(180);
/// Re-admission waits (Go: 90 s sessions + membership).
const READMIT_TIMEOUT: Duration = Duration::from_secs(90);
/// Post-resume convergence (Go: 60 s).
const POST_TIMEOUT: Duration = Duration::from_secs(60);
/// Convergence poll interval (Go: 200 ms).
const POLL: Duration = Duration::from_millis(200);
/// Membership/session poll interval (Go: 250 ms).
const MEMBER_POLL: Duration = Duration::from_millis(250);
/// A peer unseen this long counts as stale (freeze-detection proof).
const STALE_MILLIS: u64 = 5_000;

/// Canonical ordered names on one node (Go: `Cluster.TypedNames`).
async fn names(cluster: &Cluster, idx: usize) -> Result<Vec<String>, String> {
    let rows = cluster
        .node(idx)
        .query("SELECT name FROM live_typed_records ORDER BY name", &[])
        .await
        .map_err(|e| format!("node{} read names: {e}", idx + 1))?;
    rows.iter()
        .map(|row| {
            row.try_get::<_, String>(0)
                .map_err(|e| format!("node{} decode name: {e}", idx + 1))
        })
        .collect()
}

/// Best-effort insert; true when acknowledged.
async fn try_insert(cluster: &Cluster, idx: usize, name: String) -> bool {
    cluster
        .node(idx)
        .execute("INSERT INTO live_typed_records(name) VALUES($1)", &[&name])
        .await
        .is_ok()
}

/// One `MURMUR SHOW REPLICATION` row: (peers, members, staged, applied).
async fn replication_summary(cluster: &Cluster, idx: usize) -> Result<(u64, u64, u64, u64), String> {
    let rows = cluster
        .node(idx)
        .simple("MURMUR SHOW REPLICATION")
        .await
        .map_err(|e| format!("node{} SHOW REPLICATION: {e}", idx + 1))?;
    let row = rows
        .first()
        .ok_or_else(|| format!("node{} SHOW REPLICATION returned no rows", idx + 1))?;
    if row.len() < 6 {
        return Err(format!("node{} SHOW REPLICATION has {} columns, want >= 6", idx + 1, row.len()));
    }
    let num = |i: usize, what: &str| {
        row[i]
            .parse::<u64>()
            .map_err(|e| format!("node{} {what} {:?}: {e}", idx + 1, row[i]))
    };
    Ok((num(2, "peers")?, num(3, "members")?, num(4, "staged")?, num(5, "applied")?))
}

/// Live session count (Go: `connected_peers`).
async fn peer_count(cluster: &Cluster, node: usize) -> Result<u64, String> {
    Ok(replication_summary(cluster, node).await?.0)
}

/// Discovered-member count, excluding self (Go `membership_count` minus one).
async fn member_count(cluster: &Cluster, node: usize) -> Result<u64, String> {
    Ok(replication_summary(cluster, node).await?.1)
}

/// Cumulative log-applied (incoming) transactions on one node. Local
/// commits do not move it, and snapshot application bypasses it, so a
/// post-resume delta proves the log-path catch-up (the wire has no
/// gap-pull `Need` counter). (`staged_transactions` is a live gauge
/// that drains to zero on convergence, so it cannot prove anything
/// after the fact.)
async fn applied(cluster: &Cluster, node: usize) -> Result<u64, String> {
    Ok(replication_summary(cluster, node).await?.3)
}

/// How one survivor currently sees the frozen peer: (row_present,
/// connected, last_seen_millis_ago or u64::MAX when absent/unparseable).
async fn frozen_view(cluster: &Cluster, idx: usize, frozen_hex: &str) -> Result<(bool, bool, u64), String> {
    let rows = cluster
        .node(idx)
        .simple("MURMUR SHOW PEERS")
        .await
        .map_err(|e| format!("node{} SHOW PEERS: {e}", idx + 1))?;
    for row in &rows {
        if row.first().is_some_and(|id| id == frozen_hex) {
            let connected = row.get(2).is_some_and(|c| c == "true");
            let last_seen = row
                .get(3)
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(u64::MAX);
            return Ok((true, connected, last_seen));
        }
    }
    Ok((false, false, u64::MAX))
}

/// Wait until every node reports `want` live sessions (Go: `waitConnectedPeers`).
async fn wait_connected_peers(cluster: &Cluster, want: u64, timeout: Duration) -> Result<(), String> {
    let converged = wait_until_async(timeout, MEMBER_POLL, || async {
        for i in 0..cluster.len() {
            if peer_count(cluster, i).await.unwrap_or(u64::MAX) != want {
                return false;
            }
        }
        true
    })
    .await;
    if !converged {
        return Err(format!("nodes did not reach {want} connected peers within {timeout:?}"));
    }
    Ok(())
}

/// Wait until every node sees `want` discovered members (Go: `waitMembership`).
async fn wait_membership(cluster: &Cluster, want: u64, timeout: Duration) -> Result<(), String> {
    let converged = wait_until_async(timeout, MEMBER_POLL, || async {
        for i in 0..cluster.len() {
            if member_count(cluster, i).await.unwrap_or(u64::MAX) != want {
                return false;
            }
        }
        true
    })
    .await;
    if !converged {
        return Err(format!("nodes did not reach membership {want} (excl. self) within {timeout:?}"));
    }
    Ok(())
}

/// Poll exact counts plus ordered-name equality (Go: `waitConverged`).
async fn wait_converged(cluster: &Cluster, want: usize, timeout: Duration) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let mut first: Option<Vec<String>> = None;
        let mut ok = true;
        for i in 0..cluster.len() {
            match names(cluster, i).await {
                Ok(got) if got.len() == want => match &first {
                    None => first = Some(got),
                    Some(prev) if *prev == got => {}
                    _ => {
                        ok = false;
                        break;
                    }
                },
                _ => {
                    ok = false;
                    break;
                }
            }
        }
        if ok {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            for i in 0..cluster.len() {
                let n = names(cluster, i).await.map(|v| v.len()).unwrap_or(usize::MAX);
                eprintln!("node{} at timeout: count={n}", i + 1);
            }
            return Err(format!(
                "nodes did not converge on {want} records with equal contents within {timeout:?}"
            ));
        }
        tokio::time::sleep(POLL).await;
    }
}

/// Require every acknowledged value present on every node
/// (Go: `assertAcknowledgedPresent`).
async fn assert_acknowledged_present(
    cluster: &Cluster,
    acknowledged: &HashSet<String>,
) -> Result<(), String> {
    for i in 0..cluster.len() {
        let present: HashSet<_> = names(cluster, i).await?.into_iter().collect();
        for value in acknowledged {
            if !present.contains(value) {
                return Err(format!("node{} lost acknowledged write {value:?} after resume", i + 1));
            }
        }
    }
    Ok(())
}

/// Prove the frozen member was not permanently evicted: every node holds
/// a live session to both peers and the member view is full again
/// (Go: `assertNoEviction`, via peer diagnostics + member counts).
async fn assert_no_eviction(cluster: &Cluster) -> Result<(), String> {
    for i in 0..cluster.len() {
        let rows = cluster
            .node(i)
            .simple("MURMUR SHOW PEERS")
            .await
            .map_err(|e| format!("node{} SHOW PEERS: {e}", i + 1))?;
        if rows.len() != 2 {
            return Err(format!(
                "node{} sees {} peers, want 2 (a peer was evicted)",
                i + 1,
                rows.len()
            ));
        }
        for row in &rows {
            if row.get(2).is_some_and(|c| c != "true") {
                return Err(format!("node{} peer {:?} not connected after resume (evicted?)", i + 1, row.first()));
            }
        }
        let members = member_count(cluster, i).await?;
        if members != 2 {
            return Err(format!(
                "node{} members = {members}, want 2 (frozen member never re-admitted)",
                i + 1
            ));
        }
    }
    Ok(())
}

/// One continuous writer. Runs as a borrowed future inside `tokio::join!`
/// (no `'static` spawn), so it always observes the stop flag. The frozen
/// node's writer pauses while it is suspended (a frozen node accepts
/// nothing by definition).
async fn writer(
    cluster: &Cluster,
    node: usize,
    acknowledged: &Arc<Mutex<HashSet<String>>>,
    paused: &AtomicBool,
    seq: &AtomicI64,
    stop: &AtomicBool,
) {
    loop {
        tokio::time::sleep(WRITE_TICK).await;
        if stop.load(Ordering::Relaxed) {
            break;
        }
        if paused.load(Ordering::Relaxed) {
            continue;
        }
        let n = seq.fetch_add(1, Ordering::Relaxed) + 1;
        let value = format!("flow-n{}-{n:08}", node + 1);
        if try_insert(cluster, node, value.clone()).await {
            acknowledged.lock().expect("acked lock").insert(value);
        }
    }
}

async fn run_body(cluster: &Cluster, stop_secs: u64) -> Result<(), String> {
    // Same schema on every node (Go: typed fixture tables), flagged
    // replicated on each node (row convergence needs the per-node flag).
    for i in 0..cluster.len() {
        cluster
            .ensure_migrated(i, TABLE_DDL)
            .await
            .map_err(|e| format!("migrate node{}: {e}", i + 1))?;
    }
    for i in 0..cluster.len() {
        cluster
            .node(i)
            .simple("MURMUR SET TABLE live_typed_records REPLICATED true")
            .await
            .map_err(|e| format!("node{} SET TABLE REPLICATED: {e}", i + 1))?;
        cluster
            .node(i)
            .simple("MURMUR FLUSH")
            .await
            .map_err(|e| format!("node{} flush after SET TABLE: {e}", i + 1))?;
    }

    // Positive control baseline: full mesh (sessions + membership).
    wait_connected_peers(cluster, 2, MESH_TIMEOUT).await?;
    wait_membership(cluster, 2, MESH_TIMEOUT).await?;
    for i in 0..BASELINE_ROWS {
        let name = format!("base-{i}");
        if !try_insert(cluster, 0, name).await {
            return Err("baseline write failed".into());
        }
        cluster
            .node(0)
            .simple("MURMUR FLUSH")
            .await
            .map_err(|e| format!("flush baseline: {e}"))?;
    }
    wait_converged(cluster, BASELINE_ROWS, BASELINE_TIMEOUT).await?;

    let acknowledged = Arc::new(Mutex::new(HashSet::from([
        "base-0".to_owned(),
        "base-1".to_owned(),
        "base-2".to_owned(),
        "base-3".to_owned(),
        "base-4".to_owned(),
    ])));
    let applied_before = applied(cluster, FROZEN).await?;
    let frozen_hex = murmur_live_harness::hex(&cluster.node_id(FROZEN));

    let paused = [AtomicBool::new(false), AtomicBool::new(false), AtomicBool::new(false)];
    let stop = AtomicBool::new(false);
    let seqs = [AtomicI64::new(0), AtomicI64::new(0), AtomicI64::new(0)];

    // The driver sets `stop` on every exit path so a failed phase cannot
    // hang the join.
    let driver = async {
        let result = async {
            tokio::time::sleep(PRE_ROLL).await;

            // Freeze node 2 mid-replication: the process stops cold.
            paused[FROZEN].store(true, Ordering::Relaxed);
            tokio::time::sleep(DRAIN).await; // drain in-flight requests first
            let acked_at_stop = acknowledged.lock().expect("acked lock").len();
            eprintln!("freezing node2 with {acked_at_stop} rows acknowledged so far");
            cluster
                .suspend_node(FROZEN)
                .await
                .map_err(|e| format!("suspend node2: {e}"))?;

            // While frozen: survivors keep writing; sample the alive view
            // from the outside. Healthy reads 2 (excludes self); detection
            // of the frozen member may dip survivors to 1, stale the peer,
            // or drop its session.
            let mut min_alive = 2_u64;
            let mut saw_stale = false;
            let mut saw_disconnected = false;
            let sample_end = tokio::time::Instant::now() + Duration::from_secs(stop_secs);
            while tokio::time::Instant::now() < sample_end {
                for idx in [0, 2] {
                    if let Ok(m) = member_count(cluster, idx).await {
                        min_alive = min_alive.min(m);
                    }
                    if let Ok((present, connected, last_seen)) =
                        frozen_view(cluster, idx, &frozen_hex).await
                    {
                        if last_seen > STALE_MILLIS {
                            saw_stale = true;
                        }
                        if !present || !connected {
                            saw_disconnected = true;
                        }
                    }
                }
                tokio::time::sleep(SAMPLE_TICK).await;
            }
            let acked_during_pause =
                acknowledged.lock().expect("acked lock").len() - acked_at_stop;
            eprintln!(
                "during {stop_secs}s freeze: minAlivePeers={min_alive} stale={saw_stale} disconnected={saw_disconnected} ackedDuringPause={acked_during_pause}"
            );
            // Freeze detection proof: survivors must notice the frozen
            // member. Without this, broken probing would pass silently.
            // (The wire has no probe-failure counter; staleness and
            // session state take its place.)
            if !(min_alive < 2 || saw_stale || saw_disconnected) {
                return Err::<(), String>(format!(
                    "freeze undetected: survivors kept alive={min_alive} with a fresh connected peer view during {stop_secs}s (probing ineffective?)"
                ));
            }
            if acked_during_pause == 0 {
                return Err("no writes acknowledged while node2 was frozen; pause proved nothing".into());
            }

            // Resume and prove streams come back.
            cluster
                .resume_node(FROZEN)
                .await
                .map_err(|e| format!("resume node2: {e}"))?;
            // Health: the node answers reads immediately.
            let resume_start = tokio::time::Instant::now();
            let _ = names(cluster, FROZEN).await?;
            eprintln!("node2 healthy {:?} after resume", resume_start.elapsed());
            // Sessions re-establish over the reopened path (explicit
            // re-mesh: the freeze killed the QUIC sessions and there is
            // no auto-redial).
            cluster.mesh().await.map_err(|e| format!("re-mesh after resume: {e}"))?;
            cluster.sync_all().await.map_err(|e| format!("sync after resume: {e}"))?;
            paused[FROZEN].store(false, Ordering::Relaxed);
            Ok::<(), String>(())
        }
        .await;
        stop.store(true, Ordering::Relaxed);
        result
    };
    let (_, _, _, driver_result) = tokio::join!(
        writer(cluster, 0, &acknowledged, &paused[0], &seqs[0], &stop),
        writer(cluster, 1, &acknowledged, &paused[1], &seqs[1], &stop),
        writer(cluster, 2, &acknowledged, &paused[2], &seqs[2], &stop),
        driver,
    );
    driver_result?;

    let total = acknowledged.lock().expect("acked lock").len();
    // 180 s bound (Go: post-resume range-repair normally lands fast,
    // but cold page cache once needed past 120 s).
    wait_converged(cluster, total, CONVERGE_TIMEOUT).await?;
    assert_acknowledged_present(cluster, &acknowledged.lock().expect("acked lock").clone()).await?;

    // Sessions resumed on every link; membership re-admitted the member.
    wait_connected_peers(cluster, 2, READMIT_TIMEOUT).await?;
    wait_membership(cluster, 2, READMIT_TIMEOUT).await?;
    assert_no_eviction(cluster).await?;

    // Record the rejoin path: with default retention the log still holds
    // everything, so catch-up must arrive over the log path (applied
    // advances; a snapshot here would mean ranges vanished early, and
    // snapshot application bypasses the applied counter while local
    // commits never move it).
    let applied_after = applied(cluster, FROZEN).await?;
    if applied_after <= applied_before {
        return Err(format!(
            "node2 rejoined with no log-path apply (applied {applied_before}->{applied_after}); catch-up path unproven"
        ));
    }
    eprintln!("node2 rejoin path: log apply (applied {applied_before}->{applied_after})");

    // Post-resume honest write still replicates everywhere.
    if !try_insert(cluster, FROZEN, "post-resume".into()).await {
        return Err("post-resume write failed".into());
    }
    cluster
        .node(FROZEN)
        .simple("MURMUR FLUSH")
        .await
        .map_err(|e| format!("flush post-resume: {e}"))?;
    acknowledged.lock().expect("acked lock").insert("post-resume".into());
    let total = acknowledged.lock().expect("acked lock").len();
    wait_converged(cluster, total, POST_TIMEOUT).await?;
    assert_acknowledged_present(cluster, &acknowledged.lock().expect("acked lock").clone()).await?;
    eprintln!("pause/resume proven: {stop_secs}s freeze, {total} rows intact, digests converge");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pause_resume_loses_no_writes() {
    if cfg!(not(unix)) {
        eprintln!("SKIP: pause-resume needs unix signals (SIGSTOP/SIGCONT) to freeze a node");
        return;
    }
    let mut stop_secs = std::env::var(STOP_ENV)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_STOP_SECS);
    if stop_secs < MIN_STOP_SECS {
        eprintln!("stop window {stop_secs}s below the 15s floor; using 15s");
        stop_secs = MIN_STOP_SECS;
    }

    let cluster = Cluster::new(ClusterOptions {
        name: "pause-resume".into(),
        num_nodes: 3,
        bootstrap_seeds: vec![0],
        ..Default::default()
    })
    .await
    .expect("cluster setup");
    assert_eq!(cluster.len(), 3, "this scenario is written for 3 nodes");
    for i in 0..cluster.len() {
        let info = cluster.node_info(i);
        eprintln!(
            "{} dir={} sql={} repl={}",
            info.label,
            info.dir.display(),
            info.sql_addr,
            info.repl_addr
        );
    }
    let result = run_body(&cluster, stop_secs).await;
    let failed = result.is_err();
    if let Err(e) = &result {
        eprintln!("scenario failed: {e}");
    }
    if failed {
        cluster.dump_forensics("pause-resume-no-convergence").await;
    }
    cluster.finish(failed).await;
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}
