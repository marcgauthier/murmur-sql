//! Rust port of the Go `impaired-network` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/impaired-network/`
//! (`impaired_network_test.go`). A 3-node mesh of `murmur-server` processes
//! keeps converging while tc/netem impairs ONLY the replication ports:
//! (a) +150 ms latency, (b) 3% packet loss, (c) a 1 Mbit cap during a
//! stale-node snapshot resync under sustained writes. Membership must stay
//! stable (no false evictions) and per-row visibility latencies are recorded
//! with relaxed p95 bounds. Plain SQL replaces the Go typed-contention HTTP
//! API (see the scenario README.md for deviations).
//!
//! Env-gated like Go: SKIPs without tc/netem + CAP_NET_ADMIN on lo;
//! `MURMUR_IMPAIRED_NETWORK_FORCE=1` runs the same write/converge
//! workload with no impairment (logic verification only).

use std::process::Command;
use std::time::{Duration, Instant};

use murmur_live_harness::{wait_until_async, Cluster, ClusterOptions, ReplicationOptions};

/// Contention table DDL (Go: typed contention rows).
const TABLE_DDL: &str = "CREATE TABLE contention(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL)";
/// Force knob (Go: `MURMUR_IMPAIRED_NETWORK_FORCE=1`).
const FORCE_ENV: &str = "MURMUR_IMPAIRED_NETWORK_FORCE";
/// Row-count knob (Go: `MURMUR_IMPAIRED_NETWORK_ROWS`, default 8).
const ROWS_ENV: &str = "MURMUR_IMPAIRED_NETWORK_ROWS";
const DEFAULT_ROWS: usize = 8;
/// Baseline rows (Go: 10).
const BASELINE_ROWS: usize = 10;
/// Baseline/capped convergence deadline (Go: 150 s, 180 s capped).
const BASELINE_TIMEOUT: Duration = Duration::from_secs(150);
const CAPPED_TIMEOUT: Duration = Duration::from_secs(180);
/// Per-row visibility deadlines (Go: 30 s latency, 45 s loss).
const LAT_ROW_TIMEOUT: Duration = Duration::from_secs(30);
const LOSS_ROW_TIMEOUT: Duration = Duration::from_secs(45);
/// p95 visibility bounds (Go: 15 s latency, 30 s loss).
const LAT_P95_BOUND: Duration = Duration::from_secs(15);
const LOSS_P95_BOUND: Duration = Duration::from_secs(30);
/// Membership waits (Go: 60 s) and poll (Go: 250 ms).
const MEMBERSHIP_TIMEOUT: Duration = Duration::from_secs(60);
const MEMBERSHIP_POLL: Duration = Duration::from_millis(250);
/// Row-visibility poll interval (Go: 50 ms).
const VISIBILITY_POLL: Duration = Duration::from_millis(50);
/// Convergence poll interval (Go: 200 ms).
const POLL: Duration = Duration::from_millis(200);
/// Survivor-count deadline (Go: 60 s).
const COUNT_TIMEOUT: Duration = Duration::from_secs(60);
/// GC-pass wait (Go: 60 s after a 12 s expiry sleep).
const EXPIRY_SLEEP: Duration = Duration::from_secs(12);
const GC_TIMEOUT: Duration = Duration::from_secs(60);

/// Ordered contention rows on one node (Go: `TypedContentionRows`).
async fn rows(cluster: &Cluster, idx: usize) -> Result<Vec<(String, String)>, String> {
    let db_rows = cluster
        .node(idx)
        .query("SELECT id, name FROM contention ORDER BY id", &[])
        .await
        .map_err(|e| format!("node{} read contention rows: {e}", idx + 1))?;
    db_rows
        .iter()
        .map(|row| {
            let id = row
                .try_get::<_, String>(0)
                .map_err(|e| format!("node{} decode id: {e}", idx + 1))?;
            let name = row
                .try_get::<_, String>(1)
                .map_err(|e| format!("node{} decode name: {e}", idx + 1))?;
            Ok((id, name))
        })
        .collect()
}

/// Insert one row (Go: `TypedContentionInsert`).
async fn insert_row(cluster: &Cluster, idx: usize, id: String, name: &str) -> Result<(), String> {
    cluster
        .node(idx)
        .execute("INSERT INTO contention(id, name) VALUES($1, $2)", &[&id, &name])
        .await
        .map_err(|e| format!("node{} insert failed: {e}", idx + 1))?;
    cluster
        .node(idx)
        .flush()
        .await
        .map_err(|e| format!("node{} flush after insert failed: {e}", idx + 1))?;
    Ok(())
}

/// Rows named `name` on one node (Go: `countWhere`).
async fn count_where(cluster: &Cluster, idx: usize, name: &str) -> Result<usize, String> {
    Ok(rows(cluster, idx).await?.iter().filter(|(_, n)| n == name).count())
}

/// Discovered-member count, excluding self (Go `membership_count` minus one).
async fn member_count(cluster: &Cluster, node: usize) -> Result<u64, String> {
    let rows = cluster
        .node(node)
        .simple("MURMUR SHOW REPLICATION")
        .await
        .map_err(|e| format!("node{} SHOW REPLICATION: {e}", node + 1))?;
    let cell = rows
        .first()
        .and_then(|r| r.get(3).cloned())
        .ok_or_else(|| format!("node{} SHOW REPLICATION returned no members", node + 1))?;
    cell.parse::<u64>()
        .map_err(|e| format!("node{} members {cell:?}: {e}", node + 1))
}

/// Wait until every node sees `want` discovered members (Go: `waitMembership`).
async fn wait_membership(cluster: &Cluster, want: u64, timeout: Duration) -> Result<(), String> {
    let converged = wait_until_async(timeout, MEMBERSHIP_POLL, || async {
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

/// Require a full healthy membership view on every node: all members
/// present (Go also checks alive == 2, which has no Rust equivalent;
/// any eviction still drops the member view and fails here).
async fn assert_stable_membership(cluster: &Cluster) -> Result<(), String> {
    for i in 0..cluster.len() {
        let got = member_count(cluster, i).await?;
        if got != 2 {
            return Err(format!(
                "node{} members = {got} during impairment, want 2 (false eviction?)",
                i + 1
            ));
        }
    }
    Ok(())
}

/// Wait until `name` is visible exactly once on every node (Go: `waitRowVisible`).
async fn wait_row_visible(cluster: &Cluster, name: &str, timeout: Duration) -> Result<(), String> {
    let converged = wait_until_async(timeout, VISIBILITY_POLL, || async {
        for i in 0..cluster.len() {
            if count_where(cluster, i, name).await.unwrap_or(usize::MAX) != 1 {
                return false;
            }
        }
        true
    })
    .await;
    if !converged {
        return Err(format!("row {name:?} not visible on all nodes within {timeout:?}"));
    }
    Ok(())
}

/// Poll exact counts plus ordered-digest equality, logging counts every
/// 10 s while waiting (Go: `waitConverged`).
async fn wait_converged(cluster: &Cluster, want: usize, timeout: Duration) -> Result<(), String> {
    let mut last_log = Instant::now();
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let mut first: Option<Vec<(String, String)>> = None;
        let mut ok = true;
        for i in 0..cluster.len() {
            match rows(cluster, i).await {
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
        if last_log.elapsed() > Duration::from_secs(10) {
            last_log = Instant::now();
            let mut counts = Vec::new();
            for i in 0..cluster.len() {
                counts.push(rows(cluster, i).await.map(|r| r.len()).unwrap_or(usize::MAX));
            }
            eprintln!("converge progress: counts={counts:?} want={want}");
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "nodes did not converge on {want} rows with equal digests within {timeout:?}"
            ));
        }
        tokio::time::sleep(POLL).await;
    }
}

/// Wait until one node holds exactly `want` rows (Go: `waitRowCount`).
async fn wait_row_count(cluster: &Cluster, idx: usize, want: usize, timeout: Duration) -> Result<(), String> {
    let converged = wait_until_async(timeout, POLL, || async {
        rows(cluster, idx).await.map(|r| r.len()).unwrap_or(usize::MAX) == want
    })
    .await;
    if !converged {
        let got = rows(cluster, idx).await.map(|r| r.len()).unwrap_or(usize::MAX);
        return Err(format!("node{} count = {got}, want {want} within {timeout:?}", idx + 1));
    }
    Ok(())
}

/// Nearest-rank percentile (Go: `percentile`; k = ceil(len*p/100)).
fn percentile(d: &mut [Duration], p: usize) -> Duration {
    if d.is_empty() {
        return Duration::ZERO;
    }
    d.sort();
    let mut k = (d.len() * p + 99) / 100;
    if k < 1 {
        k = 1;
    }
    d[k - 1]
}

/// Probe tc/netem usability on lo (Go: `tcCapable`). Returns `(true, "")`
/// when impairment can be applied, else `(false, reason)`.
fn tc_capable() -> (bool, String) {
    if Command::new("tc").arg("--version").output().is_err() {
        return (false, "tc binary not found".into());
    }
    for _ in 0..2 {
        let show = Command::new("tc").args(["qdisc", "show", "dev", "lo"]).output();
        let out = match show {
            Ok(o) if o.status.success() => String::from_utf8_lossy(&o.stdout).into_owned(),
            Ok(o) => {
                return (
                    false,
                    format!("tc qdisc show failed: {}", String::from_utf8_lossy(&o.stderr).trim()),
                )
            }
            Err(e) => return (false, format!("tc qdisc show failed: {e}")),
        };
        if out.contains("77:") {
            // Our own leftover from a crashed run: remove and re-probe.
            let cleared = Command::new("tc")
                .args(["qdisc", "del", "dev", "lo", "root"])
                .output()
                .is_ok_and(|o| o.status.success());
            if !cleared {
                return (false, "cannot clear stale qdisc 77:".into());
            }
            continue;
        }
        if out.contains("qdisc") && !out.contains("noqueue") {
            return (
                false,
                format!(
                    "lo already has a root qdisc ({}); refusing to disturb shared box",
                    out.trim()
                ),
            );
        }
        // Privilege probe: install and immediately remove an empty prio.
        let added = Command::new("tc")
            .args(["qdisc", "add", "dev", "lo", "root", "handle", "77:", "prio"])
            .output()
            .is_ok_and(|o| o.status.success());
        if !added {
            return (false, "tc qdisc add failed (need CAP_NET_ADMIN)".into());
        }
        let cleaned = Command::new("tc")
            .args(["qdisc", "del", "dev", "lo", "root"])
            .output()
            .is_ok_and(|o| o.status.success());
        if !cleaned {
            return (false, "tc probe cleanup failed".into());
        }
        return (true, String::new());
    }
    (false, "cannot clear stale qdisc 77:".into())
}

/// Remove qdisc 77: when present (Go: `clearImpairment`; always runs).
fn clear_impairment() {
    let show = Command::new("tc").args(["qdisc", "show", "dev", "lo"]).output();
    match show {
        Ok(o) if o.status.success() && String::from_utf8_lossy(&o.stdout).contains("77:") => {}
        _ => return,
    }
    let _ = Command::new("tc").args(["qdisc", "del", "dev", "lo", "root"]).status();
}

/// Run one tc command; on failure clears the impairment and panics
/// (Go: the `run` closure inside `applyImpairment`).
fn tc_run(what: &str, args: &[&str]) {
    let out = Command::new("tc").args(args).output();
    match out {
        Ok(o) if o.status.success() => {}
        Ok(o) => {
            clear_impairment();
            panic!("tc {what} failed ({})", String::from_utf8_lossy(&o.stderr).trim());
        }
        Err(e) => {
            clear_impairment();
            panic!("tc {what} failed: {e}");
        }
    }
}

/// Attach the impairment to lo as root qdisc 77: (prio) with the
/// netem/tbf child on band 77:3; u32 filters steer only packets whose
/// source OR destination port is a cluster replication port into the
/// impaired band (Go: `applyImpairment`).
fn apply_impairment(ports: &[u16], child: &str, args: &[&str]) {
    tc_run(
        "root",
        &["qdisc", "replace", "dev", "lo", "root", "handle", "77:", "prio", "bands", "3"],
    );
    let mut child_args = vec![
        "qdisc".to_owned(),
        "replace".to_owned(),
        "dev".to_owned(),
        "lo".to_owned(),
        "parent".to_owned(),
        "77:3".to_owned(),
        "handle".to_owned(),
        "773:".to_owned(),
        child.to_owned(),
    ];
    child_args.extend(args.iter().map(|s| s.to_string()));
    let child_refs: Vec<&str> = child_args.iter().map(String::as_str).collect();
    tc_run("child", &child_refs);
    for port in ports {
        let p = port.to_string();
        // Both directions: every replication packet touches a repl port
        // as either source or destination.
        tc_run(
            "filter-dport",
            &[
                "filter", "add", "dev", "lo", "protocol", "ip", "parent", "77:0", "prio",
                "77", "u32", "match", "ip", "dport", &p, "0xffff", "flowid", "77:3",
            ],
        );
        tc_run(
            "filter-sport",
            &[
                "filter", "add", "dev", "lo", "protocol", "ip", "parent", "77:0", "prio",
                "77", "u32", "match", "ip", "sport", &p, "0xffff", "flowid", "77:3",
            ],
        );
    }
}

/// Retention-collection marker on one node: max `last_collection_millis`
/// across `MURMUR SHOW RETENTION` rows (node-global; repeated per row).
async fn retention_marker(cluster: &Cluster, idx: usize) -> Result<u64, String> {
    let rows = cluster
        .node(idx)
        .simple("MURMUR SHOW RETENTION")
        .await
        .map_err(|e| format!("node{} SHOW RETENTION: {e}", idx + 1))?;
    let mut best = 0_u64;
    let mut seen = false;
    for row in &rows {
        if let Some(cell) = row.get(6) {
            if let Ok(v) = cell.parse::<u64>() {
                seen = true;
                best = best.max(v);
            }
        }
    }
    if !seen {
        return Err(format!("node{} SHOW RETENTION has no collection marker", idx + 1));
    }
    Ok(best)
}

/// Run on-demand retention passes until one completes past the baseline
/// (Go: `waitGCPass` on the background ticker; the wire exposes the pass
/// directly via `MURMUR RETENTION COLLECT`).
async fn wait_gc_pass(cluster: &Cluster, idx: usize, timeout: Duration) -> Result<(), String> {
    let base = retention_marker(cluster, idx).await?;
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        cluster
            .node(idx)
            .simple("MURMUR RETENTION COLLECT")
            .await
            .map_err(|e| format!("node{} RETENTION COLLECT: {e}", idx + 1))?;
        if retention_marker(cluster, idx).await? > base {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!("no GC pass observed within {timeout:?}"));
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// Log-path activity counters `(staged, applied)`: snapshot application
/// bypasses both, so a flat pair proves the snapshot path (Go reads
/// `spedsql_repl_snapshots_received_total`, which has no Rust equivalent).
async fn log_counters(cluster: &Cluster, idx: usize) -> Result<(u64, u64), String> {
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
    let staged = row[4]
        .parse::<u64>()
        .map_err(|e| format!("node{} staged {:?}: {e}", idx + 1, row[4]))?;
    let applied = row[5]
        .parse::<u64>()
        .map_err(|e| format!("node{} applied {:?}: {e}", idx + 1, row[5]))?;
    Ok((staged, applied))
}

/// Insert rows round-robin and record per-row full-mesh visibility
/// latency; every row also samples stable membership (Go: `timedWrites`).
async fn timed_writes(
    cluster: &Cluster,
    prefix: &str,
    base: usize,
    n: usize,
    per_row: Duration,
) -> Result<Vec<Duration>, String> {
    let mut lat = Vec::with_capacity(n);
    for i in 0..n {
        let name = format!("{prefix}-{i}");
        let id = format!("{:032x}", (base + 1) * 100000 + i);
        let start = Instant::now();
        insert_row(cluster, i % cluster.len(), id, &name).await?;
        wait_row_visible(cluster, &name, per_row).await?;
        lat.push(start.elapsed());
        assert_stable_membership(cluster).await?;
    }
    Ok(lat)
}

fn repl_ports(cluster: &Cluster) -> Vec<u16> {
    (0..cluster.len()).map(|i| cluster.node_info(i).repl_addr.port()).collect()
}

async fn run_body(cluster: &mut Cluster, force: bool, rows_n: usize) -> Result<(), String> {
    // Same schema on every node (Go: typed contention fixture tables),
    // flagged replicated on each node (row convergence needs the per-node
    // flag).
    for i in 0..cluster.len() {
        cluster
            .ensure_migrated(i, TABLE_DDL)
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
            .flush()
            .await
            .map_err(|e| format!("node{} flush after SET TABLE: {e}", i + 1))?;
    }
    wait_membership(cluster, 2, MEMBERSHIP_TIMEOUT).await?;

    // Baseline converges unimpaired (honest-path control).
    let mut base = BASELINE_ROWS;
    for i in 0..base {
        insert_row(cluster, 0, format!("{:032x}", 1000 + i), &format!("base-{i}")).await?;
    }
    wait_converged(cluster, base, BASELINE_TIMEOUT).await?;

    // Phase (a): +150 ms latency on replication traffic.
    if force {
        eprintln!("FORCE mode: NO 150ms latency applied; green here proves the workload only, not impairment tolerance");
    } else {
        apply_impairment(&repl_ports(cluster), "netem", &["delay", "150ms"]);
    }
    let mut lat = timed_writes(cluster, "lat", base, rows_n, LAT_ROW_TIMEOUT).await?;
    let p95 = percentile(&mut lat, 95);
    eprintln!("latency phase: {} rows p95 visibility={p95:?} (bound 15s)", lat.len());
    if p95 > LAT_P95_BOUND {
        return Err(format!("p95 visibility {p95:?} exceeds 15s bound"));
    }
    if !force {
        clear_impairment();
    }
    base += rows_n;

    // Phase (b): 3% packet loss on replication traffic.
    if force {
        eprintln!("FORCE mode: NO 3% loss applied; green here proves the workload only, not impairment tolerance");
    } else {
        apply_impairment(&repl_ports(cluster), "netem", &["loss", "3%"]);
    }
    let mut lat = timed_writes(cluster, "loss", base, rows_n, LOSS_ROW_TIMEOUT).await?;
    let p95 = percentile(&mut lat, 95);
    eprintln!("loss phase: {} rows p95 visibility={p95:?} (bound 30s)", lat.len());
    if p95 > LOSS_P95_BOUND {
        return Err(format!("p95 visibility {p95:?} exceeds 30s bound"));
    }
    if !force {
        clear_impairment();
    }
    base += rows_n;

    // Phase (c): 1 Mbit cap during a stale-node snapshot resync under
    // sustained writes.
    if force {
        eprintln!("FORCE mode: NO bandwidth cap applied; green here proves the workload only, not impairment tolerance");
    }
    for i in 0..5 {
        insert_row(cluster, 2, format!("{:032x}", 5000 + i), &format!("stale-{i}")).await?;
    }
    wait_converged(cluster, base + 5, BASELINE_TIMEOUT).await?;
    cluster.stop_node(2).await;

    // Survivors write far past the short retention while node 3 is
    // offline (sustained writes continue below).
    for i in 0..60 {
        insert_row(cluster, 0, format!("{:032x}", 6000 + i), &format!("fresh-{i}")).await?;
    }
    wait_row_count(cluster, 0, base + 65, COUNT_TIMEOUT).await?;
    wait_row_count(cluster, 1, base + 65, COUNT_TIMEOUT).await?;

    // Node 3's 10 s retention pin must lapse, then one GC pass must
    // collect its ranges so the rejoin needs a snapshot. Poll the GC
    // marker instead of a fixed sleep.
    tokio::time::sleep(EXPIRY_SLEEP).await;
    wait_gc_pass(cluster, 0, GC_TIMEOUT).await?;

    // Cap replication throughput, then restart node 3 into it and
    // keep writing while the snapshot transfers. The restart rebinds a
    // fresh replication port, so the filters are re-steered after the
    // restart (replace semantics make the re-apply cheap).
    if !force {
        apply_impairment(&repl_ports(cluster), "tbf", &["rate", "1mbit", "burst", "32kbit", "latency", "400ms"]);
    }
    cluster.start_node(2).await.map_err(|e| format!("restart stale node3: {e}"))?;
    if !force {
        apply_impairment(&repl_ports(cluster), "tbf", &["rate", "1mbit", "burst", "32kbit", "latency", "400ms"]);
    }
    cluster.mesh().await.map_err(|e| format!("re-mesh stale node3: {e}"))?;
    for i in 0..20 {
        insert_row(cluster, 1, format!("{:032x}", 8000 + i), &format!("sustain-{i}")).await?;
    }
    wait_converged(cluster, base + 85, CAPPED_TIMEOUT).await?;
    let (received, applied) = log_counters(cluster, 2).await?;
    if (received, applied) != (0, 0) {
        return Err(format!(
            "node3 rejoin arrived over the log path (received={received} applied={applied}), want snapshot (log catch-up would hide a snapshot-path regression)"
        ));
    }
    wait_membership(cluster, 2, MEMBERSHIP_TIMEOUT).await?;
    eprintln!("capped resync converged with snapshot path proven");
    if !force {
        clear_impairment();
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn impaired_network_converges() {
    let force = std::env::var(FORCE_ENV).as_deref() == Ok("1");
    let (tc_ok, skip_reason) = tc_capable();
    if !force && !tc_ok {
        eprintln!(
            "SKIP: impaired-network needs tc/netem + CAP_NET_ADMIN on lo: {skip_reason} (set {FORCE_ENV}=1 to run the workload unimpaired)"
        );
        return;
    }
    if force && !tc_ok {
        eprintln!("FORCE mode: tc unusable ({skip_reason}); running workload WITHOUT impairment");
    }
    let rows_n: usize = std::env::var(ROWS_ENV)
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_ROWS);

    let mut cluster = Cluster::new(ClusterOptions {
        name: "impaired-network".into(),
        num_nodes: 3,
        // Aggressive retention so phase (c) can force a snapshot resync
        // after a short offline window; harmless to phases (a)/(b).
        replication: Some(ReplicationOptions {
            retention: Some(Duration::from_secs(1)),
            minimum_retained: Some(10),
            maximum_offline: Some(Duration::from_secs(10)),
            ..Default::default()
        }),
        bootstrap_seeds: vec![0],
        ..Default::default()
    })
    .await
    .expect("cluster setup");
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
    let result = run_body(&mut cluster, force, rows_n).await;
    let failed = result.is_err();
    if let Err(e) = &result {
        eprintln!("scenario failed: {e}");
    }
    // The suite always removes qdisc 77:, on success and on failure.
    if !force {
        clear_impairment();
    }
    if failed {
        cluster.dump_forensics("impaired-network-no-convergence").await;
    }
    cluster.finish(failed).await;
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}
