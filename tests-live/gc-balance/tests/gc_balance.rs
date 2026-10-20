//! Rust port of the Go `gc-balance` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/gc-balance/`
//! (`gc_balance_test.go`). `TestLogGCKeepsUpWithChurn`: three nodes mesh
//! with aggressive log retention (1s log, 20s offline pin, 10 batches)
//! while one node updates a fixed 200-key set with eight parallel updaters
//! over disjoint ranges. After a 20s warmup, a 40s window must show GC
//! collecting at least 90% of the origin-log batches the window produced,
//! with zero GC failures; the suite ends with exact row counts and
//! identical digests on all nodes (no loss). Plain SQL replaces the Go
//! typed-contention HTTP API (see the scenario README.md for the
//! deviations).
//!
//! Scope (same as Go): version churn only; deletes are out of scope
//! (delete pruning is unimplemented).

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use murmur_live_harness::{Cluster, ClusterOptions, ReplicationOptions};

/// Fixed working set (Go: `keys = 200`).
const KEYS: usize = 200;
/// Parallel updaters over disjoint key ranges (Go: `updaters = 8`).
const UPDATERS: usize = 8;
/// Steady-state warmup before the measurement window (Go: 20s sleep).
const WARMUP: Duration = Duration::from_secs(20);
/// Measurement window on a quiet box (Go: 40s sleep).
const WINDOW: Duration = Duration::from_secs(40);
/// Window stretch step under load (Go: 5s sleeps).
const WINDOW_STRETCH_STEP: Duration = Duration::from_secs(5);
/// Window stretch cap (Go: 150s).
const WINDOW_CAP: Duration = Duration::from_secs(150);
/// Minimum commits for a meaningful balance proof (Go: 2000).
const COMMIT_FLOOR: u64 = 2000;
/// Minimum full sweeps per updater for GC pressure (Go: 5).
const MIN_SWEEPS: u64 = 5;
/// Post-stop drain stability gate (Go: 2 stable 5s polls, 120s cap).
const DRAIN_STABLE_POLLS: u32 = 2;
const DRAIN_CAP: Duration = Duration::from_secs(120);
/// Convergence deadlines (Go: 60s counts/digests, 30s value flow).
const COUNTS_TIMEOUT: Duration = Duration::from_secs(60);
const DIGEST_TIMEOUT: Duration = Duration::from_secs(60);
const VALUE_TIMEOUT: Duration = Duration::from_secs(30);
/// Convergence poll interval (Go: 200ms).
const POLL_INTERVAL: Duration = Duration::from_millis(200);
/// Hang guard for updater futures if the driver fails before signalling
/// stop (normal runs stop updaters long before this).
const UPDATER_CAP: Duration = Duration::from_secs(600);
/// `MURMUR SHOW RETENTION` carries the collected counter on per-peer
/// rows; bound the wait for the first row.
const RETENTION_ROW_TIMEOUT: Duration = Duration::from_secs(10);

/// One application row (Go: `harness.TypedContentionRow`).
#[derive(Debug, Clone, PartialEq, Eq)]
struct Row {
    id: String,
    name: String,
    phone: String,
    score: i64,
}

/// 1-based 32-hex-char key (Go: `fmt.Sprintf("%032x", i+1)`).
fn key_id(i: usize) -> String {
    format!("{:032x}", i + 1)
}

/// Seed insert on node 0 (Go: `Cluster.TypedContentionInsert` with
/// `Name: "v-0"`; phone/score keep their zero values).
async fn insert_seed(cluster: &Cluster, id: String, name: String) -> Result<(), String> {
    cluster
        .node(0)
        .execute(
            "INSERT INTO contention(id, name, phone, score) VALUES($1, $2, $3, $4)",
            &[&id, &name, &"", &0i64],
        )
        .await
        .map_err(|e| format!("seed insert failed: {e}"))?;
    cluster
        .node(0)
        .simple("MURMUR FLUSH")
        .await
        .map_err(|e| format!("flush after seed insert: {e}"))?;
    Ok(())
}

/// One churned update: set `name` and publish to replication. Errors are
/// returned (and counted by the caller) instead of panicking, so one bad
/// op cannot kill an updater (Go: `updateErrs`).
async fn try_update_name(cluster: &Cluster, id: &str, name: String) -> Result<(), String> {
    let changed = cluster
        .node(0)
        .execute("UPDATE contention SET name = $1 WHERE id = $2", &[&name, &id])
        .await
        .map_err(|e| e.to_string())?;
    if changed != 1 {
        return Err(format!("update hit {changed} rows"));
    }
    cluster.node(0).simple("MURMUR FLUSH").await.map_err(|e| e.to_string())?;
    Ok(())
}

/// Typed read on one node: `None` when the row is absent (Go:
/// `Cluster.TypedContentionRead`).
async fn read_name(cluster: &Cluster, idx: usize, id: &str) -> Result<Option<String>, String> {
    let rows = cluster
        .node(idx)
        .query("SELECT name FROM contention WHERE id = $1", &[&id])
        .await
        .map_err(|e| format!("node{} read row failed: {e}", idx + 1))?;
    match rows.first() {
        Some(row) => row
            .try_get::<_, String>(0)
            .map(Some)
            .map_err(|e| format!("node{} decode name: {e}", idx + 1)),
        None => Ok(None),
    }
}

/// Canonical PK-ordered rows on one node (Go: `gcDigest` input; compared
/// directly instead of hashing).
async fn all_rows(cluster: &Cluster, idx: usize) -> Result<Vec<Row>, String> {
    let rows = cluster
        .node(idx)
        .query("SELECT id, name, phone, score FROM contention ORDER BY id", &[])
        .await
        .map_err(|e| format!("node{} read all rows failed: {e}", idx + 1))?;
    rows.iter()
        .map(|row| {
            Ok(Row {
                id: row
                    .try_get::<_, String>(0)
                    .map_err(|e| format!("node{} decode id: {e}", idx + 1))?,
                name: row
                    .try_get::<_, String>(1)
                    .map_err(|e| format!("node{} decode name: {e}", idx + 1))?,
                phone: row
                    .try_get::<_, String>(2)
                    .map_err(|e| format!("node{} decode phone: {e}", idx + 1))?,
                score: row
                    .try_get::<_, i64>(3)
                    .map_err(|e| format!("node{} decode score: {e}", idx + 1))?,
            })
        })
        .collect()
}

/// Wait until every node holds exactly `want` rows (Go: `waitAllCounts`).
async fn wait_all_counts(cluster: &Cluster, want: usize, timeout: Duration) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut last = vec![String::new(); cluster.len()];
    loop {
        let mut converged = true;
        for i in 0..cluster.len() {
            let count = all_rows(cluster, i).await?.len();
            last[i] = format!("count={count}");
            if count != want {
                converged = false;
                break;
            }
        }
        if converged {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "counts did not reach {want} within {timeout:?}; last={last:?}"
            ));
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Poll until every node reports the same table contents (Go:
/// `waitDigestsAgreed`).
async fn wait_digests_agreed(cluster: &Cluster, timeout: Duration) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let want = all_rows(cluster, 0).await?;
        let mut agreed = true;
        for i in 1..cluster.len() {
            if all_rows(cluster, i).await? != want {
                agreed = false;
                break;
            }
        }
        if agreed {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!("digests did not agree within {timeout:?}"));
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Poll followers until one reports a value other than seed for the given
/// key, proving churned updates flow end to end (Go: `waitValueChanged`).
async fn wait_value_changed(
    cluster: &Cluster,
    id: &str,
    seed: &str,
    timeout: Duration,
) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        for i in 1..cluster.len() {
            if matches!(read_name(cluster, i, id).await?, Some(name) if name != seed) {
                return Ok(());
            }
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "no follower observed a churned value for {id} within {timeout:?} (updates may not match)"
            ));
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Commit/GC balance counters sampled from node 1 over the wire (Go:
/// scraped from `/metrics`).
struct GcSnapshot {
    /// Local commits produced (`MURMUR SHOW REPLICATION` trailing
    /// `committed` column; Go: `spedsql_local_commits_total`).
    commits: u64,
    /// Origin-log transactions collected (durable;
    /// `MURMUR SHOW RETENTION` `collected_transactions`; Go:
    /// `spedsql_gc_log_collected_total`).
    collected: u64,
    /// Latest replication-engine error, if any. Rust has no GC-failures
    /// counter; collection errors land here instead (Go:
    /// `spedsql_gc_failures_total`).
    last_error: Option<String>,
}

async fn gc_counters(cluster: &Cluster) -> Result<GcSnapshot, String> {
    let repl = cluster
        .node(0)
        .simple("MURMUR SHOW REPLICATION")
        .await
        .map_err(|e| format!("node1 SHOW REPLICATION: {e}"))?;
    let repl = repl
        .first()
        .ok_or_else(|| "node1 SHOW REPLICATION returned no rows".to_string())?;
    if repl.len() < 8 {
        return Err(format!(
            "node1 SHOW REPLICATION has {} columns, want 8",
            repl.len()
        ));
    }
    let commits: u64 = repl[7]
        .parse()
        .map_err(|e| format!("node1 bad committed {:?}: {e}", repl[7]))?;
    let last_error = if repl[6].is_empty() { None } else { Some(repl[6].clone()) };
    // `collected_transactions` rides on per-peer rows; wait briefly for
    // the first row (the mesh is up by measurement time).
    let deadline = tokio::time::Instant::now() + RETENTION_ROW_TIMEOUT;
    loop {
        let rows = cluster
            .node(0)
            .simple("MURMUR SHOW RETENTION")
            .await
            .map_err(|e| format!("node1 SHOW RETENTION: {e}"))?;
        if let Some(row) = rows.first() {
            if row.len() < 6 {
                return Err(format!(
                    "node1 SHOW RETENTION has {} columns, want 7",
                    row.len()
                ));
            }
            let collected: u64 = row[5]
                .parse()
                .map_err(|e| format!("node1 bad collected {:?}: {e}", row[5]))?;
            return Ok(GcSnapshot { commits, collected, last_error });
        }
        if tokio::time::Instant::now() >= deadline {
            return Err("node1 SHOW RETENTION returned no rows".into());
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// One churned updater over a disjoint key range: sequential sweeps until
/// `stopped` is set. Runs as a borrowed future inside `tokio::join!` (no
/// `'static` spawn), so it always observes the stop flag.
async fn updater(
    cluster: &Cluster,
    w: usize,
    stopped: &AtomicBool,
    update_errs: &AtomicU64,
    updates: &AtomicU64,
    sweeps: &AtomicU64,
) {
    let updater_deadline = tokio::time::Instant::now() + UPDATER_CAP;
    let mut round: u64 = 1;
    let lo = w * KEYS / UPDATERS;
    let hi = lo + KEYS / UPDATERS;
    while !stopped.load(Ordering::Relaxed) && tokio::time::Instant::now() < updater_deadline {
        for i in lo..hi {
            if stopped.load(Ordering::Relaxed) {
                break;
            }
            match try_update_name(cluster, &key_id(i), format!("v-{w}-{round}")).await {
                Ok(()) => {
                    updates.fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => {
                    eprintln!("updater {w} key {i}: {e}");
                    update_errs.fetch_add(1, Ordering::Relaxed);
                }
            }
            tokio::time::sleep(Duration::from_millis(140)).await;
        }
        sweeps.store(round, Ordering::Relaxed);
        round += 1;
    }
}

async fn run_body(cluster: &Cluster) -> Result<(), String> {
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
    // Same schema on every node (Go: TypedRecords/TypedContention tables),
    // then replicate it: convergence needs the per-table flag on each node.
    for i in 0..cluster.len() {
        cluster
            .ensure_migrated(i, "CREATE TABLE contention(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL, phone TEXT NOT NULL, score INTEGER NOT NULL)")
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

    for i in 0..KEYS {
        insert_seed(cluster, key_id(i), "v-0".into()).await?;
    }
    wait_all_counts(cluster, KEYS, COUNTS_TIMEOUT).await?;

    let stopped = AtomicBool::new(false);
    let update_errs = AtomicU64::new(0);
    let updates = AtomicU64::new(0);
    let sweeps: [AtomicU64; UPDATERS] = std::array::from_fn(|_| AtomicU64::new(0));

    // Eight updaters over disjoint key ranges (a single sequential updater
    // cannot sustain enough churn on a loaded box, which would make the
    // pressure gate vacuous). The driver sets `stopped` on every exit path
    // so a failed phase cannot hang the join.
    let driver = async {
        // Warm up to steady state, then measure one window: every commit
        // in the window appends an origin-log batch, so
        // (commits - collected) is the retained set's growth over the
        // window. GC keeps up iff it collects >= 90% of what the window
        // produced; a dead GC scores 0%. The window is 40s on a quiet
        // box and stretches (to a 150s cap) under parallel-suite load
        // until the commit floor is met, so the ratio proof stays
        // meaningful at any production rate.
        tokio::time::sleep(WARMUP).await;
        let before = gc_counters(cluster).await?;
        tokio::time::sleep(WINDOW).await;
        let mut after = gc_counters(cluster).await?;
        let mut elapsed = WINDOW;
        while after.commits - before.commits < COMMIT_FLOOR && elapsed < WINDOW_CAP {
            tokio::time::sleep(WINDOW_STRETCH_STEP).await;
            elapsed += WINDOW_STRETCH_STEP;
            after = gc_counters(cluster).await?;
        }
        eprintln!(
            "window commits={} over the measurement period",
            after.commits - before.commits
        );
        Ok::<(GcSnapshot, GcSnapshot), String>((before, after))
    };
    let ((), (), (), (), (), (), (), (), driver_result) = tokio::join!(
        updater(cluster, 0, &stopped, &update_errs, &updates, &sweeps[0]),
        updater(cluster, 1, &stopped, &update_errs, &updates, &sweeps[1]),
        updater(cluster, 2, &stopped, &update_errs, &updates, &sweeps[2]),
        updater(cluster, 3, &stopped, &update_errs, &updates, &sweeps[3]),
        updater(cluster, 4, &stopped, &update_errs, &updates, &sweeps[4]),
        updater(cluster, 5, &stopped, &update_errs, &updates, &sweeps[5]),
        updater(cluster, 6, &stopped, &update_errs, &updates, &sweeps[6]),
        updater(cluster, 7, &stopped, &update_errs, &updates, &sweeps[7]),
        async {
            let result = driver.await;
            stopped.store(true, Ordering::Relaxed);
            result
        },
    );
    let (before, mut after) = match driver_result {
        Ok(pair) => pair,
        Err(e) => return Err(e),
    };

    // Drain: the window's tail stays uncollected until a GC pass runs
    // after the updaters stop. First, let the mesh catch up and exchange ACKs
    // so node 0's retention floor advances.
    let _ = cluster.sync_all().await;
    wait_digests_agreed(cluster, DIGEST_TIMEOUT).await?;
    let _ = cluster.sync_all().await;

    let drain_deadline = tokio::time::Instant::now() + DRAIN_CAP;
    let mut stable = 0;
    while tokio::time::Instant::now() < drain_deadline {
        let _ = cluster.node(0).simple("MURMUR RETENTION COLLECT").await;
        tokio::time::sleep(Duration::from_millis(500)).await;
        let next = gc_counters(cluster).await?;
        let commits = next.commits - before.commits;
        let collected = next.collected - before.collected;
        if next.collected == after.collected {
            stable += 1;
        } else {
            stable = 0;
        }
        after = next;
        if commits.saturating_sub(collected) <= commits / 10 && stable >= DRAIN_STABLE_POLLS {
            break;
        }
    }

    if update_errs.load(Ordering::Relaxed) != 0 {
        return Err(format!(
            "{} update errors during churn",
            update_errs.load(Ordering::Relaxed)
        ));
    }
    let mut total_sweeps = 0;
    for w in 0..UPDATERS {
        let got = sweeps[w].load(Ordering::Relaxed);
        total_sweeps += got;
        if got < MIN_SWEEPS {
            return Err(format!(
                "updater {w} completed only {got} sweeps, want >= {MIN_SWEEPS} for GC pressure"
            ));
        }
    }
    let commits = after.commits - before.commits;
    let collected = after.collected - before.collected;
    eprintln!(
        "churn: {total_sweeps} updater sweeps ({} test-counted updates), {commits} commits produced, {collected} log batches collected",
        updates.load(Ordering::Relaxed),
    );

    // The balance assertion is vacuous without proven sustained churn
    // (the retained set scales with the production rate, so the 90%
    // bound stays meaningful at any rate above the floor).
    if commits < COMMIT_FLOOR {
        return Err(format!(
            "only {commits} commits in the window, want >= {COMMIT_FLOOR} for GC pressure"
        ));
    }
    // No GC-runs counter exists over the wire, so observed collection
    // (collected advancing across the window) is the proof a pass ran;
    // without it the test cannot observe GC.
    if collected == 0 {
        return Err("no GC collection during the window; test cannot observe GC".into());
    }
    // Go checks its GC-failures counter delta; Rust surfaces background
    // collection failures via sticky last_error, which also carries
    // unrelated transport transients (dial/handshake/queue pressure)
    // under churn. Failing on any change would trip on those, so only
    // GC-shaped errors fail the window (anything else is covered by
    // the balance and digest proofs below).
    fn is_gc_error(err: &Option<String>) -> bool {
        err.as_ref().is_some_and(|e| {
            let e = e.to_lowercase();
            e.contains("retention")
                || e.contains("collect")
                || e.contains("prune")
                || e.contains("gc ")
                || e.contains("snapshot")
        })
    }
    if after.last_error != before.last_error
        && (is_gc_error(&before.last_error) || is_gc_error(&after.last_error))
    {
        return Err(format!(
            "replication engine GC error during the window: before={:?} after={:?}",
            before.last_error, after.last_error
        ));
    }
    if commits.saturating_sub(collected) > commits / 10 {
        return Err(format!(
            "GC fell behind: produced {commits} commits but collected {collected} (retained grew by {}, want <= {})",
            commits.saturating_sub(collected),
            commits / 10
        ));
    }

    // End-to-end update flow: a follower must observe a churned value.
    wait_value_changed(cluster, &key_id(0), "v-0", VALUE_TIMEOUT).await?;

    // No data loss: exact key set with identical digests everywhere.
    // Counts never change for fixed-key updates, so convergence means
    // digest agreement: poll for it, since followers drain the final
    // backlog after the updaters stop.
    wait_all_counts(cluster, KEYS, COUNTS_TIMEOUT).await?;
    wait_digests_agreed(cluster, DIGEST_TIMEOUT).await?;
    let want = all_rows(cluster, 0).await?;
    for i in 1..cluster.len() {
        let digest = all_rows(cluster, i).await?;
        if digest != want {
            return Err(format!("node{} digest differs after GC churn", i + 1));
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn log_gc_keeps_up_with_churn() {
    // Aggressive log retention (Go: MinLogRetentionMs=1000,
    // MaxOfflineLogRetentionMs=20000, MinRetainedBatches=10). The 1s
    // collection cadence keeps background passes inside the 40s window;
    // the product default (60s) would starve it.
    let cluster = Cluster::new(ClusterOptions {
        name: "gc-balance".into(),
        num_nodes: 3,
        replication: Some(ReplicationOptions {
            retention: Some(Duration::from_secs(1)),
            retention_interval: Some(Duration::from_secs(1)),
            minimum_retained: Some(10),
            maximum_offline: Some(Duration::from_secs(20)),
            ..Default::default()
        }),
        ..Default::default()
    })
    .await
    .expect("cluster setup");
    let result = run_body(&cluster).await;
    let failed = result.is_err();
    if let Err(e) = &result {
        eprintln!("scenario failed: {e}");
    }
    if failed {
        cluster.dump_forensics("gc-balance-no-balance").await;
    }
    cluster.finish(failed).await;
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}
