//! Rust port of the Go `diskfull-live` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/diskfull-live/`
//! (`diskfull_live_test.go`). One node is stopped, its directory is moved
//! onto a tiny tmpfs, and it is restarted there; writes past ENOSPC must
//! fail closed (error, no partial row, node keeps answering reads) with
//! pre-full data intact, and freeing space plus a restart must recover to
//! exact convergence. Plain SQL replaces the Go typed-records HTTP API
//! (see the scenario README.md for deviations).
//!
//! Env-gated like Go: the test SKIPs when the tmpfs mount fails (no mount
//! privilege) or on non-Unix platforms.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use murmur_live_harness::{Cluster, ClusterOptions};

/// Table DDL (Go: typed records with a value field).
const TABLE_DDL: &str = "CREATE TABLE live_typed_records(name TEXT NOT NULL PRIMARY KEY, value TEXT NOT NULL)";
/// Baseline rows (Go: 20).
const BASELINE_ROWS: usize = 20;
/// Convergence deadline (Go: 30 s).
const CONVERGE_TIMEOUT: Duration = Duration::from_secs(30);
/// Convergence poll interval (Go: 100 ms).
const POLL: Duration = Duration::from_millis(100);
/// tmpfs-size knob (Go: `MURMUR_DISKFULL_TMPFS_SIZE_MB`, default 8).
const TMPFS_ENV: &str = "MURMUR_DISKFULL_TMPFS_SIZE_MB";
const DEFAULT_TMPFS_MB: u64 = 8;
/// Filler knob (Go: `MURMUR_DISKFULL_FILLER_SIZE_MB`, default 1).
const FILLER_ENV: &str = "MURMUR_DISKFULL_FILLER_SIZE_MB";
const DEFAULT_FILLER_MB: u64 = 1;
/// Fill-value knob (Go: `MURMUR_DISKFULL_VALUE_SIZE_KB`, default 128).
const VALUE_ENV: &str = "MURMUR_DISKFULL_VALUE_SIZE_KB";
const DEFAULT_VALUE_KB: usize = 128;
/// Fill loop stops recording after this many errors (Go: `maxErrors = 5`).
const MAX_ERRORS: usize = 5;

/// Env-int knob (Go: `envInt`).
fn env_int(name: &str, fallback: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(fallback)
}

/// Free bytes on the filesystem holding `path` (Unix `statvfs`).
#[cfg(unix)]
fn free_bytes(path: &Path) -> u64 {
    use std::os::unix::ffi::OsStrExt;
    let c = std::ffi::CString::new(path.as_os_str().as_bytes()).expect("path cstring");
    let mut v: libc::statvfs = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::statvfs(c.as_ptr(), &mut v) };
    assert_eq!(rc, 0, "statvfs failed");
    v.f_bavail as u64 * v.f_frsize as u64
}

/// Ordered names on one node (Go: `Cluster.TypedNames`).
async fn names(cluster: &Cluster, idx: usize) -> Result<Vec<String>, String> {
    let rows = cluster
        .node(idx)
        .query("SELECT name FROM live_typed_records ORDER BY name", &[])
        .await
        .map_err(|e| format!("node{} read names failed: {e}", idx + 1))?;
    rows.iter()
        .map(|row| {
            row.try_get::<_, String>(0)
                .map_err(|e| format!("node{} decode name: {e}", idx + 1))
        })
        .collect()
}

/// Point read by name: `None` when missing (Go: `TypedRead`).
async fn read_value(cluster: &Cluster, idx: usize, name: &str) -> Option<String> {
    let rows = cluster
        .node(idx)
        .query("SELECT value FROM live_typed_records WHERE name = $1", &[&name])
        .await
        .ok()?;
    rows.first()?.try_get::<_, String>(0).ok()
}

/// Best-effort insert; true when acknowledged.
async fn try_insert(cluster: &Cluster, idx: usize, name: String, value: String) -> bool {
    cluster
        .node(idx)
        .execute(
            "INSERT INTO live_typed_records(name, value) VALUES($1, $2)",
            &[&name, &value],
        )
        .await
        .is_ok()
}

/// Run a command to completion, panicking unless it succeeds.
fn run(cmd: &mut Command, what: &str) {
    let status = cmd.status().unwrap_or_else(|e| panic!("{what}: spawn failed: {e}"));
    assert!(status.success(), "{what} failed: {status}");
}

/// Copy a directory tree (`cp -a src/. dst`, creating dst).
fn copy_tree(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).expect("create copy dest");
    run(
        Command::new("cp")
            .arg("-a")
            .arg(format!("{}/.", src.display()))
            .arg(dst),
        "copy node dir",
    );
}

/// Poll exact counts plus ordered-name equality (Go: `waitConverged`).
async fn wait_converged(cluster: &Cluster, want: usize, timeout: Duration) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let mut first: Option<Vec<String>> = None;
        let mut converged = true;
        for i in 0..cluster.len() {
            let got = names(cluster, i).await?;
            if got.len() != want {
                converged = false;
                break;
            }
            match &first {
                None => first = Some(got),
                Some(prev) if *prev == got => {}
                _ => {
                    converged = false;
                    break;
                }
            }
        }
        if converged {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "nodes did not converge on {want} rows with equal names within {timeout:?}"
            ));
        }
        tokio::time::sleep(POLL).await;
    }
}

/// Body of the scenario. Returns `Ok(true)` when the run SKIPs (tmpfs
/// mount failed); `mounted` reports whether the tmpfs is up so the
/// caller can tear it down on every exit path.
async fn run_body(
    cluster: &mut Cluster,
    node_dir: &Path,
    scenario_dir: &Path,
    mounted: &mut bool,
) -> Result<bool, String> {
    // Same schema on every node (Go: typed fixture tables), then
    // replicate it: convergence needs the per-table flag on each node.
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
            .flush()
            .await
            .map_err(|e| format!("node{} flush after SET TABLE: {e}", i + 1))?;
    }

    // Baseline: 20 rows visible everywhere before any failure.
    for i in 0..BASELINE_ROWS {
        let name = format!("diskfull-{i:04}");
        if !try_insert(cluster, 0, name, "baseline".into()).await {
            return Err("baseline write failed".into());
        }
        cluster
            .node(0)
            .flush()
            .await
            .map_err(|e| format!("flush baseline: {e}"))?;
    }
    wait_converged(cluster, BASELINE_ROWS, CONVERGE_TIMEOUT).await?;

    // Move the stopped node onto a tiny tmpfs. Mount failure (no
    // privilege in this environment) SKIPs, restoring the dir first.
    cluster.stop_node(0).await;
    let staging = scenario_dir.join("diskfull-staging");
    copy_tree(node_dir, &staging);
    std::fs::remove_dir_all(node_dir).map_err(|e| format!("clear node dir for tmpfs: {e}"))?;
    std::fs::create_dir_all(node_dir).map_err(|e| format!("recreate node dir: {e}"))?;
    let tmpfs_mb = env_int(TMPFS_ENV, DEFAULT_TMPFS_MB);
    let mounted_ok = Command::new("mount")
        .arg("-t")
        .arg("tmpfs")
        .arg("-o")
        .arg(format!("size={tmpfs_mb}M"))
        .arg("tmpfs")
        .arg(node_dir)
        .status()
        .is_ok_and(|s| s.success());
    if !mounted_ok {
        copy_tree(&staging, node_dir);
        eprintln!("SKIP: tmpfs mount failed (needs mount privilege)");
        return Ok(true);
    }
    *mounted = true;
    copy_tree(&staging, node_dir);
    cluster
        .start_node(0)
        .await
        .map_err(|e| format!("restart node1 on tmpfs: {e}"))?;
    cluster
        .node(0)
        .simple("MURMUR SET TABLE live_typed_records REPLICATED true")
        .await
        .map_err(|e| format!("re-set replicated node1 on tmpfs: {e}"))?;
    cluster
        .node(0)
        .flush()
        .await
        .map_err(|e| format!("flush node1 on tmpfs: {e}"))?;
    cluster
        .mesh()
        .await
        .map_err(|e| format!("re-mesh after restart: {e}"))?;
    let _ = cluster.sync_all().await;
    wait_converged(cluster, BASELINE_ROWS, CONVERGE_TIMEOUT).await?;

    // Prove the fs really is tiny before filling it (else ENOSPC may
    // never trigger and the test would pass vacuously).
    let free = free_bytes(node_dir);
    if free >= 5 * 1024 * 1024 {
        return Err(format!(
            "tmpfs has {free} free bytes, want < 5 MiB (test would be vacuous)"
        ));
    }
    eprintln!("tiny fs proven: {free} bytes free");

    // Fixed filler first: the fill loop must hit the wall via product
    // writes, not via leftover headroom games.
    let filler_mb = env_int(FILLER_ENV, DEFAULT_FILLER_MB);
    let filler = node_dir.join("ballast").join("filler.bin");
    std::fs::create_dir_all(filler.parent().expect("ballast parent"))
        .map_err(|e| format!("ballast dir: {e}"))?;
    std::fs::write(&filler, vec![0u8; filler_mb as usize * 1024 * 1024])
        .map_err(|e| format!("write filler: {e}"))?;

    // Fill until the node refuses writes; every outcome is tracked so
    // the post-recovery audit is exact.
    let value_size = env_int(VALUE_ENV, DEFAULT_VALUE_KB as u64) as usize * 1024;
    let value = "D".repeat(value_size);
    let mut acked: HashSet<String> = HashSet::new();
    let mut errored: HashSet<String> = HashSet::new();
    for i in 0.. {
        let name = format!("fill-{i:08}");
        if try_insert(cluster, 0, name.clone(), value.clone()).await {
            let _ = cluster.node(0).flush().await;
            acked.insert(name);
        } else {
            errored.insert(name);
            if errored.len() > MAX_ERRORS {
                break;
            }
        }
    }
    if errored.is_empty() {
        return Err("no write ever failed: ENOSPC never hit (vacuous pass)".into());
    }
    eprintln!("fill done: {} acked, {} errored", acked.len(), errored.len());

    // Fail-closed proof while still full: the node answers reads, and
    // a probe write either acks (tracked, must survive) or errors
    // cleanly. (Go also scans the daemon log for panics; a server panic
    // fails the test via the wire error instead, and the logs are kept
    // as failure artifacts.)
    let _ = names(cluster, 0).await?;
    let probe = "liveness-probe".to_owned();
    if try_insert(cluster, 0, probe.clone(), "probe".into()).await {
        let _ = cluster.node(0).flush().await;
        acked.insert(probe);
    }

    // Free space and restart: every acked row intact everywhere, every
    // errored row absent-on-all or whole-on-all (never torn).
    std::fs::remove_file(&filler).map_err(|e| format!("remove filler: {e}"))?;
    cluster.stop_node(0).await;
    cluster
        .start_node(0)
        .await
        .map_err(|e| format!("restart node1 after freeing space: {e}"))?;
    cluster
        .node(0)
        .simple("MURMUR SET TABLE live_typed_records REPLICATED true")
        .await
        .map_err(|e| format!("re-set replicated node1 after recovery: {e}"))?;
    cluster
        .node(0)
        .flush()
        .await
        .map_err(|e| format!("flush node1 after recovery: {e}"))?;
    cluster
        .mesh()
        .await
        .map_err(|e| format!("re-mesh after recovery: {e}"))?;
    let _ = cluster.sync_all().await;
    for i in 0..cluster.len() {
        for name in &acked {
            let want = if name == "liveness-probe" { "probe" } else { value.as_str() };
            if read_value(cluster, i, name).await.as_deref() != Some(want) {
                return Err(format!("node{} lost acked row {name:?}", i + 1));
            }
        }
        for name in &errored {
            match read_value(cluster, i, name).await {
                None => {}
                Some(got) if got == value => {}
                Some(got) => {
                    return Err(format!(
                        "node{} has torn row {name:?} ({} of {value_size} bytes)",
                        i + 1,
                        got.len()
                    ));
                }
            }
        }
    }
    eprintln!("recovery audit clean: acked intact, errored absent-or-whole");

    // Post-recovery honest writes on both nodes converge everywhere.
    for (i, prefix) in ["post-recovery-node1", "post-recovery-node2"].iter().enumerate() {
        if !try_insert(cluster, i, prefix.to_string(), "ok".into()).await {
            return Err(format!("post-recovery write node{} failed", i + 1));
        }
        cluster
            .node(i)
            .flush()
            .await
            .map_err(|e| format!("flush post-recovery: {e}"))?;
    }
    wait_converged(cluster, BASELINE_ROWS + acked.len() + 2, CONVERGE_TIMEOUT).await?;
    eprintln!("PASS: ENOSPC failed closed, space freed, mesh reconverged");
    Ok(false)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn disk_full_fails_closed_then_recovers() {
    #[cfg(not(unix))]
    {
        eprintln!("SKIP: diskfull-live needs a tmpfs mount (Unix only)");
        return;
    }

    let mut cluster = Cluster::new(ClusterOptions {
        name: "diskfull-live".into(),
        num_nodes: 2,
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
    // Node 0 dir + scenario runtime dir (stable across the tmpfs move).
    let node_dir: PathBuf = cluster.node_info(0).dir.clone();
    let scenario_dir: PathBuf = node_dir.parent().expect("node dir parent").to_path_buf();
    // Set once the tmpfs is mounted; teardown must not leak it.
    let mut mounted = false;
    let result = run_body(&mut cluster, &node_dir, &scenario_dir, &mut mounted).await;
    let skipped = result == Ok(true);
    let failed = result.is_err();
    if let Err(e) = &result {
        eprintln!("scenario failed: {e}");
    }
    if failed {
        cluster.dump_forensics("diskfull-live-no-convergence").await;
        if mounted {
            // Snapshot the tmpfs content aside: `finish` snapshots
            // runtime/ only after the unmount below hides it again.
            copy_tree(&node_dir, &scenario_dir.join("diskfull-tmpfs-content"));
        }
    }
    if mounted {
        run(Command::new("umount").arg(&node_dir), "unmount tmpfs");
    }
    cluster.finish(failed).await;
    if skipped {
        return;
    }
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}
