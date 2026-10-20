//! Rust port of the Go `overload-budgets` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/overload-budgets/`
//! (`overload_budgets_test.go`, `README.md`). Two `murmur-server` processes
//! run with small commit budgets; an oversize write must be rejected cleanly
//! with no partial row on either node and no writer poisoning, then a
//! concurrent blast (8 writers x 50 inserts) must fully converge with bounded
//! tail latency while both nodes stay responsive. Plain SQL replaces the Go
//! typed-contention HTTP API; the HTTP 1 MiB body-cap leg has no Rust
//! equivalent (no HTTP server exists) and the per-value-bytes leg is adapted
//! to the `transaction_bytes` commit budget (no per-value product limit
//! exists). See the scenario README.md.

use std::time::{Duration, Instant};

use murmur_live_harness::{Cluster, ClusterOptions, ReplicationOptions, SqliteOptions};

/// Commit budget on every node (Go: 64 KiB transactions; the Go 4 KiB
/// per-value budget has no Rust product equivalent).
const TX_BUDGET: usize = 64 << 10;
/// Admission queue: comfortably above the commit budget so the oversize leg
/// trips the commit budget deterministically (and validation requires
/// `transaction_bytes <= queue_bytes`).
const QUEUE_BYTES: usize = 8 << 20;
/// Capture worker budget (`queue_bytes >= capture_bytes` per validation).
const CAPTURE_BYTES: usize = 1 << 20;

/// Post-rejection convergence deadline (Go: 30 s).
const PROBE_TIMEOUT: Duration = Duration::from_secs(30);
/// Blast convergence deadline (Go: 60 s).
const BLAST_TIMEOUT: Duration = Duration::from_secs(60);
/// Convergence poll interval (Go: 200 ms).
const CONVERGE_POLL: Duration = Duration::from_millis(200);
/// Final readiness deadline per node (Go: `WaitNodeReady`).
const READY_TIMEOUT: Duration = Duration::from_secs(30);
/// Readiness poll interval.
const READY_POLL: Duration = Duration::from_millis(100);
/// Blast tail-latency bound (Go: p99 under 10 s).
const P99_BOUND: Duration = Duration::from_secs(10);

/// Blast shape (Go: 8 writers x 50 inserts).
const WRITERS: usize = 8;
const PER_WRITER: usize = 50;

/// All rows on one node as text cells (Go: `Cluster.TypedContentionRows`).
async fn rows(cluster: &Cluster, idx: usize) -> Result<Vec<Vec<String>>, String> {
    cluster
        .node(idx)
        .simple("SELECT id, name, phone, score FROM records")
        .await
        .map_err(|e| format!("node{} SELECT rows: {e}", idx + 1))
}

/// Canonical digest of a row set: sorted `Debug` form (Go hashes ordered
/// `ID:Name:Phone:Score` lines; same criterion, cheaper to express).
fn digest(mut rows: Vec<Vec<String>>) -> String {
    rows.sort();
    format!("{rows:?}")
}

/// Row count on one node (cheap pre-check before full digest reads).
async fn row_count(cluster: &Cluster, idx: usize) -> Result<usize, String> {
    let rows = cluster
        .node(idx)
        .simple("SELECT COUNT(*) FROM records")
        .await
        .map_err(|e| format!("node{} COUNT: {e}", idx + 1))?;
    let cell = rows
        .first()
        .and_then(|r| r.first())
        .ok_or_else(|| format!("node{} COUNT returned no rows", idx + 1))?;
    cell.parse::<usize>()
        .map_err(|e| format!("node{} COUNT {cell:?}: {e}", idx + 1))
}

/// Wait until every node holds exactly `want` rows with equal digests
/// (Go: `waitConverged`).
async fn wait_converged(cluster: &Cluster, want: usize, timeout: Duration, what: &str) -> Result<(), String> {
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
                let d = digest(rows(cluster, idx).await?);
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
            return Err(format!(
                "nodes did not converge on {want} rows within {timeout:?} ({what})"
            ));
        }
        tokio::time::sleep(CONVERGE_POLL).await;
    }
}

/// Wait until one node is responsive with its full peer set
/// (Go: `Cluster.WaitNodeReady` over the admin API).
async fn wait_node_ready(cluster: &Cluster, idx: usize, timeout: Duration) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let repl = cluster
            .node(idx)
            .simple("MURMUR SHOW REPLICATION")
            .await
            .map_err(|e| format!("node{} SHOW REPLICATION: {e}", idx + 1))?;
        let peers = repl
            .first()
            .and_then(|r| r.get(2).cloned())
            .unwrap_or_default();
        // A successful read proves the writer path is responsive, not just
        // the replication endpoint.
        let readable = row_count(cluster, idx).await.is_ok();
        if peers.parse::<usize>().unwrap_or(usize::MAX) == cluster.len() - 1 && readable {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "node{} not ready within {timeout:?} (peers={peers}, readable={readable})",
                idx + 1
            ));
        }
        tokio::time::sleep(READY_POLL).await;
    }
}

/// Insert one contention row on one node (Go: `TypedContentionInsert`).
async fn insert(cluster: &Cluster, node: usize, id: String, name: String) -> Result<(), String> {
    let phone = String::new();
    let score = 0_i64;
    cluster
        .node(node)
        .execute(
            "INSERT INTO records(id, name, phone, score) VALUES($1, $2, $3, $4)",
            &[&id, &name, &phone, &score],
        )
        .await
        .map_err(|e| format!("node{} insert '{name}': {}", node + 1, murmur_live_harness::db_message(&e)))?;
    Ok(())
}

/// Insert one contention row, retrying replayable `40001` pressure
/// (post-rejection probe only; blast writers stay single-shot so pressure
/// during the blast still fails loudly).
async fn insert_retry(cluster: &Cluster, node: usize, id: String, name: String) -> Result<(), String> {
    let phone = String::new();
    let score = 0_i64;
    cluster
        .node(node)
        .execute_retry(
            "INSERT INTO records(id, name, phone, score) VALUES($1, $2, $3, $4)",
            &[&id, &name, &phone, &score],
            PROBE_TIMEOUT,
        )
        .await
        .map_err(|e| format!("node{} insert '{name}': {}", node + 1, murmur_live_harness::db_message(&e)))?;
    Ok(())
}

/// One blast writer: `PER_WRITER` sequential inserts, recording per-insert
/// latency. Returns `(latencies, failed_writes, first_error)`.
async fn writer(cluster: &Cluster, w: usize) -> (Vec<Duration>, u64, Option<String>) {
    let mut lat = Vec::with_capacity(PER_WRITER);
    let mut failed = 0_u64;
    for i in 0..PER_WRITER {
        let id = format!("{:032x}", 10000 + w * 1000 + i);
        let name = format!("w{w}-{i}");
        let start = Instant::now();
        let result = insert(cluster, 0, id, name).await;
        lat.push(start.elapsed());
        if let Err(e) = result {
            failed += 1;
            return (lat, failed, Some(e));
        }
        // Bound the session capture: session capture can combine pending
        // transactions, so without a durability boundary the 400-row blast
        // would sign as one capture past the 64 KiB replication budget
        // (Go captures per HTTP write; see the README deviation).
        if (i + 1) % 10 == 0 {
            if let Err(e) = cluster.node(0).simple("MURMUR FLUSH").await {
                failed += 1;
                return (
                    lat,
                    failed,
                    Some(format!("blast flush: {}", murmur_live_harness::db_message(&e))),
                );
            }
        }
    }
    (lat, failed, None)
}

async fn run_body(cluster: &Cluster) -> Result<(), String> {
    // Same schema on every node (Go: TypedRecords fixture tables), flagged
    // replicated on each node (row convergence needs the per-node flag).
    for i in 0..cluster.len() {
        cluster
            .ensure_migrated(
                i,
                "CREATE TABLE records(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL, phone TEXT NOT NULL, score INTEGER NOT NULL)",
            )
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

    // Past the 64 KiB commit budget: clean rejection naming the budget,
    // no partial row. (Go sends a 5 KiB value against a 4 KiB per-value
    // budget; Rust has no per-value limit, so one insert carries a value
    // past the transaction budget instead.)
    cluster
        .node(0)
        .simple("MURMUR FLUSH")
        .await
        .map_err(|e| format!("flush before oversize: {e}"))?;
    let big = "V".repeat(TX_BUDGET + 1);
    let big_id = format!("{:032x}", 2);
    let phone = String::new();
    let score = 0_i64;
    let err = cluster
        .node(0)
        .execute(
            "INSERT INTO records(id, name, phone, score) VALUES($1, $2, $3, $4)",
            &[&big_id, &big, &phone, &score],
        )
        .await
        .expect_err("oversize value accepted, want budget rejection");
    // `Display` is a bare `db error`; the budget text lives in the
    // source chain (`54000: transaction/session capture budget exceeded`).
    assert!(
        murmur_live_harness::db_message(&err).contains("budget exceeded"),
        "rejection error {err:?} does not name the budget"
    );
    for i in 0..cluster.len() {
        let n = row_count(cluster, i).await?;
        if n != 0 {
            return Err(format!("node{} has {n} rows after rejection, want 0 (partial apply?)", i + 1));
        }
    }

    // Writer not poisoned: a small write works and converges. The
    // rejection leaves momentary queue pressure (`40001`, documented
    // back-off-and-retry), so the probe retries it.
    insert_retry(cluster, 0, format!("{:032x}", 3), "ok".into()).await?;
    cluster
        .node(0)
        .simple("MURMUR FLUSH")
        .await
        .map_err(|e| format!("flush after post-rejection write: {e}"))?;
    wait_converged(cluster, 1, PROBE_TIMEOUT, "post-rejection probe").await?;

    // Pressure blast: concurrent writers converge with bounded tail.
    let [w0, w1, w2, w3, w4, w5, w6, w7] = [
        writer(cluster, 0),
        writer(cluster, 1),
        writer(cluster, 2),
        writer(cluster, 3),
        writer(cluster, 4),
        writer(cluster, 5),
        writer(cluster, 6),
        writer(cluster, 7),
    ];
    let (r0, r1, r2, r3, r4, r5, r6, r7) = tokio::join!(w0, w1, w2, w3, w4, w5, w6, w7);
    let mut lat = Vec::with_capacity(WRITERS * PER_WRITER);
    let mut failed_writes = 0_u64;
    let mut first_error = None;
    for (mut l, f, e) in [r0, r1, r2, r3, r4, r5, r6, r7] {
        lat.append(&mut l);
        failed_writes += f;
        if first_error.is_none() {
            first_error = e;
        }
    }
    if failed_writes != 0 {
        return Err(format!("{failed_writes} blast writes failed: {first_error:?}"));
    }
    if lat.len() != WRITERS * PER_WRITER {
        return Err(format!("blast recorded {} latencies, want {}", lat.len(), WRITERS * PER_WRITER));
    }
    lat.sort();
    let p99 = lat[(lat.len() as f64 * 0.99) as usize];
    eprintln!(
        "blast: {} inserts p50={:?} p99={p99:?} max={:?}",
        lat.len(),
        lat[lat.len() / 2],
        lat[lat.len() - 1]
    );
    if p99 > P99_BOUND {
        return Err(format!("blast p99 {p99:?} exceeds {P99_BOUND:?} bound"));
    }
    wait_converged(cluster, 1 + WRITERS * PER_WRITER, BLAST_TIMEOUT, "blast").await?;
    wait_node_ready(cluster, 0, READY_TIMEOUT).await?;
    wait_node_ready(cluster, 1, READY_TIMEOUT).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn budget_rejection_and_pressure_convergence() {
    let cluster = Cluster::new(ClusterOptions {
        name: "overload-budgets".into(),
        num_nodes: 2,
        replication: Some(ReplicationOptions {
            max_transaction_bytes: Some(TX_BUDGET),
            ..Default::default()
        }),
        sqlite: Some(SqliteOptions {
            transaction_bytes: Some(TX_BUDGET),
            queue_bytes: Some(QUEUE_BYTES),
            capture_bytes: Some(CAPTURE_BYTES),
            ..Default::default()
        }),
        ..Default::default()
    })
    .await
    .expect("cluster setup");
    assert_eq!(cluster.len(), 2, "this scenario is written for 2 nodes");
    let result = run_body(&cluster).await;
    let failed = result.is_err();
    if let Err(e) = &result {
        eprintln!("scenario failed: {e}");
    }
    if failed {
        cluster.dump_forensics("overload-budgets-no-convergence").await;
    }
    cluster.finish(failed).await;
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}
