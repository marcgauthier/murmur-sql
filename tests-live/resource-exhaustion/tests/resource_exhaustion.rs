//! Rust port of the Go `resource-exhaustion` live scenario (3 of 5 legs).
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/resource-exhaustion/`
//! (`oom_test.go`, `fd_test.go`, `compaction_test.go`, `slow_peer_test.go`,
//! `proxy_test.go`, `huge_txn_test.go`, `helpers_test.go`). Ported legs:
//! stalled compaction stays correct, slow peer converges without wedging
//! (userspace UDP throttle proxies), and huge transactions at default
//! budgets. The near-OOM and fd-exhaustion legs are NOT portable
//! (per-process `GOMEMLIMIT`/cgroup/prlimit mechanisms plus a victims-only
//! squeeze and, for fd, held TLS connections to an HTTPS API that does not
//! exist in Rust); see the scenario README.md.
//!
//! Nodes are `murmur-server` child processes driven over the PostgreSQL
//! wire. Every leg demands the same contract: fail closed under pressure,
//! stay exactly converged after.

use std::collections::HashMap;
use std::net::{SocketAddr, UdpSocket};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender, TryRecvError};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use murmur_live_harness::{wait_until_async, Cluster, ClusterOptions, ReplicationOptions, SpoolOptions};

/// Contention table DDL (Go: shared `live_typed_contention` fixture).
const TABLE_DDL: &str = "CREATE TABLE contention(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL)";
/// Count poll interval (Go: 500 ms `waitCounts`).
const COUNT_POLL: Duration = Duration::from_millis(500);
/// Digest poll interval (Go: 1 s `waitDigests`).
const DIGEST_POLL: Duration = Duration::from_secs(1);
/// Point-read poll interval (Go: 200 ms `readValue`).
const READ_POLL: Duration = Duration::from_millis(200);

/// Deterministic xorshift64* (Go legs use seeded `math/rand` for
/// incompressible values; this is its dependency-free equivalent).
struct XorShift(u64);

impl XorShift {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    /// `n` distinct incompressible alphanumeric strings of `size` bytes
    /// (Go: `uniqueRandomValues`; repeats would compress away and the
    /// pressure would never bind).
    fn alnum_strings(&mut self, n: usize, size: usize) -> Vec<String> {
        const ALPHABET: &[u8] =
            b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        (0..n)
            .map(|_| {
                (0..size)
                    .map(|_| ALPHABET[(self.next() % ALPHABET.len() as u64) as usize] as char)
                    .collect()
            })
            .collect()
    }
}

/// Same schema on every node (Go: typed contention fixture tables),
/// flagged replicated on each node (row convergence needs the per-node
/// flag).
async fn migrate_all(cluster: &Cluster) -> Result<(), String> {
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
            .simple("MURMUR FLUSH")
            .await
            .map_err(|e| format!("node{} flush after SET TABLE: {e}", i + 1))?;
    }
    Ok(())
}

/// Ordered contention rows on one node, or `None` when unreadable (wait
/// loops retry on `None`, distinguishing unreadable nodes from true
/// content divergence like Go's `waitDigests`).
async fn try_rows(cluster: &Cluster, idx: usize) -> Option<Vec<(String, String)>> {
    let db_rows = cluster.node(idx).query("SELECT id, name FROM contention ORDER BY id", &[]).await.ok()?;
    let mut out = Vec::with_capacity(db_rows.len());
    for row in &db_rows {
        out.push((row.try_get::<_, String>(0).ok()?, row.try_get::<_, String>(1).ok()?));
    }
    Some(out)
}

/// Ordered contention rows on one node (Go: `rrRows`); errors when unreadable.
async fn rows(cluster: &Cluster, idx: usize) -> Result<Vec<(String, String)>, String> {
    try_rows(cluster, idx)
        .await
        .ok_or_else(|| format!("node{} read contention rows failed", idx + 1))
}

/// Row count on one node (Go: `rxCount`).
async fn row_count(cluster: &Cluster, idx: usize) -> Result<usize, String> {
    Ok(rows(cluster, idx).await?.len())
}

/// Insert one row, erroring on failure.
async fn insert_row(cluster: &Cluster, idx: usize, id: String, name: String) -> Result<(), String> {
    cluster
        .node(idx)
        .execute("INSERT INTO contention(id, name) VALUES($1, $2)", &[&id, &name])
        .await
        .map_err(|e| format!("node{} insert failed: {e}", idx + 1))?;
    cluster
        .node(idx)
        .simple("MURMUR FLUSH")
        .await
        .map_err(|e| format!("node{} flush after insert failed: {e}", idx + 1))?;
    Ok(())
}

/// Insert one row, retrying replayable `40001` pressure (post-rejection
/// probe only; all other writers stay single-shot).
async fn insert_row_retry(cluster: &Cluster, idx: usize, id: String, name: String) -> Result<(), String> {
    cluster
        .node(idx)
        .execute_retry(
            "INSERT INTO contention(id, name) VALUES($1, $2)",
            &[&id, &name],
            Duration::from_secs(30),
        )
        .await
        .map_err(|e| format!("node{} insert failed: {e}", idx + 1))?;
    cluster
        .node(idx)
        .simple("MURMUR FLUSH")
        .await
        .map_err(|e| format!("node{} flush after insert failed: {e}", idx + 1))?;
    Ok(())
}

/// Point read by id: `None` when missing (Go: `TypedContentionRead`).
async fn read_name(cluster: &Cluster, idx: usize, id: &str) -> Option<String> {
    let db_rows = cluster
        .node(idx)
        .query("SELECT name FROM contention WHERE id = $1", &[&id])
        .await
        .ok()?;
    db_rows.first()?.try_get::<_, String>(0).ok()
}

/// Poll until every listed node reports `want` rows (Go: `waitCounts`).
async fn wait_counts(cluster: &Cluster, nodes: &[usize], want: usize, timeout: Duration) -> Result<(), String> {
    let converged = wait_until_async(timeout, COUNT_POLL, || async {
        for i in nodes {
            if try_rows(cluster, *i).await.is_none_or(|r| r.len() != want) {
                return false;
            }
        }
        true
    })
    .await;
    if !converged {
        return Err(format!("row count did not reach {want} on nodes {nodes:?} within {timeout:?}"));
    }
    Ok(())
}

/// Poll until every listed node reports the same ordered digest; returns a
/// summary for logging (Go: `waitDigests`; ordered-vec equality replaces
/// the SHA-256 digest with identical strength).
async fn wait_digests(cluster: &Cluster, nodes: &[usize], timeout: Duration) -> Result<String, String> {
    let mut last_err: Option<String> = None;
    let mut last: HashMap<usize, String> = HashMap::new();
    let deadline = tokio::time::Instant::now() + timeout;
    let converged = loop {
        let mut first: Option<Vec<(String, String)>> = None;
        let mut ok = true;
        for i in nodes {
            match try_rows(cluster, *i).await {
                None => {
                    last_err = Some(format!("node {}", i + 1));
                    ok = false;
                    break;
                }
                Some(got) => {
                    last.insert(*i, format!("{} rows", got.len()));
                    match &first {
                        None => first = Some(got),
                        Some(prev) if *prev == got => {}
                        _ => {
                            last_err = None;
                            ok = false;
                            break;
                        }
                    }
                }
            }
        }
        if ok {
            break true;
        }
        if tokio::time::Instant::now() >= deadline {
            break false;
        }
        tokio::time::sleep(DIGEST_POLL).await;
    };
    if !converged {
        if let Some(who) = last_err {
            return Err(format!("digests unreadable on nodes {nodes:?} within {timeout:?}: last error {who}"));
        }
        return Err(format!("digest divergence on nodes {nodes:?} within {timeout:?}: {last:?}"));
    }
    let n = try_rows(cluster, nodes[0]).await.map(|r| r.len()).unwrap_or(0);
    Ok(format!("{n} rows identical on nodes {nodes:?}"))
}

/// Poll until `id` is readable on `idx` and return its value (Go: `readValue`).
async fn read_value(cluster: &Cluster, idx: usize, id: &str, timeout: Duration) -> Result<String, String> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(v) = read_name(cluster, idx, id).await {
            return Ok(v);
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!("value {id} never readable on node {}", idx + 1));
        }
        tokio::time::sleep(READ_POLL).await;
    }
}

/// Count encrypted spool segment files under `dir` (recursive), an
/// observable proxy for compaction debt (Go: `countSegments`).
fn count_segments(dir: &Path) -> usize {
    fn walk(dir: &Path, n: &mut usize) {
        let entries = match std::fs::read_dir(dir) {
            Ok(e) => e,
            Err(_) => return,
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, n);
            } else if path
                .file_name()
                .is_some_and(|f| f.to_string_lossy().ends_with(".spool"))
            {
                *n += 1;
            }
        }
    }
    let mut n = 0;
    walk(dir, &mut n);
    n
}

/// Update one row's value with no durability barrier (Go:
/// `TypedContentionUpdate` on `name`); the churn loop flushes every
/// [`CHURN_FLUSH_EVERY`] updates instead of per-write.
async fn update_value(cluster: &Cluster, idx: usize, id: &str, name: String) -> Result<(), String> {
    let changed = cluster
        .node(idx)
        .execute("UPDATE contention SET name = $1 WHERE id = $2", &[&name, &id])
        .await
        .map_err(|e| format!("node{} update failed: {e}", idx + 1))?;
    if changed != 1 {
        return Err(format!("node{} update hit {changed} rows", idx + 1));
    }
    Ok(())
}

/// Churn-loop durability batching (see `run_compact`).
const CHURN_FLUSH_EVERY: usize = 20;
/// Max batches the victim may run ahead of the control's applied
/// counter (see `run_compact`).
const CHURN_AHEAD_LAG: u64 = 5;
/// Per-batch catch-up wait while pacing (see `run_compact`).
const CHURN_CATCHUP_TIMEOUT: Duration = Duration::from_secs(120);
/// Catch-up poll interval.
const CHURN_CATCHUP_POLL: Duration = Duration::from_millis(200);

/// Cumulative log-applied transactions on one node.
async fn applied(cluster: &Cluster, idx: usize) -> Result<u64, String> {
    let rows = cluster
        .node(idx)
        .simple("MURMUR SHOW REPLICATION")
        .await
        .map_err(|e| format!("node{} SHOW REPLICATION: {e}", idx + 1))?;
    let row = rows
        .first()
        .ok_or_else(|| format!("node{} SHOW REPLICATION returned no rows", idx + 1))?;
    row.get(5)
        .ok_or_else(|| format!("node{} SHOW REPLICATION has no applied column", idx + 1))?
        .parse::<u64>()
        .map_err(|e| format!("node{} parse applied: {e}", idx + 1))
}

async fn run_compact(cluster: &mut Cluster, updates: usize) -> Result<(), String> {
    migrate_all(cluster).await?;

    // Fixed 1-based key set: every update supersedes the previous
    // version, so sealed segments go fully dead and are collectible.
    const KEYS: usize = 200;
    // 64 KiB values (Go: same): ~141 MiB logical churn lands ~420 MiB
    // of spool with write overhead, sealing ~6 victim segments while
    // the 256 MiB control still holds ~2.
    const VAL_BYTES: usize = 64 << 10;
    let mut rng = XorShift(1);
    for i in 0..KEYS {
        let id = format!("{:032x}", i + 1);
        let val = rng.alnum_strings(1, VAL_BYTES).pop().unwrap();
        cluster
            .node(1)
            .execute("INSERT INTO contention(id, name) VALUES($1, $2)", &[&id, &val])
            .await
            .map_err(|e| format!("node2 insert failed: {e}"))?;
        // Same batching as the churn loop (one barrier per 20 writes).
        if (i + 1) % CHURN_FLUSH_EVERY == 0 || i + 1 == KEYS {
            cluster
                .node(1)
                .simple("MURMUR FLUSH")
                .await
                .map_err(|e| format!("node2 flush after insert batch failed: {e}"))?;
        }
    }
    wait_counts(cluster, &[0, 1], KEYS, Duration::from_secs(60)).await?;

    // Churn through the victim so its sealed-segment debt is its own
    // writes (plus what replicates back); probe responsiveness along
    // the way. Each batch paces to the control's applied counter with
    // a bounded lag so the debug-build control never faces an
    // unbounded backlog (background retention is stalled for this leg,
    // so the backlog is pruned only by the explicit drain collects).
    let applied0 = applied(cluster, 0).await?;
    let mut batches: u64 = 0;
    for i in 0..updates {
        let id = format!("{:032x}", i % KEYS + 1);
        let val = rng.alnum_strings(1, VAL_BYTES).pop().unwrap();
        update_value(cluster, 1, &id, val).await?;
        // One durability barrier per 20 updates instead of Go's
        // per-write capture: the same bytes and supersede pattern (each
        // ~1.3 MiB capture stays far under the 64 MiB budget), but 2000
        // per-update barriers would cost ~700 s alone against the 600 s
        // run cap. Unflushed updates are still locally visible, so the
        // responsiveness probes below are unaffected.
        if (i + 1) % CHURN_FLUSH_EVERY == 0 || i + 1 == updates {
            cluster
                .node(1)
                .simple("MURMUR FLUSH")
                .await
                .map_err(|e| format!("node2 flush after churn batch failed: {e}"))?;
            batches += 1;
            // A flush ships its pending session as >= 1 capture, so the
            // control's counter advances >= 1 per batch (background
            // ticks may split a batch into more; `>=` tolerates that).
            let want = applied0 + batches.saturating_sub(CHURN_AHEAD_LAG);
            let deadline = tokio::time::Instant::now() + CHURN_CATCHUP_TIMEOUT;
            loop {
                if applied(cluster, 0).await? >= want {
                    break;
                }
                if tokio::time::Instant::now() >= deadline {
                    return Err(format!(
                        "control did not apply batch {batches} within {CHURN_CATCHUP_TIMEOUT:?} (want applied >= {want})"
                    ));
                }
                tokio::time::sleep(CHURN_CATCHUP_POLL).await;
            }
        }
        if (i + 1) % 500 == 0 {
            let n = row_count(cluster, 1).await?;
            eprintln!(
                "churn {}/{} updates; victim segments={} control segments={} rows={n}",
                i + 1,
                updates,
                count_segments(&cluster.node_info(1).dir),
                count_segments(&cluster.node_info(0).dir),
            );
        }
    }

    // Stall proof: the victim must pile up observably more sealed
    // segments than the control.
    let stalled = wait_until_async(Duration::from_secs(120), Duration::from_secs(2), || async {
        let (v, c) = (
            count_segments(&cluster.node_info(1).dir),
            count_segments(&cluster.node_info(0).dir),
        );
        v >= 3 && v > c
    })
    .await;
    let (v, c) = (
        count_segments(&cluster.node_info(1).dir),
        count_segments(&cluster.node_info(0).dir),
    );
    if !stalled {
        return Err(format!("no compaction debt: victim segments={v} control segments={c}"));
    }
    eprintln!("stall proven: victim segments={v} control segments={c}");

    // Correct under stall: exact convergence with the control node.
    wait_counts(cluster, &[0, 1], KEYS, Duration::from_secs(180)).await?;
    let d = wait_digests(cluster, &[0, 1], Duration::from_secs(180)).await?;
    eprintln!("converged under stall: {d}");

    // Re-enable reclamation (drop the storage override) and restart
    // on the same data dir, then collect retention until fully-dead
    // segments drain. The pre-restart count is the baseline: sampling
    // after the restart would race the drain itself.
    let before = count_segments(&cluster.node_info(1).dir);
    cluster.stop_node(1).await;
    cluster
        .set_spool_options(1, None)
        .map_err(|e| format!("drop victim spool override: {e}"))?;
    cluster.start_node(1).await.map_err(|e| format!("restart victim: {e}"))?;
    cluster.mesh().await.map_err(|e| format!("re-mesh after restart: {e}"))?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(240);
    // Retention collection drives the internal reclaim that deletes
    // fully-dead files (Go triggers GC passes the same way). Poll the
    // count every 2 s but COLLECT at most every 10 s: a collection
    // scans hundreds of MiB in debug builds, so a coarse sleep would
    // sit through the drop it just caused.
    let mut last_collect: Option<tokio::time::Instant> = None;
    let drained = loop {
        if count_segments(&cluster.node_info(1).dir) < before {
            break true;
        }
        if tokio::time::Instant::now() >= deadline {
            break false;
        }
        if last_collect.is_none_or(|t| t.elapsed() >= Duration::from_secs(10)) {
            let _ = cluster.node(1).simple("MURMUR RETENTION COLLECT").await;
            last_collect = Some(tokio::time::Instant::now());
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    };
    let after = count_segments(&cluster.node_info(1).dir);
    if !drained {
        return Err(format!(
            "debt did not drain after re-enable: segments stuck at {after} (was {before})"
        ));
    }
    eprintln!("debt draining: victim segments {before} -> {after}");

    wait_counts(cluster, &[0, 1], KEYS, Duration::from_secs(180)).await?;
    let d2 = wait_digests(cluster, &[0, 1], Duration::from_secs(180)).await?;
    if d2 != d {
        return Err(format!("digest moved across compaction drain: {d} -> {d2}"));
    }
    eprintln!("PASS: stall survived, debt drained, digest stable {d2}");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stalled_compaction_stays_correct() {
    // 1000 updates (Go: 2000): the update count is a means to pile
    // debt, not a pass criterion. ~77 MiB of churn lands ~250 MiB of
    // spool, sealing ~4 victim segments while the control holds ~1;
    // 2000 would cost past the 600 s run cap at debug-build flush
    // throughput with no additional proof.
    let updates: usize = std::env::var("MURMUR_RX_COMPACT_ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|v| *v > 0)
        .unwrap_or(1000);

    let mut cluster = Cluster::new(ClusterOptions {
        name: "resource-compact".into(),
        num_nodes: 2,
        replication: Some(ReplicationOptions {
            retention: Some(Duration::from_secs(1)),
            minimum_retained: Some(10),
            maximum_offline: Some(Duration::from_secs(20)),
            // Stall background reclamation for the whole run (Go:
            // `ReclaimIntervalSeconds: 3600` on the victim): without a
            // stall, working reclamation deletes fully-dead files as
            // fast as churn seals them, so no debt ever piles and the
            // stall proof below is vacuous. All retention checkpoints,
            // prunes, and physical reclaims are collect-gated (the
            // capture-path snapshot thresholds, 128 deltas / 256 MiB,
            // never fire on this workload), so an hourly cadence piles
            // debt deterministically on both nodes. The drain loop
            // drives pruning and reclamation explicitly with
            // `MURMUR RETENTION COLLECT`, which ignores this cadence.
            retention_interval: Some(Duration::from_secs(3600)),
            ..Default::default()
        }),
        // 66 MiB victim segments pile ~4 sealed files over the ~77 MiB
        // churn while the 256 MiB control holds ~1 (Go: `SpoolByNode`
        // with 32 MiB victim segments, which the Rust product rejects:
        // `segment_bytes` must exceed 64 MiB; spool overhead lands ~3x
        // logical bytes, so 66 MiB still separates cleanly; see README).
        spool_by_node: HashMap::from([(
            1,
            SpoolOptions {
                segment_bytes: Some(66 << 20),
            },
        )]),
        ..Default::default()
    })
    .await
    .expect("cluster setup");
    assert_eq!(cluster.len(), 2, "this scenario is written for 2 nodes");
    let result = run_compact(&mut cluster, updates).await;
    let failed = result.is_err();
    if let Err(e) = &result {
        eprintln!("scenario failed: {e}");
    }
    if failed {
        cluster.dump_forensics("resource-compact-no-convergence").await;
    }
    cluster.finish(failed).await;
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}

/// Token-bucket pacer: sustained `rate` bytes per second with a `burst`
/// allowance. The burst absorbs QUIC's windowed bursts without loss; only
/// sustained overload is paced (Go: `bucket`).
struct Bucket {
    inner: Mutex<BucketState>,
}

struct BucketState {
    rate: f64,
    burst: f64,
    tokens: f64,
    last: Instant,
}

impl Bucket {
    fn new(rate: usize, burst: usize) -> Self {
        Self {
            inner: Mutex::new(BucketState {
                rate: rate as f64,
                burst: burst as f64,
                tokens: burst as f64,
                last: Instant::now(),
            }),
        }
    }

    /// Block until `n` bytes fit the bucket (Go: `pace`).
    fn pace(&self, n: usize) {
        loop {
            let sleep = {
                let mut b = self.inner.lock().expect("bucket lock");
                let now = Instant::now();
                b.tokens += now.duration_since(b.last).as_secs_f64() * b.rate;
                if b.tokens > b.burst {
                    b.tokens = b.burst;
                }
                b.last = now;
                if b.tokens >= n as f64 {
                    b.tokens -= n as f64;
                    None
                } else {
                    let deficit = n as f64 - b.tokens;
                    b.tokens = 0.0;
                    Some(Duration::from_secs_f64(deficit / b.rate))
                }
            };
            match sleep {
                None => return,
                Some(d) => std::thread::sleep(d),
            }
        }
    }
}

struct QueuedDatagram {
    dst: Option<Arc<UdpSocket>>,
    peer: Option<SocketAddr>,
    data: Vec<u8>,
}

/// Forwarding counters so a stall can be attributed to the proxy (flows
/// frozen) or the endpoints (bytes flow, rows stall) (Go: `ProxyStats`).
#[derive(Clone, Copy, Default)]
struct ProxyStats {
    fwd_packets: u64,
    fwd_bytes: u64,
    fwd_drops: u64,
    bwd_packets: u64,
    bwd_bytes: u64,
    bwd_drops: u64,
}

/// Transparent UDP forwarder with a token-bucket rate limit in BOTH
/// directions: emulates a slow link the way tc-tbf does, including
/// drop-tail on queue overflow (Go: `throttleProxy`). QUIC identifies
/// connections by connection ID, so NAT-style forwarding (one
/// server-side socket per client address) is transparent to both ends.
///
/// Two properties are load-bearing for QUIC stability (same as Go):
/// 1. Read loops never block on pacing (reads enqueue; sender threads
///    pace the drain). 2. Queues are bounded with drop-tail (drops are
///    the backpressure signal congestion control is designed for).
struct ThrottleProxy {
    inner: Arc<ProxyInner>,
}

struct ProxyInner {
    listener: UdpSocket,
    target: SocketAddr,
    servers: Mutex<HashMap<String, Arc<UdpSocket>>>,
    closed: AtomicBool,
    fwd_hi: SyncSender<QueuedDatagram>,
    fwd_lo: SyncSender<QueuedDatagram>,
    bwd_hi: SyncSender<QueuedDatagram>,
    bwd_lo: SyncSender<QueuedDatagram>,
    fwd_bucket: Bucket,
    bwd_bucket: Bucket,
    stats: Mutex<ProxyStats>,
}

/// Queue depth per lane (Go: `proxyQueueDepth = 32`; burst absorption
/// comes from the token bucket, not the queue).
const PROXY_QUEUE_DEPTH: usize = 32;
/// Token-bucket burst allowance per direction (Go: 256 KiB).
const PROXY_BURST_BYTES: usize = 256 << 10;
/// Small-packet priority cutoff (Go: QUIC ACKs/keepalives take the
/// priority lane so bulk data cannot starve the ACK clock).
const PROXY_PRIORITY_CUTOFF: usize = 300;

impl ThrottleProxy {
    fn start(target: SocketAddr, rate: usize) -> Self {
        let listener = UdpSocket::bind("127.0.0.1:0").expect("proxy listen");
        listener
            .set_read_timeout(Some(Duration::from_millis(100)))
            .expect("proxy read timeout");
        // NOTE: Go sets a 4 MiB socket read buffer; std has no read-buffer
        // API, so kernel defaults apply (any kernel drops under burst read
        // as ordinary loss, which the 90 s wedge trip still guards).
        let (fwd_hi_tx, fwd_hi_rx) = sync_channel(PROXY_QUEUE_DEPTH);
        let (fwd_lo_tx, fwd_lo_rx) = sync_channel(PROXY_QUEUE_DEPTH);
        let (bwd_hi_tx, bwd_hi_rx) = sync_channel(PROXY_QUEUE_DEPTH);
        let (bwd_lo_tx, bwd_lo_rx) = sync_channel(PROXY_QUEUE_DEPTH);
        let inner = Arc::new(ProxyInner {
            listener,
            target,
            servers: Mutex::new(HashMap::new()),
            closed: AtomicBool::new(false),
            fwd_hi: fwd_hi_tx,
            fwd_lo: fwd_lo_tx,
            bwd_hi: bwd_hi_tx,
            bwd_lo: bwd_lo_tx,
            fwd_bucket: Bucket::new(rate, PROXY_BURST_BYTES),
            bwd_bucket: Bucket::new(rate, PROXY_BURST_BYTES),
            stats: Mutex::new(ProxyStats::default()),
        });
        // Serve/sender/backward loops all poll `closed` with <= 100 ms
        // timeouts, so every thread exits promptly after `close` (and via
        // `Drop` on panic paths); none need joining.
        std::thread::spawn({
            let inner = inner.clone();
            move || Self::serve(inner)
        });
        std::thread::spawn({
            let inner = inner.clone();
            move || Self::sender(inner, fwd_hi_rx, fwd_lo_rx, true)
        });
        std::thread::spawn({
            let inner = inner.clone();
            move || Self::sender(inner, bwd_hi_rx, bwd_lo_rx, false)
        });
        Self { inner }
    }

    /// Proxy listen address (what slow peers dial through).
    fn addr(&self) -> SocketAddr {
        self.inner.listener.local_addr().expect("proxy addr")
    }

    fn stats(&self) -> ProxyStats {
        *self.inner.stats.lock().expect("proxy stats")
    }

    fn close(&self) {
        self.inner.closed.store(true, Ordering::Relaxed);
    }

    /// Enqueue a datagram with drop-tail on overflow; small packets take
    /// the priority lane (Go: `enqueue`; priority still consumes wire
    /// tokens — it is ordering, not free bandwidth).
    fn enqueue(inner: &ProxyInner, d: QueuedDatagram, fwd: bool) {
        let q = if fwd {
            if d.data.len() <= PROXY_PRIORITY_CUTOFF {
                &inner.fwd_hi
            } else {
                &inner.fwd_lo
            }
        } else if d.data.len() <= PROXY_PRIORITY_CUTOFF {
            &inner.bwd_hi
        } else {
            &inner.bwd_lo
        };
        if q.try_send(d).is_err() {
            let mut stats = inner.stats.lock().expect("proxy stats");
            if fwd {
                stats.fwd_drops += 1;
            } else {
                stats.bwd_drops += 1;
            }
        }
    }

    fn serve(inner: Arc<ProxyInner>) {
        let mut buf = vec![0u8; 65535];
        loop {
            if inner.closed.load(Ordering::Relaxed) {
                return;
            }
            let (n, client) = match inner.listener.recv_from(&mut buf) {
                Ok(v) => v,
                Err(_) => continue, // timeout (or closed): re-check flag
            };
            let datagram = buf[..n].to_vec();
            let server = {
                let mut servers = inner.servers.lock().expect("proxy servers");
                if inner.closed.load(Ordering::Relaxed) {
                    return;
                }
                match servers.get(&client.to_string()).cloned() {
                    Some(s) => s,
                    None => {
                        let sock = match UdpSocket::bind("127.0.0.1:0") {
                            Ok(s) => s,
                            Err(e) => {
                                eprintln!("proxy: bind server socket: {e}");
                                continue;
                            }
                        };
                        if let Err(e) = sock.connect(inner.target) {
                            eprintln!("proxy: dial target: {e}");
                            continue;
                        }
                        if sock.set_read_timeout(Some(Duration::from_millis(100))).is_err() {
                            continue;
                        }
                        let sock = Arc::new(sock);
                        servers.insert(client.to_string(), sock.clone());
                        let relay = sock.clone();
                        std::thread::spawn({
                            let inner = inner.clone();
                            move || Self::backward(inner, relay, client)
                        });
                        sock
                    }
                }
            };
            Self::enqueue(
                &inner,
                QueuedDatagram {
                    dst: Some(server),
                    peer: None,
                    data: datagram,
                },
                true,
            );
        }
    }

    /// Relay target->client datagrams for one server-side socket into the
    /// paced backward queue (Go: `backward`; never times out logically —
    /// an idle relay survives so a quiet connection's return path works).
    fn backward(inner: Arc<ProxyInner>, server: Arc<UdpSocket>, client: SocketAddr) {
        let mut buf = vec![0u8; 65535];
        loop {
            if inner.closed.load(Ordering::Relaxed) {
                return;
            }
            match server.recv(&mut buf) {
                Ok(n) => Self::enqueue(
                    &inner,
                    QueuedDatagram {
                        dst: None,
                        peer: Some(client),
                        data: buf[..n].to_vec(),
                    },
                    false,
                ),
                Err(_) => continue, // timeout: re-check flag
            }
        }
    }

    /// Drain one direction's lanes through its token bucket, serving the
    /// priority lane first (Go: `sender`). std has no channel `select`,
    /// so an empty poll parks 1 ms; lane preference is otherwise exact.
    fn sender(
        inner: Arc<ProxyInner>,
        hi: Receiver<QueuedDatagram>,
        lo: Receiver<QueuedDatagram>,
        fwd: bool,
    ) {
        let bucket = if fwd { &inner.fwd_bucket } else { &inner.bwd_bucket };
        loop {
            if inner.closed.load(Ordering::Relaxed) {
                // Queues shut: pending items drop with the proxy.
                while hi.try_recv().is_ok() {}
                while lo.try_recv().is_ok() {}
                return;
            }
            let next = match hi.try_recv() {
                Ok(d) => Some(d),
                Err(TryRecvError::Empty) => match lo.try_recv() {
                    Ok(d) => Some(d),
                    Err(TryRecvError::Empty) => None,
                    Err(TryRecvError::Disconnected) => None,
                },
                Err(TryRecvError::Disconnected) => match lo.try_recv() {
                    Ok(d) => Some(d),
                    _ => None,
                },
            };
            let Some(d) = next else {
                std::thread::sleep(Duration::from_millis(1));
                continue;
            };
            bucket.pace(d.data.len());
            let wrote = if let Some(peer) = d.peer {
                inner.listener.send_to(&d.data, peer)
            } else if let Some(dst) = d.dst {
                dst.send(&d.data)
            } else {
                Ok(0)
            };
            if wrote.is_err() {
                return;
            }
            let mut stats = inner.stats.lock().expect("proxy stats");
            if fwd {
                stats.fwd_packets += 1;
                stats.fwd_bytes += d.data.len() as u64;
            } else {
                stats.bwd_packets += 1;
                stats.bwd_bytes += d.data.len() as u64;
            }
        }
    }
}

impl Drop for ThrottleProxy {
    fn drop(&mut self) {
        self.close();
    }
}

async fn run_slow(cluster: &Cluster, rate: usize, rows_n: usize, val_bytes: usize, bound: Duration) -> Result<(), String> {
    migrate_all(cluster).await?;

    // Both directions cross a throttle: replication data follows the
    // connection the receiver initiated, so throttling only one way
    // would leave the data path unthrottled.
    let (real0, real1) = (cluster.node_info(0).repl_addr, cluster.node_info(1).repl_addr);
    let to_slow = ThrottleProxy::start(real1, rate);
    let to_fast = ThrottleProxy::start(real0, rate);
    eprintln!(
        "throttle: node0 -> {} -> node1, node1 -> {} -> node0 at {rate} B/s",
        to_slow.addr(),
        to_fast.addr()
    );
    cluster
        .add_peer_addr(0, to_slow.addr())
        .await
        .map_err(|e| format!("add peer 0->1 via proxy: {e}"))?;
    cluster
        .add_peer_addr(1, to_fast.addr())
        .await
        .map_err(|e| format!("add peer 1->0 via proxy: {e}"))?;

    // Baseline through the throttle proves the proxied path works
    // before the timed leg starts.
    insert_row(cluster, 0, format!("{:032x}", 1), "baseline".into()).await?;
    wait_counts(cluster, &[1], 1, Duration::from_secs(120)).await?;
    eprintln!("baseline crossed the throttle");

    // Churn on the fast node; the slow node must make steady progress
    // (no wedge) until fully converged within the bound. Values are
    // unique incompressible bytes per row: repeats would compress away
    // and the throttle would never bind.
    let mut rng = XorShift(0x5eed);
    let vals = rng.alnum_strings(rows_n, val_bytes);
    for (i, val) in vals.into_iter().enumerate() {
        insert_row(cluster, 0, format!("{:032x}", i + 2), val).await?;
    }
    let total = rows_n + 1;
    let converge_start = Instant::now();
    let deadline = Instant::now() + bound;
    let mut last = 0;
    let mut last_increase = Instant::now();
    let mut stalled = false;
    loop {
        let n = row_count(cluster, 1).await?;
        if n > last {
            last = n;
            last_increase = Instant::now();
        }
        if n == total {
            break;
        }
        if last_increase.elapsed() > Duration::from_secs(90) {
            stalled = true;
            break;
        }
        if Instant::now() > deadline {
            break;
        }
        let s1 = to_slow.stats();
        let s2 = to_fast.stats();
        eprintln!(
            "slow node {n}/{total} rows; toSlow fwd {}B({}d) bwd {}B({}d) | toFast fwd {}B({}d) bwd {}B({}d)",
            s1.fwd_bytes, s1.fwd_drops, s1.bwd_bytes, s1.bwd_drops,
            s2.fwd_bytes, s2.fwd_drops, s2.bwd_bytes, s2.bwd_drops
        );
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
    if stalled {
        return Err(format!("WEDGE: slow node stuck at {last}/{total} rows for 90s"));
    }
    if last != total {
        return Err(format!("slow node reached {last}/{total} rows within {bound:?}"));
    }
    let took = converge_start.elapsed();
    // Byte-level proof the data crossed the throttles: ~1 MB of
    // incompressible values must have traversed the slow paths (data
    // may flow through either proxy in either direction). This is the
    // non-vacuous guard: a fast pass with few throttled bytes means
    // traffic bypassed the proxies and the test is broken.
    let (s1, s2) = (to_slow.stats(), to_fast.stats());
    let throttled = s1.fwd_bytes + s1.bwd_bytes + s2.fwd_bytes + s2.bwd_bytes;
    if throttled < (rows_n * val_bytes / 2) as u64 {
        return Err(format!(
            "only {throttled} throttled bytes for {rows_n} x {val_bytes}B rows: data bypassed the proxies?"
        ));
    }
    let d = wait_digests(cluster, &[0, 1], Duration::from_secs(60)).await?;
    eprintln!(
        "converged through {rate} B/s throttles in {took:?}: {total} rows, {throttled} throttled bytes ({} drops), {d}",
        s1.fwd_drops + s1.bwd_drops + s2.fwd_drops + s2.bwd_drops
    );

    // Heal: remove the throttles, re-peer direct, and prove fast
    // convergence is restored.
    to_slow.close();
    to_fast.close();
    cluster.add_peer(0, 1).await.map_err(|e| format!("re-peer 0->1 direct: {e}"))?;
    cluster.add_peer(1, 0).await.map_err(|e| format!("re-peer 1->0 direct: {e}"))?;
    cluster.sync_all().await.map_err(|e| format!("sync after heal: {e}"))?;
    for i in 0..10 {
        insert_row(cluster, 0, format!("{:032x}", total + i + 1), "healed".into()).await?;
    }
    wait_counts(cluster, &[0, 1], total + 10, Duration::from_secs(60)).await?;
    let d2 = wait_digests(cluster, &[0, 1], Duration::from_secs(60)).await?;
    eprintln!("PASS: healed to direct peering, {} rows, {d2}", total + 10);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_peer_converges_without_wedge() {
    let rate: usize = std::env::var("MURMUR_RX_SLOW_RATE")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|v| *v > 0)
        .unwrap_or(262144);
    let rows_n: usize = std::env::var("MURMUR_RX_SLOW_ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|v| *v > 0)
        .unwrap_or(600);
    let val_bytes: usize = std::env::var("MURMUR_RX_SLOW_VALBYTES")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|v| *v > 0)
        .unwrap_or(1024);
    let bound = Duration::from_secs(
        std::env::var("MURMUR_RX_SLOW_BOUND_SECONDS")
            .ok()
            .and_then(|v| v.parse().ok())
            .filter(|v| *v > 0)
            .unwrap_or(300),
    );

    let cluster = Cluster::new(ClusterOptions {
        name: "resource-slow".into(),
        num_nodes: 2,
        manual_peers: true,
        ..Default::default()
    })
    .await
    .expect("cluster setup");
    assert_eq!(cluster.len(), 2, "this scenario is written for 2 nodes");
    let result = run_slow(&cluster, rate, rows_n, val_bytes, bound).await;
    let failed = result.is_err();
    if let Err(e) = &result {
        eprintln!("scenario failed: {e}");
    }
    if failed {
        cluster.dump_forensics("resource-slow-no-convergence").await;
    }
    cluster.finish(failed).await;
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}

async fn run_huge(cluster: &Cluster) -> Result<(), String> {
    migrate_all(cluster).await?;

    // (a) 8 MiB value: accepted, byte-exact on both nodes.
    let huge = "H".repeat(8 << 20);
    let huge_id = format!("{:032x}", 1);
    insert_row(cluster, 0, huge_id.clone(), huge.clone()).await?;
    for i in [0, 1] {
        let got = read_value(cluster, i, &huge_id, Duration::from_secs(120)).await?;
        if got.len() != huge.len() || got != huge {
            return Err(format!("node{} value mismatch: len={}", i + 1, got.len()));
        }
    }
    eprintln!("8 MiB value byte-exact on both nodes");

    // (b) Oversize statement: clean rejection naming the budget, no
    // partial row, writer healthy. (Go sends one 20 MiB value against
    // a 16 MiB per-value budget; Rust has no per-value limit, so one
    // statement carries a 64 MiB + 1 B value past the default 64 MiB
    // transaction budget instead — same fail-closed proof at default
    // budgets. A multi-row BEGIN/COMMIT cannot express this: wire
    // transaction words are accepted no-ops, so each statement would
    // autocommit and only trip retryable 40001 pressure mid-stream.)
    const TX_BUDGET: usize = 64 << 20;
    let over_id = format!("{:032x}", 1000);
    let over_val = "O".repeat(TX_BUDGET + 1);
    // `Display` is a bare `db error`; the budget text lives in the
    // source chain (`54000: ... budget exceeded`).
    match cluster
        .node(0)
        .execute(
            "INSERT INTO contention(id, name) VALUES($1, $2)",
            &[&over_id, &over_val],
        )
        .await
    {
        Ok(_) => return Err("oversize statement accepted, want budget rejection".into()),
        Err(e) => {
            let msg = murmur_live_harness::db_message(&e);
            if !msg.contains("budget exceeded") {
                return Err(format!("rejection error {msg:?} does not name the budget"));
            }
        }
    }
    for i in [0, 1] {
        let n = row_count(cluster, i).await?;
        if n != 1 {
            return Err(format!("node{} has {n} rows after rejection, want 1 (partial apply?)", i + 1));
        }
    }
    // The rejection leaves momentary queue pressure (`40001`,
    // documented back-off-and-retry), so the probe retries it.
    insert_row_retry(cluster, 0, format!("{:032x}", 3), "probe".into()).await?;
    eprintln!("oversize statement rejected as budget exceeded, no partial row, writer healthy");

    // (c) Sustained large-value load: 20 x 512 KiB converge exactly.
    for i in 0..20 {
        let v = format!("{}", i % 10).repeat(512 << 10);
        let node = i % 2;
        insert_row(cluster, node, format!("{:032x}", 100 + i), v).await?;
    }
    wait_counts(cluster, &[0, 1], 22, Duration::from_secs(180)).await?;
    let d = wait_digests(cluster, &[0, 1], Duration::from_secs(180)).await?;
    eprintln!("PASS: 22 rows (8MiB + probe + 20x512KiB) converged, {d}");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn huge_transactions_at_default_budgets() {
    // Production default budgets (Go: 16 MiB values, 64 MiB transactions;
    // Rust defaults: 64 MiB `transaction_bytes`, no per-value limit, so
    // the rejection leg trips the transaction budget instead — see above).
    let cluster = Cluster::new(ClusterOptions {
        name: "resource-huge".into(),
        num_nodes: 2,
        ..Default::default()
    })
    .await
    .expect("cluster setup");
    assert_eq!(cluster.len(), 2, "this scenario is written for 2 nodes");
    let result = run_huge(&cluster).await;
    let failed = result.is_err();
    if let Err(e) = &result {
        eprintln!("scenario failed: {e}");
    }
    if failed {
        cluster.dump_forensics("resource-huge-no-convergence").await;
    }
    cluster.finish(failed).await;
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}
