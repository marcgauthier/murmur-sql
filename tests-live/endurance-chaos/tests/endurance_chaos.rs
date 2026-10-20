//! Rust port of the Go `endurance-chaos` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/endurance-chaos/`
//! (`endurance_chaos_test.go`: `TestEnduranceChaos`, plus the
//! `faults_test.go` helpers and `statfs_*_test.go` free-space probes). A
//! live multi-node mesh absorbs continuous writes while rolling restarts,
//! peer flaps, disk pressure, online key rotation, snapshot resyncs, and
//! origin-log GC overlap for a configured chaos window. The verdict is
//! exact cross-node convergence (equal row counts plus identical
//! PK-ordered row contents) with counter-proven GC progress on every
//! node, at least one proven snapshot resync, successful key rotations, a
//! rejoined full mesh, and a post-chaos write. Plain SQL replaces the Go
//! typed-contention HTTP API; tc/netem impairment degrades to logical
//! peer flaps and libfaketime clock skew is skipped (see the scenario
//! README.md for the deviations).
//!
//! Nodes are `murmur-server` processes driven over the PostgreSQL wire
//! protocol. Writers, the heartbeat sampler, the disk cycler, and the
//! chaos director run as tokio tasks sharing the cluster through one
//! async mutex (the Go goroutine/thread structure); every cluster touch
//! locks, so lifecycle (stop/start) and reads never race.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use murmur_live_harness::{env_seconds, hex, Cluster, ClusterOptions, ReplicationOptions};
use tokio::sync::Mutex as AsyncMutex;

/// Shared cluster handle: every task locks for each cluster touch.
type Shared = Arc<AsyncMutex<Cluster>>;

/// Mesh-size clamp (Go: `min(max(nodes, 3), 16)`).
const MIN_NODES: usize = 3;
const MAX_NODES: usize = 16;
/// Writer-task cap (Go: `min(max(nodes, 2), 8)`).
const MAX_WRITERS: usize = 8;
/// Bounded working-set floor (Go: `maxRows < 100` is fatal).
const MIN_MAX_ROWS: usize = 100;
/// Snapshot offline-window floor (Go: `snapshotOff < 60` is fatal:
/// retention expiry + GC tick).
const MIN_SNAPSHOT_OFF: Duration = Duration::from_secs(60);
/// Baseline replication deadline (Go: 2 minutes).
const BASELINE_TIMEOUT: Duration = Duration::from_secs(120);
/// Director tick (Go: 1 s ticker).
const DIRECTOR_TICK: Duration = Duration::from_secs(1);
/// First impairment delay (Go: 15 s stagger after writers warm up).
const IMPAIR_WARMUP: Duration = Duration::from_secs(15);
/// Disk-loop stagger (Go: 30 s).
const DISK_WARMUP: Duration = Duration::from_secs(30);
/// Convergence poll interval (Go: 2 s).
const VERDICT_POLL: Duration = Duration::from_secs(2);
/// Full-mesh wait bound (Rust-only settle; Go asserts the mesh instantly
/// off daemon status endpoints).
const MESH_TIMEOUT: Duration = Duration::from_secs(60);
/// Snapshot catch-up observation bound (see `observe_snapshot_proof`):
/// the victim's own restart plus a donor build plus the chunk transfer can
/// take ~45 s under chaos (spool rescans dominate), and the victim can
/// lose the donor race to a stale donor (a concurrently catching-up
/// node), forcing a second snapshot cycle. The event-driven proof polls
/// up to 240 s so two full cycles fit instead of sampling once at a
/// fixed delay.
const SNAP_PROOF_TIMEOUT: Duration = Duration::from_secs(240);
/// Snapshot proof poll interval.
const SNAP_PROOF_POLL: Duration = Duration::from_secs(2);
/// Distinct error-message sample cap (Go: first 8).
const ERR_SAMPLE_CAP: usize = 8;
/// Stored error-message truncation (Go: 200 bytes).
const ERR_TRUNC_CHARS: usize = 200;
/// Headroom the disk filler must leave free (Go: 1 GiB).
const DISK_HEADROOM_BYTES: u64 = 1 << 30;
/// Minimum filler size before the disk leg degrades away (Go: 16 MiB).
const DISK_MIN_MB: u64 = 16;

/// usize env knob with fallback (Go: `harness.EnvInt`).
fn env_usize(name: &str, fallback: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(fallback)
}

/// u64 env knob with fallback (Go: `harness.EnvInt` for seeds/sizes).
fn env_u64(name: &str, fallback: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(fallback)
}

/// Millisecond-duration env knob with fallback.
fn env_millis(name: &str, fallback_ms: u64) -> Duration {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .map(Duration::from_millis)
        .unwrap_or_else(|| Duration::from_millis(fallback_ms))
}

/// Go-style duration rendering (`90s` -> `1m30s`) for progress lines.
fn fmt_dur(d: Duration) -> String {
    let s = d.as_secs();
    if s >= 3600 {
        format!("{}h{}m{}s", s / 3600, s % 3600 / 60, s % 60)
    } else if s >= 60 {
        format!("{}m{}s", s / 60, s % 60)
    } else if s >= 1 {
        format!("{s}s")
    } else {
        format!("{}ms", d.as_millis())
    }
}

/// Current unix time in seconds (report filename stamp).
fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Deterministic splitmix64 (dependency-free `math/rand` replacement).
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: usize) -> usize {
        assert!(n > 0, "rng range must be non-empty");
        (self.next() % n as u64) as usize
    }

    /// Uniform float in `[0, 1)` (53-bit precision).
    fn float(&mut self) -> f64 {
        ((self.next() >> 11) as f64) / ((1u64 << 53) as f64)
    }
}

/// Chaos configuration (Go: `enduranceConfig`; same env names/defaults).
struct Config {
    nodes: usize,
    duration: Duration,
    settle: Duration,
    write_interval: Duration,
    max_rows: usize,
    seed: u64,
    status_every: Duration,
    restart_every: Duration,
    restart_down: Duration,
    rotate_every: Duration,
    snapshot_every: Duration,
    snapshot_off: Duration,
    impair_on: Duration,
    impair_off: Duration,
    impair_latency_ms: u64,
    impair_loss_pct: u64,
    disk_every: Duration,
    disk_mb: u64,
    disk_hold: Duration,
    skew_dur: Duration,
}

fn load_config() -> Config {
    let cfg = Config {
        nodes: env_usize("MURMUR_ENDURANCE_NODES", 5).clamp(MIN_NODES, MAX_NODES),
        duration: env_seconds("MURMUR_ENDURANCE_DURATION_SECONDS", 600),
        settle: env_seconds("MURMUR_ENDURANCE_SETTLE_SECONDS", 300),
        write_interval: env_millis("MURMUR_ENDURANCE_WRITE_INTERVAL_MS", 25),
        max_rows: env_usize("MURMUR_ENDURANCE_MAX_ROWS", 10_000),
        seed: env_u64("MURMUR_ENDURANCE_SEED", 1),
        status_every: env_seconds("MURMUR_ENDURANCE_STATUS_SECONDS", 30),
        restart_every: env_seconds("MURMUR_ENDURANCE_RESTART_SECONDS", 90),
        restart_down: env_seconds("MURMUR_ENDURANCE_RESTART_DOWN_SECONDS", 8),
        rotate_every: env_seconds("MURMUR_ENDURANCE_ROTATE_SECONDS", 180),
        snapshot_every: env_seconds("MURMUR_ENDURANCE_SNAPSHOT_SECONDS", 300),
        snapshot_off: env_seconds("MURMUR_ENDURANCE_SNAPSHOT_OFFLINE_SECONDS", 75),
        impair_on: env_seconds("MURMUR_ENDURANCE_IMPAIR_ON_SECONDS", 60),
        impair_off: env_seconds("MURMUR_ENDURANCE_IMPAIR_OFF_SECONDS", 20),
        impair_latency_ms: env_u64("MURMUR_ENDURANCE_IMPAIR_LATENCY_MS", 150),
        impair_loss_pct: env_u64("MURMUR_ENDURANCE_IMPAIR_LOSS_PCT", 3),
        disk_every: env_seconds("MURMUR_ENDURANCE_DISK_SECONDS", 180),
        disk_mb: env_u64("MURMUR_ENDURANCE_DISK_MB", 128),
        disk_hold: env_seconds("MURMUR_ENDURANCE_DISK_HOLD_SECONDS", 60),
        skew_dur: env_seconds("MURMUR_ENDURANCE_SKEW_SECONDS", 120),
    };
    assert!(
        cfg.max_rows >= MIN_MAX_ROWS,
        "MURMUR_ENDURANCE_MAX_ROWS must be >= 100"
    );
    assert!(
        cfg.snapshot_off >= MIN_SNAPSHOT_OFF,
        "MURMUR_ENDURANCE_SNAPSHOT_OFFLINE_SECONDS must be >= 60 (retention expiry + GC tick)"
    );
    cfg
}

/// Executed-fault tallies (Go: `faultCounts`).
#[derive(Debug, Clone, Copy, Default)]
struct FaultCounts {
    restarts: u64,
    rotations: u64,
    impair_cycles: u64,
    disk_cycles: u64,
    snapshots: u64,
    skews: u64,
}

/// Live progress reporter (Go: `reporter`): timestamped phase/log lines to
/// stderr plus the same events for the final markdown report. All methods
/// are safe for concurrent use.
struct Reporter {
    start: Instant,
    events: Mutex<Vec<String>>,
    attempted: AtomicU64,
    succeeded: AtomicU64,
    failed: AtomicU64,
    faults: Mutex<FaultCounts>,
    rot_seq: AtomicU64,
    err_seen: Mutex<HashMap<String, usize>>,
    err_sample: Mutex<Vec<String>>,
}

impl Reporter {
    fn new() -> Self {
        Self {
            start: Instant::now(),
            events: Mutex::new(Vec::new()),
            attempted: AtomicU64::new(0),
            succeeded: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            faults: Mutex::new(FaultCounts::default()),
            rot_seq: AtomicU64::new(0),
            err_seen: Mutex::new(HashMap::new()),
            err_sample: Mutex::new(Vec::new()),
        }
    }

    fn attempted(&self) -> u64 {
        self.attempted.load(Ordering::SeqCst)
    }

    fn succeeded(&self) -> u64 {
        self.succeeded.load(Ordering::SeqCst)
    }

    fn failed(&self) -> u64 {
        self.failed.load(Ordering::SeqCst)
    }

    fn stamp(&self) -> String {
        format!("[T+{}]", fmt_dur(self.start.elapsed()))
    }

    fn emit(&self, msg: String) {
        eprintln!("{msg}");
        self.events.lock().unwrap().push(msg);
    }

    /// Announce a phase transition (Go: `reporter.phase`).
    fn phase(&self, name: &str, detail: &str) {
        self.emit(format!("{} === PHASE: {name} === {detail}", self.stamp()));
    }

    /// Record a timestamped progress line (Go: `reporter.logf`).
    fn log(&self, msg: String) {
        let stamped = format!("{} {msg}", self.stamp());
        self.emit(stamped);
    }

    fn count_attempt(&self, ok: bool) {
        self.attempted.fetch_add(1, Ordering::SeqCst);
        if ok {
            self.succeeded.fetch_add(1, Ordering::SeqCst);
        } else {
            self.failed.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn count_fault(&self, kind: &str) {
        let mut faults = self.faults.lock().unwrap();
        match kind {
            "restart" => faults.restarts += 1,
            "rotate" => faults.rotations += 1,
            "impair" => faults.impair_cycles += 1,
            "disk" => faults.disk_cycles += 1,
            "snapshot" => faults.snapshots += 1,
            "skew" => faults.skews += 1,
            _ => {}
        }
    }

    fn next_rotation(&self) -> u64 {
        self.rot_seq.fetch_add(1, Ordering::SeqCst) + 1
    }

    fn fault_snapshot(&self) -> FaultCounts {
        *self.faults.lock().unwrap()
    }

    /// Record distinct write-error messages (first 8) with counts (Go: `sampleErr`).
    fn sample_err(&self, err: &str) {
        let msg: String = err.chars().take(ERR_TRUNC_CHARS).collect();
        *self.err_seen.lock().unwrap().entry(msg.clone()).or_insert(0) += 1;
        let mut sample = self.err_sample.lock().unwrap();
        if sample.len() < ERR_SAMPLE_CAP && !sample.contains(&msg) {
            sample.push(msg);
        }
    }

    /// Log the final PASS/FAIL line and write the markdown report (Go:
    /// `verdict`). Failure artifact retention itself is handled by
    /// `Cluster::finish`.
    fn verdict(&self, pass: bool, report_path: &Path, summary: &str) {
        let status = if pass { "PASS" } else { "FAIL" };
        self.emit(format!("{} === VERDICT: {status} === {summary}", self.stamp()));
        let events = self.events.lock().unwrap().clone();
        let err_sample = self.err_sample.lock().unwrap().clone();
        let err_seen = self.err_seen.lock().unwrap().clone();
        let f = self.fault_snapshot();
        let mut b = String::new();
        b.push_str(&format!("# Endurance-chaos run report — {status}\n\n"));
        b.push_str(&format!(
            "- finished (unix): {}\n- wall time: {}\n- writes attempted: {}\n- writes acked: {}\n- writes failed (transient): {}\n- faults: restarts={} rotations={} impairCycles={} diskCycles={} snapshots={} skews={}\n- verdict: {summary}\n\n",
            unix_now(),
            fmt_dur(self.start.elapsed()),
            self.attempted(),
            self.succeeded(),
            self.failed(),
            f.restarts,
            f.rotations,
            f.impair_cycles,
            f.disk_cycles,
            f.snapshots,
            f.skews,
        ));
        if !err_sample.is_empty() {
            b.push_str("## Distinct write errors\n\n");
            for s in &err_sample {
                b.push_str(&format!("- ({}x) {s}\n", err_seen.get(s).copied().unwrap_or(0)));
            }
            b.push('\n');
        }
        b.push_str("## Timeline\n\n");
        for e in &events {
            b.push_str(&format!("- {e}\n"));
        }
        match std::fs::write(report_path, b) {
            Ok(()) => eprintln!("report written to {}", report_path.display()),
            Err(e) => eprintln!("report write failed: {e}"),
        }
    }
}

/// Bounded working-set registry (Go: `idRegistry`): every appended id plus
/// the live subset; deletes remove from live only.
struct IdRegistry {
    inner: Mutex<IdRegistryInner>,
}

struct IdRegistryInner {
    ids: Vec<String>,
    live: HashSet<String>,
}

impl IdRegistry {
    fn new() -> Self {
        Self {
            inner: Mutex::new(IdRegistryInner { ids: Vec::new(), live: HashSet::new() }),
        }
    }

    fn add(&self, id: String) {
        let mut inner = self.inner.lock().unwrap();
        inner.ids.push(id.clone());
        inner.live.insert(id);
    }

    fn remove(&self, id: &str) {
        self.inner.lock().unwrap().live.remove(id);
    }

    fn len(&self) -> usize {
        self.inner.lock().unwrap().live.len()
    }

    /// Pick a random live id, tolerating entries deleted since append (Go:
    /// `idRegistry.pick`).
    fn pick(&self, rng: &mut Rng) -> Option<String> {
        let inner = self.inner.lock().unwrap();
        if inner.live.is_empty() || inner.ids.is_empty() {
            return None;
        }
        for _ in 0..8 {
            let id = &inner.ids[rng.below(inner.ids.len())];
            if inner.live.contains(id) {
                return Some(id.clone());
            }
        }
        None
    }
}

/// Restart-proof per-node GC history (Go: `chaosControl` GC latches).
/// The sampler records high-water observations and the verdict asserts on
/// those. No GC-runs counter exists over the wire, so a run is proven by
/// `collected` advancing across samples (durable, restart-proof).
struct GcHistory {
    inner: Mutex<Vec<GcHist>>,
}

struct GcHist {
    first: Option<u64>,
    max: u64,
    gc_err: Option<String>,
}

impl GcHistory {
    fn new(nodes: usize) -> Self {
        Self {
            inner: Mutex::new(
                (0..nodes)
                    .map(|_| GcHist { first: None, max: 0, gc_err: None })
                    .collect(),
            ),
        }
    }

    fn observe(&self, idx: usize, collected: u64, last_error: &Option<String>) {
        let mut inner = self.inner.lock().unwrap();
        let hist = &mut inner[idx];
        hist.first.get_or_insert(collected);
        hist.max = hist.max.max(collected);
        if let Some(e) = last_error {
            if hist.gc_err.is_none() && is_gc_error(e) {
                hist.gc_err = Some(e.chars().take(ERR_TRUNC_CHARS).collect());
            }
        }
    }

    /// (runs observed, collected observed, first GC-shaped error).
    fn snapshot(&self, idx: usize) -> (bool, bool, Option<String>) {
        let inner = self.inner.lock().unwrap();
        let hist = &inner[idx];
        let ever = hist.first.is_some_and(|f| hist.max > f);
        (ever, hist.max > 0, hist.gc_err.clone())
    }
}

/// True when an engine error string plausibly comes from log-GC
/// collection. The Rust engine multiplexes every subsystem error into one
/// `last_error` latch (peer disconnects during restarts/flaps latch here
/// too), while Go asserts the GC-specific `spedsql_gc_failures_total`
/// counter — so only GC-substring matches fail the verdict.
fn is_gc_error(msg: &str) -> bool {
    let lower = msg.to_lowercase();
    lower.contains("retention") || lower.contains("collect") || lower.contains("gc_") || lower.contains("prune")
}

/// GC counters for one node (Go: scraped from `/metrics`): collected
/// transactions (`MURMUR SHOW RETENTION`) plus the latest engine error
/// (`MURMUR SHOW REPLICATION`). `None` when the node is unreachable
/// (stopped or mid-restart) or the mesh has no retention row yet.
async fn gc_sample(cluster: &Shared, idx: usize) -> Option<(u64, Option<String>)> {
    let guard = cluster.lock().await;
    if guard.node_pid(idx).is_none() {
        return None;
    }
    let repl = guard.node(idx).simple("MURMUR SHOW REPLICATION").await.ok()?;
    let repl = repl.first()?;
    if repl.len() < 8 {
        return None;
    }
    let last_error = if repl[6].is_empty() { None } else { Some(repl[6].clone()) };
    let rows = guard.node(idx).simple("MURMUR SHOW RETENTION").await.ok()?;
    let row = rows.first()?;
    if row.len() < 6 {
        return None;
    }
    let collected: u64 = row[5].parse().ok()?;
    Some((collected, last_error))
}

/// Log-path applied-transaction counter for one node (snapshot
/// application bypasses it, so it underpins the snapshot proof).
async fn applied_count(cluster: &Shared, idx: usize) -> Option<u64> {
    let guard = cluster.lock().await;
    if guard.node_pid(idx).is_none() {
        return None;
    }
    let rows = guard.node(idx).simple("MURMUR SHOW REPLICATION").await.ok()?;
    rows.first()?.get(5)?.parse::<u64>().ok()
}

/// Spool on-disk bytes for one node (`MURMUR SHOW METRICS SPOOL`).
async fn spool_disk_bytes(cluster: &Shared, idx: usize) -> Option<u64> {
    let guard = cluster.lock().await;
    if guard.node_pid(idx).is_none() {
        return None;
    }
    let rows = guard.node(idx).simple("MURMUR SHOW METRICS SPOOL").await.ok()?;
    rows.iter()
        .find(|r| r.len() == 3 && r[0] == "spool" && r[1] == "disk_bytes")
        .and_then(|r| r[2].parse::<u64>().ok())
}

/// Row count on one node, or `None` when the read fails.
async fn row_count(cluster: &Shared, idx: usize) -> Option<usize> {
    let guard = cluster.lock().await;
    if guard.node_pid(idx).is_none() {
        return None;
    }
    let rows = guard
        .node(idx)
        .query("SELECT COUNT(*) FROM contention", &[])
        .await
        .ok()?;
    let n: i64 = rows.first()?.try_get(0).ok()?;
    Some(n as usize)
}

/// Canonical PK-ordered encoded rows on one node (Go: `enduranceNodeState`
/// input: `id:name:phone:score` sorted by id), or `None` when the read
/// fails.
async fn canonical_rows(cluster: &Shared, idx: usize) -> Option<Vec<String>> {
    let guard = cluster.lock().await;
    if guard.node_pid(idx).is_none() {
        return None;
    }
    let rows = guard
        .node(idx)
        .query("SELECT id, name, phone, score FROM contention ORDER BY id", &[])
        .await
        .ok()?;
    let mut out = Vec::new();
    for row in &rows {
        let id: String = row.try_get(0).ok()?;
        let name: String = row.try_get(1).ok()?;
        let phone: String = row.try_get(2).ok()?;
        let score: i64 = row.try_get(3).ok()?;
        out.push(format!("{id}:{name}:{phone}:{score}"));
    }
    Some(out)
}

/// Connected-peer count on one node (`MURMUR SHOW PEERS`), or `None` when
/// the node is down or unreadable.
async fn connected_peers(cluster: &Shared, idx: usize) -> Option<usize> {
    let guard = cluster.lock().await;
    if guard.node_pid(idx).is_none() {
        return None;
    }
    let rows = guard.node(idx).simple("MURMUR SHOW PEERS").await.ok()?;
    Some(rows.iter().filter(|r| r.len() >= 3 && r[2] == "true").count())
}

/// Deterministic log-only digest over canonical rows (Go: sha256 hex of
/// the sorted table; comparisons use byte-identical vectors, which imply
/// identical digests). FNV-1a keeps this dependency-free.
fn digest_hex(rows: &[String]) -> String {
    let mut h: u64 = 0xcbf29ce484222325;
    for row in rows {
        for b in row.as_bytes() {
            h ^= *b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
        h ^= b'\n' as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    format!("{h:016x}")
}

/// Size of the symmetric difference between two canonical row sets.
fn row_diff(a: &[String], b: &[String]) -> usize {
    let sa: HashSet<&String> = a.iter().collect();
    let sb: HashSet<&String> = b.iter().collect();
    sa.symmetric_difference(&sb).count()
}

/// Pick a random live node (Go: `chaosControl.pickAlive`); `None` when all
/// are down.
fn pick_alive(alive: &[AtomicBool], rng: &mut Rng) -> Option<usize> {
    let mut up = Vec::new();
    for (i, a) in alive.iter().enumerate() {
        if a.load(Ordering::SeqCst) {
            up.push(i);
        }
    }
    if up.is_empty() {
        None
    } else {
        Some(up[rng.below(up.len())])
    }
}

/// One bounded insert/update/delete write on a live node (Go: `writerLoop`
/// body): inserts until `max_rows` live ids, then updates/deletes recycle
/// the set so a long run keeps churning replication and GC without
/// unbounded growth. Transient errors are counted, never fatal. The
/// alive re-check and the write run under one cluster lock, so a node
/// the director stops mid-op is never touched.
#[allow(clippy::too_many_arguments)]
async fn write_one(
    id: usize,
    cluster: &Shared,
    alive: &Arc<Vec<AtomicBool>>,
    registry: &Arc<IdRegistry>,
    rep: &Arc<Reporter>,
    rng: &mut Rng,
    seq: &mut u64,
    max_rows: usize,
    target: usize,
) {
    let roll = rng.float();
    if registry.len() < max_rows && roll < 0.7 {
        *seq += 1;
        let row_id = format!("{id:08x}{:024x}", *seq);
        let name = format!("writer-{id}");
        let phone = format!("v-{id}-{seq}", seq = *seq);
        let insert_id = row_id.clone();
        let guard = cluster.lock().await;
        if !alive[target].load(Ordering::SeqCst) {
            return;
        }
        let outcome: Result<(), String> = async {
            guard
                .node(target)
                .execute(
                    "INSERT INTO contention(id, name, phone, score) VALUES($1, $2, $3, $4)",
                    &[&row_id, &name, &phone, &0i64],
                )
                .await
                .map_err(|e| e.to_string())?;
            guard.node(target).simple("MURMUR FLUSH").await.map(|_| ()).map_err(|e| e.to_string())
        }
        .await;
        drop(guard);
        match outcome {
            Ok(()) => {
                registry.add(insert_id);
                rep.count_attempt(true);
            }
            Err(e) => {
                rep.count_attempt(false);
                rep.sample_err(&e);
            }
        }
    } else if roll < 0.85 {
        let Some(row_id) = registry.pick(rng) else {
            return;
        };
        *seq += 1;
        let name = format!("writer-{id}");
        let phone = format!("u-{id}-{seq}", seq = *seq);
        let guard = cluster.lock().await;
        if !alive[target].load(Ordering::SeqCst) {
            return;
        }
        let outcome = async {
            let changed = guard
                .node(target)
                .execute(
                    "UPDATE contention SET name = $1, phone = $2 WHERE id = $3",
                    &[&name, &phone, &row_id],
                )
                .await
                .map_err(|e| e.to_string())?;
            if changed != 1 {
                return Err(format!("update hit {changed} rows"));
            }
            guard.node(target).simple("MURMUR FLUSH").await.map(|_| ()).map_err(|e| e.to_string())
        }
        .await;
        drop(guard);
        match outcome {
            Ok(()) => rep.count_attempt(true),
            Err(e) => {
                rep.count_attempt(false);
                rep.sample_err(&e);
            }
        }
    } else {
        let Some(row_id) = registry.pick(rng) else {
            return;
        };
        let del_id = row_id.clone();
        let guard = cluster.lock().await;
        if !alive[target].load(Ordering::SeqCst) {
            return;
        }
        let outcome: Result<(), String> = async {
            guard
                .node(target)
                .execute("DELETE FROM contention WHERE id = $1", &[&row_id])
                .await
                .map_err(|e| e.to_string())?;
            guard.node(target).simple("MURMUR FLUSH").await.map(|_| ()).map_err(|e| e.to_string())
        }
        .await;
        drop(guard);
        match outcome {
            Ok(()) => {
                registry.remove(&del_id);
                rep.count_attempt(true);
            }
            Err(e) => {
                rep.count_attempt(false);
                rep.sample_err(&e);
            }
        }
    }
}

/// Bounded writer task (Go: `writerLoop`).
#[allow(clippy::too_many_arguments)]
async fn writer_loop(
    id: usize,
    cluster: Shared,
    alive: Arc<Vec<AtomicBool>>,
    registry: Arc<IdRegistry>,
    rep: Arc<Reporter>,
    seed: u64,
    write_interval: Duration,
    max_rows: usize,
    stop: Arc<AtomicBool>,
) {
    let mut rng = Rng::new(seed.wrapping_mul(1000).wrapping_add(id as u64));
    let mut seq: u64 = 0;
    loop {
        if stop.load(Ordering::SeqCst) {
            return;
        }
        let target = match pick_alive(&alive, &mut rng) {
            Some(t) => t,
            None => {
                tokio::time::sleep(write_interval).await;
                continue;
            }
        };
        write_one(id, &cluster, &alive, &registry, &rep, &mut rng, &mut seq, max_rows, target).await;
        if stop.load(Ordering::SeqCst) {
            return;
        }
        tokio::time::sleep(write_interval).await;
    }
}

/// Heartbeat sampler (Go: `statusLoop`): row spread, fault tallies, GC
/// high-water observations, spool/disk pressure.
#[allow(clippy::too_many_arguments)]
async fn status_loop(
    cluster: Shared,
    node_dirs: Vec<PathBuf>,
    rep: Arc<Reporter>,
    gc_hist: Arc<GcHistory>,
    nodes: usize,
    status_every: Duration,
    stop: Arc<AtomicBool>,
) {
    loop {
        if !sleep_stop(status_every, &stop).await {
            return;
        }
        let mut min: i64 = -1;
        let mut max: i64 = -1;
        let mut down = 0usize;
        for i in 0..nodes {
            match row_count(&cluster, i).await {
                Some(c) => {
                    let c = c as i64;
                    if min < 0 || c < min {
                        min = c;
                    }
                    if c > max {
                        max = c;
                    }
                }
                None => down += 1,
            }
        }
        let mut spool_max: u64 = 0;
        let mut collected_max: u64 = 0;
        for i in 0..nodes {
            if let Some(bytes) = spool_disk_bytes(&cluster, i).await {
                spool_max = spool_max.max(bytes);
            }
            if let Some((collected, last_error)) = gc_sample(&cluster, i).await {
                collected_max = collected_max.max(collected);
                gc_hist.observe(i, collected, &last_error);
            }
        }
        let mut disk_min: Option<u64> = None;
        for dir in &node_dirs {
            if let Some(free) = dir_free_bytes(dir) {
                disk_min = Some(disk_min.map_or(free, |m: u64| m.min(free)));
            }
        }
        let f = rep.fault_snapshot();
        rep.log(format!(
            "status: writes ok={} err={} | rows min={min} max={max} down={down}/{nodes} | restart={} rotate={} impair={} disk={} snap={} skew={} | gcCollectedMax={collected_max} spoolMax={}MiB diskFreeMin={}MiB",
            rep.succeeded(),
            rep.failed(),
            f.restarts,
            f.rotations,
            f.impair_cycles,
            f.disk_cycles,
            f.snapshots,
            f.skews,
            spool_max >> 20,
            disk_min.unwrap_or(0) >> 20,
        ));
    }
}

/// Disk-pressure cycler (Go: `diskLoop`): holds a filler file in each node
/// dir in turn, then releases. Filesystem paths only, no cluster use.
async fn disk_loop(
    node_dirs: Vec<PathBuf>,
    rep: Arc<Reporter>,
    disk_every: Duration,
    disk_hold: Duration,
    filler_mb: u64,
    stop: Arc<AtomicBool>,
) {
    if !sleep_stop(DISK_WARMUP, &stop).await {
        return;
    }
    let mut idx = 0usize;
    loop {
        let dir = &node_dirs[idx % node_dirs.len()];
        idx += 1;
        let label = dir
            .file_name()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_else(|| "?".to_string());
        let path = dir.join("endurance-filler.bin");
        let free_before = dir_free_bytes(dir).unwrap_or(0);
        match write_filler(&path, filler_mb) {
            Ok(()) => {
                let free_after = dir_free_bytes(dir).unwrap_or(0);
                rep.log(format!(
                    "disk: {label} holding {filler_mb} MiB (free {} -> {} MiB) for {}",
                    free_before >> 20,
                    free_after >> 20,
                    fmt_dur(disk_hold),
                ));
                let interrupted = !sleep_stop(disk_hold, &stop).await;
                remove_filler(&path);
                rep.count_fault("disk");
                if interrupted {
                    return;
                }
            }
            Err(e) => {
                rep.log(format!("disk: {label} filler FAILED: {e}"));
                remove_filler(&path);
            }
        }
        // Wait out the remainder of the period after the hold.
        if disk_every > disk_hold && !sleep_stop(disk_every - disk_hold, &stop).await {
            return;
        }
    }
}

/// Sleep `d`, waking early when `stop` is set. Reports false when
/// interrupted (Go: `sleepInterruptible`).
async fn sleep_stop(d: Duration, stop: &AtomicBool) -> bool {
    let deadline = tokio::time::Instant::now() + d;
    loop {
        if stop.load(Ordering::SeqCst) {
            return false;
        }
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100).min(deadline - now)).await;
    }
}

/// Consume `mb` megabytes at `path` with incompressible-ish bytes (Go:
/// `writeFiller`).
fn write_filler(path: &Path, mb: u64) -> std::io::Result<()> {
    use std::io::Write;
    let mut f = std::fs::File::create(path)?;
    let mut buf = vec![0u8; 1 << 20];
    let mut x: u64 = 0x9e3779b97f4a7c15;
    for _ in 0..mb {
        for chunk in buf.chunks_mut(8) {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let bytes = x.to_le_bytes();
            let n = chunk.len().min(8);
            chunk[..n].copy_from_slice(&bytes[..n]);
        }
        f.write_all(&buf)?;
    }
    f.sync_all()?;
    Ok(())
}

fn remove_filler(path: &Path) {
    let _ = std::fs::remove_file(path);
}

/// Free bytes on the filesystem holding `path` (Go: `dirFreeBytes`).
#[cfg(unix)]
fn dir_free_bytes(path: &Path) -> Option<u64> {
    use std::os::unix::ffi::OsStrExt;
    let cpath = std::ffi::CString::new(path.as_os_str().as_bytes()).ok()?;
    let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statvfs(cpath.as_ptr(), &mut stat) } != 0 {
        return None;
    }
    Some((stat.f_bavail as u64).saturating_mul(stat.f_frsize as u64))
}

#[cfg(not(unix))]
fn dir_free_bytes(_path: &Path) -> Option<u64> {
    None
}

/// Fresh 32-byte storage key (Go: `crypto/rand`).
fn random_key() -> [u8; 32] {
    use std::io::Read;
    let mut key = [0u8; 32];
    if let Ok(mut f) = std::fs::File::open("/dev/urandom") {
        if f.read_exact(&mut key).is_ok() {
            return key;
        }
    }
    // Fallback when urandom is unavailable: splitmix over time + pid.
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0x1234_5678);
    let mut rng = Rng::new(nanos ^ (std::process::id() as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15));
    for chunk in key.chunks_mut(8) {
        let bytes = rng.next().to_le_bytes();
        chunk.copy_from_slice(&bytes[..chunk.len()]);
    }
    key
}

/// Per-window snapshot-path observation (see `observe_snapshot_proof`).
#[derive(Debug)]
struct SnapObservation {
    victim: usize,
    outage_acked: u64,
    applied_delta: u64,
    diff_rows: usize,
    rows: usize,
    proven: bool,
}

/// Decide whether a rejoined snapshot victim caught up via the snapshot
/// path. Go proves this with `spedsql_repl_snapshots_received_total`;
/// the Rust wire protocol exposes no snapshot counter, so the proof is
/// event-driven off the log-path `applied_transactions` counter
/// instead: every writer op is one transaction, and the victim missed
/// every acked op from its outage, so the window counts as proven when
/// the post-rejoin sample shows the bulk arrived off the log path (row
/// drift below the outage's acked count while the log-path applied
/// delta since the rejoin stays below it too — a pure log catch-up
/// would advance `applied_transactions` by at least the outage's
/// count). The outage (75 s floor) outlasts retention plus the offline
/// pin, so survivors GC the victim's ranges and only a snapshot can
/// deliver the bulk. Writers keep running throughout (Go parity: no
/// quiesce); the victim's own restart plus a donor build plus the chunk
/// transfer can take ~45 s under chaos, and a stale donor winning the
/// race forces a second cycle, so the proof polls up to
/// `SNAP_PROOF_TIMEOUT` instead of sampling once at a fixed delay.
async fn observe_snapshot_proof(
    cluster: &Shared,
    victim: usize,
    rep: &Reporter,
    acked_at_stop: u64,
    nodes: usize,
) -> SnapObservation {
    let acked_at_rejoin = rep.succeeded();
    let outage_acked = acked_at_rejoin.saturating_sub(acked_at_stop);
    let applied_at_rejoin = applied_count(cluster, victim).await.unwrap_or(0);
    let start = tokio::time::Instant::now();
    let mut obs = SnapObservation {
        victim,
        outage_acked,
        applied_delta: 0,
        diff_rows: row_diff(
            &canonical_rows(cluster, victim).await.unwrap_or_default(),
            &canonical_rows(cluster, (victim + 1) % nodes).await.unwrap_or_default(),
        ),
        rows: 0,
        proven: false,
    };
    while start.elapsed() < SNAP_PROOF_TIMEOUT {
        tokio::time::sleep(SNAP_PROOF_POLL).await;
        let applied_now = applied_count(cluster, victim).await.unwrap_or(applied_at_rejoin);
        let victim_rows = canonical_rows(cluster, victim).await.unwrap_or_default();
        let survivor = (victim + 1) % nodes;
        let survivor_rows = canonical_rows(cluster, survivor).await.unwrap_or_default();
        obs.diff_rows = row_diff(&victim_rows, &survivor_rows);
        obs.rows = survivor_rows.len();
        obs.applied_delta = applied_now.saturating_sub(applied_at_rejoin);
        // The snapshot covered the bulk of the outage when neither the
        // remaining drift nor the post-rejoin log tail reaches the
        // outage's acked count.
        obs.proven = outage_acked > 100
            && (obs.diff_rows as u64) < outage_acked
            && obs.applied_delta < outage_acked;
        if obs.proven {
            break;
        }
    }
    rep.log(format!(
        "snapshot: node{} proof outageAcked={} appliedDelta={} diffRows={}/{} proven={}",
        obs.victim + 1,
        obs.outage_acked,
        obs.applied_delta,
        obs.diff_rows,
        obs.rows,
        obs.proven,
    ));
    obs
}

/// Director outcome (Go: snapshot/skew scheduling flags).
struct DirectorReport {
    snapshots_possible: bool,
    skew_scheduled: bool,
}

#[allow(clippy::too_many_arguments)]
async fn run_director(
    cluster: &Shared,
    alive: &Arc<Vec<AtomicBool>>,
    rep: &Arc<Reporter>,
    cfg: &Config,
    cur_keys: &mut [[u8; 32]],
    snap_proofs: &mut Vec<SnapObservation>,
) -> Result<DirectorReport, String> {
    let mut rng = Rng::new(cfg.seed.wrapping_mul(7919).wrapping_add(17));
    let start = Instant::now();
    let deadline = start + cfg.duration;

    let mut next_restart = start + cfg.restart_every / 2;
    let mut next_rotate = start + cfg.rotate_every / 2;
    let mut rotate_idx = 0usize;

    // Snapshot windows need a full offline window plus reconvergence room
    // before the verdict; short runs skip the leg loudly.
    let mut snap_ok = true;
    let mut next_snap_stop = start + cfg.duration / 4;
    if next_snap_stop + cfg.snapshot_off + Duration::from_secs(60) > deadline {
        next_snap_stop = start + Duration::from_secs(30);
        if next_snap_stop + cfg.snapshot_off + Duration::from_secs(60) > deadline {
            snap_ok = false;
            rep.log("snapshot: run too short for an offline window, leg skipped".to_string());
        }
    }
    let mut snap_victim: Option<usize> = None;
    let mut snap_restart_at = Instant::now();
    let mut snap_acked_at_stop: u64 = 0;

    // Clock skew is degraded away: server children share the host clock,
    // so no node can run under libfaketime. (Go runs one +5min window on
    // the last node when libfaketime calibrates.)
    let skew_scheduled = false;

    // Impairment phases cycle latency -> loss -> both, all implemented as
    // logical edge flaps (no tc privilege model for child processes).
    let phases = ["latency", "loss", "latency+loss"];
    let mut phase = 0usize;
    let mut next_impair = start + IMPAIR_WARMUP;
    let mut impair_active: Option<(usize, usize, Instant)> = None;

    loop {
        if Instant::now() >= deadline {
            break;
        }
        // Sleep to the next tick, then re-sample time (Go: 1 s ticker).
        tokio::time::sleep(DIRECTOR_TICK).await;
        let now = Instant::now();
        if now >= deadline {
            break;
        }

        // Restore an expired impairment flap.
        if let Some((a, b, restore_at)) = impair_active {
            if now >= restore_at {
                {
                    let guard = cluster.lock().await;
                    let _ = guard.add_peer(a, b).await;
                    let _ = guard.add_peer(b, a).await;
                }
                impair_active = None;
                rep.count_fault("impair");
                rep.log(format!("impair: edge node{} <-> node{} restored", a + 1, b + 1));
                next_impair = now + cfg.impair_off;
            }
        }
        // Open the next impairment flap.
        if impair_active.is_none() && now >= next_impair {
            let a = rng.below(cfg.nodes);
            let b = (a + 1 + rng.below(cfg.nodes - 1)) % cfg.nodes;
            {
                let guard = cluster.lock().await;
                let _ = guard.remove_peer(a, b).await;
                let _ = guard.remove_peer(b, a).await;
            }
            rep.log(format!(
                "impair: flap {} (logical) node{} <-> node{} for {} (no tc privilege model for child processes)",
                phases[phase % phases.len()],
                a + 1,
                b + 1,
                fmt_dur(cfg.impair_on),
            ));
            phase += 1;
            impair_active = Some((a, b, now + cfg.impair_on));
        }

        if now >= next_restart {
            // Never restart the offline snapshot victim out from under
            // its window.
            let mut idx = rng.below(cfg.nodes);
            for _ in 0..16 {
                if Some(idx) != snap_victim {
                    break;
                }
                idx = rng.below(cfg.nodes);
            }
            // Alternate graceful and crash restarts (Go parity): the
            // harness kills real child processes either way.
            let crash = rng.below(2) == 0;
            rep.log(format!(
                "restart: node{} going down ({} downtime, {})",
                idx + 1,
                fmt_dur(cfg.restart_down),
                if crash { "crash" } else { "graceful" },
            ));
            {
                let mut guard = cluster.lock().await;
                alive[idx].store(false, Ordering::SeqCst);
                if crash {
                    guard.kill_node(idx).await;
                } else {
                    guard.stop_node(idx).await;
                }
            }
            tokio::time::sleep(cfg.restart_down).await;
            {
                let mut guard = cluster.lock().await;
                guard
                    .start_node(idx)
                    .await
                    .map_err(|e| format!("node{} restart failed: {e}", idx + 1))?;
                guard.mesh().await.map_err(|e| format!("re-mesh after restart: {e}"))?;
                alive[idx].store(true, Ordering::SeqCst);
            }
            rep.count_fault("restart");
            rep.log(format!("restart: node{} back", idx + 1));
            let half_ms = (cfg.restart_every.as_millis() / 2).min(usize::MAX as u128).max(1) as usize;
            let jitter = Duration::from_millis(rng.below(half_ms) as u64);
            next_restart = Instant::now() + cfg.restart_every + jitter;
        }

        if now >= next_rotate {
            let mut idx = None;
            for k in 0..cfg.nodes {
                let cand = (rotate_idx + k) % cfg.nodes;
                if alive[cand].load(Ordering::SeqCst) && Some(cand) != snap_victim {
                    idx = Some(cand);
                    break;
                }
            }
            rotate_idx = (rotate_idx + 1) % cfg.nodes;
            match idx {
                None => rep.log("rotate: skipped, no node available".to_string()),
                Some(i) => {
                    if let Err(e) = rotate_node(cluster, cur_keys, i, rep).await {
                        rep.log(format!("rotate: node{} FAILED: {e} (counted, chaos continues)", i + 1));
                    }
                }
            }
            next_rotate = Instant::now() + cfg.rotate_every;
        }

        if snap_ok && snap_victim.is_none() && now >= next_snap_stop {
            let v = rng.below(cfg.nodes);
            rep.log(format!(
                "snapshot: node{} going offline for {} (ranges must expire past retention)",
                v + 1,
                fmt_dur(cfg.snapshot_off),
            ));
            {
                let mut guard = cluster.lock().await;
                alive[v].store(false, Ordering::SeqCst);
                guard.stop_node(v).await;
            }
            snap_victim = Some(v);
            snap_acked_at_stop = rep.succeeded();
            snap_restart_at = Instant::now() + cfg.snapshot_off;
        }
        if let Some(v) = snap_victim {
            if now >= snap_restart_at {
                {
                    let mut guard = cluster.lock().await;
                    guard
                        .start_node(v)
                        .await
                        .map_err(|e| format!("node{} snapshot rejoin failed: {e}", v + 1))?;
                    guard.mesh().await.map_err(|e| format!("re-mesh after snapshot rejoin: {e}"))?;
                    alive[v].store(true, Ordering::SeqCst);
                }
                rep.count_fault("snapshot");
                rep.log(format!("snapshot: node{} rejoined, must resync via snapshot", v + 1));
                let obs = observe_snapshot_proof(cluster, v, rep, snap_acked_at_stop, cfg.nodes).await;
                snap_proofs.push(obs);
                snap_victim = None;
                next_snap_stop = Instant::now() + cfg.snapshot_every;
            }
        }
    }

    // Restore any open impairment flap without counting it (Go: an
    // interrupted cycle returns before the count).
    if let Some((a, b, _)) = impair_active {
        let guard = cluster.lock().await;
        let _ = guard.add_peer(a, b).await;
        let _ = guard.add_peer(b, a).await;
        rep.log(format!("impair: edge node{} <-> node{} restored at chaos end (uncounted)", a + 1, b + 1));
    }
    cluster.lock().await.mesh().await.map_err(|e| format!("final re-mesh: {e}"))?;

    // Rejoin anyone still down when the chaos window closes.
    if let Some(v) = snap_victim {
        {
            let mut guard = cluster.lock().await;
            guard
                .start_node(v)
                .await
                .map_err(|e| format!("node{} end-of-chaos rejoin failed: {e}", v + 1))?;
            guard.mesh().await.map_err(|e| format!("re-mesh after end-of-chaos rejoin: {e}"))?;
            alive[v].store(true, Ordering::SeqCst);
        }
        rep.count_fault("snapshot");
        rep.log(format!("snapshot: node{} rejoined at chaos end", v + 1));
        let obs = observe_snapshot_proof(cluster, v, rep, snap_acked_at_stop, cfg.nodes).await;
        snap_proofs.push(obs);
    }
    Ok(DirectorReport { snapshots_possible: snap_ok, skew_scheduled })
}

/// Online storage-key rotation on one node (Go: `rotateNode`): rotate to
/// a fresh random key without downtime via `MURMUR ROTATE`, then record
/// it for later restarts (Go rewrites the node config; here the harness
/// stored key).
async fn rotate_node(
    cluster: &Shared,
    cur_keys: &mut [[u8; 32]],
    idx: usize,
    rep: &Reporter,
) -> Result<(), String> {
    let new_key = random_key();
    let n = rep.next_rotation();
    let rows = {
        let mut guard = cluster.lock().await;
        guard
            .node(idx)
            .simple(&format!("MURMUR ROTATE KEY '{}' '{}'", hex(&cur_keys[idx]), hex(&new_key)))
            .await
            .map_err(|e| format!("rotate failed: {e}"))?;
        // No key IDs exist in the Rust product (Go asserts the active
        // ApplicationKeyID); staying online plus later restarts on the new
        // key carry that proof instead.
        let rows = guard
            .node(idx)
            .query("SELECT COUNT(*) FROM contention", &[])
            .await
            .map_err(|_| "node unreadable after rotation".to_string())?;
        let rows: i64 = rows
            .first()
            .and_then(|r| r.try_get(0).ok())
            .ok_or_else(|| "node unreadable after rotation".to_string())?;
        cur_keys[idx] = new_key;
        guard.set_node_key(idx, new_key).map_err(|e| format!("{e}"))?;
        rows
    };
    rep.count_fault("rotate");
    rep.log(format!("rotate: node{} now on endurance-k{n} ({rows} rows live)", idx + 1));
    Ok(())
}

/// Restart any node the director left down so the verdict window starts
/// with a full mesh (Go: `ensureAllUp`).
async fn ensure_all_up(cluster: &Shared, alive: &[AtomicBool], rep: &Reporter) -> Result<(), String> {
    let mut rejoined = false;
    let nodes = alive.len();
    for i in 0..nodes {
        if alive[i].load(Ordering::SeqCst) {
            continue;
        }
        rep.log(format!("cleanup: node{} still down at chaos end, restarting", i + 1));
        cluster
            .lock()
            .await
            .start_node(i)
            .await
            .map_err(|e| format!("node{} cleanup restart failed: {e}", i + 1))?;
        rejoined = true;
    }
    if rejoined {
        cluster.lock().await.mesh().await.map_err(|e| format!("re-mesh after cleanup restarts: {e}"))?;
    }
    for a in alive.iter() {
        a.store(true, Ordering::SeqCst);
    }
    Ok(())
}

/// Poll until every node holds `want` rows with identical contents (Go:
/// `waitAgreement`).
async fn wait_agreement(cluster: &Shared, nodes: usize, want: usize, timeout: Duration) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let mut ok = true;
        let mut first: Option<Vec<String>> = None;
        for i in 0..nodes {
            match canonical_rows(cluster, i).await {
                Some(rows) if rows.len() == want => match &first {
                    None => first = Some(rows),
                    Some(f) => {
                        if *f != rows {
                            ok = false;
                            break;
                        }
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
            return Err(format!("row agreement on {want} rows not reached within {timeout:?}"));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Poll until every node is reachable with equal row counts and identical
/// digests. Counts are compared to each other, not to a fixed target,
/// because deletes move the total during chaos (Go: `waitConverged`).
async fn wait_converged(cluster: &Shared, nodes: usize, timeout: Duration) -> (Vec<i64>, Vec<String>, Result<(), String>) {
    let deadline = tokio::time::Instant::now() + timeout;
    let start = tokio::time::Instant::now();
    let mut last_sample = tokio::time::Instant::now() - Duration::from_secs(3600);
    loop {
        let mut counts = vec![0i64; nodes];
        let mut digests = vec![String::new(); nodes];
        let mut first: Option<Vec<String>> = None;
        let mut agree = 0usize;
        let mut ok = true;
        for i in 0..nodes {
            match canonical_rows(cluster, i).await {
                Some(rows) => {
                    counts[i] = rows.len() as i64;
                    digests[i] = digest_hex(&rows);
                    match &first {
                        None => {
                            agree = 1;
                            first = Some(rows);
                        }
                        Some(f) => {
                            if *f != rows {
                                ok = false;
                            } else {
                                agree += 1;
                            }
                        }
                    }
                }
                None => {
                    ok = false;
                    break;
                }
            }
        }
        if ok {
            return (counts, digests, Ok(()));
        }
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        if last_sample.elapsed() >= Duration::from_secs(15) {
            last_sample = tokio::time::Instant::now();
            let lo = counts.iter().min().copied().unwrap_or(-1);
            let hi = counts.iter().max().copied().unwrap_or(-1);
            eprintln!(
                "[verdict-sample T+{}s] counts={counts:?} spread={} agree={agree}/{nodes}",
                start.elapsed().as_secs(),
                hi - lo,
            );
        }
        tokio::time::sleep(VERDICT_POLL).await;
    }
    // Final best-effort re-sample for the failure report (Go parity).
    let mut counts = vec![0i64; nodes];
    let mut digests = vec![String::new(); nodes];
    let mut snapshots: Vec<Vec<String>> = Vec::new();
    for i in 0..nodes {
        match canonical_rows(cluster, i).await {
            Some(rows) => {
                counts[i] = rows.len() as i64;
                digests[i] = digest_hex(&rows);
                snapshots.push(rows);
            }
            None => {
                counts[i] = -1;
                digests[i] = "unreachable".to_string();
                snapshots.push(Vec::new());
            }
        }
    }
    // Diagnostic sample: first few rows where node0 and the first
    // disagreeing node differ (id + full values). Resurrected deletes show
    // stale values; slow repair shows recently-written values.
    if snapshots.len() == nodes {
        for j in 1..nodes {
            if snapshots[j] == snapshots[0] {
                continue;
            }
            let a: HashSet<&String> = snapshots[0].iter().collect();
            let b: HashSet<&String> = snapshots[j].iter().collect();
            let only_a: Vec<&&String> = a.difference(&b).take(5).collect();
            let only_b: Vec<&&String> = b.difference(&a).take(5).collect();
            eprintln!("[verdict-diff] node1-only({}) sample={only_a:?}", a.difference(&b).count());
            eprintln!("[verdict-diff] node{}-only({}) sample={only_b:?}", j + 1, b.difference(&b).count());
            break;
        }
    }
    (counts, digests, Err(format!("nodes did not converge within {timeout:?}")))
}

/// True when every node reports a live replication session to every other
/// node (Go: `connectedPeers == nodes-1` per node off `/v1/status`).
async fn mesh_connected(cluster: &Shared, nodes: usize) -> bool {
    for i in 0..nodes {
        match connected_peers(cluster, i).await {
            Some(n) if n >= nodes - 1 => {}
            _ => return false,
        }
    }
    true
}

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn endurance_chaos() {
    let cfg = load_config();
    // Deterministic distinct node ids so snapshot trust can be wired up
    // front (the engine only requests snapshots from explicitly trusted
    // sources; Go trusts all nodes via the fixture policy).
    let node_ids: Vec<[u8; 16]> = (0..cfg.nodes)
        .map(|i| {
            let mut id = [0xC3u8; 16];
            id[0] = i as u8;
            id[1] = (i as u8).wrapping_mul(131).wrapping_add(7);
            id
        })
        .collect();
    let all: BTreeSet<[u8; 16]> = node_ids.iter().copied().collect();
    let mut trusted_snapshot_sources: HashMap<usize, BTreeSet<[u8; 16]>> = HashMap::new();
    for i in 0..cfg.nodes {
        trusted_snapshot_sources.insert(i, all.clone());
    }
    let initial_key = [9u8; 32];
    let cluster = Cluster::new(ClusterOptions {
        name: "endurance-chaos".into(),
        num_nodes: cfg.nodes,
        node_ids,
        key: initial_key,
        // Aggressive retention keeps log GC running under the sustained
        // workload and lets bounded offline windows force real snapshot
        // resyncs instead of log catch-up (Go: MinLogRetentionMs=1000,
        // MaxOfflineLogRetentionMs=20000, MinRetainedBatches=10).
        replication: Some(ReplicationOptions {
            retention: Some(Duration::from_secs(1)),
            retention_interval: Some(Duration::from_secs(1)),
            minimum_retained: Some(10),
            maximum_offline: Some(Duration::from_secs(20)),
            ..Default::default()
        }),
        trusted_snapshot_sources,
        ..Default::default()
    })
    .await
    .expect("cluster setup");
    let runtime_dir = cluster.runtime_dir().clone();
    let cluster: Shared = Arc::new(AsyncMutex::new(cluster));

    let result = run_body(&cluster, &cfg, &runtime_dir, initial_key).await;
    let failed = result.is_err();
    if let Err(e) = &result {
        eprintln!("scenario failed: {e}");
    }
    // All spawned tasks joined inside run_body, so the only remaining
    // reference is this one; unwrap for forensics/finish.
    let cluster = match Arc::try_unwrap(cluster) {
        Ok(mutex) => mutex.into_inner(),
        Err(_) => {
            eprintln!("scenario failed: cluster still shared at teardown");
            return;
        }
    };
    if failed {
        cluster.dump_forensics("endurance-verdict").await;
    }
    cluster.finish(failed).await;
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}

async fn run_body(cluster: &Shared, cfg: &Config, runtime_dir: &Path, initial_key: [u8; 32]) -> Result<(), String> {
    let rep = Arc::new(Reporter::new());
    rep.log(format!(
        "config: nodes={} duration={} settle={} writeInterval={} maxRows={} seed={}",
        cfg.nodes,
        fmt_dur(cfg.duration),
        fmt_dur(cfg.settle),
        fmt_dur(cfg.write_interval),
        cfg.max_rows,
        cfg.seed,
    ));
    rep.log(format!(
        "fault cadence: restart={} rotate={} snapshot={}/{} impair={}/{} disk={}/{} skew={} (degraded)",
        fmt_dur(cfg.restart_every),
        fmt_dur(cfg.rotate_every),
        fmt_dur(cfg.snapshot_every),
        fmt_dur(cfg.snapshot_off),
        fmt_dur(cfg.impair_on),
        fmt_dur(cfg.impair_off),
        fmt_dur(cfg.disk_every),
        fmt_dur(cfg.disk_hold),
        fmt_dur(cfg.skew_dur),
    ));

    // Same schema on every node (Go: typed-contention fixture table),
    // then replicate it: convergence needs the per-table flag on each
    // node.
    for i in 0..cfg.nodes {
        cluster
            .lock()
            .await
            .ensure_migrated(i, "CREATE TABLE contention(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL, phone TEXT NOT NULL, score INTEGER NOT NULL)")
            .await
            .map_err(|e| format!("migrate node{}: {e}", i + 1))?;
    }
    for i in 0..cfg.nodes {
        let guard = cluster.lock().await;
        guard
            .node(i)
            .simple("MURMUR SET TABLE contention REPLICATED true")
            .await
            .map_err(|e| format!("node{} SET TABLE REPLICATED: {e}", i + 1))?;
        guard.node(i).simple("MURMUR FLUSH").await.map_err(|e| format!("node{} flush after SET TABLE: {e}", i + 1))?;
    }

    // Baseline: one row everywhere before chaos starts.
    {
        let guard = cluster.lock().await;
        guard
            .node(0)
            .execute(
                "INSERT INTO contention(id, name, phone, score) VALUES($1, $2, $3, $4)",
                &[&format!("{:032x}", 0), &"baseline", &"baseline", &0i64],
            )
            .await
            .map_err(|e| format!("baseline insert: {e}"))?;
        guard.node(0).simple("MURMUR FLUSH").await.map_err(|e| format!("flush baseline: {e}"))?;
    }
    wait_agreement(cluster, cfg.nodes, 1, BASELINE_TIMEOUT).await?;
    rep.log(format!("baseline row converged on all {} nodes", cfg.nodes));

    // Preflights: detect degraded modes before the long run.
    rep.log(format!(
        "network impairment: logical peer flaps (tc/netem latency={}ms loss={}% has no child-process QUIC equivalent)",
        cfg.impair_latency_ms, cfg.impair_loss_pct,
    ));
    rep.log("clock skew: DEGRADED away (server children share the host clock; no daemon for libfaketime)".to_string());

    let mut filler_mb = cfg.disk_mb;
    let free_probe = {
        let guard = cluster.lock().await;
        let dir = guard.runtime_dir().clone();
        dir_free_bytes(&dir)
    };
    if let Some(free) = free_probe {
        // Never risk the host filesystem: filler must leave 1 GiB free.
        if free.saturating_sub(DISK_HEADROOM_BYTES) < (filler_mb << 20) {
            let headroom = free.saturating_sub(DISK_HEADROOM_BYTES) >> 20;
            if headroom < DISK_MIN_MB {
                filler_mb = 0;
                rep.log(format!("disk pressure: DEGRADED away (only {} MiB free)", free >> 20));
            } else {
                filler_mb = headroom;
                rep.log(format!("disk pressure: filler reduced to {filler_mb} MiB ({} MiB free)", free >> 20));
            }
        } else {
            rep.log(format!("disk pressure: {filler_mb} MiB filler ({} MiB free)", free >> 20));
        }
    } else {
        rep.log(format!("disk pressure: {filler_mb} MiB filler (free-space probe unavailable)"));
    }

    // Shared chaos state.
    let mut cur_keys = vec![initial_key; cfg.nodes];
    let registry = Arc::new(IdRegistry::new());
    let gc_hist = Arc::new(GcHistory::new(cfg.nodes));
    let mut snap_proofs = Vec::<SnapObservation>::new();
    let stop = Arc::new(AtomicBool::new(false));

    // An alive-set steers writer targeting away from stopped nodes (Go:
    // writers address daemons by stable address across restarts); every
    // write re-checks under the cluster lock, so lifecycle and reads
    // never race.
    let alive: Arc<Vec<AtomicBool>> =
        Arc::new((0..cfg.nodes).map(|_| AtomicBool::new(true)).collect());
    let node_dirs: Vec<PathBuf> = {
        let guard = cluster.lock().await;
        (0..cfg.nodes).map(|i| guard.node_info(i).dir.clone()).collect()
    };

    let num_writers = cfg.nodes.clamp(2, MAX_WRITERS);
    let mut writers = Vec::with_capacity(num_writers);
    for w in 0..num_writers {
        let (cluster, alive, registry, rep, stop) =
            (cluster.clone(), alive.clone(), registry.clone(), rep.clone(), stop.clone());
        let (seed, write_interval, max_rows) = (cfg.seed, cfg.write_interval, cfg.max_rows);
        writers.push(tokio::spawn(async move {
            writer_loop(w, cluster, alive, registry, rep, seed, write_interval, max_rows, stop).await;
        }));
    }

    let status_handle = {
        let (cluster, rep, gc_hist, stop) =
            (cluster.clone(), rep.clone(), gc_hist.clone(), stop.clone());
        let node_dirs = node_dirs.clone();
        let (status_every, nodes) = (cfg.status_every, cfg.nodes);
        tokio::spawn(async move {
            status_loop(cluster, node_dirs, rep, gc_hist, nodes, status_every, stop).await;
        })
    };
    let disk_handle = if filler_mb > 0 {
        let (rep, stop) = (rep.clone(), stop.clone());
        let (disk_every, disk_hold) = (cfg.disk_every, cfg.disk_hold);
        Some(
            tokio::spawn(async move {
                disk_loop(node_dirs, rep, disk_every, disk_hold, filler_mb, stop).await;
            }),
        )
    } else {
        None
    };

    // Chaos director (test task: owns all node lifecycle).
    rep.phase("chaos", &format!("all fault injectors live for {}", fmt_dur(cfg.duration)));
    let directive = run_director(cluster, &alive, &rep, cfg, &mut cur_keys, &mut snap_proofs).await?;

    // Faults stop; every node must be up before the verdict window.
    stop.store(true, Ordering::SeqCst);
    if let Some(h) = disk_handle {
        h.await.map_err(|e| format!("disk task panicked: {e}"))?;
    }
    status_handle.await.map_err(|e| format!("status task panicked: {e}"))?;
    ensure_all_up(cluster, &alive, &rep).await?;
    rep.log(format!(
        "writers stopping: attempted={} acked={} transient-errors={} live-ids={}",
        rep.attempted(),
        rep.succeeded(),
        rep.failed(),
        registry.len(),
    ));
    for (w, worker) in writers.into_iter().enumerate() {
        worker.await.map_err(|e| format!("writer task {w} panicked: {e}"))?;
    }

    let report_path = runtime_dir.join(format!("endurance-report-{}.md", unix_now()));
    if rep.succeeded() == 0 {
        rep.verdict(false, &report_path, "no writes acked during the entire run");
        return Err(format!(
            "endurance verdict: 0 of {} writes acked; see report for distinct errors",
            rep.attempted()
        ));
    }

    rep.phase("verdict", &format!("waiting up to {} for exact convergence", fmt_dur(cfg.settle)));
    let (counts, digests, converged) = wait_converged(cluster, cfg.nodes, cfg.settle).await;
    if let Err(e) = converged {
        rep.verdict(false, &report_path, &e);
        return Err(format!("endurance verdict: {e}\ncounts={counts:?}\ndigests={digests:?}"));
    }
    rep.log(format!("converged: all {} nodes hold {} rows with digest {}", cfg.nodes, counts[0], digests[0]));

    // Counter-proven post-checks: every claimed fault must have fired.
    // GC history (not base/after snapshots): `collected` is durable, so
    // high-water observations survive restarts; a node revived seconds
    // before the verdict still carries its proven history.
    for i in 0..cfg.nodes {
        let (ever, coll_ever, gc_err) = gc_hist.snapshot(i);
        if !ever {
            rep.verdict(false, &report_path, &format!("node{} GC never ran", i + 1));
            return Err(format!("node{} GC never observed running during chaos", i + 1));
        }
        if !coll_ever {
            rep.verdict(false, &report_path, &format!("node{} GC collected nothing", i + 1));
            return Err(format!("node{} GC never observed collecting during chaos", i + 1));
        }
        if let Some(e) = gc_err {
            rep.verdict(false, &report_path, &format!("node{} GC failures observed", i + 1));
            return Err(format!("node{i} GC failure observed during chaos: {e}", i = i + 1));
        }
    }
    rep.log(format!("GC proven on all {} nodes (collection observed advancing, GC failures flat)", cfg.nodes));

    let faults = rep.fault_snapshot();
    if faults.restarts == 0 {
        rep.verdict(false, &report_path, "zero restarts executed");
        return Err("chaos ran zero restarts; restart injector never fired".into());
    }
    if faults.rotations == 0 {
        rep.verdict(false, &report_path, "zero key rotations executed");
        return Err("chaos ran zero key rotations; rotation injector never fired".into());
    }
    if faults.impair_cycles == 0 {
        rep.verdict(false, &report_path, "zero network-impairment cycles executed");
        return Err("chaos ran zero network-impairment cycles".into());
    }
    if filler_mb > 0 && faults.disk_cycles == 0 {
        rep.verdict(false, &report_path, "zero disk-pressure cycles executed");
        return Err("chaos ran zero disk-pressure cycles".into());
    }
    if directive.snapshots_possible {
        if faults.snapshots == 0 {
            rep.verdict(false, &report_path, "zero snapshot offline windows executed");
            return Err("chaos ran zero snapshot offline windows".into());
        }
        let proven = snap_proofs.iter().filter(|o| o.proven).count();
        if proven < 1 {
            rep.verdict(false, &report_path, "no snapshot resync observed");
            return Err("offline windows ran but no resync proved the snapshot path (log catch-up hid the path?)".into());
        }
        rep.log(format!("snapshots proven: {proven} resyncs over {} offline windows", snap_proofs.len()));
    } else {
        rep.log("snapshots: leg skipped (run too short for an offline window)".to_string());
    }
    if directive.skew_scheduled && faults.skews == 0 {
        rep.verdict(false, &report_path, "skew scheduled but never executed");
        return Err("clock skew was scheduled but the skew window never ran".into());
    }

    // Full mesh must be back: every node sees every peer.
    let meshed = {
        let deadline = tokio::time::Instant::now() + MESH_TIMEOUT;
        loop {
            if mesh_connected(cluster, cfg.nodes).await {
                break true;
            }
            if tokio::time::Instant::now() >= deadline {
                break false;
            }
            tokio::time::sleep(VERDICT_POLL).await;
        }
    };
    if !meshed {
        let mut detail = Vec::new();
        for i in 0..cfg.nodes {
            detail.push(format!("node{}:{}", i + 1, connected_peers(cluster, i).await.unwrap_or(0)));
        }
        rep.verdict(false, &report_path, "mesh not fully reconnected");
        return Err(format!(
            "mesh not fully reconnected (connected peers per node: {}), want {}",
            detail.join(" "),
            cfg.nodes - 1
        ));
    }

    // A post-chaos write must traverse the healed mesh.
    {
        let guard = cluster.lock().await;
        if let Err(e) = guard
            .node(0)
            .execute(
                "INSERT INTO contention(id, name, phone, score) VALUES($1, $2, $3, $4)",
                &[&format!("{:032x}", 999_999_999u64), &"post-chaos", &"post-chaos", &0i64],
            )
            .await
        {
            rep.verdict(false, &report_path, "post-chaos write rejected");
            return Err(format!("post-chaos write: {e}"));
        }
        guard.node(0).simple("MURMUR FLUSH").await.map_err(|e| format!("flush post-chaos write: {e}"))?;
    }
    if let Err(e) = wait_agreement(cluster, cfg.nodes, counts[0] as usize + 1, cfg.settle).await {
        rep.verdict(false, &report_path, "post-chaos write did not converge");
        return Err(format!("post-chaos convergence: {e}"));
    }

    rep.verdict(
        true,
        &report_path,
        &format!(
            "all {} nodes hold {} rows digest {} after {} of combined chaos (restarts={} rotations={} impair=flap/{} disk={} snapshots={} skews={})",
            cfg.nodes,
            counts[0] + 1,
            digests[0],
            fmt_dur(cfg.duration),
            faults.restarts,
            faults.rotations,
            faults.impair_cycles,
            faults.disk_cycles,
            faults.snapshots,
            faults.skews,
        ),
    );
    Ok(())
}

