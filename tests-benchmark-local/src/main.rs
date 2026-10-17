//! Local (non-replicated) throughput benchmark for Murmur, driven through
//! the pure-Rust client (`murmur-client`) over the PostgreSQL wire protocol.
//!
//! The benchmark spawns a scratch `murmur-server` (release build), connects
//! with the Rust client, and measures maximum throughput for inserts, point
//! selects, indexed queries, scans, and updates across transaction sizes,
//! plus an 80% read / 20% write mixed workload. Every scenario runs for
//! `--seconds` (default 60) against default server options with background
//! persistence left running; no explicit flush happens during measurement.
//!
//! ```sh
//! cargo run --release --manifest-path tests-benchmark-local/Cargo.toml -- [--seconds 60] [--preload 100000] [--dir PATH] [--json out.json] [--quick]
//! ```

use murmur_client::tokio_postgres::Statement;
use murmur_client::{Client, Config};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const DEFAULT_SECONDS: u64 = 60;
const DEFAULT_PRELOAD: u64 = 100_000;
/// Record latency for every Nth operation; counts cover all operations.
const LATENCY_SAMPLE_EVERY: u64 = 16;
const TXN_SIZES: [usize; 4] = [1, 10, 100, 1000];
const PASSWORD: &str = "bench-password";
const KEY: &str = "bench-encryption-key";

struct Args {
    dir: Option<PathBuf>,
    seconds: u64,
    preload: u64,
    json: Option<PathBuf>,
    quick: bool,
}

fn parse_args() -> Result<Args, String> {
    let mut args = Args { dir: None, seconds: DEFAULT_SECONDS, preload: DEFAULT_PRELOAD, json: None, quick: false };
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--dir" => args.dir = Some(it.next().ok_or("--dir needs a path")?.into()),
            "--seconds" => args.seconds = it.next().ok_or("--seconds needs a value")?.parse().map_err(|_| "bad --seconds")?,
            "--preload" => args.preload = it.next().ok_or("--preload needs a value")?.parse().map_err(|_| "bad --preload")?,
            "--json" => args.json = Some(it.next().ok_or("--json needs a path")?.into()),
            "--quick" => args.quick = true,
            "--help" | "-h" => {
                println!("usage: murmur-local-bench [--dir PATH] [--seconds N] [--preload N] [--json PATH] [--quick]");
                std::process::exit(0);
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }
    if args.quick {
        args.seconds = 5;
        args.preload = 10_000;
    }
    if args.seconds == 0 {
        return Err("--seconds must be positive".into());
    }
    Ok(args)
}

/// Deterministic xorshift64: reproducible key streams without dependencies.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

#[derive(Default)]
struct Stats {
    txns: u64,
    units: u64,
    lat_nanos: Vec<u64>,
}

impl Stats {
    fn record(&mut self, units: u64, elapsed: Duration) {
        self.txns += 1;
        self.units += units;
        if self.txns % LATENCY_SAMPLE_EVERY == 0 {
            self.lat_nanos.push(elapsed.as_nanos() as u64);
        }
    }

    fn report(&mut self, name: &str, unit: &str, elapsed: Duration) -> Report {
        self.lat_nanos.sort_unstable();
        let n = self.lat_nanos.len();
        let pct = |p: f64| -> f64 {
            if n == 0 {
                return 0.0;
            }
            self.lat_nanos[((n as f64 * p) as usize).min(n - 1)] as f64 / 1000.0
        };
        let avg = if n == 0 { 0.0 } else { self.lat_nanos.iter().sum::<u64>() as f64 / n as f64 / 1000.0 };
        let secs = elapsed.as_secs_f64();
        let report = Report {
            name: name.to_string(),
            unit: unit.to_string(),
            seconds: secs,
            txns: self.txns,
            units: self.units,
            units_per_sec: self.units as f64 / secs,
            avg_us: avg,
            p50_us: pct(0.50),
            p99_us: pct(0.99),
        };
        println!(
            "{:<22} {:>12} {:>6} {:>14.0}/s  avg {:>9.1}us  p50 {:>9.1}us  p99 {:>9.1}us",
            report.name,
            format!("{} {}", report.units, unit),
            format!("{:.0}s", secs),
            report.units_per_sec,
            report.avg_us,
            report.p50_us,
            report.p99_us,
        );
        report
    }
}

struct Report {
    name: String,
    unit: String,
    seconds: f64,
    txns: u64,
    units: u64,
    units_per_sec: f64,
    avg_us: f64,
    p50_us: f64,
    p99_us: f64,
}

fn write_json(path: &PathBuf, reports: &[Report]) -> std::io::Result<()> {
    let mut out = String::from("{\"scenarios\":[");
    for (i, r) in reports.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(&format!(
            "{{\"name\":\"{}\",\"unit\":\"{}\",\"seconds\":{:.2},\"txns\":{},\"units\":{},\"units_per_sec\":{:.1},\"avg_us\":{:.1},\"p50_us\":{:.1},\"p99_us\":{:.1}}}",
            r.name, r.unit, r.seconds, r.txns, r.units, r.units_per_sec, r.avg_us, r.p50_us, r.p99_us
        ));
    }
    out.push_str("]}");
    std::fs::write(path, out)
}

fn sku(id: u64) -> String {
    format!("SKU-{:07}", id % 50_000)
}

fn payload(id: u64) -> String {
    format!("payload-{id:010}-abcdefghijklmnopqrstuvwxyz0123456789")
}

fn lit(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn exe(name: &str) -> String {
    if cfg!(windows) { format!("{name}.exe") } else { name.to_string() }
}

/// Release server binary: `$MURMUR_SERVER_BIN`, an existing release build,
/// or a fresh `cargo build --release`. Benchmarks must never run a debug server.
fn ensure_server_binary() -> Result<PathBuf, String> {
    if let Ok(path) = std::env::var("MURMUR_SERVER_BIN") {
        return Ok(PathBuf::from(path));
    }
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    let release = root.join("target").join("release").join(exe("murmur-server"));
    if release.exists() {
        return Ok(release);
    }
    println!("building release murmur-server + murmur-admin (one-time cost) ...");
    let status = Command::new("cargo")
        .args(["build", "--release", "--bin", "murmur-server", "--bin", "murmur-admin"])
        .current_dir(&root)
        .status()
        .map_err(|e| format!("cargo build failed to start: {e}"))?;
    if !status.success() {
        return Err("cargo build --release failed".into());
    }
    Ok(release)
}

fn password_hash(server: &Path) -> Result<String, String> {
    let admin = server.with_file_name(exe("murmur-admin"));
    let out = Command::new(&admin)
        .args(["hash-password", "--password", PASSWORD])
        .output()
        .map_err(|e| format!("murmur-admin failed to start: {e}"))?;
    if !out.status.success() {
        return Err("murmur-admin hash-password failed".into());
    }
    Ok(String::from_utf8(out.stdout).map_err(|e| e.to_string())?.trim().to_string())
}

enum Root {
    Temp(tempfile::TempDir),
    Keep(PathBuf),
}

impl Root {
    fn path(&self) -> &Path {
        match self {
            Root::Temp(dir) => dir.path(),
            Root::Keep(path) => path,
        }
    }
}

struct Server {
    _root: Root,
    child: Child,
    port: u16,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Server {
    fn dir(&self) -> &Path {
        self._root.path()
    }
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

fn start_server(root: Root, server_bin: &Path, hash: &str) -> Result<Server, String> {
    let port = free_port();
    let dir = root.path();
    let config = format!(
        "[server]\nbind = \"127.0.0.1:{port}\"\nproduction = false\n\n\
         [storage]\ndata_dir = \"{}\"\n\n\
         [[auth.users]]\nname = \"admin\"\npassword_hash = \"{hash}\"\n\n\
         [logging]\ndir = \"{}\"\n\n\
         [backups]\ndir = \"{}\"\n",
        dir.join("data").to_string_lossy().replace('\\', "/"),
        dir.join("logs").to_string_lossy().replace('\\', "/"),
        dir.join("backups").to_string_lossy().replace('\\', "/"),
    );
    std::fs::write(dir.join("config.toml"), config).map_err(|e| e.to_string())?;
    let mut child = Command::new(server_bin)
        .arg("--config")
        .arg(dir.join("config.toml"))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|e| format!("spawn murmur-server: {e}"))?;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            break;
        }
        if let Ok(Some(status)) = child.try_wait() {
            return Err(format!("server exited early: {status}"));
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            return Err(format!("server on port {port} did not start"));
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    Ok(Server { _root: root, child, port })
}

fn dir_size(path: &Path) -> u64 {
    let mut total = 0;
    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            let p = entry.path();
            if p.is_dir() {
                total += dir_size(&p);
            } else if let Ok(meta) = p.metadata() {
                total += meta.len();
            }
        }
    }
    total
}

fn client_error(error: murmur_client::Error) -> String {
    error.to_string()
}

fn pg_error(error: murmur_client::tokio_postgres::Error) -> String {
    error.to_string()
}

/// One multi-row INSERT statement for `size` rows starting at `first`.
fn insert_batch(first: u64, size: usize) -> String {
    let mut sql = String::from("INSERT INTO items VALUES");
    for row in first..first + size as u64 {
        if row > first {
            sql.push(',');
        }
        sql.push_str(&format!("({},{},{},{})", row, lit(&sku(row)), (row % 1000) as i64, lit(&payload(row))));
    }
    sql
}

struct Prepared {
    sel_id: Statement,
    sel_sku: Statement,
    cnt_qty: Statement,
    sel_qty: Statement,
    sel_range: Statement,
    ins_one: Statement,
    upd_one: Statement,
}

async fn prepare(client: &Client) -> Result<Prepared, String> {
    let inner = client.inner();
    let sel_id = inner.prepare("SELECT qty FROM items WHERE id=$1").await.map_err(pg_error)?;
    let sel_sku = inner.prepare("SELECT id FROM items WHERE sku=$1 LIMIT 10").await.map_err(pg_error)?;
    let cnt_qty = inner.prepare("SELECT count(*) FROM items WHERE qty=$1").await.map_err(pg_error)?;
    let sel_qty = inner.prepare("SELECT id FROM items WHERE qty=$1 LIMIT 20").await.map_err(pg_error)?;
    let sel_range = inner.prepare("SELECT id, sku, qty, payload FROM items WHERE id>=$1 ORDER BY id LIMIT $2").await.map_err(pg_error)?;
    let ins_one = inner.prepare("INSERT INTO items VALUES($1,$2,$3,$4)").await.map_err(pg_error)?;
    let upd_one = inner.prepare("UPDATE items SET qty=qty+1 WHERE id=$1").await.map_err(pg_error)?;
    Ok(Prepared { sel_id, sel_sku, cnt_qty, sel_qty, sel_range, ins_one, upd_one })
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("murmur-local-bench: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    #[cfg(debug_assertions)]
    eprintln!("warning: debug benchmark build; use --release for real numbers");
    let args = parse_args()?;
    let server_bin = ensure_server_binary()?;
    println!("server: {}", server_bin.display());
    let root = match &args.dir {
        Some(dir) => {
            if dir.exists() {
                return Err(format!("refusing to use existing directory {}", dir.display()));
            }
            std::fs::create_dir_all(dir).map_err(|e| e.to_string())?;
            Root::Keep(dir.clone())
        }
        None => Root::Temp(tempfile::tempdir().map_err(|e| e.to_string())?),
    };
    let hash = password_hash(&server_bin)?;
    let server = start_server(root, &server_bin, &hash)?;
    let mut cfg = Config::new("admin", PASSWORD, KEY);
    cfg.port = server.port;
    let client = Client::connect(&cfg).await.map_err(client_error)?;
    client
        .simple("CREATE TABLE items(id INTEGER PRIMARY KEY, sku TEXT NOT NULL, qty INTEGER NOT NULL, payload TEXT NOT NULL); CREATE INDEX items_sku ON items(sku)")
        .await
        .map_err(client_error)?;

    println!("preload: {} rows ...", args.preload);
    let setup_start = Instant::now();
    let mut id = 1u64;
    while id <= args.preload {
        let end = (id + 999).min(args.preload);
        client.simple(&insert_batch(id, (end - id + 1) as usize)).await.map_err(client_error)?;
        id = end + 1;
    }
    let setup_secs = setup_start.elapsed().as_secs_f64();
    println!("preload: {:.0} rows/s ({:.1}s, 1000-row statements)", args.preload as f64 / setup_secs, setup_secs);
    println!();
    println!("each scenario runs {}s over one client connection; background persistence on, no flush during measurement", args.seconds);
    println!();

    let stmts = prepare(&client).await?;
    let inner = client.inner();
    let scenario = Duration::from_secs(args.seconds);
    let mut reports = Vec::new();
    let mut rng = Rng(0x243f_6a88_85a3_08d3);
    // Fresh ids stay ahead of every row any scenario adds.
    let mut next_id = args.preload + 1;

    // 1. Inserts at several batch sizes (one round trip per batch). Size 1
    // uses a prepared statement, matching the point-select path so the
    // single-row numbers compare fairly.
    for size in TXN_SIZES {
        let name = format!("insert-txn-{size}");
        let mut stats = Stats::default();
        let deadline = Instant::now() + scenario;
        while Instant::now() < deadline {
            let first = next_id;
            next_id += size as u64;
            let start = Instant::now();
            if size == 1 {
                let (id, s, q, p) = (first as i64, sku(first), (first % 1000) as i64, payload(first));
                inner.execute(&stmts.ins_one, &[&id, &s, &q, &p]).await.map_err(pg_error)?;
            } else {
                client.simple(&insert_batch(first, size)).await.map_err(client_error)?;
            }
            stats.record(size as u64, start.elapsed());
        }
        reports.push(stats.report(&name, "rows", scenario));
    }

    // 2. Point select by primary key.
    {
        let mut stats = Stats::default();
        let deadline = Instant::now() + scenario;
        while Instant::now() < deadline {
            let id = (1 + rng.below(next_id - 1)) as i64;
            let start = Instant::now();
            let row = inner.query_one(&stmts.sel_id, &[&id]).await.map_err(pg_error)?;
            let qty: i64 = row.get(0);
            debug_assert!(qty >= 0);
            stats.record(1, start.elapsed());
        }
        reports.push(stats.report("select-by-id", "queries", scenario));
    }

    // 3. Secondary-index lookup.
    {
        let mut stats = Stats::default();
        let deadline = Instant::now() + scenario;
        while Instant::now() < deadline {
            let want = sku(1 + rng.below(next_id - 1));
            let start = Instant::now();
            let rows = inner.query(&stmts.sel_sku, &[&want]).await.map_err(pg_error)?;
            stats.record(rows.len() as u64, start.elapsed());
        }
        reports.push(stats.report("select-indexed", "rows", scenario));
    }

    // 4. Unindexed predicate: full scan with an aggregate.
    {
        let mut stats = Stats::default();
        let deadline = Instant::now() + scenario;
        while Instant::now() < deadline {
            let qty = rng.below(1000) as i64;
            let start = Instant::now();
            let row = inner.query_one(&stmts.cnt_qty, &[&qty]).await.map_err(pg_error)?;
            let count: i64 = row.get(0);
            debug_assert!(count >= 0);
            stats.record(1, start.elapsed());
        }
        reports.push(stats.report("select-scan", "queries", scenario));
    }

    // 5. Large result sets: PK-range scans returning 1k/10k/100k full rows.
    // Measures result buffering, encoding, and wire transfer; also reports
    // payload MiB/s summed from decoded field lengths.
    for size in [1_000u64, 10_000, 100_000] {
        let name = format!("select-large-{size}");
        if next_id <= size + 1 {
            println!("{name:<22} skipped (table too small)");
            continue;
        }
        let mut stats = Stats::default();
        let mut bytes = 0u64;
        let deadline = Instant::now() + scenario;
        while Instant::now() < deadline {
            let lo = (1 + rng.below(next_id - size - 1)) as i64;
            let limit = size as i64;
            let start = Instant::now();
            let rows = inner.query(&stmts.sel_range, &[&lo, &limit]).await.map_err(pg_error)?;
            for row in &rows {
                let sku: String = row.get(1);
                let payload: String = row.get(3);
                bytes += 16 + sku.len() as u64 + payload.len() as u64;
            }
            stats.record(rows.len() as u64, start.elapsed());
        }
        let report = stats.report(&name, "rows", scenario);
        println!("  {:.1} MiB/s payload", bytes as f64 / report.seconds / (1 << 20) as f64);
        reports.push(report);
    }

    // 6. Updates by primary key at several batch sizes. Size 1 uses a
    // prepared statement, matching the point-select path.
    for size in [1usize, 10, 100] {
        let name = format!("update-txn-{size}");
        let mut stats = Stats::default();
        let deadline = Instant::now() + scenario;
        while Instant::now() < deadline {
            let start = Instant::now();
            if size == 1 {
                let id = (1 + rng.below(next_id - 1)) as i64;
                inner.execute(&stmts.upd_one, &[&id]).await.map_err(pg_error)?;
            } else {
                let mut batch = String::new();
                for _ in 0..size {
                    let id = 1 + rng.below(next_id - 1);
                    batch.push_str(&format!("UPDATE items SET qty=qty+1 WHERE id={id};"));
                }
                client.simple(&batch).await.map_err(client_error)?;
            }
            stats.record(size as u64, start.elapsed());
        }
        reports.push(stats.report(&name, "rows", scenario));
    }

    // 7. Mixed 80% read / 20% write: 35% by-id, 25% indexed, 20% unindexed,
    // 10% insert, 10% update.
    {
        let mut stats = Stats::default();
        let mut reads = 0u64;
        let mut writes = 0u64;
        let deadline = Instant::now() + scenario;
        while Instant::now() < deadline {
            let pick = rng.below(100);
            let start = Instant::now();
            if pick < 35 {
                let id = (1 + rng.below(next_id - 1)) as i64;
                let row = inner.query_one(&stmts.sel_id, &[&id]).await.map_err(pg_error)?;
                let qty: i64 = row.get(0);
                debug_assert!(qty >= 0);
                reads += 1;
            } else if pick < 60 {
                let want = sku(1 + rng.below(next_id - 1));
                let rows = inner.query(&stmts.sel_sku, &[&want]).await.map_err(pg_error)?;
                debug_assert!(!rows.is_empty());
                reads += 1;
            } else if pick < 80 {
                let qty = rng.below(1000) as i64;
                let rows = inner.query(&stmts.sel_qty, &[&qty]).await.map_err(pg_error)?;
                debug_assert!(!rows.is_empty());
                reads += 1;
            } else if pick < 90 {
                let row = next_id;
                next_id += 1;
                let (id, s, q, p) = (row as i64, sku(row), (row % 1000) as i64, payload(row));
                inner.execute(&stmts.ins_one, &[&id, &s, &q, &p]).await.map_err(pg_error)?;
                writes += 1;
            } else {
                let id = (1 + rng.below(next_id - 1)) as i64;
                inner.execute(&stmts.upd_one, &[&id]).await.map_err(pg_error)?;
                writes += 1;
            }
            stats.record(1, start.elapsed());
        }
        let report = stats.report("mixed-80-20", "ops", scenario);
        println!("  reads={reads} writes={writes} ({:.1}% read)", 100.0 * reads as f64 / (reads + writes) as f64);
        reports.push(report);
    }

    client.flush().await.map_err(client_error)?;
    let row = inner.query_one("SELECT count(*) FROM items", &[]).await.map_err(pg_error)?;
    let rows: i64 = row.get(0);
    drop(client);
    let disk_mib = dir_size(&server.dir().join("data")) / (1 << 20);
    drop(server);
    println!();
    println!("final rows: {rows}; data dir on disk: {disk_mib} MiB");
    if let Some(path) = &args.json {
        write_json(path, &reports).map_err(|e| e.to_string())?;
        println!("json written to {}", path.display());
    }
    Ok(())
}
