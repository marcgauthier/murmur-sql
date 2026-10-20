//! PostgreSQL-wire throughput benchmark for Murmur-SQL.
//!
//! Uses an 80% read / 20% write mix over a 10-column users table. Each
//! request processes a batch in one SQL statement (one engine transaction for
//! writes, one engine read call for reads). Defaults to comparing 1/100/1000
//! items per request; no BEGIN/COMMIT wrappers or per-item network waits.
//!
//! By default the benchmark builds (if needed) and spawns its own
//! `murmur-server` on a loopback port. Pass `--port` to drive an externally
//! managed server instead (it must accept `KEY`, or be fresh).

use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio_postgres::{
    types::{ToSql, Type},
    Client, NoTls, Statement,
};

const PASSWORD: &str = "benchmark-s3cret";
// `murmur-admin hash-password --password benchmark-s3cret` (fixed salt output).
const PASSWORD_HASH: &str = "$argon2id$v=19$m=19456,t=2,p=1$KO2NuZ4MfxFJugdlJ7Wgsg$1FVhr7TV7+Cr1QhUovw8mIcop/4d0Mi8ZhmMANROMww";
const KEY: &str = "benchmark-encryption-key-0123456789";
const USER: &str = "bench";

const SCHEMA: &str = "CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    username TEXT NOT NULL,
    email TEXT NOT NULL,
    full_name TEXT NOT NULL,
    role TEXT NOT NULL,
    status INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    login_count INTEGER NOT NULL,
    bio TEXT NOT NULL
)";
const INDEX_EMAIL: &str = "CREATE INDEX idx_users_email ON users(email)";
const INDEX_STATUS: &str = "CREATE INDEX idx_users_status_created ON users(status, created_at)";

struct Args {
    duration_secs: u64,
    seed_rows: i64,
    clients: usize,
    batches: Vec<usize>,
    capture: String,
    port: u16,
    host: String,
    server_bin: Option<String>,
}

impl Args {
    fn parse() -> Self {
        let mut args = Self {
            duration_secs: 30,
            seed_rows: 20_000,
            clients: 8,
            batches: vec![1, 100, 1000],
            capture: "1s".to_string(),
            port: 0,
            host: "127.0.0.1".to_string(),
            server_bin: None,
        };
        let mut it = std::env::args().skip(1);
        while let Some(arg) = it.next() {
            match arg.as_str() {
                "--duration" => {
                    args.duration_secs = it
                        .next()
                        .expect("--duration needs seconds")
                        .parse()
                        .expect("bad --duration")
                }
                "--seed" => {
                    args.seed_rows = it
                        .next()
                        .expect("--seed needs rows")
                        .parse()
                        .expect("bad --seed")
                }
                "--clients" => {
                    args.clients = it
                        .next()
                        .expect("--clients needs N")
                        .parse()
                        .expect("bad --clients")
                }
                "--batch" | "--batches" => {
                    args.batches = it
                        .next()
                        .expect("--batches needs comma-separated sizes")
                        .split(',')
                        .map(|n| n.parse().expect("bad batch size"))
                        .collect();
                }
                "--capture" => args.capture = it.next().expect("--capture needs a duration"),
                "--port" => {
                    args.port = it
                        .next()
                        .expect("--port needs a port")
                        .parse()
                        .expect("bad --port")
                }
                "--host" => args.host = it.next().expect("--host needs an address"),
                "--server-bin" => {
                    args.server_bin = Some(it.next().expect("--server-bin needs a path"))
                }
                "-h" | "--help" => {
                    println!("usage: murmur-sql-bench [--duration S] [--seed ROWS] [--clients N] [--batches 1,100,1000] [--capture 1s] [--host H] [--port P] [--server-bin PATH]");
                    println!(
                        "  default spawns its own murmur-server; --port drives an external one"
                    );
                    std::process::exit(0);
                }
                other => panic!("unknown argument {other:?}"),
            }
        }
        assert!(args.clients >= 1, "--clients must be >= 1");
        assert!(
            args.batches.iter().all(|n| (1..=1000).contains(n)),
            "batch sizes must be 1..=1000"
        );
        assert!(args.duration_secs >= 1, "--duration must be >= 1");
        assert!(args.seed_rows >= 1000, "--seed must be >= 1000");
        args
    }
}

/// Deterministic xorshift64: no extra dependency, reproducible runs.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0 | 1;
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

struct Server {
    child: Option<Child>,
}

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn resolve_server_bin(explicit: Option<String>) -> String {
    if let Some(path) = explicit {
        return path;
    }
    if let Ok(path) = std::env::var("MURMUR_SERVER_BIN") {
        return path;
    }
    // Build the parent package's server binary (debug or release to match us).
    let manifest = format!("{}/../Cargo.toml", env!("CARGO_MANIFEST_DIR"));
    let mut build = Command::new("cargo");
    build
        .arg("build")
        .arg("--manifest-path")
        .arg(&manifest)
        .arg("--bin")
        .arg("murmur-server");
    let profile = if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    };
    if profile == "release" {
        build.arg("--release");
    }
    assert!(
        build
            .status()
            .expect("cargo build failed to spawn")
            .success(),
        "cargo build murmur-server failed"
    );
    format!(
        "{}/../target/{profile}/murmur-server",
        env!("CARGO_MANIFEST_DIR")
    )
}

fn spawn_server(bin: &str, dir: &tempfile::TempDir, port: u16, capture: &str) -> Server {
    let config = dir.path().join("config.toml");
    // Engine tuning mirrors ../MURMUR/tests-benchmark persistence settings.
    std::fs::write(
        &config,
        format!(
            "[server]\nbind = \"127.0.0.1:{port}\"\n\
             [storage]\ndata_dir = \"{}\"\n\
             [engine]\ncapture_interval = \"{capture}\"\ncapture_bytes = \"8MiB\"\nqueue_bytes = \"128MiB\"\ntransaction_bytes = \"128MiB\"\n\
             [[auth.users]]\nname = \"{USER}\"\npassword_hash = \"{PASSWORD_HASH}\"\n\
             [logging]\ndir = \"{}\"\n",
            dir.path().join("data").display(),
            dir.path().join("logs").display(),
        ),
    )
    .unwrap();
    let mut child = Command::new(bin)
        .arg("--config")
        .arg(&config)
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn murmur-server");
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            break;
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            panic!("server on port {port} did not start in time");
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    Server { child: Some(child) }
}

async fn connect(host: &str, port: u16) -> Client {
    let (client, connection) = tokio_postgres::connect(
        &format!("host={host} port={port} user={USER} password={PASSWORD} dbname=murmur"),
        NoTls,
    )
    .await
    .expect("connect to murmur-server");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .simple_query(&format!("MURMUR UNLOCK KEY '{KEY}'"))
        .await
        .expect("unlock");
    client
}

fn lit(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// Seed with multi-row literal INSERTs (generated data is quote-free).
async fn seed(client: &Client, rows: i64) {
    client
        .simple_query("DROP TABLE IF EXISTS users")
        .await
        .unwrap();
    client.simple_query(SCHEMA).await.unwrap();
    client.simple_query(INDEX_EMAIL).await.unwrap();
    client.simple_query(INDEX_STATUS).await.unwrap();
    let mut id = 1i64;
    while id <= rows {
        let end = (id + 499).min(rows);
        let mut sql = String::from("INSERT INTO users VALUES ");
        for (i, row) in (id..=end).enumerate() {
            if i > 0 {
                sql.push(',');
            }
            sql.push_str(&format!(
                "({},{},{},{},{},{},{},{},{},{})",
                row,
                lit(&format!("user{row}")),
                lit(&format!("user{row}@example.com")),
                lit(&format!("User {row}")),
                lit("member"),
                row % 4,
                1_700_000_000 + row,
                1_700_000_000 + row,
                row % 100,
                lit(&format!("bio {row}")),
            ));
        }
        client.simple_query(&sql).await.unwrap();
        id = end + 1;
    }
    client.simple_query("MURMUR FLUSH").await.unwrap();
}

struct WorkerStat {
    reads: u64,
    writes: u64,
    requests: u64,
    errors: u64,
    first_error: Option<String>,
    lat_nanos: Vec<u64>,
    inserts: u64,
}

/// Explicit wire types avoid relying on inference for VALUES-derived tables.
struct Statements {
    select_id: Statement,
    select_email: Statement,
    update: Statement,
    insert: Statement,
}

fn placeholders(rows: usize, columns: usize) -> String {
    (0..rows)
        .map(|row| {
            let params = (1..=columns)
                .map(|col| format!("${}", row * columns + col))
                .collect::<Vec<_>>()
                .join(",");
            format!("({params})")
        })
        .collect::<Vec<_>>()
        .join(",")
}

impl Statements {
    async fn prepare(client: &Client, batch: usize) -> Self {
        let lookups = placeholders(batch, 1);
        let select_id = client.prepare_typed(
            &format!("SELECT users.* FROM (VALUES {lookups}) AS requested JOIN users ON users.id = requested.column1"),
            &vec![Type::INT8; batch],
        ).await.unwrap();
        let select_email = client.prepare_typed(
            &format!("SELECT users.* FROM (VALUES {lookups}) AS requested JOIN users ON users.email = requested.column1"),
            &vec![Type::TEXT; batch],
        ).await.unwrap();
        let update = client.prepare_typed(
            &format!("UPDATE users SET updated_at = incoming.column2, login_count = incoming.column3, bio = incoming.column4, status = incoming.column5 FROM (VALUES {}) AS incoming WHERE users.id = incoming.column1", placeholders(batch, 5)),
            &[Type::INT8, Type::INT8, Type::INT8, Type::TEXT, Type::INT8].iter().cloned().cycle().take(batch * 5).collect::<Vec<_>>(),
        ).await.unwrap();
        let insert = client
            .prepare_typed(
                &format!("INSERT INTO users VALUES {}", placeholders(batch, 10)),
                &[
                    Type::INT8,
                    Type::TEXT,
                    Type::TEXT,
                    Type::TEXT,
                    Type::TEXT,
                    Type::INT8,
                    Type::INT8,
                    Type::INT8,
                    Type::INT8,
                    Type::TEXT,
                ]
                .iter()
                .cloned()
                .cycle()
                .take(batch * 10)
                .collect::<Vec<_>>(),
            )
            .await
            .unwrap();
        Self {
            select_id,
            select_email,
            update,
            insert,
        }
    }
}

type Param = Box<dyn ToSql + Sync + Send>;

async fn worker(
    client: Client,
    statements: Statements,
    stop: Instant,
    seed_max: i64,
    next_id: Arc<AtomicI64>,
    rng_seed: u64,
    batch: usize,
) -> WorkerStat {
    let mut rng = Rng(rng_seed);
    let mut stat = WorkerStat {
        reads: 0,
        writes: 0,
        requests: 0,
        errors: 0,
        first_error: None,
        lat_nanos: Vec::new(),
        inserts: 0,
    };
    let mut checksum: i64 = 0;
    while Instant::now() < stop {
        // Include request construction, wire transfer and result consumption.
        let start = Instant::now();
        let roll = rng.below(100);
        let mut params: Vec<Param> = Vec::new();
        let statement = if roll < 80 {
            for _ in 0..batch {
                let id = 1 + rng.below(seed_max as u64) as i64;
                if roll < 40 {
                    params.push(Box::new(id));
                } else {
                    params.push(Box::new(format!("user{id}@example.com")));
                }
            }
            if roll < 40 {
                &statements.select_id
            } else {
                &statements.select_email
            }
        } else if roll < 90 {
            // Distinct seeded IDs ensure exactly batch rows are updated.
            let first = rng.below(seed_max as u64) as i64;
            for offset in 0..batch {
                let id = 1 + (first + offset as i64) % seed_max;
                params.push(Box::new(id));
                params.push(Box::new(1_700_100_000 + rng.below(1_000_000) as i64));
                params.push(Box::new(rng.below(10_000) as i64));
                params.push(Box::new(format!("bio update {}", rng.next())));
                params.push(Box::new(rng.below(4) as i64));
            }
            &statements.update
        } else {
            let first = next_id.fetch_add(batch as i64, Ordering::Relaxed);
            for id in first..first + batch as i64 {
                let now = 1_700_100_000 + rng.below(1_000_000) as i64;
                params.push(Box::new(id));
                params.push(Box::new(format!("user{id}")));
                params.push(Box::new(format!("user{id}@example.com")));
                params.push(Box::new(format!("User {id}")));
                params.push(Box::new(String::from("member")));
                params.push(Box::new(id % 4));
                params.push(Box::new(now));
                params.push(Box::new(now));
                params.push(Box::new(0i64));
                params.push(Box::new(format!("bio {id}")));
            }
            &statements.insert
        };
        let refs: Vec<&(dyn ToSql + Sync)> = params
            .iter()
            .map(|p| p.as_ref() as &(dyn ToSql + Sync))
            .collect();
        let outcome: Result<(), String> = if roll < 80 {
            match client.query(statement, &refs).await {
                Ok(rows) => {
                    assert_eq!(rows.len(), batch, "bulk lookup returned wrong row count");
                    for row in &rows {
                        for index in [0, 5, 6, 7, 8] {
                            checksum ^= row.get::<_, i64>(index);
                        }
                        for index in [1, 2, 3, 4, 9] {
                            checksum ^= row.get::<_, String>(index).len() as i64;
                        }
                    }
                    stat.reads += batch as u64;
                    Ok(())
                }
                Err(e) => Err(format!("{e:?}")),
            }
        } else {
            match client.execute(statement, &refs).await {
                Ok(count) => {
                    assert_eq!(count, batch as u64, "bulk write affected wrong row count");
                    stat.writes += count;
                    if roll >= 90 {
                        stat.inserts += count;
                    }
                    Ok(())
                }
                Err(e) => Err(format!("{e:?}")),
            }
        };
        stat.requests += 1;
        if let Err(error) = outcome {
            stat.errors += 1;
            stat.first_error.get_or_insert(error);
        }
        stat.lat_nanos.push(start.elapsed().as_nanos() as u64);
    }
    std::hint::black_box(checksum);
    stat
}

fn percentile(sorted_nanos: &[u64], p: f64) -> u64 {
    if sorted_nanos.is_empty() {
        return 0;
    }
    sorted_nanos[((p / 100.0 * sorted_nanos.len() as f64) as usize).min(sorted_nanos.len() - 1)]
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let dir = tempfile::TempDir::new().unwrap();
    let (host, port, _server) = if args.port == 0 {
        let port = free_port();
        let bin = resolve_server_bin(args.server_bin);
        println!("spawning {bin} on 127.0.0.1:{port}");
        let server = spawn_server(&bin, &dir, port, &args.capture);
        ("127.0.0.1".to_string(), port, Some(server))
    } else {
        (args.host.clone(), args.port, None)
    };

    let setup = connect(&host, port).await;
    let mut results = Vec::new();
    let mut any_errors = false;
    for batch in args.batches {
        println!("\nseeding {} rows for batch={batch}...", args.seed_rows);
        seed(&setup, args.seed_rows).await;
        let next_id = Arc::new(AtomicI64::new(args.seed_rows + 1));
        let mut clients = Vec::new();
        for _ in 0..args.clients {
            let client = connect(&host, port).await;
            let statements = Statements::prepare(&client, batch).await;
            clients.push((client, statements));
        }
        println!(
            "running {} clients for {}s, {batch} items/request (80/20 mix)...",
            args.clients, args.duration_secs
        );
        let run_start = Instant::now();
        let stop = run_start + Duration::from_secs(args.duration_secs);
        let mut tasks = Vec::new();
        for (i, (client, statements)) in clients.into_iter().enumerate() {
            tasks.push(tokio::spawn(worker(
                client,
                statements,
                stop,
                args.seed_rows,
                next_id.clone(),
                0x9e3779b97f4a7c15u64.wrapping_add(i as u64),
                batch,
            )));
        }
        let (mut reads, mut writes, mut requests, mut errors, mut inserts) = (0, 0, 0, 0, 0);
        let mut lat = Vec::new();
        for task in tasks {
            let stat = task.await.unwrap();
            reads += stat.reads;
            writes += stat.writes;
            requests += stat.requests;
            errors += stat.errors;
            inserts += stat.inserts;
            lat.extend(stat.lat_nanos);
            if let Some(error) = stat.first_error {
                eprintln!("batch={batch} worker error: {error}");
            }
        }
        // Include completion of requests still in flight at the deadline.
        let elapsed = run_start.elapsed().as_secs_f64();
        lat.sort_unstable();
        let total = reads + writes;
        let rate = total as f64 / elapsed;
        let request_rate = requests as f64 / elapsed;
        let p50 = percentile(&lat, 50.0) / 1000;
        let p99 = percentile(&lat, 99.0) / 1000;
        println!("batch={batch}: {total} successful items in {elapsed:.2}s ({rate:.0} items/s)");
        println!(
            "reads={reads} writes={writes} requests/s={request_rate:.0} failed_requests={errors}"
        );
        println!(
            "batch latency (us): p50={p50} p99={p99} max={}",
            lat.last().copied().unwrap_or(0) / 1000
        );
        let count: i64 = setup
            .query_one("SELECT COUNT(*) FROM users", &[])
            .await
            .unwrap()
            .get(0);
        assert_eq!(
            count,
            args.seed_rows + inserts as i64,
            "final inserted row count"
        );
        setup
            .simple_query("MURMUR FLUSH")
            .await
            .expect("flush after measurement");
        any_errors |= errors != 0;
        results.push((batch, rate, request_rate, p50, p99, errors));
    }
    println!("\nitems/batch | items/s | requests/s | batch p50 us | batch p99 us | errors");
    for (batch, rate, request_rate, p50, p99, errors) in results {
        println!(
            "{batch:>11} | {rate:>7.0} | {request_rate:>10.0} | {p50:>12} | {p99:>12} | {errors}"
        );
    }
    // Let the server guard clean up even when a run failed.
    drop(_server);
    assert!(
        !any_errors,
        "benchmark had failed requests; see errors above"
    );
}
