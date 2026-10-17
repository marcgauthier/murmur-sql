//! Rust port of the Go `compression` live benchmark.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/compression/`
//! (`compression_test.go`, `TestSpoolCompressionSizes`). For several
//! realistic traffic shapes, compare the on-disk store size across Spool
//! compression levels. Each cell runs a real single-node `murmur-server`
//! on real disk, writes a seeded deterministic dataset through plain SQL,
//! shuts down cleanly, and measures the data directory. The Rust engine
//! has no compression on/off knob (Spool always uses zstd with a level),
//! so cells compare the lowest and a high `compression_level` instead of
//! Go's `none` vs `deflate` (see the scenario README.md).

use std::time::{Duration, Instant};

use murmur_live_harness::tokio_postgres::types::ToSql;
use murmur_live_harness::{Cluster, ClusterOptions};

/// Compression levels under test: lowest vs high zstd (Go compares
/// `none` vs `deflate`, which has no Rust equivalent).
const LEVELS: [i32; 2] = [1, 12];
/// Insert batch size (Go: 500 rows per write batch).
const BATCH: usize = 500;
/// Flush/update stride inside the churn pass.
const CHURN_FLUSH_STRIDE: usize = 500;
/// Fixed dataset seed: every level of one profile gets byte-identical keys
/// and datasets (Go: `seed = 42`).
const SEED: u64 = 42;

const LOREM: &str = "lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod tempor incididunt ut labore et dolore magna aliqua enim ad minim veniam quis nostrud exercitation ullamco laboris nisi aliquip ex ea commodo consequat duis aute irure in reprehenderit voluptate velit esse cillum fugiat nulla pariatur excepteur sint occaecat cupidatat non proident sunt culpa qui officia deserunt mollit anim id est laborum perspiciatis unde omnis natus error voluptatem";

/// Minimal deterministic PRNG (xorshift64*): only same-seed equality across
/// levels within one process is needed, not cross-language equality.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545f4914f6cdd1d)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    fn bytes(&mut self, out: &mut [u8]) {
        let mut i = 0;
        while i < out.len() {
            let word = self.next().to_le_bytes();
            let take = (out.len() - i).min(8);
            out[i..i + take].copy_from_slice(&word[..take]);
            i += take;
        }
    }

    fn float(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
}

fn lorem(rng: &mut Rng, words: usize) -> String {
    let vocab: Vec<&str> = LOREM.split(' ').collect();
    (0..words)
        .map(|_| vocab[rng.below(vocab.len() as u64) as usize])
        .collect::<Vec<_>>()
        .join(" ")
}

/// One traffic profile (Go: `trafficProfile`).
struct Profile {
    name: &'static str,
    rows: usize,
    /// 0 = oltp-small, 1 = text-heavy, 2 = json-docs, 3 = blobs-random.
    kind: u8,
}

const PROFILES: [Profile; 4] = [
    Profile { name: "oltp-small", rows: 20_000, kind: 0 },
    Profile { name: "text-heavy", rows: 8_000, kind: 1 },
    Profile { name: "json-docs", rows: 8_000, kind: 2 },
    Profile { name: "blobs-random", rows: 2_000, kind: 3 },
];

/// One generated row: all columns (Go: full `compressionRecord` with zero
/// values for untouched fields).
struct Record {
    id: String,
    code: String,
    qty: i64,
    price: f64,
    title: String,
    body: String,
    tags: String,
    kind: String,
    payload: String,
    data: Vec<u8>,
}

fn create(rng: &mut Rng, profile: &Profile, i: usize) -> Record {
    let mut id_bytes = [0u8; 16];
    rng.bytes(&mut id_bytes);
    let id = id_bytes.iter().map(|b| format!("{b:02x}")).collect();
    let mut record = Record {
        id,
        code: String::new(),
        qty: 0,
        price: 0.0,
        title: String::new(),
        body: String::new(),
        tags: String::new(),
        kind: String::new(),
        payload: String::new(),
        data: Vec::new(),
    };
    match profile.kind {
        0 => {
            record.code = format!("SKU-{:05}", rng.below(2000));
            record.qty = rng.below(100) as i64;
            record.price = rng.float() * 999.99;
        }
        1 => {
            let title_words = 4 + rng.below(6) as usize;
            let body_words = 100 + rng.below(200) as usize;
            record.title = lorem(rng, title_words);
            record.body = lorem(rng, body_words);
            record.tags = "news,tech,local".to_string();
        }
        2 => {
            record.kind = "pageview".to_string();
            record.payload = format!(
                "{{\"event_id\":{i},\"user\":{{\"id\":{},\"name\":\"user-{}\",\"roles\":[\"reader\",\"editor\"]}},\"context\":{{\"app\":\"storefront\",\"region\":\"eu-west\",\"build\":{}}},\"metrics\":{{\"latency_ms\":{:.3},\"retries\":{},\"flags\":[true,false,true]}},\"trace\":[\"recv\",\"auth\",\"load\",\"render\",\"send\"]}}",
                rng.below(50_000),
                rng.below(50_000),
                1000 + rng.below(50),
                rng.float() * 250.0,
                rng.below(4),
            );
        }
        _ => {
            let mut buf = vec![0u8; 4096];
            rng.bytes(&mut buf);
            record.data = buf;
        }
    }
    record
}

/// Churn pass for one row id: update every 10th row like live traffic
/// would (Go: per-profile `update`; blobs are immutable).
async fn churn(cluster: &Cluster, profile: &Profile, id: &str) -> Result<bool, String> {
    match profile.kind {
        0 => {
            let qty = (id.len() % 100) as i64;
            let params = [&qty as &(dyn ToSql + Sync), &id as _];
            cluster
                .node(0)
                .execute("UPDATE cells SET qty = $1 WHERE id = $2", &params)
                .await
                .map_err(|e| format!("churn update {id:?}: {e}"))?;
            Ok(true)
        }
        1 => {
            let tags = "news,tech,local,updated";
            let params = [&tags as &(dyn ToSql + Sync), &id as _];
            cluster
                .node(0)
                .execute("UPDATE cells SET tags = $1 WHERE id = $2", &params)
                .await
                .map_err(|e| format!("churn update {id:?}: {e}"))?;
            Ok(true)
        }
        2 => {
            let kind = "pageview-v2";
            let params = [&kind as &(dyn ToSql + Sync), &id as _];
            cluster
                .node(0)
                .execute("UPDATE cells SET kind = $1 WHERE id = $2", &params)
                .await
                .map_err(|e| format!("churn update {id:?}: {e}"))?;
            Ok(true)
        }
        _ => Ok(false),
    }
}

struct CellResult {
    rows: usize,
    total: u64,
    files: usize,
    write_dur: Duration,
    checksum: u64,
}

fn fnv1a(ids: &[String]) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for id in ids {
        for b in id.bytes() {
            hash ^= b as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
    }
    hash
}

fn dir_size(dir: &std::path::Path) -> (u64, usize) {
    let mut total = 0;
    let mut files = 0;
    let mut stack = vec![dir.to_path_buf()];
    while let Some(path) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&path) else { continue };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if let Ok(meta) = entry.metadata() {
                total += meta.len();
                files += 1;
            }
        }
    }
    (total, files)
}

fn format_bytes(n: u64) -> String {
    const UNIT: u64 = 1024;
    if n < UNIT {
        return format!("{n}B");
    }
    let mut div = UNIT;
    let mut exp = 0;
    while n / div >= UNIT && exp < 3 {
        div *= UNIT;
        exp += 1;
    }
    format!("{:.1}{}B", n as f64 / div as f64, ["K", "M", "G", "T"][exp])
}

/// Run one (profile, level) cell: fresh single-node server, seeded
/// dataset, churn pass, clean shutdown, data-dir measurement.
async fn run_cell(profile: &Profile, level: i32, scale: usize) -> Result<CellResult, String> {
    let name = format!("compression-{}-l{level}", profile.name);
    let mut cluster = Cluster::new(ClusterOptions {
        name: name.clone(),
        num_nodes: 1,
        extra_toml: format!("[engine]\ncompression_level = {level}\n"),
        ..Default::default()
    })
    .await
    .map_err(|e| format!("cell {} setup: {e}", profile.name))?;

    let body = async {
        match cluster.node(0).simple("CREATE TABLE cells(id TEXT NOT NULL PRIMARY KEY, code TEXT NOT NULL, qty INTEGER NOT NULL, price REAL NOT NULL, title TEXT NOT NULL, body TEXT NOT NULL, tags TEXT NOT NULL, kind TEXT NOT NULL, payload TEXT NOT NULL, data BLOB NOT NULL)").await {
            Ok(_) => {
                if let Err(e) = cluster.node(0).flush().await {
                    return Err(format!("flush error: {e:#?}"));
                }
            }
            Err(e) => return Err(format!("create table error: {e:#?}")),
        }

        let rows = profile.rows * scale;
        let mut rng = Rng(SEED);
        let mut ids = Vec::with_capacity(rows);
        let start = Instant::now();
        for base in (0..rows).step_by(BATCH) {
            let end = (base + BATCH).min(rows);
            let mut batch = Vec::with_capacity(end - base);
            for i in base..end {
                let record = create(&mut rng, profile, i);
                ids.push(record.id.clone());
                batch.push(record);
            }
            // One multi-row INSERT per batch.
            let mut sql = String::from("INSERT INTO cells(id, code, qty, price, title, body, tags, kind, payload, data) VALUES");
            let mut refs: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(batch.len() * 10);
            for (n, record) in batch.iter().enumerate() {
                let base_param = n * 10 + 1;
                sql.push_str(&format!(
                    "{}( ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${})",
                    if n == 0 { "" } else { "," },
                    base_param,
                    base_param + 1,
                    base_param + 2,
                    base_param + 3,
                    base_param + 4,
                    base_param + 5,
                    base_param + 6,
                    base_param + 7,
                    base_param + 8,
                    base_param + 9,
                ));
                refs.push(&record.id);
                refs.push(&record.code);
                refs.push(&record.qty);
                refs.push(&record.price);
                refs.push(&record.title);
                refs.push(&record.body);
                refs.push(&record.tags);
                refs.push(&record.kind);
                refs.push(&record.payload);
                refs.push(&record.data);
            }
            cluster
                .node(0)
                .execute(&sql, &refs)
                .await
                .map_err(|e| format!("batch insert at {base}: {e}"))?;
            cluster.node(0).flush().await.map_err(|e| format!("flush batch at {base}: {e}"))?;
        }
        // Churn pass: update every 10th row like live traffic would.
        let mut churned = 0;
        for (i, id) in ids.iter().enumerate() {
            if i % 10 != 0 {
                continue;
            }
            if churn(&cluster, profile, id).await? {
                churned += 1;
                if churned % CHURN_FLUSH_STRIDE == 0 {
                    cluster.node(0).flush().await.map_err(|e| format!("flush churn: {e}"))?;
                }
            }
        }
        cluster.node(0).flush().await.map_err(|e| format!("final flush: {e}"))?;
        let write_dur = start.elapsed();

        let count_rows = cluster
            .node(0)
            .query("SELECT COUNT(*) FROM cells", &[])
            .await
            .map_err(|e| format!("count: {e}"))?;
        let count: i64 = count_rows
            .first()
            .ok_or_else(|| "count returned no rows".to_string())?
            .try_get(0)
            .map_err(|e| format!("decode count: {e}"))?;
        if count as usize != rows {
            return Err(format!("level {level} profile {}: count = {count}, want {rows}", profile.name));
        }
        let checksum = fnv1a(&ids);
        let data_dir = cluster.node_info(0).data_dir.clone();
        cluster.stop_node(0).await;
        let (total, files) = dir_size(&data_dir);
        Ok::<CellResult, String>(CellResult { rows: count as usize, total, files, write_dur, checksum })
    }
    .await;

    let failed = body.is_err();
    if let Err(e) = &body {
        eprintln!("cell {} l{level} failed: {e}", profile.name);
    }
    if failed {
        cluster.dump_forensics("compression-cell").await;
    }
    // The node is already stopped on success; finish reaps children and
    // retains-or-cleans the runtime dir + RESULT line.
    cluster.finish(failed).await;
    let result = body?;
    eprintln!(
        "{:<12} l{:<2} rows={} total={} ({} files) write={:?}",
        profile.name,
        level,
        result.rows,
        format_bytes(result.total),
        result.files,
        result.write_dur
    );
    Ok(result)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn spool_compression_sizes() {
    let scale = std::env::var("MURMUR_LIVE_COMPRESSION_SCALE")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v >= 1)
        .unwrap_or(1);

    let mut failed = false;
    // profile -> level -> result.
    let mut results: Vec<(&str, Vec<(i32, CellResult)>)> = Vec::new();
    for profile in &PROFILES {
        let mut cells = Vec::new();
        for level in LEVELS {
            match run_cell(profile, level, scale).await {
                Ok(result) => cells.push((level, result)),
                Err(e) => {
                    eprintln!("scenario failed: {e}");
                    failed = true;
                    break;
                }
            }
        }
        if failed {
            break;
        }
        results.push((profile.name, cells));
    }
    if failed {
        panic!("scenario failed; artifacts retained under tests-live/failures/");
    }

    eprintln!("level \\ profile | {}", PROFILES.iter().map(|p| p.name).collect::<Vec<_>>().join(" | "));
    for level in LEVELS {
        let mut cells = Vec::new();
        for (name, level_results) in &results {
            let base = level_results.iter().find(|(l, _)| *l == LEVELS[0]).unwrap().1.total;
            let total = level_results.iter().find(|(l, _)| *l == level).unwrap().1.total;
            let ratio = if base > 0 { total as f64 / base as f64 * 100.0 } else { 0.0 };
            cells.push(format!("{} ({ratio:.0}%)", format_bytes(total)));
            let _ = name;
        }
        eprintln!("l{level:<7} | {}", cells.join(" | "));
    }

    // Sanity gates: identical datasets must hold identical rows with
    // identical checksums; compressible traffic must shrink at the high
    // level; incompressible traffic must not explode; and segments must
    // exist.
    for (name, level_results) in &results {
        let profile = PROFILES.iter().find(|p| p.name == *name).unwrap();
        let mut want_sum = None;
        for (level, result) in level_results {
            match want_sum {
                None => want_sum = Some(result.checksum),
                Some(want) if want == result.checksum => {}
                _ => panic!("profile {name}: level {level} dataset diverged"),
            }
            assert_eq!(result.rows, profile.rows * scale, "profile {name} level {level}: row count");
            assert!(result.files > 0 && result.total > 0, "profile {name} level {level}: no store bytes measured");
        }
    }
    let get = |profile: &str, level: i32| -> u64 {
        results
            .iter()
            .find(|(n, _)| *n == profile)
            .unwrap()
            .1
            .iter()
            .find(|(l, _)| *l == level)
            .unwrap()
            .1
            .total
    };
    let text_low = get("text-heavy", LEVELS[0]);
    let text_high = get("text-heavy", LEVELS[1]);
    assert!(
        text_high < text_low,
        "text-heavy l{} ({text_high}) not smaller than l{} ({text_low}): compression disengaged?",
        LEVELS[1],
        LEVELS[0]
    );
    let blob_low = get("blobs-random", LEVELS[0]);
    let blob_high = get("blobs-random", LEVELS[1]);
    assert!(
        blob_high <= blob_low * 11 / 10,
        "blobs-random l{} grew >10% over l{} ({blob_high} vs {blob_low})",
        LEVELS[1],
        LEVELS[0]
    );
}
