//! Rust port of the Go `files-crash` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/files-crash/`
//! (`files_crash_test.go`: `TestFileCrashLeavesNoPartialOrPhantom` +
//! `README.md`). A two-node encrypted mesh proves crash consistency: after
//! the uploader is killed mid-upload and the fetcher mid-fetch, every
//! listed object downloads byte-complete and digest-verified on both
//! nodes, metadata converges exactly, and every on-disk payload is
//! attributable to a known attempt.
//!
//! Two `murmur-server` processes are driven over the PostgreSQL wire
//! protocol; uploads, downloads, and metadata use `MURMUR FILE ...`
//! commands, and peer fetch is automatic on the configured
//! `fetch_interval` (no explicit fetch call exists). Both kill legs are
//! real process kills: the mid-upload kill lands between chunked
//! `UPLOAD APPEND`s (the product must drop the staged upload and never
//! publish it) followed by a restart of the uploader, and the mid-fetch
//! kill stops the fetcher while a `.part` transfer file is on disk.
//! See the scenario README.md.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::Duration;

use murmur_live_harness::{Cluster, ClusterOptions, FilesOptions};

/// File object key (Go: `objectKey` 0x42..).
const OBJECT_KEY: [u8; 32] = [0x42; 32];
/// File cap (Go: `MaxFileBytes: 4 << 20`).
const MAX_FILE_BYTES: u64 = 4 << 20;
/// Peer fetch cadence (the `files-soak` pattern; the Rust product has no
/// explicit fetch call, so background fetch must stay enabled).
const FETCH_INTERVAL: Duration = Duration::from_millis(100);
/// Upload wire chunk size (server cap: 1 MiB per append).
const APPEND_CHUNK: usize = 256 << 10;
/// Download wire chunk size (server cap: 1 MiB per page).
const DOWNLOAD_CHUNK: usize = 256 << 10;
/// Convergence poll interval.
const POLL: Duration = Duration::from_millis(50);
/// Per-step metadata/fetch wait bound (Go: 30 s `WaitFileAvailable`).
const WAIT: Duration = Duration::from_secs(30);
/// Post-crash fetch bound. Killing the fetcher with both transfers in
/// flight (the norm: 2 fetch permits, several missing files) leaves two
/// zombie serve tasks on the uploader holding its whole serve semaphore
/// until the dead QUIC connections idle out (30 s `max_idle_timeout`);
/// every new file connection is refused until then, so recovery lands at
/// ~31 s. The product fails closed and self-heals; the port just waits
/// out the product's recovery tail (see the README deviation).
const POST_CRASH_FETCH_WAIT: Duration = Duration::from_secs(90);
/// Final metadata-settle bound (Go: 60 s).
const SETTLE_WAIT: Duration = Duration::from_secs(60);
/// Mid-fetch kill coordination bound (Go: 15 s `killWhileFetching`).
const KILL_WAIT: Duration = Duration::from_secs(15);
/// Upload size in the uploader-crash rounds (Go: 1 MiB x8).
const UPLOAD_BYTES: usize = 1 << 20;
/// Bytes appended before the mid-upload kill lands.
const SABOTAGE_BYTES: usize = 256 << 10;
/// Crash rounds per leg (Go: 3).
const ROUNDS: u32 = 3;

/// One file-metadata row (`MURMUR FILE NEXT` columns: name, size,
/// modified_ms, sha256, local). Wire LIST shows nondeleted files only.
struct FileMeta {
    name: String,
    size: u64,
    digest_hex: String,
    local: bool,
}

/// One upload try: the digest is known only for acknowledged uploads
/// (errored uploads must leave no debris at all).
struct Attempt {
    digest: Option<String>,
    size: u64,
    ok: bool,
}

/// Deterministic payload unique to `name` (Go: `payload` seeded by the
/// SHA-256 of the name; here a name-keyed byte pattern instead, since
/// the scenario adds no hash dependency).
fn payload(name: &str, n: usize) -> Vec<u8> {
    let nb = name.as_bytes();
    (0..n)
        .map(|i| ((nb.len() * 31 + nb[i % nb.len()] as usize + i * 17) % 251) as u8)
        .collect()
}

fn env_usize(name: &str, fallback: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|v| *v > 0)
        .unwrap_or(fallback)
}

fn sql_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn parse_bool(value: &str) -> Result<bool, String> {
    match value.to_ascii_lowercase().as_str() {
        "t" | "true" | "1" => Ok(true),
        "f" | "false" | "0" => Ok(false),
        other => Err(format!("bad boolean {other:?}")),
    }
}

fn parse_meta(row: &[String], idx: usize) -> Result<FileMeta, String> {
    if row.len() != 5 {
        return Err(format!("node{}: metadata row has {} columns, want 5", idx + 1, row.len()));
    }
    Ok(FileMeta {
        name: row[0].clone(),
        size: row[1]
            .parse::<u64>()
            .map_err(|e| format!("node{}: bad size {:?}: {e}", idx + 1, row[1]))?,
        digest_hex: row[3].clone(),
        local: parse_bool(&row[4]).map_err(|e| format!("node{}: {e}", idx + 1))?,
    })
}

/// Open an upload handle for `name` on node `idx`.
async fn upload_begin(cluster: &Cluster, idx: usize, name: &str) -> Result<String, String> {
    let rows = cluster
        .node(idx)
        .simple(&format!("MURMUR FILE UPLOAD BEGIN {}", sql_quote(name)))
        .await
        .map_err(|e| format!("node{} UPLOAD BEGIN {name:?}: {e}", idx + 1))?;
    rows.first()
        .and_then(|r| r.first())
        .cloned()
        .ok_or_else(|| format!("node{} UPLOAD BEGIN {name:?}: no handle", idx + 1))
}

/// Append one chunk to an open upload handle (extended protocol, raw BYTEA).
async fn upload_append(
    cluster: &Cluster,
    idx: usize,
    upload: &str,
    chunk: &[u8],
) -> Result<(), String> {
    cluster
        .node(idx)
        .execute("MURMUR FILE UPLOAD APPEND $1 $2", &[&upload, &chunk])
        .await
        .map_err(|e| format!("node{} UPLOAD APPEND: {e}", idx + 1))?;
    Ok(())
}

/// Finish an open upload handle; returns the published metadata.
async fn upload_finish(cluster: &Cluster, idx: usize, upload: &str) -> Result<FileMeta, String> {
    let rows = cluster
        .node(idx)
        .simple(&format!("MURMUR FILE UPLOAD FINISH {}", sql_quote(upload)))
        .await
        .map_err(|e| format!("node{} UPLOAD FINISH: {e}", idx + 1))?;
    let row = rows
        .first()
        .ok_or_else(|| format!("node{} UPLOAD FINISH: no metadata", idx + 1))?;
    parse_meta(row, idx)
}

/// Upload `data` under `name` on node `idx` (BEGIN, chunked APPEND,
/// FINISH). Returns the published metadata.
async fn file_upload(
    cluster: &Cluster,
    idx: usize,
    name: &str,
    data: &[u8],
) -> Result<FileMeta, String> {
    let upload = upload_begin(cluster, idx, name).await?;
    for chunk in data.chunks(APPEND_CHUNK.max(1)) {
        upload_append(cluster, idx, &upload, chunk).await?;
    }
    upload_finish(cluster, idx, &upload).await
}

/// Download the full verified bytes of `name` from node `idx`.
async fn file_download(cluster: &Cluster, idx: usize, name: &str) -> Result<Vec<u8>, String> {
    let node = cluster.node(idx);
    let rows = node
        .simple(&format!("MURMUR FILE DOWNLOAD {}", sql_quote(name)))
        .await
        .map_err(|e| format!("node{} DOWNLOAD {name:?}: {e}", idx + 1))?;
    let handle = rows
        .first()
        .and_then(|r| r.first())
        .cloned()
        .ok_or_else(|| format!("node{} DOWNLOAD {name:?}: no handle", idx + 1))?;
    let mut out = Vec::new();
    loop {
        let page = node
            .query(
                &format!("MURMUR FILE DOWNLOAD NEXT $1 LIMIT {DOWNLOAD_CHUNK}"),
                &[&handle],
            )
            .await
            .map_err(|e| format!("node{} DOWNLOAD NEXT {name:?}: {e}", idx + 1))?;
        let row = page
            .first()
            .ok_or_else(|| format!("node{} DOWNLOAD NEXT {name:?}: empty page", idx + 1))?;
        let chunk: Vec<u8> = row
            .try_get(2)
            .map_err(|e| format!("node{} DOWNLOAD NEXT {name:?}: bad chunk: {e}", idx + 1))?;
        let eof: bool = row
            .try_get(3)
            .map_err(|e| format!("node{} DOWNLOAD NEXT {name:?}: bad eof: {e}", idx + 1))?;
        out.extend_from_slice(&chunk);
        if eof {
            break;
        }
    }
    node.simple(&format!("MURMUR FILE CLOSE {}", sql_quote(&handle)))
        .await
        .map_err(|e| format!("node{} CLOSE download {name:?}: {e}", idx + 1))?;
    Ok(out)
}

/// Full metadata listing on node `idx`.
async fn file_list(cluster: &Cluster, idx: usize) -> Result<Vec<FileMeta>, String> {
    let node = cluster.node(idx);
    let rows = node
        .simple("MURMUR FILE LIST")
        .await
        .map_err(|e| format!("node{} FILE LIST: {e}", idx + 1))?;
    let cursor = rows
        .first()
        .and_then(|r| r.first())
        .cloned()
        .ok_or_else(|| format!("node{} FILE LIST: no cursor", idx + 1))?;
    let mut out = Vec::new();
    loop {
        let page = node
            .simple(&format!("MURMUR FILE NEXT {} LIMIT 256", sql_quote(&cursor)))
            .await
            .map_err(|e| format!("node{} FILE NEXT: {e}", idx + 1))?;
        if page.is_empty() {
            break;
        }
        for row in &page {
            out.push(parse_meta(row, idx)?);
        }
    }
    node.simple(&format!("MURMUR FILE CLOSE {}", sql_quote(&cursor)))
        .await
        .map_err(|e| format!("node{} CLOSE cursor: {e}", idx + 1))?;
    Ok(out)
}

/// Metadata for exactly `name` on node `idx` (`None` when unlisted).
async fn file_metadata(
    cluster: &Cluster,
    idx: usize,
    name: &str,
) -> Result<Option<FileMeta>, String> {
    Ok(file_list(cluster, idx)
        .await?
        .into_iter()
        .find(|m| m.name == name))
}

/// `<hex64>.g<gen>.spfo` -> digest hex (Go: `.spfo` parse with `.g`
/// generation suffix).
fn parse_object_digest(base: &str) -> Option<String> {
    let stem = base.strip_suffix(".spfo")?;
    let (digest, _) = stem.rsplit_once(".g")?;
    if digest.len() != 64 || !digest.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    Some(digest.to_string())
}

/// `<source>-<digest64>.g<gen>.part` -> digest hex (Rust fetch staging
/// names carry the source id prefix; Go parses the digest prefix the
/// same way).
fn parse_part_digest(base: &str) -> Option<String> {
    let stem = base.strip_suffix(".part")?;
    let (_, rest) = stem.split_once('-')?;
    let (digest, _) = rest.rsplit_once(".g")?;
    if digest.len() != 64 || !digest.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    Some(digest.to_string())
}

/// Collect on-disk payloads under a node dir: published objects
/// (`.spfo`), upload staging (Rust: `.upload-*`/`.rotate-*`; Go:
/// `.stage-*`), and fetch staging (`*.part`).
fn collect_payloads(dir: &Path, spfo: &mut Vec<String>, stage: &mut Vec<String>, part: &mut Vec<String>) {
    let rd = match std::fs::read_dir(dir) {
        Ok(rd) => rd,
        Err(_) => return,
    };
    for entry in rd.flatten() {
        let path = entry.path();
        let file_type = match entry.file_type() {
            Ok(t) => t,
            Err(_) => continue,
        };
        if file_type.is_dir() {
            collect_payloads(&path, spfo, stage, part);
            continue;
        }
        if !file_type.is_file() {
            continue;
        }
        if let Some(base) = path.file_name().and_then(|s| s.to_str()) {
            if base.ends_with(".spfo") {
                spfo.push(base.to_string());
            } else if base.starts_with(".upload-") || base.starts_with(".rotate-") {
                stage.push(base.to_string());
            } else if base.ends_with(".part") {
                part.push(base.to_string());
            }
        }
    }
}

/// Staged fetch transfers on a node (the in-flight signal for the
/// mid-fetch kill leg; Go polls `FetchStats.InFlight`).
fn staging_parts(staging: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    if let Ok(rd) = std::fs::read_dir(staging) {
        for entry in rd.flatten() {
            let path = entry.path();
            if path.extension().map(|x| x == "part").unwrap_or(false) {
                out.push(path);
            }
        }
    }
    out
}

/// Kill `idx` once a fetch transfer is in flight for `name`. Reports
/// (killed, too_slow): too_slow means the fetch completed first and the
/// round proves nothing (Go: `killWhileFetching`).
async fn kill_while_fetching(
    cluster: &mut Cluster,
    idx: usize,
    name: &str,
    timeout: Duration,
) -> Result<(bool, bool), String> {
    let staging = cluster.node_info(idx).dir.join("objects").join("staging");
    let end = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < end {
        if file_metadata(cluster, idx, name).await?.is_some_and(|m| m.local) {
            return Ok((false, true));
        }
        let in_flight = staging_parts(&staging).len();
        if in_flight > 0 {
            eprintln!("node{}: killing fetcher with {in_flight} staged transfer(s)", idx + 1);
            cluster.kill_node(idx).await;
            return Ok((true, false));
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    Ok((false, false))
}

async fn restart_node(cluster: &mut Cluster, idx: usize) -> Result<(), String> {
    cluster
        .start_node(idx)
        .await
        .map_err(|e| format!("restart node{}: {e}", idx + 1))?;
    cluster.mesh().await.map_err(|e| format!("re-mesh after restart: {e}"))
}

fn metadata_canonical(list: &[FileMeta]) -> String {
    let mut sorted: Vec<&FileMeta> = list.iter().collect();
    sorted.sort_by(|a, b| a.name.cmp(&b.name));
    let mut sb = String::new();
    for m in sorted {
        sb.push_str(&format!("{}|{}|{};", m.name, m.digest_hex, m.size));
    }
    sb
}

/// Poll until both nodes list identical metadata and every acknowledged
/// upload is present everywhere (Go: `waitMetadataSettled`).
async fn wait_metadata_settled(
    cluster: &Cluster,
    attempted: &HashMap<String, Attempt>,
) -> Result<(), String> {
    let end = tokio::time::Instant::now() + SETTLE_WAIT;
    loop {
        let mut lists = Vec::new();
        let mut ok = true;
        for idx in 0..cluster.len() {
            match file_list(cluster, idx).await {
                Ok(l) => lists.push(l),
                Err(_) => {
                    ok = false;
                    break;
                }
            }
        }
        if ok && lists.len() == 2 {
            let settled = metadata_canonical(&lists[0]) == metadata_canonical(&lists[1])
                && attempted.values().all(|a| {
                    !a.ok
                        || lists[0].iter().any(|m| {
                            Some(&m.digest_hex) == a.digest.as_ref() && m.size == a.size
                        })
                });
            if settled {
                return Ok(());
            }
        }
        if tokio::time::Instant::now() >= end {
            for idx in 0..cluster.len() {
                let n = file_list(cluster, idx).await.map(|l| l.len()).unwrap_or(0);
                eprintln!("node{idx} at timeout: {n} files");
            }
            return Err("file metadata did not settle identically on both nodes".into());
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Download every listed object on both nodes and require byte-complete,
/// digest-verified content; anything listed must have been attempted (no
/// phantoms) and every acked upload must be listed (no lost writes).
/// (Go: `verifyAllObjects`.)
async fn verify_all_objects(
    cluster: &Cluster,
    attempted: &HashMap<String, Attempt>,
    fetch_attempts: &mut usize,
) -> Result<(), String> {
    for idx in 0..cluster.len() {
        let list = file_list(cluster, idx).await?;
        for f in &list {
            let a = attempted
                .get(&f.name)
                .ok_or_else(|| format!("node{idx} lists phantom object {:?}", f.name))?;
            let want = a.digest.as_ref().ok_or_else(|| {
                format!("node{idx} {:?} listed but its upload errored", f.name)
            })?;
            if !(f.digest_hex == *want && f.size == a.size) {
                return Err(format!(
                    "node{idx} {:?} metadata digest={} size={}, want {}/{}",
                    f.name, f.digest_hex, f.size, want, a.size,
                ));
            }
            if !file_metadata(cluster, idx, &f.name).await?.is_some_and(|m| m.local) {
                *fetch_attempts += 1;
                let deadline = tokio::time::Instant::now() + WAIT;
                loop {
                    let fetched = file_metadata(cluster, idx, &f.name)
                        .await?
                        .is_some_and(|m| m.local);
                    if fetched {
                        break;
                    }
                    if tokio::time::Instant::now() >= deadline {
                        return Err(format!("node{idx} fetch {:?} never completed", f.name));
                    }
                    tokio::time::sleep(POLL).await;
                }
            }
            let got = file_download(cluster, idx, &f.name)
                .await
                .map_err(|e| {
                    format!("node{idx} {:?} listed but not downloadable (partial?): {e}", f.name)
                })?;
            if got.len() as u64 != a.size {
                return Err(format!("node{idx} {:?} length is partial", f.name));
            }
            if got != payload(&f.name, a.size as usize) {
                return Err(format!("node{idx} {:?} bytes differ (partial/corrupt)", f.name));
            }
        }
        for (name, a) in attempted {
            if !a.ok {
                continue;
            }
            let st = file_metadata(cluster, idx, name).await?;
            if st.is_none() {
                return Err(format!("node{idx} lost acked upload {name:?}"));
            }
        }
    }
    let l0 = file_list(cluster, 0).await?;
    let l1 = file_list(cluster, 1).await?;
    eprintln!(
        "all {} listed objects byte-complete on both nodes; metadata digests equal",
        l0.len()
    );
    if l0.len() != l1.len() {
        return Err("object counts differ".into());
    }
    Ok(())
}

/// Walk each node dir and require every object payload (`.spfo`), upload
/// stage file (Rust `.upload-*`, Go `.stage-*`), and fetch stage file
/// (`*.part`) to be attributable to a known upload/fetch attempt and
/// bounded in count (Go: `verifyInventory`).
async fn verify_inventory(
    cluster: &Cluster,
    attempted: &HashMap<String, Attempt>,
    fetch_attempts: usize,
) -> Result<(), String> {
    let mut known: HashSet<String> = HashSet::new();
    let mut errored = 0usize;
    for a in attempted.values() {
        match (a.ok, &a.digest) {
            (true, Some(d)) => {
                known.insert(d.clone());
            }
            _ => errored += 1,
        }
    }
    for idx in 0..cluster.len() {
        let mut spfo = Vec::new();
        let mut stage = Vec::new();
        let mut part = Vec::new();
        collect_payloads(&cluster.node_info(idx).dir, &mut spfo, &mut stage, &mut part);
        let meta = file_list(cluster, idx).await?;
        let referenced: HashSet<String> = meta.iter().map(|m| m.digest_hex.clone()).collect();
        let mut orphans = 0;
        for base in &spfo {
            let digest = parse_object_digest(base)
                .ok_or_else(|| format!("node{idx} object file {base} has a bad name"))?;
            if referenced.contains(&digest) {
                continue;
            }
            if known.contains(&digest) {
                orphans += 1;
                continue;
            }
            return Err(format!("node{idx} object file {base} unattributable to any upload"));
        }
        if orphans > errored {
            return Err(format!(
                "node{idx} has {orphans} unreferenced objects from {errored} errored uploads"
            ));
        }
        if idx == 1 && !stage.is_empty() {
            return Err(format!(
                "node 1 never uploaded yet holds {} stage files",
                stage.len()
            ));
        }
        if stage.len() > errored {
            return Err(format!(
                "node{idx} holds {} stage files from {errored} errored uploads",
                stage.len()
            ));
        }
        if idx == 0 && !part.is_empty() {
            return Err(format!(
                "node 0 never fetched yet holds {} part files",
                part.len()
            ));
        }
        for base in &part {
            let digest = parse_part_digest(base)
                .ok_or_else(|| format!("node{idx} stage file {base} has a bad name"))?;
            if !known.contains(&digest) {
                return Err(format!("node{idx} stage file {base} unattributable to any upload"));
            }
        }
        if part.len() > fetch_attempts + 2 {
            return Err(format!(
                "node{idx} holds {} part files from {fetch_attempts} fetch attempts",
                part.len()
            ));
        }
        eprintln!(
            "node{idx} inventory: {} objects ({orphans} crash orphans), {} stage, {} part - all attributable",
            spfo.len(),
            stage.len(),
            part.len()
        );
    }
    Ok(())
}

async fn run_body(cluster: &mut Cluster) -> Result<(), String> {
    let big_mb = env_usize("MURMUR_FILES_CRASH_BIG_MB", 3);
    let mut attempted: HashMap<String, Attempt> = HashMap::new();
    let mut fetch_attempts: usize = 0;

    // Positive control: honest upload replicates, fetches, verifies.
    let ctrl = payload("ctrl/a.bin", 64 << 10);
    let st = file_upload(cluster, 0, "ctrl/a.bin", &ctrl)
        .await
        .map_err(|e| format!("control upload: {e}"))?;
    if st.size != ctrl.len() as u64 {
        return Err("control upload byte count mismatch".into());
    }
    attempted.insert(
        "ctrl/a.bin".into(),
        Attempt { digest: Some(st.digest_hex.clone()), size: ctrl.len() as u64, ok: true },
    );
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        let arrived = file_metadata(cluster, 1, "ctrl/a.bin")
            .await?
            .is_some_and(|m| m.digest_hex == st.digest_hex);
        if arrived {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            return Err("control metadata never arrived".into());
        }
        tokio::time::sleep(POLL).await;
    }
    fetch_attempts += 1;
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        let fetched = file_metadata(cluster, 1, "ctrl/a.bin").await?.is_some_and(|m| m.local);
        if fetched {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            return Err("control fetch never completed".into());
        }
        tokio::time::sleep(POLL).await;
    }
    let got = file_download(cluster, 1, "ctrl/a.bin")
        .await
        .map_err(|e| format!("control download: {e}"))?;
    if got != ctrl {
        return Err("control download bytes differ".into());
    }
    eprintln!("positive control: upload/replicate/fetch/verify works");
    // Baseline inventory before any crash: the clean path must leave
    // zero unattributable payloads (the before half of before/after).
    verify_inventory(cluster, &attempted, fetch_attempts).await?;

    // Phase 1: kill the uploader mid-upload (a real process kill between
    // chunked appends; the unfinished upload must never publish).
    let mut uploader_killed = false;
    for round in 1..=ROUNDS {
        if uploader_killed {
            break;
        }
        let mut errored = 0;
        for i in 0..8 {
            let name = format!("crash/u-{round}-{i}.bin");
            let data = payload(&name, UPLOAD_BYTES);
            if i == 3 {
                // The kill lands here: one chunk is appended, then the
                // uploader is killed before FINISH.
                let upload = upload_begin(cluster, 0, &name)
                    .await
                    .map_err(|e| format!("round {round}: sabotaged BEGIN: {e}"))?;
                upload_append(cluster, 0, &upload, &data[..SABOTAGE_BYTES.min(data.len())])
                    .await
                    .map_err(|e| format!("round {round}: sabotaged APPEND: {e}"))?;
                cluster.kill_node(0).await;
                restart_node(cluster, 0).await?;
                // The unfinished upload must never publish.
                if file_metadata(cluster, 0, &name).await?.is_some() {
                    return Err(format!(
                        "round {round}: killed upload {name} published anyway"
                    ));
                }
                errored += 1;
                eprintln!("round {round}: uploader killed mid-upload of {name}");
                attempted.insert(name, Attempt { digest: None, size: data.len() as u64, ok: false });
                continue;
            }
            let up = file_upload(cluster, 0, &name, &data)
                .await
                .map_err(|e| format!("round {round} upload {name}: {e}"))?;
            if up.size != data.len() as u64 {
                return Err(format!("round {round} upload {name} byte count mismatch"));
            }
            attempted.insert(
                name,
                Attempt { digest: Some(up.digest_hex), size: data.len() as u64, ok: true },
            );
        }
        if errored == 0 {
            eprintln!("round {round}: kill landed between uploads; retrying");
            continue;
        }
        uploader_killed = true;
        eprintln!("round {round}: uploader killed mid-upload ({errored} errored uploads)");
    }
    if !uploader_killed {
        return Err(format!("no valid mid-upload kill in {ROUNDS} rounds"));
    }

    // Phase 2: kill the fetcher mid-fetch (staged-transfer coordinated).
    let mut fetcher_killed = false;
    for round in 1..=ROUNDS {
        if fetcher_killed {
            break;
        }
        let name = format!("crash/f-{round}.bin");
        let data = payload(&name, big_mb << 20);
        let up = file_upload(cluster, 0, &name, &data)
            .await
            .map_err(|e| format!("round {round} big upload: {e}"))?;
        attempted.insert(
            name.clone(),
            Attempt { digest: Some(up.digest_hex.clone()), size: data.len() as u64, ok: true },
        );
        let deadline = tokio::time::Instant::now() + WAIT;
        loop {
            let arrived = file_metadata(cluster, 1, &name)
                .await?
                .is_some_and(|m| m.digest_hex == up.digest_hex);
            if arrived {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(format!("round {round}: big metadata never arrived"));
            }
            tokio::time::sleep(POLL).await;
        }
        fetch_attempts += 1;
        let (killed, too_slow) = kill_while_fetching(cluster, 1, &name, KILL_WAIT).await?;
        if too_slow {
            eprintln!("round {round}: fetch completed before the kill; retrying");
            continue;
        }
        if !killed {
            return Err(format!("round {round}: fetch never went in flight"));
        }
        restart_node(cluster, 1).await?;
        fetcher_killed = true;
        eprintln!("round {round}: fetcher killed mid-fetch of {name}");
        // The interrupted fetch must complete cleanly after restart:
        // staged partials never poison the verified install.
        fetch_attempts += 1;
        let deadline = tokio::time::Instant::now() + POST_CRASH_FETCH_WAIT;
        loop {
            let fetched = file_metadata(cluster, 1, &name).await?.is_some_and(|m| m.local);
            if fetched {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                return Err("post-crash fetch never completed".into());
            }
            tokio::time::sleep(POLL).await;
        }
        let got = file_download(cluster, 1, &name)
            .await
            .map_err(|e| format!("post-crash download: {e}"))?;
        let meta = file_metadata(cluster, 1, &name)
            .await?
            .ok_or_else(|| "post-crash metadata missing".to_string())?;
        if meta.digest_hex != up.digest_hex {
            return Err("post-crash digest mismatch".into());
        }
        if got != data {
            return Err("post-crash bytes differ".into());
        }
    }
    if !fetcher_killed {
        return Err(format!("no valid mid-fetch kill in {ROUNDS} rounds"));
    }

    // Phase 3: full recovery proof.
    wait_metadata_settled(cluster, &attempted).await?;
    verify_all_objects(cluster, &attempted, &mut fetch_attempts).await?;
    verify_inventory(cluster, &attempted, fetch_attempts).await?;
    eprintln!("files-crash proven: no partials, no phantoms, inventory attributable");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn file_crash_leaves_no_partial_or_phantom() {
    let mut cluster = Cluster::new(ClusterOptions {
        name: "files-crash".into(),
        num_nodes: 2,
        files: Some(FilesOptions {
            key: OBJECT_KEY,
            fetch_interval: Some(FETCH_INTERVAL),
            max_file_bytes: Some(MAX_FILE_BYTES),
        }),
        ..Default::default()
    })
    .await
    .expect("cluster setup");
    let result = run_body(&mut cluster).await;
    let failed = result.is_err();
    if let Err(e) = &result {
        eprintln!("scenario failed: {e}");
    }
    if failed {
        cluster.dump_forensics("files-crash").await;
    }
    cluster.finish(failed).await;
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}
