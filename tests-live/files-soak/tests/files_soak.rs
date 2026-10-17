//! Rust port of the Go `files-soak` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/files-soak/`
//! (`files_soak_test.go`: `TestEncryptedFileFetchSoak` + `README.md`). A
//! two-node encrypted mesh soaks 64 KiB file uploads for
//! `MURMUR_FILES_SOAK_DURATION_SECONDS` (default 5 s), one upload every
//! `MURMUR_FILES_SOAK_INTERVAL_SECONDS` (default 1 s): upload to node 1,
//! wait for replicated metadata on node 2, fetch, download, verify the
//! digest, and search the replicated names. Upload p95 must stay under 5 s
//! and metadata-to-verified-fetch p95 under 20 s, and the fetch gate must
//! show every file completed with zero failures.
//!
//! Two `murmur-server` processes are driven over the PostgreSQL wire
//! protocol; uploads, downloads, metadata, and search use `MURMUR FILE ...`
//! commands. Peer fetch is automatic on the configured `fetch_interval`
//! (no explicit fetch call exists). See the scenario README.md.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use murmur_live_harness::{env_seconds, Cluster, ClusterOptions, FilesOptions};

/// File object key (Go: `soakObjectKey` 0x51..).
const OBJECT_KEY: [u8; 32] = [0x51; 32];
/// Upload size per round (Go: 64 KiB).
const FILE_BYTES: usize = 64 << 10;
/// File cap (Go: `MaxFileBytes: 4 << 20`).
const MAX_FILE_BYTES: u64 = 4 << 20;
/// Upload wire chunk size (server cap: 1 MiB per append).
const APPEND_CHUNK: usize = 256 << 10;
/// Download wire chunk size (server cap: 1 MiB per page).
const DOWNLOAD_CHUNK: usize = 256 << 10;
/// Per-round metadata/fetch wait bound (Go: 20 s `WaitFileAvailable`).
const WAIT: Duration = Duration::from_secs(20);
/// Convergence poll interval.
const POLL: Duration = Duration::from_millis(50);
/// Upload p95 SLO (Go: 5 s).
const UPLOAD_P95_SLO: Duration = Duration::from_secs(5);
/// Metadata-to-verified-fetch p95 SLO (Go: 20 s).
const TRANSFER_P95_SLO: Duration = Duration::from_secs(20);

/// One file-metadata row (`MURMUR FILE NEXT` columns: name, size,
/// modified_ms, sha256, local).
struct FileMeta {
    name: String,
    size: u64,
    digest_hex: String,
    local: bool,
}

/// Deterministic payload (Go: `data[i] = byte((round*31 + i*17) % 251)`).
fn payload(round: usize) -> Vec<u8> {
    (0..FILE_BYTES)
        .map(|i| ((round * 31 + i * 17) % 251) as u8)
        .collect()
}

/// p95 element of a sorted latency sample (Go: `min(n-1, (95*n+99)/100-1)`).
fn p95(sorted: &[Duration]) -> Duration {
    let n = sorted.len();
    sorted[(n - 1).min((95 * n + 99) / 100 - 1)]
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

/// Upload `data` under `name` on node `idx`: BEGIN, chunked APPEND
/// (extended protocol, raw BYTEA), FINISH. Returns the published metadata.
async fn file_upload(
    cluster: &Cluster,
    idx: usize,
    name: &str,
    data: &[u8],
) -> Result<FileMeta, String> {
    let node = cluster.node(idx);
    let rows = node
        .simple(&format!("MURMUR FILE UPLOAD BEGIN {}", sql_quote(name)))
        .await
        .map_err(|e| format!("node{} UPLOAD BEGIN {name:?}: {e}", idx + 1))?;
    let upload = rows
        .first()
        .and_then(|r| r.first())
        .cloned()
        .ok_or_else(|| format!("node{} UPLOAD BEGIN {name:?}: no handle", idx + 1))?;
    for chunk in data.chunks(APPEND_CHUNK.max(1)) {
        node.execute("MURMUR FILE UPLOAD APPEND $1 $2", &[&upload, &chunk])
            .await
            .map_err(|e| format!("node{} UPLOAD APPEND {name:?}: {e}", idx + 1))?;
    }
    let rows = node
        .simple(&format!("MURMUR FILE UPLOAD FINISH {}", sql_quote(&upload)))
        .await
        .map_err(|e| format!("node{} UPLOAD FINISH {name:?}: {e}", idx + 1))?;
    let row = rows
        .first()
        .ok_or_else(|| format!("node{} UPLOAD FINISH {name:?}: no metadata", idx + 1))?;
    parse_meta(row, idx)
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

/// List metadata on node `idx`, optionally restricted by a LIKE pattern
/// (`MURMUR FILE SEARCH`); `None` lists everything.
async fn file_list(
    cluster: &Cluster,
    idx: usize,
    like: Option<&str>,
) -> Result<Vec<FileMeta>, String> {
    let node = cluster.node(idx);
    let open = match like {
        Some(pattern) => format!("MURMUR FILE SEARCH {}", sql_quote(pattern)),
        None => "MURMUR FILE LIST".to_string(),
    };
    let rows = node
        .simple(&open)
        .await
        .map_err(|e| format!("node{} {open}: {e}", idx + 1))?;
    let cursor = rows
        .first()
        .and_then(|r| r.first())
        .cloned()
        .ok_or_else(|| format!("node{} {open}: no cursor", idx + 1))?;
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

/// Metadata for exactly `name` on node `idx` (`None` when unlisted).
async fn file_metadata(
    cluster: &Cluster,
    idx: usize,
    name: &str,
) -> Result<Option<FileMeta>, String> {
    Ok(file_list(cluster, idx, None)
        .await?
        .into_iter()
        .find(|m| m.name == name))
}

async fn run_body(cluster: &Cluster, duration: Duration, interval: Duration) -> Result<(), String> {
    let deadline = Instant::now() + duration;
    let mut upload_latencies = Vec::new();
    let mut end_to_end_latencies = Vec::new();
    let mut expected: HashMap<String, String> = HashMap::new();
    let mut round = 0usize;
    while Instant::now() < deadline {
        let name = format!("soak/file-{round:04}.bin");
        let data = payload(round);

        let start = Instant::now();
        let uploaded = file_upload(cluster, 0, &name, &data)
            .await
            .map_err(|e| format!("upload round {round}: {e}"))?;
        if uploaded.size != data.len() as u64 {
            return Err(format!(
                "round {round}: upload byte count {}, want {}",
                uploaded.size,
                data.len()
            ));
        }
        upload_latencies.push(start.elapsed());
        expected.insert(name.clone(), uploaded.digest_hex.clone());

        let end_to_end_start = Instant::now();
        // Metadata leg (Go: `WaitFileAvailable` metadata wait).
        let meta_deadline = Instant::now() + WAIT;
        loop {
            let arrived = file_metadata(cluster, 1, &name)
                .await?
                .is_some_and(|m| m.digest_hex == uploaded.digest_hex);
            if arrived {
                break;
            }
            if Instant::now() >= meta_deadline {
                return Err(format!("round {round}: metadata never arrived"));
            }
            tokio::time::sleep(POLL).await;
        }
        // Fetch leg (Go: explicit `FetchFile`; the Rust product fetches
        // automatically on `fetch_interval`, so this is an availability wait
        // on the `local` flag).
        let fetch_deadline = Instant::now() + WAIT;
        loop {
            let fetched = file_metadata(cluster, 1, &name)
                .await?
                .is_some_and(|m| m.local);
            if fetched {
                break;
            }
            if Instant::now() >= fetch_deadline {
                return Err(format!("round {round}: object never fetched"));
            }
            tokio::time::sleep(POLL).await;
        }
        let got = file_download(cluster, 1, &name)
            .await
            .map_err(|e| format!("round {round}: download failed: {e}"))?;
        if got.as_slice() != data.as_slice() {
            return Err(format!("round {round}: downloaded bytes differ"));
        }
        end_to_end_latencies.push(end_to_end_start.elapsed());

        // Search across the replicated metadata (LIKE `%file-%`: the wire
        // SEARCH matches whole names, unlike the old literal-substring API).
        let found = file_list(cluster, 1, Some("%file-%")).await?;
        if found.len() != round + 1 {
            return Err(format!(
                "round {round}: search returned {} files, want {}",
                found.len(),
                round + 1
            ));
        }

        round += 1;
        if Instant::now() < deadline {
            tokio::time::sleep(interval).await;
        }
    }
    if upload_latencies.is_empty() {
        return Err("soak completed no uploads".into());
    }

    // Fetch gate (Go: `FetchStats` shows every file completed with zero
    // failures; the Rust product exposes no fetch counters over the wire):
    // every uploaded file must be locally available with its upload digest
    // on the peer.
    for (name, digest) in &expected {
        let meta = file_metadata(cluster, 1, name)
            .await?
            .ok_or_else(|| format!("file {name}: metadata missing after the soak"))?;
        if !meta.local {
            return Err(format!("file {name} not available on the peer after the soak"));
        }
        if meta.digest_hex != *digest {
            return Err(format!("file {name}: peer digest differs from the upload digest"));
        }
    }

    // First-file status (Go: `StatFile` exists + available).
    let first = file_metadata(cluster, 1, "soak/file-0000.bin").await?;
    let Some(first) = first else {
        return Err("first file metadata missing".into());
    };
    if !first.local {
        return Err("first file not available".into());
    }

    upload_latencies.sort();
    end_to_end_latencies.sort();
    let upload_p95 = p95(&upload_latencies);
    let transfer_p95 = p95(&end_to_end_latencies);
    eprintln!(
        "file soak duration={duration:?} rounds={} upload_p95={upload_p95:?} \
         metadata_fetch_verify_p95={transfer_p95:?}",
        expected.len(),
    );
    if upload_p95 > UPLOAD_P95_SLO {
        return Err(format!("upload p95 {upload_p95:?} exceeded 5s SLO"));
    }
    if transfer_p95 > TRANSFER_P95_SLO {
        return Err(format!("end-to-end p95 {transfer_p95:?} exceeded 20s SLO"));
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn encrypted_file_fetch_soak() {
    let duration = env_seconds("MURMUR_FILES_SOAK_DURATION_SECONDS", 5);
    let interval = env_seconds("MURMUR_FILES_SOAK_INTERVAL_SECONDS", 1);
    let cluster = Cluster::new(ClusterOptions {
        name: "files-soak".into(),
        num_nodes: 2,
        files: Some(FilesOptions {
            key: OBJECT_KEY,
            fetch_interval: Some(Duration::from_millis(100)),
            max_file_bytes: Some(MAX_FILE_BYTES),
        }),
        ..Default::default()
    })
    .await
    .expect("cluster setup");
    let result = run_body(&cluster, duration, interval).await;
    let failed = result.is_err();
    if let Err(e) = &result {
        eprintln!("scenario failed: {e}");
    }
    if failed {
        cluster.dump_forensics("files-soak").await;
    }
    cluster.finish(failed).await;
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}
