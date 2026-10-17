//! Rust port of the Go `files-corrupt-source` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/files-corrupt-source/`
//! (`files_corrupt_source_test.go`:
//! `TestCorruptSourceFetchFailsVerification` + `README.md`). A victim
//! object is published honestly, then its stored bytes are corrupted on
//! the source node while stopped (same length, flipped chunk
//! ciphertext). A peer fetch must then fail verification with no
//! partial install, fail identically on retry, and leave honest objects
//! fetching afterwards.
//!
//! Two `murmur-server` processes are driven over the PostgreSQL wire
//! protocol; uploads, downloads, metadata, and search use
//! `MURMUR FILE ...` commands. Peer fetch is automatic on the configured
//! `fetch_interval` (no explicit fetch call exists), so the fetcher is
//! stopped while the victim is published and corrupted, then restarted.
//! The fetch-error sentinel has no wire equivalent (it is process memory
//! in the product), so verification failure is proven behaviorally: the
//! victim never becomes locally available while its metadata replicates
//! honestly, downloads fail, and no object or staging residue lands.
//! See the scenario README.md.

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
/// Quiescent-staging wait bound (a quiet moment arrives within one fetch
/// interval once failures are being recorded).
const QUIET_WAIT: Duration = Duration::from_secs(5);
/// Retry-persistence observation window (~5 background fetch cycles).
const RETRY_WINDOW: Duration = Duration::from_millis(500);

/// One file-metadata row (`MURMUR FILE NEXT` columns: name, size,
/// modified_ms, sha256, local).
struct FileMeta {
    name: String,
    size: u64,
    digest_hex: String,
    local: bool,
}

/// Deterministic payload unique to `name` (the `files-crash` pattern:
/// name-keyed bytes, no hash dependency).
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

/// Upload `data` under `name` on node `idx` (BEGIN, chunked APPEND over
/// the extended protocol, FINISH). Returns the published metadata.
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

/// First regular file under `dir` (recursive) whose name matches `want`.
fn find_file(dir: &Path, want: &impl Fn(&str) -> bool) -> Option<PathBuf> {
    let rd = std::fs::read_dir(dir).ok()?;
    for entry in rd.flatten() {
        let path = entry.path();
        let file_type = entry.file_type().ok()?;
        if file_type.is_dir() {
            if let Some(hit) = find_file(&path, want) {
                return Some(hit);
            }
            continue;
        }
        if !file_type.is_file() {
            continue;
        }
        if let Some(base) = path.file_name().and_then(|s| s.to_str()) {
            if want(base) {
                return Some(path);
            }
        }
    }
    None
}

/// The published `.spfo` object file for `digest_hex` (Go: `findObjectFile`).
fn find_object_file(dir: &Path, digest_hex: &str) -> Option<PathBuf> {
    find_file(dir, &|base: &str| {
        base.ends_with(".spfo") && base.contains(digest_hex)
    })
}

/// Any fetch-staging `*.part` file (successful Rust fetches rename the
/// `.part` into place, so any residue is a leak, regardless of digest).
fn find_part_file(dir: &Path) -> Option<PathBuf> {
    find_file(dir, &|base: &str| base.ends_with(".part"))
}

async fn check_download(
    cluster: &Cluster,
    idx: usize,
    name: &str,
    want: &[u8],
) -> Result<(), String> {
    let got = file_download(cluster, idx, name)
        .await
        .map_err(|e| format!("node{} download {name}: {e}", idx + 1))?;
    if got.as_slice() != want {
        return Err(format!("node{} {name} bytes differ", idx + 1));
    }
    let meta = file_metadata(cluster, idx, name)
        .await?
        .ok_or_else(|| format!("node{} {name} metadata missing", idx + 1))?;
    if meta.size != want.len() as u64 {
        return Err(format!("node{} {name} size {}", idx + 1, meta.size));
    }
    Ok(())
}

/// Wait until node `idx` carries nondeleted metadata for `name` with the
/// expected digest (Go: `WaitFileAvailable` metadata leg).
async fn wait_metadata(
    cluster: &Cluster,
    idx: usize,
    name: &str,
    want_digest: &str,
    what: &str,
) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        let arrived = file_metadata(cluster, idx, name)
            .await?
            .is_some_and(|m| m.digest_hex == want_digest);
        if arrived {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!("{what}: metadata never arrived"));
        }
        tokio::time::sleep(POLL).await;
    }
}

/// Wait until node `idx` holds the object locally (Go: fetch leg; the
/// Rust product fetches automatically on `fetch_interval`).
async fn wait_available(cluster: &Cluster, idx: usize, name: &str, what: &str) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        let fetched = file_metadata(cluster, idx, name).await?.is_some_and(|m| m.local);
        if fetched {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!("{what}: object never became available"));
        }
        tokio::time::sleep(POLL).await;
    }
}

/// Assert node `idx` never holds the object locally for a full `WAIT`
/// (the corrupt leg: background fetch cycles every 100 ms, so a full
/// window of unavailability with Present metadata proves every attempt
/// failed verification).
async fn assert_never_local(
    cluster: &Cluster,
    idx: usize,
    name: &str,
    what: &str,
) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        if file_metadata(cluster, idx, name).await?.is_some_and(|m| m.local) {
            return Err(format!("{what}: corrupt object became available (partial install)"));
        }
        if tokio::time::Instant::now() >= deadline {
            return Ok(());
        }
        tokio::time::sleep(POLL).await;
    }
}

async fn run_body(cluster: &mut Cluster) -> Result<(), String> {
    let victim_size = env_usize("MURMUR_FILES_CORRUPT_VICTIM_KB", 256) << 10;

    // Positive control: honest upload replicates, fetches, verifies.
    let ctrl = payload("ctrl/a.bin", 64 << 10);
    let st = file_upload(cluster, 0, "ctrl/a.bin", &ctrl)
        .await
        .map_err(|e| format!("control upload: {e}"))?;
    if st.size != ctrl.len() as u64 {
        return Err("control upload byte count mismatch".into());
    }
    wait_metadata(cluster, 1, "ctrl/a.bin", &st.digest_hex, "control metadata").await?;
    wait_available(cluster, 1, "ctrl/a.bin", "control fetch").await?;
    check_download(cluster, 1, "ctrl/a.bin", &ctrl).await?;
    eprintln!("positive control: upload/replicate/fetch/verify works");

    // Stop the fetcher so no fetch can start before the corruption lands
    // (the Rust product fetches automatically on `fetch_interval`; there
    // is no disabled-worker mode like Go's `FetchIntervalMs: -1`).
    cluster.stop_node(1).await;

    // Publish the victim object honestly; metadata only (the fetcher is
    // stopped, so nothing can fetch yet).
    let victim = payload("victim/blob.bin", victim_size);
    let vst = file_upload(cluster, 0, "victim/blob.bin", &victim)
        .await
        .map_err(|e| format!("victim upload: {e}"))?;
    if vst.size != victim.len() as u64 {
        return Err("victim upload byte count mismatch".into());
    }
    let victim_digest = vst.digest_hex.clone();
    eprintln!("victim published: digest={victim_digest} size={}", victim.len());

    // Locate the source-side object file before stopping the node.
    let src_obj = find_object_file(&cluster.node_info(0).dir, &victim_digest)
        .ok_or_else(|| format!("no .spfo object file for digest {victim_digest}"))?;
    let before = std::fs::read(&src_obj).map_err(|e| format!("read source object: {e}"))?;
    eprintln!(
        "source object: {} ({} container bytes)",
        src_obj.display(),
        before.len()
    );
    if before.len() < 1024 {
        return Err(format!(
            "source object unexpectedly small ({} bytes); refusing to corrupt",
            before.len()
        ));
    }

    // Corrupt the published bytes while the source node is stopped
    // (honest disk-corruption simulation: same length, flipped bytes in
    // the middle of the container, i.e. inside chunk ciphertext).
    cluster.stop_node(0).await;
    let mut flip = before.clone();
    for i in 0..16 {
        let off = flip.len() / 2 + i;
        flip[off] ^= 0xFF;
    }
    std::fs::write(&src_obj, &flip).map_err(|e| format!("write corrupted object: {e}"))?;
    let after = std::fs::read(&src_obj).map_err(|e| format!("re-read corrupted object: {e}"))?;
    if after.len() != before.len() {
        return Err("corrupted object length changed".into());
    }
    eprintln!(
        "flipped 16 bytes at container offset {} (length preserved)",
        flip.len() / 2
    );

    // Restart both nodes and re-mesh (fresh ephemeral ports).
    cluster
        .start_node(0)
        .await
        .map_err(|e| format!("restart source: {e}"))?;
    cluster
        .start_node(1)
        .await
        .map_err(|e| format!("restart fetcher: {e}"))?;
    cluster.mesh().await.map_err(|e| format!("re-mesh after restart: {e}"))?;

    // Victim metadata must replicate honestly to the fetcher.
    wait_metadata(cluster, 1, "victim/blob.bin", &victim_digest, "victim metadata").await?;

    // The peer fetch must FAIL verification with no partial install. The
    // sticky fetch-error sentinel is process memory with no wire
    // equivalent, so failure is proven behaviorally: metadata is present
    // (fetch is attempted every 100 ms) yet the object never becomes
    // local across a full window, and downloads fail.
    assert_never_local(cluster, 1, "victim/blob.bin", "corrupt fetch").await?;
    eprintln!("corrupt fetch failed closed as required: never available across a full window");

    // No partial install on the fetching node.
    let pst = file_metadata(cluster, 1, "victim/blob.bin")
        .await?
        .ok_or_else(|| "victim metadata wrongly altered by failed fetch".to_string())?;
    if pst.digest_hex != victim_digest {
        return Err("victim digest altered by failed fetch".into());
    }
    if pst.local {
        return Err(
            "victim reports local after a failed-verification fetch (partial install)".into(),
        );
    }
    if file_download(cluster, 1, "victim/blob.bin").await.is_ok() {
        return Err("victim downloads after a failed-verification fetch (partial install)".into());
    }

    // Background retries run every `fetch_interval`, so wait for a
    // quiescent moment (no transfer in flight) and require no staging
    // residue and no object file for the unverified digest then.
    let node1_dir = cluster.node_info(1).dir.clone();
    let quiet_deadline = tokio::time::Instant::now() + QUIET_WAIT;
    loop {
        if find_part_file(&node1_dir).is_none() {
            break;
        }
        if tokio::time::Instant::now() >= quiet_deadline {
            return Err("fetch staging never quiesced".into());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    if find_object_file(&node1_dir, &victim_digest).is_some() {
        return Err(
            "fetching node holds an object file for an unverified digest (partial install)".into(),
        );
    }
    if find_part_file(&node1_dir).is_some() {
        return Err(
            "fetching node holds a staging file after failed verification (staging not discarded)"
                .into(),
        );
    }

    // No phantom metadata: the listing still shows the victim with its
    // original honest digest/size (corruption is bytes-only, metadata
    // untouched) on both nodes. (Wire LIST shows nondeleted files only.)
    for i in 0..cluster.len() {
        let list = file_list(cluster, i).await?;
        let f = list
            .iter()
            .find(|f| f.name == "victim/blob.bin")
            .ok_or_else(|| format!("node{} lost victim metadata after failed fetch", i + 1))?;
        if !(f.digest_hex == victim_digest && f.size == victim.len() as u64) {
            return Err(format!(
                "node{} victim metadata altered: {} {} {}",
                i + 1,
                f.name,
                f.digest_hex,
                f.size
            ));
        }
    }

    // The corruption is persistent, not transient: after more background
    // retry cycles the victim is still unavailable.
    tokio::time::sleep(RETRY_WINDOW).await;
    if file_metadata(cluster, 1, "victim/blob.bin").await?.is_some_and(|m| m.local) {
        return Err("victim became available across retries (partial install)".into());
    }
    eprintln!("retried fetch failed closed again: still unavailable");

    // Honest objects must still fetch afterwards.
    let honest = payload("honest/b.bin", 128 << 10);
    let hst = file_upload(cluster, 0, "honest/b.bin", &honest)
        .await
        .map_err(|e| format!("honest upload after corruption: {e}"))?;
    wait_metadata(cluster, 1, "honest/b.bin", &hst.digest_hex, "honest metadata after corruption")
        .await?;
    wait_available(cluster, 1, "honest/b.bin", "honest fetch after corruption").await?;
    check_download(cluster, 1, "honest/b.bin", &honest).await?;
    check_download(cluster, 1, "ctrl/a.bin", &ctrl).await?;
    eprintln!("corrupt-source proven: bad bytes fail verification, honest objects unaffected");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn corrupt_source_fetch_fails_verification() {
    let mut cluster = Cluster::new(ClusterOptions {
        name: "files-corrupt-source".into(),
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
        cluster.dump_forensics("files-corrupt-source").await;
    }
    cluster.finish(failed).await;
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}
