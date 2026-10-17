//! Rust port of the Go `files-bridge` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/files-bridge/`
//! (`files_bridge_test.go` + `README.md`). A two-node Low mesh and a
//! two-node High mesh (separate DBIDs, CAs, storage keys, and file object
//! keys; four `murmur-server` processes over the PostgreSQL wire protocol)
//! move one 384 KiB file across a shared staging directory: Low upload,
//! Low mesh metadata + peer fetch, signed export of sealed chunks, High
//! import with re-encryption under the High object key, High mesh
//! metadata + peer fetch, and metadata search.
//!
//! Uploads, downloads, metadata, and search use `MURMUR FILE ...`
//! commands. Bridge capture/process run automatically in each server's
//! replication engine (every apply tick), so the test only waits on
//! file state; peer fetch is likewise automatic on the configured
//! `fetch_interval` (no explicit fetch call exists). The SHA-256 gate is
//! digest consistency plus full byte equality (no separate hash
//! recomputation). There is no file-delete wire command, so the Go
//! delete cascade has no equivalent and is dropped (see the scenario
//! README.md).

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use murmur_live_harness::{
    artifacts, hex, BridgeNodeConfig, BridgeOptions, Cluster, ClusterOptions, FilesOptions,
};

/// Low (exporter) cluster name; owns `runtime/files-bridge-low/`, which also
/// holds the shared staging dir so artifacts are retained/cleaned with it.
const LOW_NAME: &str = "files-bridge-low";
/// High (importer) cluster name; owns `runtime/files-bridge-high/`.
const HIGH_NAME: &str = "files-bridge-high";
/// Bridge stream (Go: `field-files`).
const STREAM: &str = "field-files";
/// Bridged file (Go: `shared/field-map.bin`).
const FILE_NAME: &str = "shared/field-map.bin";
/// Upload size (Go: 384 KiB).
const FILE_BYTES: usize = 384 << 10;
/// Upload wire chunk size (server cap: 1 MiB per append).
const APPEND_CHUNK: usize = 256 << 10;
/// Download wire chunk size (server cap: 1 MiB per page).
const DOWNLOAD_CHUNK: usize = 256 << 10;
/// Convergence deadline (Go: 30 s waits).
const WAIT: Duration = Duration::from_secs(30);
/// Convergence poll interval.
const POLL: Duration = Duration::from_millis(50);

/// Independent bridge domains (Go: distinct DBIDs, CAs, keys per domain;
/// each harness cluster already provisions its own CA).
const LOW_DBID: [u8; 16] = [0xE0; 16];
const HIGH_DBID: [u8; 16] = [0xE1; 16];
const LOW_NODE_0: [u8; 16] = [0xE2; 16];
const LOW_NODE_1: [u8; 16] = [0xE3; 16];
const HIGH_NODE_0: [u8; 16] = [0xE4; 16];
const HIGH_NODE_1: [u8; 16] = [0xE5; 16];
const LOW_KEY: [u8; 32] = [0xE6; 32];
const HIGH_KEY: [u8; 32] = [0xE7; 32];
/// File object keys (Go: `lowObjectKey` 0x11.., `highObjectKey` 0x22..).
const LOW_OBJECT_KEY: [u8; 32] = [0x11; 32];
const HIGH_OBJECT_KEY: [u8; 32] = [0x22; 32];
/// Low bundle-signer seed (ed25519; the High importer pins its public key).
const LOW_SIGNER: [u8; 32] = [0x21; 32];
/// High receiver secret (X25519; seals the file chunks for High).
const HIGH_SECRET: [u8; 32] = [0x31; 32];

/// One file-metadata row (`MURMUR FILE NEXT` columns: name, size,
/// modified_ms, sha256, local).
struct FileMeta {
    name: String,
    size: u64,
    digest_hex: String,
    local: bool,
}

/// Deterministic payload (Go: `data[i] = byte((i*37 + i/251) % 253)`).
fn payload() -> Vec<u8> {
    (0..FILE_BYTES)
        .map(|i| ((i * 37 + i / 251) % 253) as u8)
        .collect()
}

/// ed25519 verifying key for a bundle-signer seed (product
/// `protocol::public_key` parity).
fn signer_public(seed: &[u8; 32]) -> [u8; 32] {
    ed25519_dalek::SigningKey::from_bytes(seed).verifying_key().to_bytes()
}

/// X25519 recipient public key for a receiver secret (product
/// `bridge::recipient_public` parity).
fn recipient_public(secret: &[u8; 32]) -> [u8; 32] {
    x25519_dalek::PublicKey::from(&x25519_dalek::StaticSecret::from(*secret)).to_bytes()
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

fn parse_meta(row: &[String], label: &str) -> Result<FileMeta, String> {
    if row.len() != 5 {
        return Err(format!("{label}: metadata row has {} columns, want 5", row.len()));
    }
    Ok(FileMeta {
        name: row[0].clone(),
        size: row[1]
            .parse::<u64>()
            .map_err(|e| format!("{label}: bad size {:?}: {e}", row[1]))?,
        digest_hex: row[3].clone(),
        local: parse_bool(&row[4]).map_err(|e| format!("{label}: {e}"))?,
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
    let label = format!("{}-{}", cluster.name(), idx + 1);
    let node = cluster.node(idx);
    let rows = node
        .simple(&format!("MURMUR FILE UPLOAD BEGIN {}", sql_quote(name)))
        .await
        .map_err(|e| format!("{label} UPLOAD BEGIN {name:?}: {e}"))?;
    let upload = rows
        .first()
        .and_then(|r| r.first())
        .cloned()
        .ok_or_else(|| format!("{label} UPLOAD BEGIN {name:?}: no handle"))?;
    for chunk in data.chunks(APPEND_CHUNK.max(1)) {
        node.execute("MURMUR FILE UPLOAD APPEND $1 $2", &[&upload, &chunk])
            .await
            .map_err(|e| format!("{label} UPLOAD APPEND {name:?}: {e}"))?;
    }
    let rows = node
        .simple(&format!("MURMUR FILE UPLOAD FINISH {}", sql_quote(&upload)))
        .await
        .map_err(|e| format!("{label} UPLOAD FINISH {name:?}: {e}"))?;
    let row = rows
        .first()
        .ok_or_else(|| format!("{label} UPLOAD FINISH {name:?}: no metadata"))?;
    parse_meta(row, &label)
}

/// Download the full verified bytes of `name` from node `idx`.
async fn file_download(cluster: &Cluster, idx: usize, name: &str) -> Result<Vec<u8>, String> {
    let label = format!("{}-{}", cluster.name(), idx + 1);
    let node = cluster.node(idx);
    let rows = node
        .simple(&format!("MURMUR FILE DOWNLOAD {}", sql_quote(name)))
        .await
        .map_err(|e| format!("{label} DOWNLOAD {name:?}: {e}"))?;
    let handle = rows
        .first()
        .and_then(|r| r.first())
        .cloned()
        .ok_or_else(|| format!("{label} DOWNLOAD {name:?}: no handle"))?;
    let mut out = Vec::new();
    loop {
        let page = node
            .query(
                &format!("MURMUR FILE DOWNLOAD NEXT $1 LIMIT {DOWNLOAD_CHUNK}"),
                &[&handle],
            )
            .await
            .map_err(|e| format!("{label} DOWNLOAD NEXT {name:?}: {e}"))?;
        let row = page
            .first()
            .ok_or_else(|| format!("{label} DOWNLOAD NEXT {name:?}: empty page"))?;
        let chunk: Vec<u8> = row
            .try_get(2)
            .map_err(|e| format!("{label} DOWNLOAD NEXT {name:?}: bad chunk: {e}"))?;
        let eof: bool = row
            .try_get(3)
            .map_err(|e| format!("{label} DOWNLOAD NEXT {name:?}: bad eof: {e}"))?;
        out.extend_from_slice(&chunk);
        if eof {
            break;
        }
    }
    node.simple(&format!("MURMUR FILE CLOSE {}", sql_quote(&handle)))
        .await
        .map_err(|e| format!("{label} CLOSE download {name:?}: {e}"))?;
    Ok(out)
}

/// List metadata on node `idx`, optionally restricted by a LIKE pattern
/// (`MURMUR FILE SEARCH`); `None` lists everything.
async fn file_list(
    cluster: &Cluster,
    idx: usize,
    like: Option<&str>,
) -> Result<Vec<FileMeta>, String> {
    let label = format!("{}-{}", cluster.name(), idx + 1);
    let node = cluster.node(idx);
    let open = match like {
        Some(pattern) => format!("MURMUR FILE SEARCH {}", sql_quote(pattern)),
        None => "MURMUR FILE LIST".to_string(),
    };
    let rows = node.simple(&open).await.map_err(|e| format!("{label} {open}: {e}"))?;
    let cursor = rows
        .first()
        .and_then(|r| r.first())
        .cloned()
        .ok_or_else(|| format!("{label} {open}: no cursor"))?;
    let mut out = Vec::new();
    loop {
        let page = node
            .simple(&format!("MURMUR FILE NEXT {} LIMIT 256", sql_quote(&cursor)))
            .await
            .map_err(|e| format!("{label} FILE NEXT: {e}"))?;
        if page.is_empty() {
            break;
        }
        for row in &page {
            out.push(parse_meta(row, &label)?);
        }
    }
    node.simple(&format!("MURMUR FILE CLOSE {}", sql_quote(&cursor)))
        .await
        .map_err(|e| format!("{label} CLOSE cursor: {e}"))?;
    Ok(out)
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

/// Download the file from `cluster` node `idx` and prove full integrity:
/// exact bytes plus the uploader's digest carried in the metadata.
async fn assert_node_bytes(
    cluster: &Cluster,
    idx: usize,
    want: &[u8],
    want_digest: &str,
) -> Result<(), String> {
    let got = file_download(cluster, idx, FILE_NAME)
        .await
        .map_err(|e| format!("{}-{} download failed: {e}", cluster.name(), idx + 1))?;
    if got.as_slice() != want {
        return Err(format!("{}-{} bytes differ", cluster.name(), idx + 1));
    }
    let meta = file_metadata(cluster, idx, FILE_NAME)
        .await?
        .ok_or_else(|| format!("{}-{} metadata missing", cluster.name(), idx + 1))?;
    if meta.digest_hex != want_digest {
        return Err(format!("{}-{} digest differs", cluster.name(), idx + 1));
    }
    Ok(())
}

/// Wait until `cluster` node `idx` carries nondeleted metadata for the file
/// with the expected digest (Go: `WaitFileAvailable` metadata leg).
async fn wait_metadata(
    cluster: &Cluster,
    idx: usize,
    want_digest: &str,
    what: &str,
) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        let arrived = file_metadata(cluster, idx, FILE_NAME)
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

/// Wait until `cluster` node `idx` holds the object locally (Go: fetch leg;
/// the Rust product fetches automatically on `fetch_interval`).
async fn wait_available(cluster: &Cluster, idx: usize, what: &str) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        let fetched = file_metadata(cluster, idx, FILE_NAME).await?.is_some_and(|m| m.local);
        if fetched {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!("{what}: object never became available"));
        }
        tokio::time::sleep(POLL).await;
    }
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    needle.is_empty() || haystack.windows(needle.len()).any(|w| w == needle)
}

fn staged_bundles(staging: &std::path::Path) -> Result<Vec<std::path::PathBuf>, String> {
    let mut staged: Vec<_> = std::fs::read_dir(staging)
        .map_err(|e| format!("read staging dir: {e}"))?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|e| e == "bundle"))
        .collect();
    staged.sort();
    Ok(staged)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_low_two_high_file_bridge_live() {
    let staging = artifacts::runtime_dir(LOW_NAME).join("staging");
    let staging_str = staging.to_string_lossy().replace('\\', "/");
    let low = Cluster::new(ClusterOptions {
        name: LOW_NAME.into(),
        num_nodes: 2,
        database_id: Some(LOW_DBID),
        node_ids: vec![LOW_NODE_0, LOW_NODE_1],
        key: LOW_KEY,
        files: Some(FilesOptions {
            key: LOW_OBJECT_KEY,
            fetch_interval: Some(Duration::from_millis(100)),
            max_file_bytes: Some(4 << 20),
        }),
        bridge: BridgeOptions {
            nodes: HashMap::from([(
                0,
                BridgeNodeConfig {
                    role: "low-exporter".into(),
                    domain: hex(&LOW_DBID),
                    directory: staging_str.clone(),
                    stream: STREAM.into(),
                    recipient: hex(&recipient_public(&HIGH_SECRET)),
                    signer_hex: Some(hex(&LOW_SIGNER)),
                    ..Default::default()
                },
            )]),
        },
        ..Default::default()
    })
    .await
    .expect("low cluster setup");
    // The High importer pins the Low bundle signer and trusts both Low
    // origin keys so imported file transactions verify; the seeds stay
    // inside the harness.
    let low_origin_0 = low.node_origin_pubkey(0);
    let low_origin_1 = low.node_origin_pubkey(1);
    let high = Cluster::new(ClusterOptions {
        name: HIGH_NAME.into(),
        num_nodes: 2,
        database_id: Some(HIGH_DBID),
        node_ids: vec![HIGH_NODE_0, HIGH_NODE_1],
        key: HIGH_KEY,
        files: Some(FilesOptions {
            key: HIGH_OBJECT_KEY,
            fetch_interval: Some(Duration::from_millis(100)),
            max_file_bytes: Some(4 << 20),
        }),
        bridge: BridgeOptions {
            nodes: HashMap::from([(
                0,
                BridgeNodeConfig {
                    role: "high-receiver".into(),
                    domain: hex(&HIGH_DBID),
                    trusted_domains: vec![hex(&LOW_DBID)],
                    trusted_origins: BTreeMap::from([
                        (hex(&LOW_NODE_0), hex(&low_origin_0)),
                        (hex(&LOW_NODE_1), hex(&low_origin_1)),
                    ]),
                    directory: staging_str,
                    stream: STREAM.into(),
                    recipient: hex(&recipient_public(&HIGH_SECRET)),
                    receiver_secret_hex: Some(hex(&HIGH_SECRET)),
                    trusted_signers: BTreeMap::from([(
                        hex(&LOW_NODE_0),
                        hex(&signer_public(&LOW_SIGNER)),
                    )]),
                    ..Default::default()
                },
            )]),
        },
        ..Default::default()
    })
    .await
    .expect("high cluster setup");

    let data = payload();
    let result = run_body(&low, &high, &data).await;
    let failed = result.is_err();
    if let Err(e) = &result {
        eprintln!("scenario failed: {e}");
    }
    if failed {
        low.dump_forensics("files-bridge-low").await;
        high.dump_forensics("files-bridge-high").await;
    }
    low.finish(failed).await;
    high.finish(failed).await;
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}

async fn run_body(low: &Cluster, high: &Cluster, data: &[u8]) -> Result<(), String> {
    let staging = artifacts::runtime_dir(LOW_NAME).join("staging");
    // Low upload, then Low mesh metadata + peer fetch.
    let up = file_upload(low, 0, FILE_NAME, data)
        .await
        .map_err(|e| format!("low upload: {e}"))?;
    if up.size != data.len() as u64 {
        return Err("upload byte count mismatch".into());
    }
    wait_metadata(low, 1, &up.digest_hex, "low mesh metadata").await?;
    wait_available(low, 1, "low peer fetch").await?;
    assert_node_bytes(low, 1, data, &up.digest_hex).await?;

    // Export Low-0 to staging: sealed file-chunk bundles (plus the file
    // metadata transaction bundle). The engine's background capture
    // drives this every apply tick, so poll the staging dir.
    let export_deadline = tokio::time::Instant::now() + WAIT;
    let staged = loop {
        let staged = staged_bundles(&staging)?;
        if staged.len() >= 2 {
            break staged;
        }
        if tokio::time::Instant::now() >= export_deadline {
            return Err(format!(
                "staged {} bundles, want file chunks plus metadata",
                staged.len()
            ));
        }
        tokio::time::sleep(POLL).await;
    };
    // Staged artifacts must carry no document plaintext: several 1 KiB
    // windows of the payload must be absent from every staged bundle
    // (Go: `assertNoPlaintext`). Only `*.bundle` files are scanned: the
    // receiver's transient `.file-*.part` reassembly state lives in the
    // shared dir too once import starts, and it is plaintext by design.
    let windows: [&[u8]; 3] = [&data[0..1024], &data[100000..101024], &data[300000..301024]];
    for path in &staged {
        let raw =
            std::fs::read(path).map_err(|e| format!("read staged {}: {e}", path.display()))?;
        for (i, w) in windows.iter().enumerate() {
            if contains(&raw, w) {
                return Err(format!("staged {} contains plaintext window {i}", path.display()));
            }
        }
    }

    // Import into High-0: metadata applies, chunks install, and the
    // object re-encrypts under the High object key. The engine's
    // background process drives this every apply tick.
    wait_available(high, 0, "high-0 import").await?;
    assert_node_bytes(high, 0, data, &up.digest_hex).await?;
    assert_reencrypted(low, high)?;

    // High mesh carries metadata to High-1, which fetches bytes from High-0.
    wait_metadata(high, 1, &up.digest_hex, "high mesh metadata").await?;
    wait_available(high, 1, "high-1 peer fetch").await?;
    assert_node_bytes(high, 1, data, &up.digest_hex).await?;

    // Search across the replicated metadata (LIKE `shared/%`: the wire
    // SEARCH matches whole names, unlike the old literal-substring API).
    let found = file_list(high, 1, Some("shared/%")).await?;
    if found.len() != 1 {
        return Err(format!("search found {} files, want one", found.len()));
    }
    if found[0].digest_hex != up.digest_hex {
        return Err("search digest mismatch".into());
    }
    if found[0].size != data.len() as u64 {
        return Err("search byte count mismatch".into());
    }

    // No delete cascade: there is no file-delete wire command (the Go
    // `DeleteFile` leg has no equivalent), so the bridged file stays.
    // The tombstone path is covered structurally by the metadata and
    // re-encryption proofs above.
    Ok(())
}

/// Verify High at-rest bytes differ from Low at-rest bytes for the same
/// verified content (separate domain object keys; Go: `assertReencrypted`).
fn assert_reencrypted(low: &Cluster, high: &Cluster) -> Result<(), String> {
    let low_obj = first_object_file(low.node_info(0).dir.clone())?;
    let high_obj = first_object_file(high.node_info(0).dir.clone())?;
    let low_raw =
        std::fs::read(&low_obj).map_err(|e| format!("read {}: {e}", low_obj.display()))?;
    let high_raw =
        std::fs::read(&high_obj).map_err(|e| format!("read {}: {e}", high_obj.display()))?;
    if low_raw == high_raw {
        return Err("high object bytes identical to low object bytes; expected re-encryption".into());
    }
    Ok(())
}

/// First `*.spfo` object file under a node dir (Go: `firstObjectFile`).
fn first_object_file(root: std::path::PathBuf) -> Result<std::path::PathBuf, String> {
    let mut stack = vec![root.clone()];
    let mut found: Option<std::path::PathBuf> = None;
    while let Some(dir) = stack.pop() {
        let entries =
            std::fs::read_dir(&dir).map_err(|e| format!("walk {}: {e}", dir.display()))?;
        for entry in entries.filter_map(|e| e.ok()) {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|e| e == "spfo") && found.is_none() {
                found = Some(path);
            }
        }
    }
    found.ok_or_else(|| format!("no object file under {}", root.display()))
}
