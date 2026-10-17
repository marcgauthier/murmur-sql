//! Live SQL test: A database store written by a NEWER release format must be
//! refused fail-closed by the server during UNLOCK, leaving the store untouched,
//! while the unmodified store reopens cleanly with exact data fidelity.

use murmur_live_harness::{Cluster, ClusterOptions, KEY};
use sha2::{Digest, Sha256};
use spool::{Codec, Durability, Mutation};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

const IDENTITY_KEY: &[u8] = b"00/identity";
const ZSTD_CODEC_ID: u8 = 128;
const ZSTD_LEVEL: i32 = 3;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct StoreIdentity {
    database: [u8; 16],
    node: [u8; 16],
    format: u32,
}

struct ZstdBlock(i32);

impl Codec for ZstdBlock {
    fn id(&self) -> u8 {
        ZSTD_CODEC_ID
    }
    fn name(&self) -> &str {
        "sqlite-session-zstd-v1"
    }
    fn compress(&self, src: &[u8]) -> spool::Result<Vec<u8>> {
        zstd::stream::encode_all(src, self.0)
            .map_err(|e| spool::Error::new(spool::ErrorKind::Io, e.to_string()))
    }
    fn decompress(&self, src: &[u8], max: usize) -> spool::Result<Vec<u8>> {
        use std::io::Read;
        let decoder = zstd::stream::read::Decoder::new(src)
            .map_err(|e| spool::Error::new(spool::ErrorKind::Corrupt, e.to_string()))?;
        let mut out = Vec::new();
        decoder
            .take(max as u64 + 1)
            .read_to_end(&mut out)
            .map_err(|e| spool::Error::new(spool::ErrorKind::Corrupt, e.to_string()))?;
        if out.len() > max {
            return Err(spool::Error::new(
                spool::ErrorKind::Corrupt,
                "zstd size limit",
            ));
        }
        Ok(out)
    }
}

fn spool_opts(dir: &Path) -> spool::Options {
    let mut opts = spool::default_options(dir.to_string_lossy().to_string());
    opts.master_key = blake3::hash(KEY.as_bytes()).as_bytes().to_vec();
    opts.codecs = vec![Arc::new(ZstdBlock(ZSTD_LEVEL))];
    opts.reclaim_interval = None;
    opts
}

fn read_identity(dir: &Path) -> StoreIdentity {
    let opts = spool_opts(dir);
    let mut raw: Option<Vec<u8>> = None;
    let store = spool::open_and_load_ordered(&opts, &mut |records: &[spool::Record]| {
        for r in records {
            if !r.deleted && r.key.as_slice() == IDENTITY_KEY {
                raw = Some(r.value.clone());
            }
        }
        Ok(())
    })
    .unwrap_or_else(|e| panic!("spool open {}: {e}", dir.display()));
    store
        .close()
        .unwrap_or_else(|e| panic!("spool close {}: {e}", dir.display()));
    let raw = raw.unwrap_or_else(|| panic!("no 00/identity record in {}", dir.display()));
    postcard::from_bytes(&raw).unwrap_or_else(|e| panic!("decode 00/identity: {e}"))
}

fn bump_identity_format(dir: &Path, current: &StoreIdentity) -> StoreIdentity {
    let mut next = current.clone();
    next.format = current.format.checked_add(1).expect("format + 1");
    let bytes = postcard::to_allocvec(&next).expect("encode bumped identity");
    let opts = spool_opts(dir);
    let store = spool::open(&opts)
        .unwrap_or_else(|e| panic!("spool open {} for format bump: {e}", dir.display()));
    store
        .commit(
            &[Mutation::put(IDENTITY_KEY.to_vec(), bytes)],
            Durability::Sync,
        )
        .unwrap_or_else(|e| panic!("commit bumped identity: {e}"));
    store
        .close()
        .unwrap_or_else(|e| panic!("spool close {} after bump: {e}", dir.display()));
    next
}

fn copy_dir_all(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).unwrap();
    let mut entries: Vec<_> = std::fs::read_dir(src)
        .unwrap()
        .collect::<std::io::Result<_>>()
        .unwrap();
    entries.sort_by_key(|e| e.file_name());
    for entry in entries {
        let from = entry.path();
        let to = dst.join(entry.file_name());
        let kind = entry.file_type().unwrap();
        if kind.is_dir() {
            copy_dir_all(&from, &to);
        } else if kind.is_file() {
            std::fs::copy(&from, &to).unwrap();
        }
    }
}

fn hash_tree(root: &Path) -> BTreeMap<String, String> {
    fn walk(root: &Path, dir: &Path, out: &mut BTreeMap<String, String>) {
        let mut entries: Vec<_> = std::fs::read_dir(dir)
            .unwrap()
            .collect::<std::io::Result<_>>()
            .unwrap();
        entries.sort_by_key(|e| e.file_name());
        for entry in entries {
            let path = entry.path();
            let kind = entry.file_type().unwrap();
            if kind.is_dir() {
                walk(root, &path, out);
            } else if kind.is_file() {
                let rel = path
                    .strip_prefix(root)
                    .unwrap()
                    .to_string_lossy()
                    .into_owned();
                let raw = std::fs::read(&path).unwrap();
                out.insert(rel, format!("{:x}", Sha256::digest(&raw)));
            }
        }
    }
    let mut out = BTreeMap::new();
    walk(root, root, &mut out);
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn downgrade_guard_refuses_new_store() {
    let mut cluster = Cluster::new(ClusterOptions {
        name: "downgrade-guard".into(),
        num_nodes: 1,
        ..Default::default()
    })
    .await
    .expect("start cluster");

    let client = cluster.node(0);
    client
        .simple("CREATE TABLE records(id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        .await
        .unwrap();

    for i in 0..25i64 {
        let id = 3000 + i;
        let name = format!("row-{i:02}");
        client
            .execute("INSERT INTO records(id, name) VALUES($1, $2)", &[&id, &name])
            .await
            .unwrap();
    }
    client.flush().await.unwrap();

    let rows_before = client
        .simple("SELECT id, name FROM records ORDER BY id")
        .await
        .unwrap();
    assert_eq!(rows_before.len(), 25);

    let pristine_data = cluster.node_info(0).data_dir.clone();
    cluster.stop_node(0);

    // Read identity from pristine store
    let current_id = read_identity(&pristine_data);

    // Copy to future store and bump format
    let future_data = cluster.node_info(0).dir.join("future-data");
    copy_dir_all(&pristine_data, &future_data);
    let bumped_id = bump_identity_format(&future_data, &current_id);
    assert_eq!(bumped_id.format, current_id.format + 1);

    let before_refusal_tree = hash_tree(&future_data);

    // Point node 0 to future store and attempt to start
    cluster
        .edit_config(0, |doc| {
            doc["storage"]["data_dir"] = toml_edit::value(future_data.to_str().unwrap());
        })
        .unwrap();

    // The server MUST fail to unlock / start against the future store
    let start_res = cluster.start_node(0).await;
    assert!(
        start_res.is_err(),
        "Expected node start to fail against future store format"
    );
    let err = start_res.err().unwrap();
    let err_debug = format!("{err:?}");
    assert!(
        err_debug.contains("unsupported database format"),
        "Expected 'unsupported database format' in {err_debug}"
    );

    // Future store must remain content-untouched
    let after_refusal_tree = hash_tree(&future_data);
    assert_eq!(
        before_refusal_tree, after_refusal_tree,
        "Future store files were modified despite refusal!"
    );

    // Stop the refused process before restarting with the pristine store
    cluster.stop_node(0);

    // Point back to pristine store and start
    cluster
        .edit_config(0, |doc| {
            doc["storage"]["data_dir"] = toml_edit::value(pristine_data.to_str().unwrap());
        })
        .unwrap();

    cluster.start_node(0).await.expect("reopen pristine store");

    let rows_after = cluster
        .node(0)
        .simple("SELECT id, name FROM records ORDER BY id")
        .await
        .unwrap();
    assert_eq!(rows_before, rows_after);
}
