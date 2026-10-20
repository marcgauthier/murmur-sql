//! Live SQL test: A database store written by a NEWER release format must be
//! refused fail-closed by the server during UNLOCK, leaving the store untouched,
//! while the unmodified store reopens cleanly with exact data fidelity.

use murmur_live_harness::{Cluster, ClusterOptions, KEY};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::Path;

/// The engine-owned test utility mutates only the stopped disposable copy.
/// Database assertions below still go through SQL to a live Murmur-SQL server.
fn bump_identity_format(dir: &Path, key_hex: &str) {
    let binary = std::env::var_os("MURMUR_FORMAT_FIXTURE_BIN").expect(
        "run tests-live/run.sh downgrade-guard to provision the engine-owned artifact injector",
    );
    let output = std::process::Command::new(binary)
        .args([
            "--ignored",
            "--exact",
            "storage::live_sql_format_fixture::write_future_format_to_copied_store",
            "--nocapture",
        ])
        .env("MURMUR_FORMAT_FIXTURE_PATH", dir)
        .env("MURMUR_FORMAT_FIXTURE_KEY_HEX", key_hex)
        .output()
        .expect("run engine-owned format injector");
    assert!(
        output.status.success(),
        "format injector failed: {} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("FUTURE_FORMAT_FIXTURE old="));
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

    let fixture_key = "22".repeat(32);
    cluster
        .node(0)
        .simple(&format!(
            "MURMUR ROTATE KEY {} {}",
            murmur_live_harness::sql_literal(KEY),
            murmur_live_harness::sql_literal(&fixture_key)
        ))
        .await
        .unwrap();
    cluster.set_key(0, &fixture_key);
    let client = cluster.node(0);
    client
        .simple("CREATE TABLE records(id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        .await
        .unwrap();

    for i in 0..25i64 {
        let id = 3000 + i;
        let name = format!("row-{i:02}");
        client
            .execute(
                "INSERT INTO records(id, name) VALUES($1, $2)",
                &[&id, &name],
            )
            .await
            .unwrap();
    }
    client.simple("MURMUR FLUSH").await.unwrap();

    let rows_before = client
        .simple("SELECT id, name FROM records ORDER BY id")
        .await
        .unwrap();
    assert_eq!(rows_before.len(), 25);

    let pristine_data = cluster.node_info(0).data_dir.clone();
    cluster.stop_node(0);

    // Copy to future store and bump format
    let future_data = cluster.node_info(0).dir.join("future-data");
    copy_dir_all(&pristine_data, &future_data);
    bump_identity_format(&future_data, &fixture_key);

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
