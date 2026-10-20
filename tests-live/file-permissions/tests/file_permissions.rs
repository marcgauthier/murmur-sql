//! Inspect product-created durable paths after SQL initialization, backup and restore.
#[cfg(unix)]
use murmur_live_harness::{Cluster, ClusterOptions, KEY};
#[cfg(unix)]
fn private_tree(root: &std::path::Path) {
    use std::os::unix::fs::PermissionsExt;
    let mut pending = vec![root.to_path_buf()];
    let mut count = 0;
    while let Some(path) = pending.pop() {
        let meta = std::fs::symlink_metadata(&path).unwrap();
        assert!(
            !meta.file_type().is_symlink(),
            "unexpected symlink in product data tree: {}",
            path.display()
        );
        assert_eq!(
            meta.permissions().mode() & 0o077,
            0,
            "group/other access on {} (mode {:o})",
            path.display(),
            meta.permissions().mode() & 0o777
        );
        count += 1;
        if meta.is_dir() {
            pending.extend(std::fs::read_dir(path).unwrap().map(|e| e.unwrap().path()));
        }
    }
    assert!(count > 0, "permission audit visited no paths");
}
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn sql_storage_and_backup_permissions_remain_private_after_restore() {
    let mut cluster = Cluster::new(ClusterOptions {
        name: "file-permissions".into(),
        num_nodes: 1,
        local_only: true,
        ..Default::default()
    })
    .await
    .unwrap();
    let data = cluster.node_info(0).data_dir.clone();
    assert!(data.join("keys.enc").is_file());
    private_tree(&data);
    cluster
        .node(0)
        .simple("CREATE TABLE records(id INTEGER PRIMARY KEY,name TEXT NOT NULL)")
        .await
        .unwrap();
    let mut expected = Vec::new();
    for id in 0..25i64 {
        let name = format!("kept-{id:03}");
        cluster
            .node(0)
            .execute("INSERT INTO records(id,name) VALUES($1,$2)", &[&id, &name])
            .await
            .unwrap();
        expected.push(vec![name]);
    }
    cluster.node(0).simple("MURMUR FLUSH").await.unwrap();
    private_tree(&data);
    let backup = cluster.node(0).simple("MURMUR BACKUP SAVE").await.unwrap();
    private_tree(&cluster.node_info(0).dir.join("backups").join(&backup[0][0]));
    let mut archive_bytes = Vec::new();
    murmur_live_harness::transfers::download(cluster
        .node(0), &format!("MURMUR BACKUP DOWNLOAD {}", murmur_live_harness::sql_literal(&backup[0][0])), &mut archive_bytes)
        .await
        .unwrap();
    murmur_live_harness::transfers::upload(cluster
        .node(0), "permissions-copy.murmur-backup", &archive_bytes[..])
        .await
        .unwrap();
    private_tree(
        &cluster
            .node_info(0)
            .dir
            .join("backups")
            .join("permissions-copy.murmur-backup"),
    );
    cluster.node(0).simple("DELETE FROM records").await.unwrap();
    cluster
        .node(0)
        .simple(&format!("MURMUR RESTORE FROM {} KEY {}",murmur_live_harness::sql_literal("permissions-copy.murmur-backup"),murmur_live_harness::sql_literal(KEY)))
        .await
        .unwrap();
    let fresh = cluster.connect(0).await.unwrap();
    assert_eq!(
        fresh
            .simple("SELECT name FROM records ORDER BY id")
            .await
            .unwrap(),
        expected
    );
    fresh.simple("MURMUR FLUSH").await.unwrap();
    drop(fresh);
    private_tree(&data);
    cluster.stop_node(0);
    cluster.start_node(0).await.unwrap();
    private_tree(&data);
    assert_eq!(
        cluster
            .node(0)
            .simple("SELECT name FROM records ORDER BY id")
            .await
            .unwrap(),
        expected
    );
}
