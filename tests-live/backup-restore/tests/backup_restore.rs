//! Backups use the Rust client's streaming SQL API, with no offline engine opens.
use murmur_live_harness::{Cluster, ClusterOptions, KEY};

#[tokio::test(flavor = "multi_thread")]
async fn sql_backup_stream_round_trip_restores_types_and_replication_flags() {
    let mut cluster = Cluster::new(ClusterOptions {
        name: "backup-restore".into(),
        num_nodes: 1,
        local_only: true,
        ..Default::default()
    })
    .await
    .unwrap();
    cluster.node(0).simple("CREATE TABLE records(id INTEGER PRIMARY KEY, title TEXT NOT NULL, score REAL, payload BLOB, optional TEXT); INSERT INTO records VALUES(1,'quoted '' title',1.25,x'0001feff',NULL); INSERT INTO records VALUES(2,'日本語',-7.5,x'', 'present')").await.unwrap();
    // Table flags are authoritative config state and must survive restore.
    cluster
        .node(0)
        .set_table_replicated("records", true)
        .await
        .unwrap();
    let query="SELECT id,title,score,hex(payload),optional,id_murmur_primary_key FROM records ORDER BY id";
    let expected = cluster.node(0).simple(query).await.unwrap();
    let saved = cluster.node(0).backup_save().await.unwrap();
    assert!(saved.size > 0);
    let mut bytes = Vec::new();
    cluster
        .node(0)
        .backup_download(&saved.name, &mut bytes)
        .await
        .unwrap();
    assert_eq!(bytes.len() as i64, saved.size);
    cluster
        .node(0)
        .backup_upload("roundtrip.murmur-backup", &bytes[..])
        .await
        .unwrap();
    assert!(cluster
        .node(0)
        .backup_list()
        .await
        .unwrap()
        .iter()
        .any(|b| b.name == "roundtrip.murmur-backup" && b.size == saved.size));
    cluster
        .node(0)
        .simple("DELETE FROM records; INSERT INTO records(id,title) VALUES(99,'after-backup')")
        .await
        .unwrap();
    cluster
        .node(0)
        .restore("roundtrip.murmur-backup", KEY)
        .await
        .unwrap();
    let fresh = cluster.connect(0).await.unwrap();
    assert_eq!(fresh.simple(query).await.unwrap(), expected);
    assert!(fresh
        .table_replication()
        .await
        .unwrap()
        .iter()
        .any(|f| f.table == "records" && f.replicated));
    fresh.flush().await.unwrap();
    drop(fresh);
    cluster.stop_node(0);
    cluster.start_node(0).await.unwrap();
    assert_eq!(cluster.node(0).simple(query).await.unwrap(), expected);
}

#[tokio::test(flavor = "multi_thread")]
async fn restore_rejects_wrong_key_and_foreign_identity_without_changing_live_rows() {
    let cluster = Cluster::new(ClusterOptions {
        name: "backup-restore-rejections".into(),
        num_nodes: 2,
        local_only: true,
        ..Default::default()
    })
    .await
    .unwrap();
    for i in 0..2 {
        cluster
            .node(i)
            .simple("CREATE TABLE records(id INTEGER PRIMARY KEY, body TEXT)")
            .await
            .unwrap();
    }
    cluster
        .node(0)
        .simple("INSERT INTO records VALUES(1,'source')")
        .await
        .unwrap();
    cluster
        .node(1)
        .simple("INSERT INTO records VALUES(2,'target-kept')")
        .await
        .unwrap();
    let saved = cluster.node(0).backup_save().await.unwrap();
    let error = cluster
        .node(0)
        .restore(&saved.name, "wrong-restore-key")
        .await
        .unwrap_err();
    assert_eq!(error.code(), Some("28000"));
    assert_eq!(
        cluster
            .node(0)
            .simple("SELECT body FROM records")
            .await
            .unwrap(),
        vec![vec!["source".to_string()]]
    );
    let mut bytes = Vec::new();
    cluster
        .node(0)
        .backup_download(&saved.name, &mut bytes)
        .await
        .unwrap();
    cluster
        .node(1)
        .backup_upload("foreign.murmur-backup", &bytes[..])
        .await
        .unwrap();
    assert!(
        cluster
            .node(1)
            .restore("foreign.murmur-backup", KEY)
            .await
            .is_err(),
        "backup with foreign writer identity accepted"
    );
    assert_eq!(
        cluster
            .node(1)
            .simple("SELECT body FROM records")
            .await
            .unwrap(),
        vec![vec!["target-kept".to_string()]]
    );
}
