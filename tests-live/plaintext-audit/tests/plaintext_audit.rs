//! Check SQL values and unlock credentials never reach durable artifacts in plaintext.
use murmur_live_harness::{assert_no_plaintext, assert_rows, Cluster, ClusterOptions, KEY};

#[tokio::test(flavor = "multi_thread")]
async fn plaintext_audit_markers_and_unlock_key_absent_on_disk() {
    let mut cluster = Cluster::new(ClusterOptions {
        name: "plaintext-audit".into(),
        num_nodes: 2,
        ..Default::default()
    })
    .await
    .unwrap();
    for i in 0..2 {
        cluster
            .node(i)
            .simple(
                "CREATE TABLE IF NOT EXISTS secrets(id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
            )
            .await
            .unwrap();
        cluster
            .node(i)
            .set_table_replicated("secrets", true)
            .await
            .unwrap();
    }
    let marker = format!("live-sql-plaintext-audit-{}", cluster.node_info(0).node_id);
    for i in 0..2 {
        let id = i as i64;
        cluster
            .node(i)
            .execute(
                "INSERT INTO secrets(id,body) VALUES($1,$2)",
                &[&id, &marker],
            )
            .await
            .unwrap();
        cluster.node(i).flush().await.unwrap();
    }
    let expected = vec![vec![marker.clone()], vec![marker.clone()]];
    assert_rows(
        &cluster,
        &[0, 1],
        "SELECT body FROM secrets ORDER BY id",
        &expected,
    )
    .await;
    // Backup archives are included in the byte-level audit.
    cluster.node(0).backup_save().await.unwrap();
    for i in 0..2 {
        cluster.node(i).flush().await.unwrap();
        cluster.stop_node(i);
    }
    assert_no_plaintext(cluster.runtime_dir(), &[marker.as_bytes(), KEY.as_bytes()]);
    for i in 0..2 {
        cluster.start_node(i).await.unwrap();
    }
    assert_rows(
        &cluster,
        &[0, 1],
        "SELECT body FROM secrets ORDER BY id",
        &expected,
    )
    .await;
}
