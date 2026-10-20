//! Rotate through the SQL client, crash the actual server, and reopen with the new key.
use murmur_client::Client;
use murmur_live_harness::{Cluster, ClusterOptions, KEY};

#[tokio::test(flavor = "multi_thread")]
async fn sql_key_rotation_survives_kill_and_rejects_old_key() {
    let mut cluster = Cluster::new(ClusterOptions {
        name: "rekey".into(),
        num_nodes: 1,
        ..Default::default()
    })
    .await
    .unwrap();
    cluster.node(0).simple("CREATE TABLE records(id INTEGER PRIMARY KEY, body TEXT); INSERT INTO records VALUES(1,'before-rotation')").await.unwrap();
    cluster.node(0).simple("MURMUR FLUSH").await.unwrap();
    let new_key = "live-new-encryption-key-after-rotation";
    cluster.node(0).simple(&format!("MURMUR ROTATE KEY {} {}",murmur_live_harness::sql_literal(KEY),murmur_live_harness::sql_literal(new_key))).await.unwrap();
    // No graceful shutdown or additional flush after the acknowledged rotation.
    cluster.stop_node(0);
    cluster.set_key(0, new_key);
    cluster.start_node(0).await.unwrap();
    let mut old = cluster.config(0);
    old.key = KEY.into();
    let error = match Client::connect(&old).await {
        Err(e) => e,
        Ok(_) => panic!("old key accepted after rotation"),
    };
    assert_eq!(error.code(), Some("28000"));
    assert_eq!(
        cluster
            .node(0)
            .simple("SELECT body FROM records")
            .await
            .unwrap(),
        vec![vec!["before-rotation".to_string()]]
    );
    cluster
        .node(0)
        .simple("INSERT INTO records VALUES(2,'after-rotation')")
        .await
        .unwrap();
    cluster.node(0).simple("MURMUR FLUSH").await.unwrap();
    cluster.stop_node(0);
    cluster.start_node(0).await.unwrap();
    assert_eq!(
        cluster
            .node(0)
            .simple("SELECT body FROM records ORDER BY id")
            .await
            .unwrap(),
        vec![
            vec!["before-rotation".to_string()],
            vec!["after-rotation".to_string()]
        ]
    );
}
