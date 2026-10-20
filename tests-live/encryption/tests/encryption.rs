//! Encrypted SQL persistence rejects a wrong unlock key across process restarts.
use murmur_client::Client;
use murmur_live_harness::{assert_no_plaintext, Cluster, ClusterOptions};
const SECRET: &str = "SQL-CONFIDENTIAL-PLAINTEXT-SECRET-246813579";

#[tokio::test(flavor = "multi_thread")]
async fn encrypted_sql_rows_survive_restart_and_reject_wrong_key() {
    let mut cluster = Cluster::new(ClusterOptions {
        name: "encryption".into(),
        num_nodes: 1,
        ..Default::default()
    })
    .await
    .unwrap();
    cluster
        .node(0)
        .simple("CREATE TABLE records(name TEXT PRIMARY KEY NOT NULL)")
        .await
        .unwrap();
    cluster
        .node(0)
        .execute("INSERT INTO records(name) VALUES($1)", &[&SECRET])
        .await
        .unwrap();
    cluster.node(0).simple("MURMUR FLUSH").await.unwrap();
    let expected = vec![vec![SECRET.to_string()]];
    assert_eq!(
        cluster
            .node(0)
            .simple("SELECT name FROM records")
            .await
            .unwrap(),
        expected
    );
    let dir = cluster.node_info(0).dir.clone();
    cluster.stop_node(0);
    assert_no_plaintext(&dir, &[SECRET.as_bytes()]);
    // The first unlock on a cold server must reject a wrong key too.
    cluster.set_key(0, "cold-start-wrong-key");
    let cold_error = cluster
        .start_node(0)
        .await
        .expect_err("cold wrong key accepted");
    assert_eq!(
        cold_error
            .downcast_ref::<murmur_client::Error>()
            .unwrap()
            .code(),
        Some("28000")
    );
    cluster.stop_node(0);
    cluster.set_key(0, murmur_live_harness::KEY);
    cluster.start_node(0).await.unwrap();
    let mut wrong = cluster.config(0);
    wrong.key = "deliberately-wrong-key".into();
    let err = match Client::connect(&wrong).await {
        Err(e) => e,
        Ok(_) => panic!("wrong SQL unlock key accepted"),
    };
    assert_eq!(err.code(), Some("28000"));
    assert!(!err.to_string().contains(&wrong.key));
    assert_eq!(
        cluster
            .node(0)
            .simple("SELECT name FROM records")
            .await
            .unwrap(),
        expected
    );
}
