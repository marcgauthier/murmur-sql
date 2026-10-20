//! Four real SQL servers retain independent writes across an isolated two-pair topology.
use murmur_live_harness::{assert_rows, Cluster, ClusterOptions};
use std::time::Duration;

async fn insert(cluster: &Cluster, node: usize, id: i64, name: &str) {
    cluster
        .node(node)
        .execute("INSERT INTO records(id,name) VALUES($1,$2)", &[&id, &name])
        .await
        .unwrap();
    cluster.node(node).simple("MURMUR FLUSH").await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn four_node_partition_isolation_and_healing() {
    let mut cluster = Cluster::new(ClusterOptions {
        name: "partition".into(),
        num_nodes: 4,
        replication_toml: "membership = false".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    for i in 0..4 {
        cluster
            .node(i)
            .simple(
                "CREATE TABLE IF NOT EXISTS records(id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
            )
            .await
            .unwrap();
        cluster
            .node(i)
            .simple("MURMUR SET TABLE records REPLICATED true")
            .await
            .unwrap();
    }
    insert(&cluster, 0, 1, "baseline").await;
    assert_rows(
        &cluster,
        &[0, 1, 2, 3],
        "SELECT name FROM records ORDER BY id",
        &[vec!["baseline".into()]],
    )
    .await;
    // Restart from persisted config to sever every cross-pair connection.
    for i in 0..4 {
        cluster.node(i).simple("MURMUR FLUSH").await.unwrap();
        cluster.stop_node(i);
    }
    for (i, peer) in [(0, 1), (1, 0), (2, 3), (3, 2)] {
        cluster.set_peers(i, &[peer]).unwrap();
    }
    for i in 0..4 {
        cluster.start_node(i).await.unwrap();
    }
    insert(&cluster, 0, 100, "side-ab").await;
    insert(&cluster, 2, 200, "side-cd").await;
    let left = vec![vec!["baseline".into()], vec!["side-ab".into()]];
    let right = vec![vec!["baseline".into()], vec!["side-cd".into()]];
    assert_rows(
        &cluster,
        &[0, 1],
        "SELECT name FROM records ORDER BY id",
        &left,
    )
    .await;
    assert_rows(
        &cluster,
        &[2, 3],
        "SELECT name FROM records ORDER BY id",
        &right,
    )
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_ne!(
        cluster
            .node(0)
            .simple("SELECT name FROM records ORDER BY id")
            .await
            .unwrap(),
        cluster
            .node(2)
            .simple("SELECT name FROM records ORDER BY id")
            .await
            .unwrap()
    );
    for i in 0..4 {
        cluster.node(i).simple("MURMUR FLUSH").await.unwrap();
        cluster.stop_node(i);
    }
    for i in 0..4 {
        cluster
            .set_peers(i, &(0..4).filter(|j| *j != i).collect::<Vec<_>>())
            .unwrap();
    }
    for i in 0..4 {
        cluster.start_node(i).await.unwrap();
    }
    let mut healed = vec![
        vec!["baseline".into()],
        vec!["side-ab".into()],
        vec!["side-cd".into()],
    ];
    assert_rows(
        &cluster,
        &[0, 1, 2, 3],
        "SELECT name FROM records ORDER BY id",
        &healed,
    )
    .await;
    insert(&cluster, 1, 300, "post-heal").await;
    healed.push(vec!["post-heal".into()]);
    assert_rows(
        &cluster,
        &[0, 1, 2, 3],
        "SELECT name FROM records ORDER BY id",
        &healed,
    )
    .await;
}
