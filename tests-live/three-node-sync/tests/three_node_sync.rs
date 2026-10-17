//! Concurrent SQL writers must replicate all rows, including stable managed keys.
use murmur_live_harness::{Cluster, ClusterOptions};
use std::time::{Duration, Instant};

#[tokio::test(flavor = "multi_thread")]
async fn three_node_sync() {
    let cluster = Cluster::new(ClusterOptions {
        name: "three-node-sync".into(),
        num_nodes: 3,
        ..Default::default()
    })
    .await
    .expect("start SQL cluster");
    for i in 0..cluster.len() {
        let client = cluster.node(i);
        client
            .simple(
                "CREATE TABLE IF NOT EXISTS records(id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
            )
            .await
            .unwrap();
        client.set_table_replicated("records", true).await.unwrap();
        client.flush().await.unwrap();
    }
    let write = |i: usize| {
        let client = cluster.node(i);
        async move {
            for row in 0..30i64 {
                let id = i as i64 * 1000 + row;
                let name = format!("node-{i}-row-{row:02}");
                assert_eq!(
                    client
                        .execute("INSERT INTO records(id,name) VALUES($1,$2)", &[&id, &name])
                        .await
                        .unwrap(),
                    1
                );
            }
            client.flush().await.unwrap();
        }
    };
    tokio::join!(write(0), write(1), write(2));
    let mut expected: Vec<Vec<String>> = (0..3)
        .flat_map(|i| (0..30).map(move |r| vec![format!("node-{i}-row-{r:02}")]))
        .collect();
    expected.sort();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let mut converged = true;
        for i in 0..cluster.len() {
            converged &= cluster
                .node(i)
                .simple("SELECT name FROM records ORDER BY name")
                .await
                .unwrap()
                == expected;
        }
        if converged {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "three SQL nodes failed to converge on 90 rows"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let keys = cluster
        .node(0)
        .simple("SELECT id,id_murmur_primary_key FROM records ORDER BY id")
        .await
        .unwrap();
    assert_eq!(keys.len(), 90);
    for i in 1..3 {
        assert_eq!(
            cluster
                .node(i)
                .simple("SELECT id,id_murmur_primary_key FROM records ORDER BY id")
                .await
                .unwrap(),
            keys
        );
    }
}
