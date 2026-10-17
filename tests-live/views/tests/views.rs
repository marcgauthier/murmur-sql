//! SQL projections reflect replicated updates and survive a real server restart.
use murmur_live_harness::{Cluster, ClusterOptions};
use std::time::{Duration, Instant};

const PROJECTION: &str = "SELECT name FROM contacts WHERE score>0 ORDER BY name";

async fn enabled(cluster: &Cluster, nodes: &[usize]) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let mut matches = true;
        for &i in nodes {
            matches &= cluster.node(i).simple(PROJECTION).await.unwrap()
                == vec![vec!["ann".to_string()], vec!["bob".to_string()]];
        }
        if matches {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "projection did not converge on ann and bob"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn sql_projection_tracks_replicated_rows_and_rebuilds() {
    let mut cluster = Cluster::new(ClusterOptions {
        name: "views".into(),
        num_nodes: 3,
        ..Default::default()
    })
    .await
    .unwrap();
    for i in 0..3 {
        cluster.node(i).simple("CREATE TABLE IF NOT EXISTS contacts(name TEXT PRIMARY KEY NOT NULL, score INTEGER NOT NULL)").await.unwrap();
        cluster
            .node(i)
            .set_table_replicated("contacts", true)
            .await
            .unwrap();
    }
    for (i, name) in ["ann", "bob", "hidden"].iter().enumerate() {
        cluster
            .node(i)
            .execute("INSERT INTO contacts(name,score) VALUES($1,0)", &[name])
            .await
            .unwrap();
        if i < 2 {
            assert_eq!(
                cluster
                    .node(i)
                    .execute("UPDATE contacts SET score=score+1 WHERE name=$1", &[name])
                    .await
                    .unwrap(),
                1
            );
        }
        cluster.node(i).flush().await.unwrap();
    }
    enabled(&cluster, &[0, 1, 2]).await;
    cluster.node(2).flush().await.unwrap();
    cluster.stop_node(2);
    cluster.start_node(2).await.unwrap();
    enabled(&cluster, &[2]).await;
}
