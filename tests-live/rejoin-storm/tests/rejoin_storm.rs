//! Live Murmur-SQL scenario: real server children, Rust wire client, SQL only.
use murmur_live_harness::sql::{converge, fixture, peer_counts, status};
use murmur_live_harness::{tokio, Cluster, ClusterOptions};
use std::time::Duration;
const WAIT: Duration = Duration::from_secs(120);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dual_kill_simultaneous_rejoin() {
    let mut cluster = Cluster::new(ClusterOptions {
        name: "rejoin-storm-sql".into(),
        num_nodes: 3,
        ..Default::default()
    })
    .await
    .unwrap();
    fixture(&cluster).await;
    let mut expected = Vec::new();
    for n in 0..10 {
        let id = format!("base-{n:03}");
        let name = format!("base-{n}");
        cluster
            .node(0)
            .execute("INSERT INTO records(id,name) VALUES($1,$2)", &[&id, &name])
            .await
            .unwrap();
        expected.push(vec![id, name]);
    }
    cluster.node(0).simple("MURMUR FLUSH").await.unwrap();
    converge(&cluster, &[0, 1, 2], &expected, WAIT).await;
    for node in [1, 2] {
        cluster.node(node).simple("MURMUR FLUSH").await.unwrap();
        cluster.kill_node(node).await;
    }
    for n in 0..30 {
        let id = format!("offline-{n:03}");
        let name = format!("acknowledged-{n}");
        cluster
            .node(0)
            .execute("INSERT INTO records(id,name) VALUES($1,$2)", &[&id, &name])
            .await
            .unwrap();
        expected.push(vec![id, name]);
    }
    cluster.node(0).simple("MURMUR FLUSH").await.unwrap();
    cluster.start_nodes_parallel(&[1, 2]).await.unwrap();
    cluster.mesh().await.unwrap();
    expected.sort();
    converge(&cluster, &[0, 1, 2], &expected, WAIT).await;
    peer_counts(&cluster, &[0, 1, 2], &[2, 2, 2], WAIT).await;
    for i in 1..3 {
        let s = status(cluster.node(i)).await;
        assert!(
            s["replication"]["metrics"]["received_transactions"]
                .as_u64()
                .unwrap()
                > 0
        );
        assert!(s["replication"]["last_error"].is_null());
    }
    cluster.finish(false).await;
}
