//! Live Murmur-SQL scenario: real server children, Rust wire client, SQL only.
use murmur_live_harness::sql::{converge, fixture, peer_counts, status};
use murmur_live_harness::{env_seconds, tokio, Cluster, ClusterOptions};
use std::time::Duration;
const WAIT: Duration = Duration::from_secs(120);

async fn discovery(restart: bool) {
    let mut cluster = Cluster::new(ClusterOptions {
        name: format!("swim-sql-{restart}"),
        num_nodes: 4,
        manual_peers: true,
        bootstrap_seeds: vec![0],
        ..Default::default()
    })
    .await
    .unwrap();
    fixture(&cluster).await;
    let timeout = env_seconds("MURMUR_SWIM_DISCOVERY_SECONDS", 60);
    peer_counts(&cluster, &[0, 1, 2, 3], &[3, 3, 3, 3], timeout).await;
    for i in 0..4 {
        assert!(
            status(cluster.node(i)).await["replication"]["members"]
                .as_u64()
                .unwrap()
                >= 3
        );
    }
    let mut expected = Vec::new();
    for node in 0..4 {
        for n in 0..10 {
            let id = format!("{node}-{n:02}");
            let name = format!("discovery-{id}");
            cluster
                .node(node)
                .execute("INSERT INTO records(id,name) VALUES($1,$2)", &[&id, &name])
                .await
                .unwrap();
            expected.push(vec![id, name]);
        }
        cluster.node(node).simple("MURMUR FLUSH").await.unwrap();
    }
    expected.sort();
    converge(&cluster, &[0, 1, 2, 3], &expected, WAIT).await;
    if restart {
        cluster.kill_node(0).await;
        peer_counts(&cluster, &[1, 2, 3], &[2, 2, 2], timeout).await;
        for n in 0..12 {
            let id = format!("down-{n:02}");
            let name = format!("survivor-{n}");
            let node = 1 + n % 3;
            cluster
                .node(node)
                .execute("INSERT INTO records(id,name) VALUES($1,$2)", &[&id, &name])
                .await
                .unwrap();
            cluster.node(node).simple("MURMUR FLUSH").await.unwrap();
            expected.push(vec![id, name]);
        }
        expected.sort();
        converge(&cluster, &[1, 2, 3], &expected, WAIT).await;
        cluster.start_node(0).await.unwrap();
        cluster.add_peer(0, 1).await.unwrap();
        peer_counts(&cluster, &[0, 1, 2, 3], &[3, 3, 3, 3], timeout).await;
        converge(&cluster, &[0, 1, 2, 3], &expected, WAIT).await;
    }
    cluster.finish(false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn seed_only_discovery_forms_full_mesh() {
    discovery(false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn seed_kill_and_rediscovery() {
    discovery(true).await;
}
