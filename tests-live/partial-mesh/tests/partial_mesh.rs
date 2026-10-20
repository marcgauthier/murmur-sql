//! Live Murmur-SQL scenario: real server children, Rust wire client, SQL only.
use murmur_live_harness::sql::{converge, fixture, peer_counts, peers};
use murmur_live_harness::{tokio, Cluster, ClusterOptions, ReplicationOptions};
use std::time::Duration;
const WAIT: Duration = Duration::from_secs(120);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn chain_peers_converge_via_forwarding() {
    let cluster = Cluster::new(ClusterOptions {
        name: "partial-mesh-chain".into(),
        num_nodes: 3,
        manual_peers: true,
        replication: Some(ReplicationOptions {
            membership: Some(false),
            ..Default::default()
        }),
        ..Default::default()
    })
    .await
    .unwrap();
    fixture(&cluster).await;
    for (a, b) in [(0, 1), (1, 0), (1, 2), (2, 1)] {
        cluster.add_peer(a, b).await.unwrap();
    }
    peer_counts(&cluster, &[0, 1, 2], &[1, 2, 1], WAIT).await;
    let mut expected = Vec::new();
    for i in 0..30 {
        let id = format!("{i:04}");
        let name = format!("chain-{i}");
        cluster
            .node(0)
            .execute("INSERT INTO records(id,name) VALUES($1,$2)", &[&id, &name])
            .await
            .unwrap();
        expected.push(vec![id, name]);
    }
    cluster.node(0).simple("MURMUR FLUSH").await.unwrap();
    converge(&cluster, &[0, 1, 2], &expected, WAIT).await;
    assert!(!peers(cluster.node(0))
        .await
        .iter()
        .any(|p| p[0] == murmur_live_harness::hex(&cluster.node_id(2)) && p[2] == "true"));
    cluster.finish(false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dynamic_bootstrap_discovery() {
    let cluster = Cluster::new(ClusterOptions {
        name: "partial-mesh-discovery".into(),
        num_nodes: 3,
        manual_peers: true,
        bootstrap_seeds: vec![0],
        ..Default::default()
    })
    .await
    .unwrap();
    fixture(&cluster).await;
    peer_counts(&cluster, &[0, 1, 2], &[2, 2, 2], WAIT).await;
    let mut expected = Vec::new();
    for i in 0..3 {
        let id = format!("{i}");
        let name = format!("discovered-{i}");
        cluster
            .node(i)
            .execute("INSERT INTO records(id,name) VALUES($1,$2)", &[&id, &name])
            .await
            .unwrap();
        cluster.node(i).simple("MURMUR FLUSH").await.unwrap();
        expected.push(vec![id, name]);
    }
    converge(&cluster, &[0, 1, 2], &expected, WAIT).await;
    cluster.finish(false).await;
}
