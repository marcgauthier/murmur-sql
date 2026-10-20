//! SQL assertions shared by live workloads. No embedded database types or calls.
use crate::{Cluster, NodeClient};
use std::time::{Duration, Instant};

pub async fn fixture(cluster: &Cluster) {
    for i in 0..cluster.len() {
        cluster.node(i).simple("CREATE TABLE IF NOT EXISTS records(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL)").await.expect("create SQL fixture");
        cluster
            .node(i)
            .simple("MURMUR SET TABLE records REPLICATED true")
            .await
            .expect("enable fixture replication");
        cluster
            .node(i)
            .simple("MURMUR FLUSH")
            .await
            .expect("flush fixture schema");
    }
}

pub async fn rows(client: &NodeClient) -> Vec<Vec<String>> {
    client
        .simple("SELECT id, name FROM records ORDER BY id")
        .await
        .expect("read SQL rows")
}

pub async fn status(client: &NodeClient) -> serde_json::Value {
    let result = client
        .simple("MURMUR SHOW STATUS")
        .await
        .expect("query engine diagnostic snapshot over SQL");
    assert_eq!(result.len(), 1);
    serde_json::from_str(&result[0][0]).expect("decode diagnostic JSON")
}

pub async fn peers(client: &NodeClient) -> Vec<Vec<String>> {
    client
        .simple("MURMUR SHOW PEERS")
        .await
        .expect("query SQL peers")
}

pub async fn converge(
    cluster: &Cluster,
    nodes: &[usize],
    expected: &[Vec<String>],
    timeout: Duration,
) {
    let end = Instant::now() + timeout;
    loop {
        let mut last = Vec::new();
        for &i in nodes {
            last.push((i, rows(cluster.node(i)).await));
        }
        if last.iter().all(|(_, actual)| actual == expected) {
            return;
        }
        assert!(
            Instant::now() < end,
            "SQL convergence timeout: expected={expected:?}, actual={last:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

pub async fn peer_counts(cluster: &Cluster, nodes: &[usize], counts: &[usize], timeout: Duration) {
    assert_eq!(nodes.len(), counts.len());
    let end = Instant::now() + timeout;
    loop {
        let mut actual = Vec::new();
        for &i in nodes {
            actual.push(
                peers(cluster.node(i))
                    .await
                    .iter()
                    .filter(|p| p[2] == "true")
                    .count(),
            );
        }
        if actual == counts {
            return;
        }
        assert!(
            Instant::now() < end,
            "SQL peer counts: actual={actual:?}, expected={counts:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
