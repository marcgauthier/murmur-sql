//! Live Murmur-SQL scenario: real server children, Rust wire client, SQL only.
use murmur_live_harness::sql::{converge, fixture, peer_counts, peers, rows};
use murmur_live_harness::{tokio, wait_until_async, Cluster, ClusterOptions, ReplicationOptions};
use std::time::Duration;
const WAIT: Duration = Duration::from_secs(120);

async fn heal(order: &[usize], tag: &str) -> Vec<Vec<String>> {
    let cluster = Cluster::new(ClusterOptions {
        name: format!("three-way-sql-{tag}"),
        num_nodes: 4,
        replication: Some(ReplicationOptions {
            membership: Some(false),
            ..Default::default()
        }),
        ..Default::default()
    })
    .await
    .unwrap();
    fixture(&cluster).await;
    let mut base = Vec::new();
    for n in 0..10 {
        let id = format!("base-{n:03}");
        let name = format!("baseline-{n}");
        cluster
            .node(0)
            .execute("INSERT INTO records(id,name) VALUES($1,$2)", &[&id, &name])
            .await
            .unwrap();
        base.push(vec![id, name]);
    }
    cluster.node(0).simple("MURMUR FLUSH").await.unwrap();
    converge(&cluster, &[0, 1, 2, 3], &base, WAIT).await;
    let links = [(0, 2), (0, 3), (1, 2), (1, 3)];
    for (a, b) in links {
        cluster.remove_peer(a, b).await.unwrap();
        cluster.remove_peer(b, a).await.unwrap();
    }
    peer_counts(&cluster, &[0, 1, 2, 3], &[1, 1, 1, 1], WAIT).await;
    let mut expected = Vec::new();
    for (node, side) in [(1, "A"), (3, "B")] {
        for n in 0..10 {
            let id = format!("side{side}-{n:03}");
            let name = format!("side-{side}-{n}");
            cluster
                .node(node)
                .execute("INSERT INTO records(id,name) VALUES($1,$2)", &[&id, &name])
                .await
                .unwrap();
            expected.push(vec![id, name]);
        }
        for n in 0..10 {
            let id = format!("base-{n:03}");
            let name = format!("conflict-{side}-{n}");
            cluster
                .node(node)
                .execute("UPDATE records SET name=$1 WHERE id=$2", &[&name, &id])
                .await
                .unwrap();
        }
        cluster.node(node).simple("MURMUR FLUSH").await.unwrap();
        // Capture times are ordered; side B must be the same conflict winner in both runs.
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        wait_until_async(WAIT, Duration::from_millis(50), || async {
            let a = rows(cluster.node(0)).await;
            let b = rows(cluster.node(2)).await;
            a.len() == 20 && b.len() == 20 && a != b
        })
        .await
    );
    for &index in order {
        let (a, b) = links[index];
        cluster.add_peer(a, b).await.unwrap();
        cluster.add_peer(b, a).await.unwrap();
        assert!(
            wait_until_async(WAIT, Duration::from_millis(50), || async {
                peers(cluster.node(a)).await.iter().any(|p| {
                    p[0] == murmur_live_harness::hex(&cluster.node_id(b)) && p[2] == "true"
                }) && rows(cluster.node(a)).await.len() == 30
            })
            .await
        );
    }
    for n in 0..10 {
        expected.push(vec![format!("base-{n:03}"), format!("conflict-B-{n}")]);
    }
    expected.sort();
    converge(&cluster, &[0, 1, 2, 3], &expected, WAIT).await;
    let final_rows = rows(cluster.node(0)).await;
    cluster.finish(false).await;
    final_rows
}
fn order(var: &str, fallback: &str) -> Vec<usize> {
    let raw = std::env::var(var).unwrap_or_else(|_| fallback.into());
    let values: Vec<usize> = raw
        .split(',')
        .map(|s| s.trim().parse().expect("heal link index"))
        .collect();
    let mut sorted = values.clone();
    sorted.sort();
    assert_eq!(sorted, vec![0, 1, 2, 3]);
    values
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn heal_order_does_not_affect_final_digest() {
    let a = order("MURMUR_THREE_WAY_HEAL_ORDER", "0,1,2,3");
    let b = order("MURMUR_THREE_WAY_HEAL_ORDER_B", "3,2,1,0");
    assert_eq!(heal(&a, "a").await, heal(&b, "b").await);
}
