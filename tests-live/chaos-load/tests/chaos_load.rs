//! Live Murmur-SQL scenario: real server children, Rust wire client, SQL only.
use murmur_live_harness::sql::{converge, fixture, peer_counts, rows};
use murmur_live_harness::{tokio, wait_until_async, Cluster, ClusterOptions, ReplicationOptions};
use std::time::Duration;
const WAIT: Duration = Duration::from_secs(120);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn continuous_writes_across_partition_and_healing() {
    let cluster = Cluster::new(ClusterOptions {
        name: "chaos-load-sql".into(),
        num_nodes: 3,
        replication: Some(ReplicationOptions {
            membership: Some(false),
            ..Default::default()
        }),
        ..Default::default()
    })
    .await
    .unwrap();
    fixture(&cluster).await;
    for other in [0, 1] {
        cluster.remove_peer(other, 2).await.unwrap();
        cluster.remove_peer(2, other).await.unwrap();
    }
    peer_counts(&cluster, &[0, 1, 2], &[1, 1, 0], WAIT).await;
    let mut tasks = Vec::new();
    for node in 0..3 {
        let client = cluster.node(node).clone();
        tasks.push(tokio::spawn(async move {
            let mut acked = Vec::new();
            for n in 0..80 {
                let id = format!("n{node}-{n:04}");
                let name = format!("continuous-{id}");
                client
                    .execute("INSERT INTO records(id,name) VALUES($1,$2)", &[&id, &name])
                    .await
                    .unwrap();
                if n % 10 == 0 {
                    client.simple("MURMUR FLUSH").await.unwrap();
                }
                acked.push(vec![id, name]);
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            client.simple("MURMUR FLUSH").await.unwrap();
            acked
        }));
    }
    // Wait for divergent acknowledged SQL state before healing, while writers run.
    assert!(
        wait_until_async(WAIT, Duration::from_millis(25), || async {
            let a = rows(cluster.node(0)).await;
            let b = rows(cluster.node(2)).await;
            a.len() >= 10 && b.len() >= 5 && a != b
        })
        .await
    );
    cluster.mesh().await.unwrap();
    let mut expected = Vec::new();
    for task in tasks {
        expected.extend(task.await.unwrap());
    }
    expected.sort();
    converge(&cluster, &[0, 1, 2], &expected, WAIT).await;
    cluster
        .node(0)
        .execute(
            "INSERT INTO records(id,name) VALUES('post-heal','still-writable')",
            &[],
        )
        .await
        .unwrap();
    cluster.node(0).simple("MURMUR FLUSH").await.unwrap();
    expected.push(vec!["post-heal".into(), "still-writable".into()]);
    expected.sort();
    converge(&cluster, &[0, 1, 2], &expected, WAIT).await;
    cluster.finish(false).await;
}
