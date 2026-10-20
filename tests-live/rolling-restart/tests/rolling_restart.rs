//! Live Murmur-SQL scenario: real server children, Rust wire client, SQL only.
use murmur_live_harness::sql::{converge, fixture, peers};
use murmur_live_harness::{tokio, Cluster, ClusterOptions};
use std::time::Duration;
const WAIT: Duration = Duration::from_secs(120);

async fn roll(rounds: usize) {
    let mut cluster = Cluster::new(ClusterOptions {
        name: format!("rolling-sql-{rounds}"),
        num_nodes: 3,
        ..Default::default()
    })
    .await
    .unwrap();
    fixture(&cluster).await;
    let mut expected = Vec::new();
    for n in 0..5 {
        let id = format!("base-{n}");
        let name = format!("baseline-{n}");
        cluster
            .node(0)
            .execute("INSERT INTO records(id,name) VALUES($1,$2)", &[&id, &name])
            .await
            .unwrap();
        expected.push(vec![id, name]);
    }
    cluster.node(0).simple("MURMUR FLUSH").await.unwrap();
    converge(&cluster, &[0, 1, 2], &expected, WAIT).await;
    for round in 0..rounds {
        let restarting = round % 3;
        // Persist acknowledged state, then write concurrently on both live peers
        // throughout this real process restart. Every successful write is checked.
        for i in 0..3 {
            cluster.node(i).simple("MURMUR FLUSH").await.unwrap();
        }
        cluster.stop_node(restarting).await;
        let mut tasks = Vec::new();
        for node in 0..3 {
            if node == restarting {
                continue;
            }
            let client = cluster.node(node).clone();
            tasks.push(tokio::spawn(async move {
                let mut acked = Vec::new();
                for n in 0..25 {
                    let id = format!("r{round}-n{node}-{n:03}");
                    let name = format!("live-{id}");
                    client
                        .execute("INSERT INTO records(id,name) VALUES($1,$2)", &[&id, &name])
                        .await
                        .unwrap();
                    acked.push(vec![id, name]);
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
                client.simple("MURMUR FLUSH").await.unwrap();
                acked
            }));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        cluster.start_node(restarting).await.unwrap();
        cluster.mesh().await.unwrap();
        for task in tasks {
            expected.extend(task.await.unwrap());
        }
        expected.sort();
        converge(&cluster, &[0, 1, 2], &expected, WAIT).await;
    }
    cluster.finish(false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rolling_restart_loses_no_writes() {
    roll(3).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rolling_restart_churn() {
    roll(6).await;
}
