//! Live Murmur-SQL scenario: real server children, Rust wire client, SQL only.
use murmur_live_harness::sql::{converge, fixture, status};
use murmur_live_harness::{tokio, Cluster, ClusterOptions};
use std::time::{Duration, Instant};
const WAIT: Duration = Duration::from_secs(120);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writer_shares_under_load() {
    let cluster = Cluster::new(ClusterOptions {
        name: "loadshare-sql".into(),
        num_nodes: 2,
        ..Default::default()
    })
    .await
    .unwrap();
    fixture(&cluster).await;
    let mut tasks = Vec::new();
    for worker in 0..4 {
        let client = cluster.connect(0).await.unwrap();
        tasks.push(tokio::spawn(async move {
            let mut acked = Vec::new();
            let mut latency = Vec::new();
            for n in 0..60 {
                let id = format!("local-{worker}-{n:04}");
                let name = format!("value-{id}");
                let start = Instant::now();
                client
                    .execute("INSERT INTO records(id,name) VALUES($1,$2)", &[&id, &name])
                    .await
                    .unwrap();
                latency.push(start.elapsed());
                acked.push(vec![id, name]);
            }
            client.simple("MURMUR FLUSH").await.unwrap();
            (acked, latency)
        }));
    }
    let mut expected = Vec::new();
    for n in 0..20 {
        let id = format!("remote-{n:04}");
        let name = format!("value-{id}");
        cluster
            .node(1)
            .execute("INSERT INTO records(id,name) VALUES($1,$2)", &[&id, &name])
            .await
            .unwrap();
        cluster.node(1).simple("MURMUR FLUSH").await.unwrap();
        expected.push(vec![id, name]);
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let mut latencies = Vec::new();
    for task in tasks {
        let (acked, lat) = task.await.unwrap();
        expected.extend(acked);
        latencies.extend(lat);
    }
    expected.sort();
    converge(&cluster, &[0, 1], &expected, WAIT).await;
    assert_eq!(latencies.len(), 240);
    latencies.sort();
    assert!(latencies[239] <= Duration::from_secs(10));
    let s = status(cluster.node(0)).await;
    let sched = &s["scheduler"];
    assert!(sched["local"]["acquisitions"].as_u64().unwrap() >= 240);
    assert!(sched["remote"]["acquisitions"].as_u64().unwrap() > 0);
    assert!(sched["debt_nanos"].as_u64().unwrap() <= 1_000_000_000);
    cluster.finish(false).await;
}
