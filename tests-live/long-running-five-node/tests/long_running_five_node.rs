//! Live Murmur-SQL scenario: real server children, Rust wire client, SQL only.
use murmur_live_harness::sql::{converge, fixture, rows, status};
use murmur_live_harness::{env_seconds, tokio, Cluster, ClusterOptions};
use std::time::{Duration, Instant};
const WAIT: Duration = Duration::from_secs(120);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn long_running_five_node() {
    let mut cluster = Cluster::new(ClusterOptions {
        name: "five-node-sql".into(),
        num_nodes: 5,
        ..Default::default()
    })
    .await
    .unwrap();
    fixture(&cluster).await;
    let duration = env_seconds("MURMUR_FIVE_NODE_DURATION_SECONDS", 30);
    assert!(duration >= Duration::from_secs(3));
    let interval = Duration::from_millis(
        std::env::var("MURMUR_FIVE_NODE_INTERVAL_MS")
            .ok()
            .map(|v| v.parse().unwrap())
            .unwrap_or(50),
    );
    let started = Instant::now();
    let end = started + duration;
    let handles: Vec<_> = (0..5)
        .map(|i| std::sync::Arc::new(tokio::sync::RwLock::new(Some(cluster.node(i).clone()))))
        .collect();
    let mut tasks = Vec::new();
    for node in 0..5 {
        let slot = handles[node].clone();
        tasks.push(tokio::spawn(async move {
            let mut acked = Vec::new();
            let mut n = 0;
            while Instant::now() < end {
                tokio::time::sleep(interval).await;
                let guard = slot.read().await;
                if let Some(client) = guard.as_ref() {
                    let id = format!("n{node}-{n:09}");
                    let name = format!("capacity-{id}");
                    client
                        .execute("INSERT INTO records(id,name) VALUES($1,$2)", &[&id, &name])
                        .await
                        .unwrap();
                    client.simple("MURMUR FLUSH").await.unwrap();
                    acked.push(vec![id, name]);
                    n += 1;
                }
            }
            assert!(n >= 10, "node {node} made insufficient progress");
            acked
        }));
    }
    for (round, node) in [(1u32, 1usize), (2, 3)] {
        tokio::time::sleep_until(tokio::time::Instant::from_std(
            started + duration * round / 3,
        ))
        .await;
        let mut slot = handles[node].write().await;
        slot.take();
        let before = rows(cluster.node(node)).await;
        cluster.stop_node(node).await;
        cluster.start_node(node).await.unwrap();
        cluster.mesh().await.unwrap();
        let recovered = rows(cluster.node(node)).await;
        for row in before {
            assert!(recovered.contains(&row), "acknowledged row lost on restart");
        }
        *slot = Some(cluster.node(node).clone());
    }
    let mut expected = Vec::new();
    for task in tasks {
        expected.extend(task.await.unwrap());
    }
    assert!(expected.len() >= 100);
    expected.sort();
    converge(
        &cluster,
        &[0, 1, 2, 3, 4],
        &expected,
        env_seconds("MURMUR_FIVE_NODE_SETTLE_SECONDS", 120),
    )
    .await;
    for i in 0..5 {
        cluster.node(i).simple("MURMUR FLUSH").await.unwrap();
        let s = status(cluster.node(i)).await;
        assert_eq!(s["pending_bytes"].as_u64(), Some(0));
        assert!(s["spool"]["disk_bytes"].as_u64().unwrap() <= 2 << 30);
        assert!(s["storage_error"].is_null());
    }
    cluster
        .node(0)
        .execute(
            "INSERT INTO records(id,name) VALUES('post-soak','ready')",
            &[],
        )
        .await
        .unwrap();
    cluster.node(0).simple("MURMUR FLUSH").await.unwrap();
    expected.push(vec!["post-soak".into(), "ready".into()]);
    expected.sort();
    converge(&cluster, &[0, 1, 2, 3, 4], &expected, WAIT).await;
    cluster.finish(false).await;
}
