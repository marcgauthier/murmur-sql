//! Live Murmur-SQL scenario: real server children, Rust wire client, SQL only.
use murmur_live_harness::sql::{converge, fixture, peer_counts, status};
use murmur_live_harness::{env_seconds, tokio, Cluster, ClusterOptions};
use std::time::{Duration, Instant};
const WAIT: Duration = Duration::from_secs(120);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn soak_slo() {
    let cluster = Cluster::new(ClusterOptions {
        name: "soak-slo-sql".into(),
        num_nodes: 3,
        ..Default::default()
    })
    .await
    .unwrap();
    fixture(&cluster).await;
    peer_counts(&cluster, &[0, 1, 2], &[2, 2, 2], WAIT).await;
    let end = Instant::now() + env_seconds("MURMUR_SLO_DURATION_SECONDS", 5);
    let mut tasks = Vec::new();
    for node in 0..3 {
        let client = cluster.node(node).clone();
        tasks.push(tokio::spawn(async move {
            let mut expected = std::collections::BTreeMap::new();
            let mut lat = Vec::new();
            let mut n = 0;
            while Instant::now() < end {
                let id = format!("n{node}-{:04}", n % 1000);
                let name = format!("revision-{n}");
                let start = Instant::now();
                if n < 1000 {
                    client
                        .execute("INSERT INTO records(id,name) VALUES($1,$2)", &[&id, &name])
                        .await
                        .unwrap();
                } else {
                    client
                        .execute("UPDATE records SET name=$1 WHERE id=$2", &[&name, &id])
                        .await
                        .unwrap();
                }
                lat.push(start.elapsed());
                expected.insert(id, name);
                n += 1;
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            client.simple("MURMUR FLUSH").await.unwrap();
            (expected, lat, n)
        }));
    }
    let mut expected = Vec::new();
    let mut latency = Vec::new();
    let mut operations = 0;
    for task in tasks {
        let (map, lat, n) = task.await.unwrap();
        expected.extend(map.into_iter().map(|(id, name)| vec![id, name]));
        latency.extend(lat);
        operations += n;
    }
    assert!(operations >= 60);
    expected.sort();
    converge(
        &cluster,
        &[0, 1, 2],
        &expected,
        env_seconds("MURMUR_SLO_SETTLE_SECONDS", 30),
    )
    .await;
    latency.sort();
    assert!(latency[latency.len() * 95 / 100] <= Duration::from_secs(5));
    assert!(*latency.last().unwrap() <= Duration::from_secs(20));
    for i in 0..3 {
        cluster.node(i).simple("MURMUR FLUSH").await.unwrap();
        let s = status(cluster.node(i)).await;
        assert!(s["replication"]["staged_transactions"].as_u64().unwrap() <= 5000);
        assert!(s["spool"]["disk_bytes"].as_u64().unwrap() <= 512 << 20);
        assert!(s["storage_error"].is_null());
        assert_eq!(s["pending_bytes"].as_u64(), Some(0));
    }
    cluster.finish(false).await;
}
