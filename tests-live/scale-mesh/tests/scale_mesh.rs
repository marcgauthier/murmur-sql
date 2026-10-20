//! Live Murmur-SQL scenario: real server children, Rust wire client, SQL only.
use murmur_live_harness::sql::{converge, fixture, peers};
use murmur_live_harness::{tokio, Cluster, ClusterOptions, ReplicationOptions};
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scale_mesh() {
    let cluster = Cluster::new(ClusterOptions {
        name: "scale-mesh-sql".into(),
        num_nodes: 10,
        replication: Some(ReplicationOptions {
            fanout: Some(4),
            ..Default::default()
        }),
        ..Default::default()
    })
    .await
    .unwrap();
    fixture(&cluster).await;
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let sampling_stop = stop.clone();
    let clients: Vec<_> = (0..10).map(|i| cluster.node(i).clone()).collect();
    let sampler = tokio::spawn(async move {
        let mut samples = 0;
        loop {
            for (i, client) in clients.iter().enumerate() {
                let selected = peers(client)
                    .await
                    .iter()
                    .filter(|p| p[2] == "true" && p[6] == "true")
                    .count();
                assert!(selected <= 4, "node {i}: push peers {selected}>fanout 4");
                samples += 1;
            }
            if sampling_stop.load(std::sync::atomic::Ordering::SeqCst) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        samples
    });
    let mut expected = Vec::new();
    for n in 0..400 {
        let node = if n < 300 { 0 } else { 7 };
        let id = format!("{n:06}");
        let name = format!("origin-{node}-{n}");
        cluster
            .node(node)
            .execute("INSERT INTO records(id,name) VALUES($1,$2)", &[&id, &name])
            .await
            .unwrap();
        expected.push(vec![id, name]);
    }
    for node in [0, 7] {
        cluster.node(node).simple("MURMUR FLUSH").await.unwrap();
    }
    converge(
        &cluster,
        &(0..10).collect::<Vec<_>>(),
        &expected,
        Duration::from_secs(300),
    )
    .await;
    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    assert!(sampler.await.unwrap() >= 10);
    cluster.finish(false).await;
}
