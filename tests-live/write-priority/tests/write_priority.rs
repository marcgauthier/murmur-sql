//! Live Murmur-SQL scenario: real server children, Rust wire client, SQL only.
use murmur_live_harness::sql::{converge, fixture, status};
use murmur_live_harness::{env_seconds, tokio, Cluster, ClusterOptions};
use std::time::{Duration, Instant};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_write_priority() {
    let cluster = Cluster::new(ClusterOptions {
        name: "write-priority-sql".into(),
        num_nodes: 2,
        ..Default::default()
    })
    .await
    .unwrap();
    fixture(&cluster).await;
    let mut before = [0u64; 2];
    for i in 0..2 {
        let s = status(cluster.node(i)).await;
        for (index, class) in ["local", "remote"].iter().enumerate() {
            before[index] += s["scheduler"][class]["service_nanos"]
                .as_u64()
                .unwrap();
        }
    }
    let mut workers = Vec::new();
    for node in 0..2 {
        for worker in 0..64 {
            workers.push((node, worker, cluster.connect(node).await.unwrap()));
        }
    }
    let deadline = Instant::now() + env_seconds("MURMUR_LIVE_WRITE_PRIORITY_SECONDS", 12);
    let mut tasks = Vec::new();
    for (node, worker, client) in workers {
        tasks.push(tokio::spawn(async move {
            // Atomic 25-row statements make engine contention measurable
            // over the wire. Reuse owned rows so state stays bounded.
            let ids: Vec<String> = (0..25)
                .map(|n| format!("n{node}-w{worker:03}-{n:03}"))
                .collect();
            let mut revision = 0;
            let mut final_name = String::new();
            while Instant::now() < deadline {
                final_name = format!("value-n{node}-w{worker}-revision-{revision}");
                let affected = if revision == 0 {
                    let values = ids
                        .iter()
                        .map(|id| format!("('{id}','{final_name}')"))
                        .collect::<Vec<_>>()
                        .join(",");
                    client
                        .execute(
                            &format!("INSERT INTO records(id,name) VALUES {values}"),
                            &[],
                        )
                        .await
                        .unwrap()
                } else {
                    client
                        .execute(
                            "UPDATE records SET name=$1 WHERE id BETWEEN $2 AND $3",
                            &[&final_name, &ids[0], &ids[24]],
                        )
                        .await
                        .unwrap()
                };
                assert_eq!(affected, 25);
                revision += 1;
                if revision % 10 == 0 {
                    client.simple("MURMUR FLUSH").await.unwrap();
                }
            }
            client.simple("MURMUR FLUSH").await.unwrap();
            if revision == 0 {
                Vec::new()
            } else {
                ids.into_iter()
                    .map(|id| vec![id, final_name.clone()])
                    .collect::<Vec<_>>()
            }
        }));
    }
    let mut reads = 0;
    while Instant::now() < deadline {
        for i in 0..2 {
            let result = cluster
                .node(i)
                .query("SELECT COUNT(*) FROM records", &[])
                .await
                .unwrap();
            assert_eq!(result.len(), 1);
            reads += 1;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let mut expected = Vec::new();
    for task in tasks {
        expected.extend(task.await.unwrap());
    }
    assert!(
        expected.len() >= 100 && reads >= 10,
        "insufficient SQL progress"
    );
    let mut after = [0u64; 2];
    for i in 0..2 {
        let s = status(cluster.node(i)).await;
        for (index, class) in ["local", "remote"].iter().enumerate() {
            after[index] += s["scheduler"][class]["service_nanos"]
                .as_u64()
                .unwrap();
        }
    }
    let local = after[0].saturating_sub(before[0]);
    let remote = after[1].saturating_sub(before[1]);
    let total = local + remote;
    // Policy outcome is measured in service time, not contested-grant
    // counts: replication applies each burst under a few long-held
    // tickets, so its total work stays below its share, it wins every
    // contested grant while behind, and local dual grants stay near
    // zero even under real overlap. Total shares are the honest signal.
    eprintln!(
        "dual SQL service local={local}ns remote={remote}ns total={total}ns share={:.3}",
        remote as f64 / total.max(1) as f64
    );
    assert!(
        total >= 1_000_000_000,
        "need >=1s writer service to measure fairness, got {total}ns"
    );
    assert!(
        remote > 0,
        "replication performed no apply work; contention was never exercised"
    );
    assert!(
        remote as f64 / total as f64 <= 0.25,
        "remote service exceeded 25% under dual contention: local={local}ns remote={remote}ns"
    );
    expected.sort();
    converge(
        &cluster,
        &[0, 1],
        &expected,
        env_seconds("MURMUR_LIVE_WRITE_PRIORITY_SYNC_TIMEOUT_SECONDS", 120),
    )
    .await;
    cluster.finish(false).await;
}
