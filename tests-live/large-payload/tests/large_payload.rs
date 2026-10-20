//! Exercise extended-protocol parameters and replication across many Spool blocks.
use murmur_live_harness::{Cluster, ClusterOptions};
use std::time::{Duration, Instant};

#[tokio::test(flavor = "multi_thread")]
async fn large_payload() {
    let cluster = Cluster::new(ClusterOptions {
        name: "large-payload".into(),
        num_nodes: 2,
        ..Default::default()
    })
    .await
    .unwrap();
    for i in 0..2 {
        cluster.node(i).simple("CREATE TABLE IF NOT EXISTS records(id TEXT PRIMARY KEY NOT NULL, phone TEXT NOT NULL)").await.unwrap();
        cluster
            .node(i)
            .simple("MURMUR SET TABLE records REPLICATED true")
            .await
            .unwrap();
    }
    for i in 0..250 {
        let id = format!("bulk-{i:04}");
        let value = format!("phone-{i}");
        cluster
            .node(0)
            .execute(
                "INSERT INTO records(id,phone) VALUES($1,$2)",
                &[&id, &value],
            )
            .await
            .unwrap();
    }
    let size = 1536 * 1024;
    let mut payload = "murmur-large-payload-".repeat(size / 21 + 1);
    payload.truncate(size);
    assert_eq!(payload.len(), size);
    cluster
        .node(0)
        .execute(
            "INSERT INTO records(id,phone) VALUES($1,$2)",
            &[&"large-payload-row", &payload],
        )
        .await
        .unwrap();
    cluster.node(0).simple("MURMUR FLUSH").await.unwrap();
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let count = cluster
            .node(1)
            .simple("SELECT count(*) FROM records")
            .await
            .unwrap();
        if count == vec![vec!["251".to_string()]] {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "large payload did not replicate; count={count:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let rows = cluster
        .node(1)
        .query(
            "SELECT phone FROM records WHERE id=$1",
            &[&"large-payload-row"],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>(0), payload);
    assert_eq!(
        cluster
            .node(1)
            .simple("SELECT id,phone FROM records ORDER BY id")
            .await
            .unwrap(),
        cluster
            .node(0)
            .simple("SELECT id,phone FROM records ORDER BY id")
            .await
            .unwrap()
    );
}
