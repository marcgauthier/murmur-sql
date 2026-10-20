//! Live SQL test: Contended same-key writes across nodes must resolve deterministically
//! to identical LWW winners on every node in the cluster.

use murmur_live_harness::{assert_rows, Cluster, ClusterOptions};
use std::time::{Duration, Instant};

const ROWS: usize = 20;

#[tokio::test(flavor = "multi_thread")]
async fn skewed_clocks_resolve_identical_winners() {
    let cluster = Cluster::new(ClusterOptions {
        name: "clock-skew".into(),
        num_nodes: 3,
        ..Default::default()
    })
    .await
    .expect("cluster setup");

    for i in 0..cluster.len() {
        let client = cluster.node(i);
        client
            .simple("CREATE TABLE IF NOT EXISTS skew(id TEXT NOT NULL PRIMARY KEY, val TEXT NOT NULL)")
            .await
            .unwrap();
        client.simple("MURMUR SET TABLE skew REPLICATED true").await.unwrap();
        client.simple("MURMUR FLUSH").await.unwrap();
    }

    // Phase 1: Concurrent same-key writes on node 0 and node 1
    let write_node0 = async {
        let client = cluster.node(0);
        for i in 0..ROWS {
            let id = format!("skew-{i:02}");
            let val = format!("val-node0-{i:02}");
            client
                .execute("INSERT OR REPLACE INTO skew(id, val) VALUES($1, $2)", &[&id, &val])
                .await
                .unwrap();
        }
        client.simple("MURMUR FLUSH").await.unwrap();
    };

    let write_node1 = async {
        let client = cluster.node(1);
        for i in 0..ROWS {
            let id = format!("skew-{i:02}");
            let val = format!("val-node1-{i:02}");
            client
                .execute("INSERT OR REPLACE INTO skew(id, val) VALUES($1, $2)", &[&id, &val])
                .await
                .unwrap();
        }
        client.simple("MURMUR FLUSH").await.unwrap();
    };

    tokio::join!(write_node0, write_node1);

    // Wait for all nodes in the cluster to converge to identical state
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let r0 = cluster.node(0).simple("SELECT id, val FROM skew ORDER BY id").await.unwrap();
        let r1 = cluster.node(1).simple("SELECT id, val FROM skew ORDER BY id").await.unwrap();
        let r2 = cluster.node(2).simple("SELECT id, val FROM skew ORDER BY id").await.unwrap();

        if r0.len() == ROWS && r0 == r1 && r1 == r2 {
            break;
        }
        assert!(Instant::now() < deadline, "Cluster failed to converge to identical winners on 20 rows");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Phase 2: Sequential updates simulating higher HLC clock (fast clock wins)
    // Node 2 updates all 20 rows
    let client2 = cluster.node(2);
    let mut expected_updated = Vec::new();
    for i in 0..ROWS {
        let id = format!("skew-{i:02}");
        let val = format!("val-node2-winner-{i:02}");
        client2
            .execute("UPDATE skew SET val = $1 WHERE id = $2", &[&val, &id])
            .await
            .unwrap();
        expected_updated.push(vec![id, val]);
    }
    client2.simple("MURMUR FLUSH").await.unwrap();

    // Verify all 3 nodes converge to node 2's later writes
    assert_rows(
        &cluster,
        &[0, 1, 2],
        "SELECT id, val FROM skew ORDER BY id",
        &expected_updated,
    )
    .await;
}
