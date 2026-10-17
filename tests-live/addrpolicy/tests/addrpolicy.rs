//! Live SQL test: Replication address policy (allowed_networks CIDR) enforcement across processes.
//! Node 1 admits loopback peers (127.0.0.0/8); Node 2's TEST-NET policy (198.51.100.0/24) excludes
//! all actual loopback socket addresses; Node 3 is unrestricted.
//! The admitted pair (Node 1 and Node 3) replicates, while the denied Node 2 keeps its write local.

use murmur_live_harness::{assert_rows, Cluster, ClusterOptions};
use std::time::Duration;

#[tokio::test(flavor = "multi_thread")]
async fn address_policy_across_processes() {
    let mut cluster = Cluster::new(ClusterOptions {
        name: "addrpolicy".into(),
        num_nodes: 3,
        ..Default::default()
    })
    .await
    .expect("cluster setup");

    // Stop all nodes to configure address policies cleanly without stale QUIC sessions
    cluster.stop_node(0);
    cluster.stop_node(1);
    cluster.stop_node(2);

    // Node 0 admits loopback peers
    cluster
        .edit_config(0, |doc| {
            let mut arr = toml_edit::Array::new();
            arr.push("127.0.0.0/8");
            doc["replication"]["allowed_networks"] = toml_edit::value(arr);
        })
        .unwrap();

    // Node 1 admits only TEST-NET peers (denies all loopback addresses)
    cluster
        .edit_config(1, |doc| {
            let mut arr = toml_edit::Array::new();
            arr.push("198.51.100.0/24");
            doc["replication"]["allowed_networks"] = toml_edit::value(arr);
        })
        .unwrap();

    // Node 2 has unrestricted allowed_networks (default empty allows all)

    // Restart all nodes
    cluster.start_node(0).await.expect("restart node 0");
    cluster.start_node(1).await.expect("restart node 1");
    cluster.start_node(2).await.expect("restart node 2");

    // Initialize replicated table on all 3 nodes
    for i in 0..cluster.len() {
        let client = cluster.node(i);
        client
            .simple("CREATE TABLE IF NOT EXISTS records(id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
            .await
            .unwrap();
        client.set_table_replicated("records", true).await.unwrap();
        client.flush().await.unwrap();
    }

    // Insert rows: Node 0, Node 1, Node 2
    cluster
        .node(0)
        .execute(
            "INSERT INTO records(id, name) VALUES($1, $2)",
            &[&1i64, &"allowed-loopback"],
        )
        .await
        .unwrap();
    cluster.node(0).flush().await.unwrap();

    cluster
        .node(1)
        .execute(
            "INSERT INTO records(id, name) VALUES($1, $2)",
            &[&2i64, &"denied-testnet"],
        )
        .await
        .unwrap();
    cluster.node(1).flush().await.unwrap();

    cluster
        .node(2)
        .execute(
            "INSERT INTO records(id, name) VALUES($1, $2)",
            &[&3i64, &"unrestricted-loopback"],
        )
        .await
        .unwrap();
    cluster.node(2).flush().await.unwrap();

    // Admitted pair (Node 0 and Node 2) must converge on both rows
    let expected_admitted = vec![
        vec!["allowed-loopback".to_string()],
        vec!["unrestricted-loopback".to_string()],
    ];
    assert_rows(
        &cluster,
        &[0, 2],
        "SELECT name FROM records ORDER BY name",
        &expected_admitted,
    )
    .await;

    // Settle window: several dial/accept retries must not move the denied node's state
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Node 1 must retain ONLY its local write
    let n1_rows = cluster
        .node(1)
        .simple("SELECT name FROM records ORDER BY name")
        .await
        .unwrap();
    assert_eq!(
        n1_rows,
        vec![vec!["denied-testnet".to_string()]],
        "denied node state changed across network: want only its local row"
    );

    // Node 0 must retain exactly the 2 admitted rows
    let n0_rows = cluster
        .node(0)
        .simple("SELECT name FROM records ORDER BY name")
        .await
        .unwrap();
    assert_eq!(
        n0_rows, expected_admitted,
        "admitted node received unexpected extra rows"
    );

    // Verify peer topology via MURMUR SHOW PEERS
    let n1_peers = cluster.node(1).simple("MURMUR SHOW PEERS").await.unwrap();
    for row in &n1_peers {
        // row[2] is connected
        assert_ne!(
            row[2], "true",
            "denied Node 1 unexpectedly formed a connected session with {}",
            row[0]
        );
    }

    for i in 0..cluster.len() {
        assert!(
            cluster.node_info(i).dir.exists(),
            "node {i} directory does not exist"
        );
    }
}
