//! Live SQL test: Node identity allow-list enforcement.
//! Node 2's allow-list permits only Node 1, while Node 1 and Node 3 accept any valid identity.
//! Node 3 communicates with Node 2 strictly indirectly through Node 1; no direct Node 2 <-> Node 3
//! session is ever established, yet all writes converge completely across all 3 nodes.

use murmur_live_harness::{assert_rows, Cluster, ClusterOptions};

#[tokio::test(flavor = "multi_thread")]
async fn allowed_node_ids_across_processes() {
    let mut cluster = Cluster::new(ClusterOptions {
        name: "allow-nodes".into(),
        num_nodes: 3,
        ..Default::default()
    })
    .await
    .expect("cluster setup");

    // Configure Node 2 (index 1) to allow only Node 1 (index 0)
    let node0_id = cluster.node_info(0).node_id.clone();
    cluster.stop_node(1);
    cluster
        .edit_config(1, |doc| {
            let mut arr = toml_edit::Array::new();
            arr.push(node0_id.as_str());
            doc["replication"]["allowed_peers"] = toml_edit::value(arr);
        })
        .unwrap();
    cluster.start_node(1).await.expect("restart node 1");

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

    // Concurrent writers on all three nodes
    let num_writes = 15;
    let write = |node_idx: usize| {
        let client = cluster.node(node_idx);
        async move {
            for seq in 0..num_writes {
                let id = ((node_idx as i64 + 1) * 10_000) + seq;
                let name = format!("node{node_idx}-write-{seq:02}");
                client
                    .execute("INSERT INTO records(id, name) VALUES($1, $2)", &[&id, &name])
                    .await
                    .unwrap();
            }
            client.flush().await.unwrap();
        }
    };

    tokio::join!(write(0), write(1), write(2));

    // Construct expected rows (15 per node = 45 total)
    let mut expected = Vec::new();
    for node_idx in 0..3 {
        for seq in 0..num_writes {
            expected.push(vec![format!("node{node_idx}-write-{seq:02}")]);
        }
    }
    expected.sort();

    // Verify all 3 nodes converge on all 45 rows
    assert_rows(
        &cluster,
        &[0, 1, 2],
        "SELECT name FROM records ORDER BY name",
        &expected,
    )
    .await;

    // Verify peer topology via MURMUR SHOW PEERS:
    // Node 1 (index 1) must have only Node 0, NEVER Node 2
    let node2_id = cluster.node_info(2).node_id.clone();
    let n1_peers = cluster.node(1).simple("MURMUR SHOW PEERS").await.unwrap();
    for row in &n1_peers {
        assert_ne!(
            row[0], node2_id,
            "Node 1 directly connected to unauthorized Node 2!"
        );
    }

    // Node 2 (index 2) must have only Node 0, NEVER Node 1
    let node1_id = cluster.node_info(1).node_id.clone();
    let n2_peers = cluster.node(2).simple("MURMUR SHOW PEERS").await.unwrap();
    for row in &n2_peers {
        assert_ne!(
            row[0], node1_id,
            "Node 2 directly connected to unauthorized Node 1!"
        );
    }
}
