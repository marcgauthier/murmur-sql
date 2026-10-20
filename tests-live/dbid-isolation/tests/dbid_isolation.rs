//! Live SQL test: Nodes from two distinct databases (different database_id)
//! must never peer or replicate across databases, even when CAs are mutually trusted.

use murmur_live_harness::{assert_rows, Cluster, ClusterOptions};
use std::fs;
use std::time::{Duration, Instant};

#[tokio::test(flavor = "multi_thread")]
async fn cross_database_peers_never_replicate() {
    let mut cluster_a = Cluster::new(ClusterOptions {
        name: "dbid-iso-a".into(),
        num_nodes: 2,
        replication_toml: "membership = false".into(),
        ..Default::default()
    })
    .await
    .expect("start cluster A");

    let mut cluster_b = Cluster::new(ClusterOptions {
        name: "dbid-iso-b".into(),
        num_nodes: 2,
        replication_toml: "membership = false".into(),
        ..Default::default()
    })
    .await
    .expect("start cluster B");

    // Initialize tables on both clusters
    for (_name, cluster) in [("A", &cluster_a), ("B", &cluster_b)] {
        for i in 0..2 {
            let client = cluster.node(i);
            client
                .simple("CREATE TABLE IF NOT EXISTS records(id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
                .await
                .unwrap();
            client.simple("MURMUR SET TABLE records REPLICATED true").await.unwrap();
            client.simple("MURMUR FLUSH").await.unwrap();
        }
    }

    // Stop nodes on both clusters to cross-trust CAs and configure cross-peering
    for i in 0..2 {
        cluster_a.stop_node(i);
        cluster_b.stop_node(i);
    }

    let ca_a = fs::read_to_string(cluster_a.node_info(0).dir.join("ca.pem")).unwrap();
    let ca_b = fs::read_to_string(cluster_b.node_info(0).dir.join("ca.pem")).unwrap();

    // Cross-trust CAs by appending peer CA to each node's ca.pem
    for i in 0..2 {
        let p_a = cluster_a.node_info(i).dir.join("ca.pem");
        fs::write(&p_a, format!("{ca_a}\n{ca_b}")).unwrap();
        let p_b = cluster_b.node_info(i).dir.join("ca.pem");
        fs::write(&p_b, format!("{ca_b}\n{ca_a}")).unwrap();
    }

    // Cross-peer: Node A[i] peers with other A nodes + all B nodes.
    // Node B[i] peers with other B nodes + all A nodes.
    let repl_b_addrs: Vec<String> = (0..2).map(|j| cluster_b.node_info(j).repl_addr.to_string()).collect();
    let repl_a_addrs: Vec<String> = (0..2).map(|j| cluster_a.node_info(j).repl_addr.to_string()).collect();

    for i in 0..2 {
        cluster_a.edit_config(i, |doc| {
            let mut arr = toml_edit::Array::new();
            for j in 0..2 {
                if j != i {
                    arr.push(cluster_a.node_info(j).repl_addr.to_string());
                }
            }
            for addr in &repl_b_addrs {
                arr.push(addr.clone());
            }
            doc["replication"]["bootstrap"] = toml_edit::value(arr);
        }).unwrap();

        cluster_b.edit_config(i, |doc| {
            let mut arr = toml_edit::Array::new();
            for j in 0..2 {
                if j != i {
                    arr.push(cluster_b.node_info(j).repl_addr.to_string());
                }
            }
            for addr in &repl_a_addrs {
                arr.push(addr.clone());
            }
            doc["replication"]["bootstrap"] = toml_edit::value(arr);
        }).unwrap();
    }

    // Restart all nodes
    for i in 0..2 {
        cluster_a.start_node(i).await.unwrap();
        cluster_b.start_node(i).await.unwrap();
    }

    // Positive controls: write marker inside cluster A, write marker inside cluster B
    cluster_a
        .node(0)
        .execute("INSERT INTO records(id, name) VALUES($1, $2)", &[&1i64, &"cluster-a-marker"])
        .await
        .unwrap();
    cluster_a.node(0).simple("MURMUR FLUSH").await.unwrap();

    cluster_b
        .node(0)
        .execute("INSERT INTO records(id, name) VALUES($1, $2)", &[&2i64, &"cluster-b-marker"])
        .await
        .unwrap();
    cluster_b.node(0).simple("MURMUR FLUSH").await.unwrap();

    // Verify cluster A converges internally on marker A
    let expected_a = vec![vec!["cluster-a-marker".into()]];
    assert_rows(&cluster_a, &[0, 1], "SELECT name FROM records ORDER BY id", &expected_a).await;

    // Verify cluster B converges internally on marker B
    let expected_b = vec![vec!["cluster-b-marker".into()]];
    assert_rows(&cluster_b, &[0, 1], "SELECT name FROM records ORDER BY id", &expected_b).await;

    // Observation window: observe that cross-cluster replication never happens
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        let rows_a0 = cluster_a.node(0).simple("SELECT name FROM records ORDER BY id").await.unwrap();
        let rows_a1 = cluster_a.node(1).simple("SELECT name FROM records ORDER BY id").await.unwrap();
        let rows_b0 = cluster_b.node(0).simple("SELECT name FROM records ORDER BY id").await.unwrap();
        let rows_b1 = cluster_b.node(1).simple("SELECT name FROM records ORDER BY id").await.unwrap();

        assert_eq!(rows_a0, expected_a, "Cluster A node 0 received cross-database data!");
        assert_eq!(rows_a1, expected_a, "Cluster A node 1 received cross-database data!");
        assert_eq!(rows_b0, expected_b, "Cluster B node 0 received cross-database data!");
        assert_eq!(rows_b1, expected_b, "Cluster B node 1 received cross-database data!");

        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
