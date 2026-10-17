#[path = "../support/mod.rs"]
mod support;
use murmur_client::Client;

#[tokio::main]
async fn main() {
    let dir0 = tempfile::tempdir().unwrap();
    let dir1 = tempfile::tempdir().unwrap();
    let mut opts = support::replication_mesh_options(&[dir0.path(), dir1.path()], "");
    let o1 = opts.pop().unwrap();
    let o0 = opts.pop().unwrap();
    let (node0, node1) = (o0.node_id.clone(), o1.node_id.clone());
    let s0 = support::start(dir0, o0);
    let s1 = support::start(dir1, o1);
    let db0 = Client::connect(&support::config(&s0, "mesh-key"))
        .await
        .unwrap();
    let db1 = Client::connect(&support::config(&s1, "mesh-key"))
        .await
        .unwrap();
    let st0 = db0.show_replication().await.unwrap();
    assert!(st0.running && st0.peers == 0);
    let addr1 = db1
        .show_replication()
        .await
        .unwrap()
        .address
        .expect("node1 repl addr");
    assert!(db0.show_peers().await.unwrap().is_empty());

    db0.peer_add(&addr1).await.unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    let peer = loop {
        let peers = db0.show_peers().await.unwrap();
        if let Some(p) = peers.iter().find(|p| p.node_id == node1 && p.connected) {
            break p.clone();
        }
        assert!(
            std::time::Instant::now() < deadline,
            "peer never connected: {peers:?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    };
    assert!(peer.sent_watermark >= 0);
    db0.peer_sync().await.unwrap();
    assert!(db0.show_replication().await.unwrap().peers >= 1);

    let collected = db0.retention_collect().await.unwrap();
    assert!(collected >= 0);
    for row in db0.show_retention().await.unwrap() {
        assert_eq!(row.node_id.len(), 32);
        assert!(row.collected_transactions >= 0);
    }

    db0.peer_remove(&node1).await.unwrap();
    db0.peer_retire(&node1).await.unwrap();
    let retention = db0.show_retention().await.unwrap();
    assert!(
        retention.iter().any(|r| r.node_id == node1 && r.retired),
        "retired peer missing from retention: {retention:?}"
    );
    // Retiring the local node is rejected.
    assert_eq!(
        db0.peer_retire(&node0).await.unwrap_err().code(),
        Some("22023")
    );
}
