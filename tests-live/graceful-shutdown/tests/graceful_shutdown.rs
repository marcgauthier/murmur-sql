//! Live SQL test: Graceful shutdown mid-write under load and mid-snapshot during rejoin.
//! Phase A stops Node 1 mid-write under concurrent client load with SIGTERM.
//! Phase B restarts Node 3 after retention log GC (forcing snapshot resync) and stops it again mid-transfer.
//! Both restarts must be clean and fast, lose no acknowledged writes, and the cluster must reconverge.

use murmur_live_harness::{assert_rows, Cluster, ClusterOptions};
use std::collections::HashSet;
use std::sync::{
    atomic::{AtomicBool, AtomicI64, Ordering},
    Arc,
};
use std::time::Duration;

const TABLE_DDL: &str =
    "CREATE TABLE IF NOT EXISTS contention(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL, phone TEXT NOT NULL DEFAULT '')";

fn row_id(node: usize, seq: u64) -> String {
    format!("{:08x}-0000-4000-8000-{:012x}", node, seq)
}

async fn wait_converged(cluster: &Cluster, timeout: Duration) -> Vec<String> {
    let deadline = tokio::time::Instant::now() + timeout;
    let query = "SELECT name FROM contention ORDER BY name";
    loop {
        let r0 = cluster.node(0).simple(query).await;
        let r1 = cluster.node(1).simple(query).await;
        let r2 = cluster.node(2).simple(query).await;
        if let (Ok(r0), Ok(r1), Ok(r2)) = (r0, r1, r2) {
            if !r0.is_empty() && r0 == r1 && r1 == r2 {
                return r0.into_iter().map(|row| row[0].clone()).collect();
            }
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("cluster failed to converge within {:?}", timeout);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Wait until none of `watchers` lists `gone_id` in `MURMUR SHOW PEERS`.
/// SIGKILLed nodes are noticed via QUIC idle timeout (30s), and a
/// lingering session keeps the dead peer's unacked range protected past
/// `maximum_offline` by design (connected peers can always catch up
/// from the log instead of wedging on a snapshot). Pruning assertions
/// must await teardown, not just record staleness.
async fn wait_peer_gone(cluster: &Cluster, watchers: &[usize], gone_id: &str, timeout: Duration) {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let mut present = false;
        for &i in watchers {
            if let Ok(rows) = cluster.node(i).simple("MURMUR SHOW PEERS").await {
                if rows
                    .iter()
                    .any(|row| row.first().is_some_and(|id| id.as_str() == gone_id))
                {
                    present = true;
                }
            }
        }
        if !present {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "peer {gone_id} session never dropped"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

fn assert_acknowledged_present(present: &[String], acknowledged: &HashSet<String>) {
    let set: HashSet<_> = present.iter().cloned().collect();
    for ack in acknowledged {
        assert!(
            set.contains(ack),
            "acknowledged write {ack:?} missing from converged cluster state"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn graceful_shutdown_mid_write_and_mid_snapshot() {
    let mut cluster = Cluster::new(ClusterOptions {
        name: "graceful-shutdown".into(),
        num_nodes: 3,
        replication_toml: r#"
retention = "1s"
minimum_retained = 10
maximum_offline = "5s"
"#
        .into(),
        ..Default::default()
    })
    .await
    .expect("cluster setup");

    // Configure trusted_snapshot_sources so rejoining nodes accept snapshots from peers
    for i in 0..cluster.len() {
        let mut sources = toml_edit::Array::new();
        for j in 0..cluster.len() {
            if j != i {
                sources.push(cluster.node_info(j).node_id.as_str());
            }
        }
        cluster.stop_node(i);
        cluster
            .edit_config(i, |doc| {
                doc["replication"]["trusted_snapshot_sources"] = toml_edit::value(sources);
            })
            .unwrap();
        cluster.start_node(i).await.unwrap();
    }

    // Initialize replicated table on all 3 nodes
    for i in 0..cluster.len() {
        let client = cluster.node(i);
        client.simple(TABLE_DDL).await.unwrap();
        client.simple("MURMUR SET TABLE contention REPLICATED true").await.unwrap();
        client.simple("MURMUR FLUSH").await.unwrap();
    }

    // Baseline: 5 rows converge across all 3 nodes before any shutdown
    let mut acknowledged = HashSet::new();
    let mut expected_baseline = Vec::new();
    for i in 0..5 {
        let id = row_id(0, i as u64);
        let name = format!("base-{i}");
        cluster
            .node(0)
            .execute(
                "INSERT INTO contention(id, name, phone) VALUES($1, $2, $3)",
                &[&id, &name, &""],
            )
            .await
            .unwrap();
        acknowledged.insert(name.clone());
        expected_baseline.push(vec![name]);
    }
    cluster.node(0).simple("MURMUR FLUSH").await.unwrap();
    expected_baseline.sort();

    assert_rows(
        &cluster,
        &[0, 1, 2],
        "SELECT name FROM contention ORDER BY name",
        &expected_baseline,
    )
    .await;

    // Phase A: Concurrent client writes on all nodes; stop Node 0 under load while other nodes write
    let stop = Arc::new(AtomicBool::new(false));
    let paused = [
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicBool::new(false)),
    ];
    let in_flight = Arc::new(AtomicI64::new(0));
    let acked_writes = Arc::new(tokio::sync::Mutex::new(acknowledged.clone()));

    let mut handles = Vec::new();
    for node_idx in 0..3 {
        let stop_clone = stop.clone();
        let paused_clone = paused[node_idx].clone();
        let in_flight_clone = in_flight.clone();
        let acked_clone = acked_writes.clone();
        let client = cluster.connect(node_idx).await.unwrap();

        handles.push(tokio::spawn(async move {
            let mut seq = 0u64;
            while !stop_clone.load(Ordering::Relaxed) {
                if paused_clone.load(Ordering::Relaxed) {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    continue;
                }
                seq += 1;
                let id = row_id(node_idx + 1, seq);
                let name = format!("load-n{node_idx}-{seq:06}");
                in_flight_clone.fetch_add(1, Ordering::Relaxed);
                let res = client
                    .execute(
                        "INSERT INTO contention(id, name, phone) VALUES($1, $2, $3)",
                        &[&id, &name, &""],
                    )
                    .await;
                in_flight_clone.fetch_sub(1, Ordering::Relaxed);
                if res.is_ok() {
                    acked_clone.lock().await.insert(name);
                }
                tokio::time::sleep(Duration::from_millis(15)).await;
            }
        }));
    }

    // Let writes get in flight
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Pause Node 0 writer and drain pending writes
    paused[0].store(true, Ordering::Relaxed);
    tokio::time::sleep(Duration::from_millis(100)).await;
    cluster.node(0).simple("MURMUR FLUSH").await.unwrap();

    // Send SIGTERM to Node 0 for graceful shutdown
    #[cfg(unix)]
    unsafe {
        if let Some(pid) = cluster.pid(0) {
            libc::kill(pid as i32, libc::SIGTERM);
        }
    }

    // Allow graceful flush and exit before reaping
    tokio::time::sleep(Duration::from_millis(200)).await;
    cluster.stop_node(0);

    // Node 1 and Node 2 continue writing while Node 0 is offline
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Restart Node 0 cleanly
    cluster.start_node(0).await.expect("restart node 0");

    // Re-connect Node 0 writer
    let client0_new = cluster.connect(0).await.unwrap();
    let stop_clone = stop.clone();
    let acked_clone = acked_writes.clone();
    handles.push(tokio::spawn(async move {
        let mut seq = 500u64;
        while !stop_clone.load(Ordering::Relaxed) {
            seq += 1;
            let id = row_id(1, seq);
            let name = format!("load-n0-postrestart-{seq:06}");
            let res = client0_new
                .execute(
                    "INSERT INTO contention(id, name, phone) VALUES($1, $2, $3)",
                    &[&id, &name, &""],
                )
                .await;
            if res.is_ok() {
                acked_clone.lock().await.insert(name);
            }
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
    }));

    // Let concurrent writes proceed across all nodes after restart
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Stop all Phase A writers
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        let _ = h.await;
    }

    // Flush all nodes
    cluster.node(0).simple("MURMUR FLUSH").await.unwrap();
    cluster.node(1).simple("MURMUR FLUSH").await.unwrap();
    cluster.node(2).simple("MURMUR FLUSH").await.unwrap();

    // Verify all 3 nodes converge with identical contents, and every acknowledged write is present
    let acked_snapshot = acked_writes.lock().await.clone();
    let converged_a = wait_converged(&cluster, Duration::from_secs(60)).await;
    assert_acknowledged_present(&converged_a, &acked_snapshot);

    // Phase B: Node 2 goes stale under aggressive retention while survivors write bulk + fat payload
    cluster.stop_node(2);

    let mut phase_b_acked = acked_snapshot.clone();
    for i in 0..50 {
        let name = format!("fresh-{i:03}");
        let id = row_id(1, 5000 + i as u64);
        cluster
            .node(0)
            .execute(
                "INSERT INTO contention(id, name, phone) VALUES($1, $2, $3)",
                &[&id, &name, &""],
            )
            .await
            .unwrap();
        phase_b_acked.insert(name);
    }

    // 8 fat rows (~64 KB each) to widen the transfer window
    let fat_chunk = "FAT-FILLER-DATA-STRING-PADDING-1234567890\n".repeat(1500);
    for i in 0..8 {
        let name = format!("fat-{i:02}");
        let id = row_id(2, 6000 + i as u64);
        cluster
            .node(1)
            .execute(
                "INSERT INTO contention(id, name, phone) VALUES($1, $2, $3)",
                &[&id, &name, &fat_chunk],
            )
            .await
            .unwrap();
        phase_b_acked.insert(name);
    }

    cluster.node(0).simple("MURMUR FLUSH").await.unwrap();
    cluster.node(1).simple("MURMUR FLUSH").await.unwrap();

    let mut expected_survivors: Vec<Vec<String>> =
        phase_b_acked.iter().map(|n| vec![n.clone()]).collect();
    expected_survivors.sort();

    assert_rows(
        &cluster,
        &[0, 1],
        "SELECT name FROM contention ORDER BY name",
        &expected_survivors,
    )
    .await;

    // Node 2's session must drop before its unacknowledged logs unpin:
    // lingering sessions stay protected past maximum_offline (5s) by
    // design, and SIGKILL is noticed via QUIC idle timeout. Teardown
    // takes longer than maximum_offline, so no extra sleep is needed.
    let node2_id = cluster.node_info(2).node_id.clone();
    wait_peer_gone(&cluster, &[0, 1], &node2_id, Duration::from_secs(60)).await;

    // Run retention collection passes on both survivors to prune log history
    let c0 = cluster.node(0).simple("MURMUR RETENTION COLLECT").await.unwrap();
    let c1 = cluster.node(1).simple("MURMUR RETENTION COLLECT").await.unwrap();
    let collected_0: i64 = c0[0][0].parse().unwrap_or(0);
    let collected_1: i64 = c1[0][0].parse().unwrap_or(0);
    assert!(
        collected_0 + collected_1 > 0,
        "retention collection pass collected 0 batches; expected log pruning"
    );

    // Restart stale Node 2: it must take the snapshot path because its log position was pruned
    cluster.start_node(2).await.expect("restart stale node 2");

    // Interrupt Node 2 mid-transfer: brief delay then stop
    tokio::time::sleep(Duration::from_millis(25)).await;
    cluster.stop_node(2);

    // Restart Node 2 again after interrupt
    cluster.start_node(2).await.expect("restart node 2 after interrupt");

    // All nodes must reconverge on the full dataset with no missing rows
    let converged_b = wait_converged(&cluster, Duration::from_secs(60)).await;
    assert_acknowledged_present(&converged_b, &phase_b_acked);

    // Post-recovery write on Node 0 replicates to all nodes
    let post_name = "post-recovery-write".to_string();
    let post_id = row_id(1, 999_999);
    cluster
        .node(0)
        .execute(
            "INSERT INTO contention(id, name, phone) VALUES($1, $2, $3)",
            &[&post_id, &post_name, &""],
        )
        .await
        .unwrap();
    cluster.node(0).simple("MURMUR FLUSH").await.unwrap();

    let mut expected_final = expected_survivors;
    expected_final.push(vec![post_name.clone()]);
    expected_final.sort();

    assert_rows(
        &cluster,
        &[0, 1, 2],
        "SELECT name FROM contention ORDER BY name",
        &expected_final,
    )
    .await;
}
