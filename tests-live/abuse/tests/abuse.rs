//! Real SQL writers cross healthy, partition, outage and whole-mesh crash phases.
use murmur_client::Client;
use murmur_live_harness::{assert_rows, toml_edit, Cluster, ClusterOptions};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, RwLock};
type Slots = Arc<Vec<RwLock<Option<Arc<Client>>>>>;
fn number(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default)
}
async fn mesh(cluster: &Cluster) {
    for from in 0..cluster.len() {
        for to in 0..cluster.len() {
            if from != to {
                cluster
                    .node(from)
                    .simple(&format!(
                        "MURMUR PEER ADD '{}'",
                        cluster.node_info(to).repl_addr
                    ))
                    .await
                    .unwrap();
            }
        }
    }
}
async fn stop(cluster: &mut Cluster, slots: &Slots, nodes: &[usize]) {
    for &i in nodes {
        // A writer holds this read lock through SQL+FLUSH. Taking the write
        // lock drains acknowledged writes before killing that server only.
        let mut slot = slots[i].write().await;
        slot.take();
        cluster.node(i).flush().await.unwrap();
        cluster.stop_node(i);
    }
}
async fn restart(cluster: &mut Cluster, slots: &Slots, nodes: &[usize]) {
    for &i in nodes {
        cluster.start_node(i).await.unwrap();
        *slots[i].write().await = Some(Arc::new(cluster.connect(i).await.unwrap()));
    }
    mesh(cluster).await;
}
async fn phase(name: &str, duration: Duration, status: Duration) {
    eprintln!("abuse phase={name} duration={duration:?}");
    let end = Instant::now() + duration;
    while Instant::now() < end {
        tokio::time::sleep(status.min(end.saturating_duration_since(Instant::now()))).await;
        eprintln!(
            "abuse phase={name} remaining={:?}",
            end.saturating_duration_since(Instant::now())
        );
    }
}
#[tokio::test(flavor = "multi_thread")]
async fn abuse_mesh_preserves_acknowledged_sql_writes_through_all_four_phases() {
    let nodes = number("MURMUR_ABUSE_NODES", 10).clamp(3, 256) as usize;
    let duration = Duration::from_secs(number("MURMUR_ABUSE_DURATION_SECONDS", 600));
    let quarter = duration / 4;
    let pace = Duration::from_millis(number("MURMUR_ABUSE_WRITE_INTERVAL_MS", 100));
    let status = Duration::from_secs(number("MURMUR_ABUSE_STATUS_SECONDS", 15));
    let settle = Duration::from_secs(number("MURMUR_ABUSE_SETTLE_SECONDS", 900));
    let seed = std::env::var("MURMUR_ABUSE_SEED")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(1);
    let mut cluster = Cluster::new(ClusterOptions {
        name: "abuse".into(),
        num_nodes: nodes,
        manual_peers: true,
        replication_toml: "membership = false\nfanout = 2".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    let all = (0..nodes).collect::<Vec<_>>();
    for i in 0..nodes {
        cluster.node(i).simple("CREATE TABLE contention(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL,phone TEXT NOT NULL,score INTEGER NOT NULL)").await.unwrap();
        cluster
            .node(i)
            .set_table_replicated("contention", true)
            .await
            .unwrap();
        let mut donors = toml_edit::Array::new();
        for j in 0..nodes {
            if j != i {
                donors.push(cluster.node_info(j).node_id.clone());
            }
        }
        cluster.node(i).flush().await.unwrap();
        cluster.stop_node(i);
        cluster
            .edit_config(i, |d| {
                d["replication"]["trusted_snapshot_sources"] = toml_edit::value(donors)
            })
            .unwrap();
        cluster.start_node(i).await.unwrap();
    }
    mesh(&cluster).await;
    cluster
        .node(0)
        .simple("INSERT INTO contention(id,name,phone,score) VALUES('baseline','baseline','',0)")
        .await
        .unwrap();
    cluster.node(0).flush().await.unwrap();
    assert_rows(
        &cluster,
        &all,
        "SELECT id,name,phone,score FROM contention ORDER BY id",
        &[vec![
            "baseline".into(),
            "baseline".into(),
            "".into(),
            "0".into(),
        ]],
    )
    .await;
    let mut connections = Vec::new();
    for i in 0..nodes {
        connections.push(RwLock::new(Some(Arc::new(
            cluster.connect(i).await.unwrap(),
        ))));
    }
    let slots: Slots = Arc::new(connections);
    let stop_writers = Arc::new(AtomicBool::new(false));
    let acked = Arc::new(Mutex::new(BTreeMap::from([(
        "baseline".to_string(),
        "baseline".to_string(),
    )])));
    let mut jobs = tokio::task::JoinSet::new();
    for worker in 0..nodes.min(8) {
        let slots = slots.clone();
        let done = stop_writers.clone();
        let acked = acked.clone();
        jobs.spawn(async move {
            let mut seq = 0usize;
            while !done.load(Ordering::Relaxed) {
                let target = (seed.wrapping_add(worker).wrapping_add(seq)) % slots.len();
                seq += 1;
                let slot = slots[target].read().await;
                if let Some(client) = slot.as_ref() {
                    let id = format!("w{worker:02}-{seq:010}");
                    let name = format!("writer-{worker}");
                    client
                        .execute(
                            "INSERT INTO contention(id,name,phone,score) VALUES($1,$2,'',0)",
                            &[&id, &name],
                        )
                        .await
                        .unwrap();
                    client.flush().await.unwrap();
                    acked.lock().await.insert(id, name);
                }
                drop(slot);
                tokio::time::sleep(pace).await;
            }
        });
    }
    phase("healthy", quarter, status).await;
    let half = nodes / 2;
    for a in 0..half {
        for b in half..nodes {
            for (from, to) in [(a, b), (b, a)] {
                cluster
                    .node(from)
                    .simple(&format!(
                        "MURMUR PEER REMOVE '{}'",
                        cluster.node_info(to).node_id
                    ))
                    .await
                    .unwrap();
            }
        }
    }
    phase("partition", quarter, status).await;
    let left = cluster
        .node(0)
        .simple("SELECT id FROM contention ORDER BY id")
        .await
        .unwrap();
    let right = cluster
        .node(nodes - 1)
        .simple("SELECT id FROM contention ORDER BY id")
        .await
        .unwrap();
    assert_ne!(
        left, right,
        "partition failed to isolate acknowledged writes"
    );
    mesh(&cluster).await;
    let outage = (nodes - nodes.div_ceil(4)..nodes).collect::<Vec<_>>();
    stop(&mut cluster, &slots, &outage).await;
    let before = acked.lock().await.len();
    phase("partial-outage", quarter, status).await;
    assert!(
        acked.lock().await.len() > before,
        "survivors made no progress during outage"
    );
    restart(&mut cluster, &slots, &outage).await;
    stop(&mut cluster, &slots, &all).await;
    restart(&mut cluster, &slots, &all).await;
    phase("whole-mesh-restart", quarter, status).await;
    stop_writers.store(true, Ordering::Relaxed);
    while let Some(result) = jobs.join_next().await {
        result.expect("SQL writer failed");
    }
    let expected = acked
        .lock()
        .await
        .iter()
        .map(|(id, name)| vec![id.clone(), name.clone(), "".into(), "0".into()])
        .collect::<Vec<_>>();
    assert!(expected.len() > 1, "no writes acknowledged during abuse");
    let deadline = Instant::now() + settle;
    loop {
        let mut equal = true;
        for i in 0..nodes {
            equal &= cluster
                .node(i)
                .simple("SELECT id,name,phone,score FROM contention ORDER BY id")
                .await
                .unwrap()
                == expected;
        }
        if equal {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "mesh failed exact convergence on {} acknowledged rows",
            expected.len()
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    eprintln!(
        "abuse verified {} acknowledged rows on {nodes} SQL servers",
        expected.len()
    );
}
