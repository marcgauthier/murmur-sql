//! Live Murmur-SQL scenario: real server children, Rust wire client, SQL only.
use murmur_live_harness::sql::status;
use murmur_live_harness::{tokio, wait_until_async, Cluster, ClusterOptions, ReplicationOptions};
use std::time::{Duration, Instant};
const WAIT: Duration = Duration::from_secs(120);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn maintenance_under_sustained_load() {
    let cluster = Cluster::new(ClusterOptions {
        name: "maintenance-sql".into(),
        num_nodes: 3,
        replication: Some(ReplicationOptions {
            retention: Some(Duration::from_millis(10)),
            retention_interval: Some(Duration::from_millis(100)),
            minimum_retained: Some(1),
            ..Default::default()
        }),
        ..Default::default()
    })
    .await
    .unwrap();
    for i in 0..3 {
        cluster.node(i).simple("CREATE TABLE IF NOT EXISTS contention(id TEXT NOT NULL PRIMARY KEY,name TEXT NOT NULL,phone TEXT NOT NULL,score INTEGER NOT NULL)").await.unwrap();
        cluster
            .node(i)
            .simple("MURMUR SET TABLE contention REPLICATED true")
            .await
            .unwrap();
        cluster.node(i).simple("MURMUR FLUSH").await.unwrap();
    }
    let seed: usize = std::env::var("MURMUR_MAINT_LOAD_SEED")
        .ok()
        .map(|v| v.parse().unwrap())
        .unwrap_or(120);
    let cap: usize = std::env::var("MURMUR_MAINT_LOAD_MAX_INSERTS")
        .ok()
        .map(|v| v.parse().unwrap())
        .unwrap_or(4000);
    assert!(cap > 0);
    let mut expected = Vec::new();
    for n in 0..seed {
        let id = format!("seed-{n:08}");
        cluster
            .node(0)
            .execute(
                "INSERT INTO contention(id,name,phone,score) VALUES($1,'seed','unchanged',0)",
                &[&id],
            )
            .await
            .unwrap();
        expected.push(vec![id, "seed".into(), "unchanged".into(), "0".into()]);
    }
    cluster.node(0).simple("MURMUR FLUSH").await.unwrap();
    assert!(
        wait_until_async(WAIT, Duration::from_millis(50), || async {
            for i in 0..3 {
                if cluster
                    .node(i)
                    .simple("SELECT id,name,phone,score FROM contention ORDER BY id")
                    .await
                    .unwrap()
                    != expected
                {
                    return false;
                }
            }
            true
        })
        .await
    );
    let mut before = Vec::new();
    for i in 0..3 {
        let snapshot = status(cluster.node(i)).await;
        let metrics = &snapshot["replication"]["metrics"];
        before.push((
            metrics["retention_runs"].as_u64().unwrap(),
            metrics["retention_pruned"].as_u64().unwrap(),
        ));
    }
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut tasks = Vec::new();
    for worker in 0..3 {
        let client = cluster.node(worker).clone();
        let stop = stop.clone();
        tasks.push(tokio::spawn(async move {
            let mut final_rows = std::collections::BTreeMap::new();
            let mut n = 0;
            let deadline = Instant::now() + Duration::from_secs(900);
            while !stop.load(std::sync::atomic::Ordering::SeqCst) && Instant::now() < deadline {
                let id = format!("worker-{worker}-{:08}", n % cap);
                let name = format!("revision-{n}");
                if n < cap {
                    client.execute("INSERT INTO contention(id,name,phone,score) VALUES($1,$2,'unchanged',0)", &[&id,&name]).await.unwrap();
                } else {
                    client.execute("UPDATE contention SET name=$1 WHERE id=$2", &[&name,&id]).await.unwrap();
                }
                final_rows.insert(id, name);
                n += 1;
                if n % 10 == 0 { client.simple("MURMUR FLUSH").await.unwrap(); }
                tokio::task::yield_now().await;
            }
            client.simple("MURMUR FLUSH").await.unwrap();
            final_rows
        }));
    }
    let timeout = {
        let raw = std::env::var("MURMUR_MAINT_LOAD_GC_TIMEOUT").unwrap_or_else(|_| "150s".into());
        let raw = raw.trim();
        let (number, multiplier) = if let Some(n) = raw.strip_suffix('m') {
            (n, 60)
        } else if let Some(n) = raw.strip_suffix('h') {
            (n, 3600)
        } else {
            (raw.trim_end_matches('s'), 1)
        };
        Duration::from_secs(
            number.parse::<u64>().ok().filter(|n| *n > 0).unwrap_or(150) * multiplier,
        )
    };
    let maintained = wait_until_async(timeout, Duration::from_millis(200), || async {
        for i in 0..3 {
            let s = status(cluster.node(i)).await;
            if s["replication"]["metrics"]["retention_runs"]
                .as_u64()
                .unwrap()
                <= before[i].0
                || s["replication"]["metrics"]["retention_pruned"]
                    .as_u64()
                    .unwrap()
                    <= before[i].1
            {
                return false;
            }
        }
        true
    })
    .await;
    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    for task in tasks {
        for (id, name) in task.await.unwrap() {
            expected.push(vec![id, name, "unchanged".into(), "0".into()]);
        }
    }
    assert!(
        maintained,
        "retention did not run and collect under sustained SQL writes"
    );
    expected.sort();
    assert!(
        wait_until_async(WAIT, Duration::from_millis(100), || async {
            for i in 0..3 {
                if cluster
                    .node(i)
                    .simple("SELECT id,name,phone,score FROM contention ORDER BY id")
                    .await
                    .unwrap()
                    != expected
                {
                    return false;
                }
            }
            true
        })
        .await
    );
    cluster.node(0).execute("INSERT INTO contention(id,name,phone,score) VALUES('post-maintenance','ready','unchanged',0)",&[]).await.unwrap();
    cluster.node(0).simple("MURMUR FLUSH").await.unwrap();
    expected.push(vec![
        "post-maintenance".into(),
        "ready".into(),
        "unchanged".into(),
        "0".into(),
    ]);
    expected.sort();
    assert!(
        wait_until_async(WAIT, Duration::from_millis(100), || async {
            for i in 0..3 {
                if cluster
                    .node(i)
                    .simple("SELECT id,name,phone,score FROM contention ORDER BY id")
                    .await
                    .unwrap()
                    != expected
                {
                    return false;
                }
            }
            true
        })
        .await
    );
    cluster.finish(false).await;
}
