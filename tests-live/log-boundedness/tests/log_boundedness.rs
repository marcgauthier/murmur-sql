//! Background retention must bound log growth while real SQL clients keep writing.
use murmur_live_harness::{assert_rows, Cluster, ClusterOptions};
use std::{
    path::Path,
    time::{Duration, Instant},
};
fn bytes(root: &Path) -> u64 {
    std::fs::read_dir(root)
        .unwrap()
        .map(|e| {
            let p = e.unwrap().path();
            if p.is_dir() {
                bytes(&p)
            } else {
                std::fs::metadata(p).unwrap().len()
            }
        })
        .sum()
}
struct Sample {
    committed: u64,
    collected: u64,
    disk: u64,
    error: String,
}
async fn sample(cluster: &Cluster, node: usize) -> Sample {
    let status = cluster
        .node(node)
        .simple("MURMUR SHOW REPLICATION")
        .await
        .unwrap();
    // Append-only extension requested from the server owner: the first seven
    // fields stay compatible with existing SQL clients and snapshot tests.
    assert!(
        status[0].len() >= 8,
        "SHOW REPLICATION must expose committed at appended column 8 for exact log-bound testing"
    );
    let retained = cluster
        .node(node)
        .simple("MURMUR SHOW RETENTION")
        .await
        .unwrap();
    let collected = retained.first().map(|r| r[5].parse().unwrap()).unwrap_or(0);
    Sample {
        committed: status[0][7].parse().unwrap(),
        collected,
        disk: bytes(&cluster.node_info(node).data_dir),
        error: status[0][6].clone(),
    }
}
#[tokio::test(flavor = "multi_thread")]
async fn retained_sql_logs_and_disk_growth_stay_bounded_under_sustained_writes() {
    let cluster = Cluster::new(ClusterOptions {
        name: "log-boundedness".into(),
        num_nodes: 2,
        manual_peers: true,
        replication_toml: "membership = false\nretention = \"1s\"\nretention_interval = \"1s\"\nminimum_retained = 10"
            .into(),
        ..Default::default()
    })
    .await
    .unwrap();
    for i in 0..2 {
        cluster.node(i).simple("CREATE TABLE IF NOT EXISTS contention(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL,phone TEXT NOT NULL,score INTEGER NOT NULL)").await.unwrap();
        cluster
            .node(i)
            .set_table_replicated("contention", true)
            .await
            .unwrap();
        cluster.node(i).flush().await.unwrap();
    }
    // Establish one intentional session only after both servers are unlocked.
    // This excludes startup bootstrap retries and duplicate-dial closes from
    // the retention measurement, while keeping the no-new-errors assertion.
    cluster
        .node(0)
        .simple(&format!(
            "MURMUR PEER ADD '{}'",
            cluster.node_info(1).repl_addr
        ))
        .await
        .unwrap();
    let control = vec![vec![
        format!("{:032x}", 0),
        format!("payload-0-{}", "x".repeat(200)),
        "".into(),
        "0".into(),
    ]];
    cluster
        .node(0)
        .execute(
            "INSERT INTO contention(id,name,phone,score) VALUES($1,$2,'',0)",
            &[&control[0][0], &control[0][1]],
        )
        .await
        .unwrap();
    cluster.node(0).flush().await.unwrap();
    assert_rows(
        &cluster,
        &[0, 1],
        "SELECT id,name,phone,score FROM contention ORDER BY id",
        &control,
    )
    .await;
    let duration = Duration::from_secs(
        std::env::var("MURMUR_LOG_BOUNDEDNESS_SECONDS")
            .ok()
            .and_then(|s| s.parse().ok())
            .filter(|n| *n > 0)
            .unwrap_or(75),
    );
    let start = vec![sample(&cluster, 0).await, sample(&cluster, 1).await];
    let mut mid = None;
    let mut expected = control;
    let end = Instant::now() + duration;
    while Instant::now() < end {
        let seq = expected.len();
        let id = format!("{seq:032x}");
        let body = format!("payload-{seq}-{}", "x".repeat(200));
        cluster
            .node(seq % 2)
            .execute(
                "INSERT INTO contention(id,name,phone,score) VALUES($1,$2,'',0)",
                &[&id, &body],
            )
            .await
            .unwrap();
        cluster.node(seq % 2).flush().await.unwrap();
        expected.push(vec![id, body, "".into(), "0".into()]);
        if mid.is_none() && end.saturating_duration_since(Instant::now()) < duration / 2 {
            mid = Some(vec![sample(&cluster, 0).await, sample(&cluster, 1).await]);
        }
    }
    assert!(
        expected.len() >= 100,
        "load too small to judge boundedness: {} acknowledged rows",
        expected.len()
    );
    let finish = vec![sample(&cluster, 0).await, sample(&cluster, 1).await];
    let mid = mid.expect("load never reached midpoint");
    for i in 0..2 {
        let produced = finish[i].committed.saturating_sub(start[i].committed);
        let collected = finish[i].collected.saturating_sub(start[i].collected);
        assert!(
            collected > 0,
            "background retention did not run on node {i}"
        );
        // Preserve the legacy growth diagnostic, but do not treat this as
        // an exact count: engine batches and collected logical transactions
        // differ when incoming work is batched. Collection and disk bounds
        // below are checked independently.
        let retained = produced.saturating_sub(collected);
        assert!(
            retained <= 512 + produced / 4,
            "node {i} retained growth={retained}, produced={produced}, collected={collected}"
        );
        assert_eq!(
            finish[i].error, start[i].error,
            "replication recorded an error under load"
        );
        let growth = finish[i].disk.saturating_sub(start[i].disk);
        assert!(
            growth <= expected.len() as u64 * 8192,
            "node {i}: disk growth {growth} exceeds 8KiB/row"
        );
        let first = mid[i].disk.saturating_sub(start[i].disk);
        let second = finish[i].disk.saturating_sub(mid[i].disk);
        assert!(
            second <= 2 * first + 1024 * 1024,
            "node {i}: disk growth accelerated: first={first}, second={second}"
        );
        eprintln!("node {i}: produced={produced}, collected={collected}, retained_growth={retained}, disk_growth={growth}");
    }
    assert_rows(
        &cluster,
        &[0, 1],
        "SELECT id,name,phone,score FROM contention ORDER BY id",
        &expected,
    )
    .await;
}
