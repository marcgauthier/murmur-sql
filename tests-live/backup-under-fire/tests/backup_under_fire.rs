//! Take streaming online backups while a separate SQL connection commits transactions.
use murmur_live_harness::{Cluster, ClusterOptions, KEY};
use std::time::Duration;

#[tokio::test(flavor = "multi_thread")]
async fn online_sql_backups_capture_atomic_commits_under_write_load() {
    let cluster = Cluster::new(ClusterOptions {
        name: "backup-under-fire".into(),
        num_nodes: 1,
        local_only: true,
        ..Default::default()
    })
    .await
    .unwrap();
    cluster
        .node(0)
        .simple("CREATE TABLE records(id INTEGER PRIMARY KEY, batch INTEGER NOT NULL)")
        .await
        .unwrap();
    let writer = cluster.connect(0).await.unwrap();
    let write =
        async {
            for batch in 0..40i64 {
                // One multi-row INSERT is the actual atomic unit: transaction
                // control words are documented no-ops on the SQL server.
                let ids = [
                    batch * 5,
                    batch * 5 + 1,
                    batch * 5 + 2,
                    batch * 5 + 3,
                    batch * 5 + 4,
                ];
                writer.execute(
                "INSERT INTO records(id,batch) VALUES($1,$6),($2,$6),($3,$6),($4,$6),($5,$6)",
                &[&ids[0], &ids[1], &ids[2], &ids[3], &ids[4], &batch],
            ).await.unwrap();
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            writer.simple("MURMUR FLUSH").await.unwrap();
        };
    let backup = async {
        let mut archives = Vec::new();
        for i in 0..4 {
            let saved = cluster.node(0).simple("MURMUR BACKUP SAVE").await.unwrap();
            let mut bytes = Vec::new();
            murmur_live_harness::transfers::download(cluster
                .node(0), &format!("MURMUR BACKUP DOWNLOAD {}", murmur_live_harness::sql_literal(&saved[0][0])), &mut bytes)
                .await
                .unwrap();
            assert_eq!(bytes.len() as i64, saved[0][1].parse::<i64>().unwrap());
            let name = format!("under-fire-{i}.murmur-backup");
            murmur_live_harness::transfers::upload(cluster
                .node(0), &name, &bytes[..])
                .await
                .unwrap();
            archives.push(name);
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
        archives
    };
    let (_, archives) = tokio::join!(write, backup);
    assert_eq!(
        cluster
            .node(0)
            .simple("SELECT count(*) FROM records")
            .await
            .unwrap(),
        vec![vec!["200".to_string()]]
    );
    let mut previous = 0usize;
    for name in archives {
        let admin = cluster.connect(0).await.unwrap();
        admin.simple(&format!("MURMUR RESTORE FROM {} KEY {}",murmur_live_harness::sql_literal(&name),murmur_live_harness::sql_literal(KEY))).await.unwrap();
        let fresh = cluster.connect(0).await.unwrap();
        let rows = fresh
            .simple("SELECT id,batch FROM records ORDER BY id")
            .await
            .unwrap();
        assert!(
            rows.len() >= previous && rows.len() <= 200,
            "backup prefix regressed"
        );
        previous = rows.len();
        assert_eq!(
            rows.len() % 5,
            0,
            "snapshot split an atomic five-row transaction"
        );
        for (id, row) in rows.iter().enumerate() {
            assert_eq!(row, &vec![id.to_string(), (id / 5).to_string()]);
        }
    }
}
