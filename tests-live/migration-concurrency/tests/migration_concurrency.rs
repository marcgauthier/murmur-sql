//! Race additive DDL on independent SQL connections while acknowledged writes continue.
use murmur_live_harness::{assert_rows, Cluster, ClusterOptions};
use std::time::Duration;

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_sql_migrations_leave_one_complete_schema_and_all_acknowledged_rows() {
    let cluster = Cluster::new(ClusterOptions {
        name: "migration-concurrency".into(),
        num_nodes: 3,
        ..Default::default()
    })
    .await
    .unwrap();
    for i in 0..3 {
        cluster
            .node(i)
            .simple(
                "CREATE TABLE IF NOT EXISTS records(id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
            )
            .await
            .unwrap();
        cluster
            .node(i)
            .set_table_replicated("records", true)
            .await
            .unwrap();
    }
    cluster
        .node(0)
        .simple("INSERT INTO records(id,name) VALUES(0,'seed')")
        .await
        .unwrap();
    cluster.node(0).flush().await.unwrap();
    assert_rows(
        &cluster,
        &[0, 1, 2],
        "SELECT name FROM records ORDER BY id",
        &[vec!["seed".into()]],
    )
    .await;
    let a = cluster.connect(0).await.unwrap();
    let b = cluster.connect(1).await.unwrap();
    let writer = cluster.connect(2).await.unwrap();
    let write = async {
        let mut expected = vec![vec!["seed".into()]];
        for id in 1..=60i64 {
            let name = format!("acknowledged-{id:03}");
            writer
                .execute("INSERT INTO records(id,name) VALUES($1,$2)", &[&id, &name])
                .await
                .unwrap();
            expected.push(vec![name]);
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        writer.flush().await.unwrap();
        expected
    };
    let (first, second, expected) = tokio::join!(
        a.simple("ALTER TABLE records ADD COLUMN note TEXT"),
        b.simple("ALTER TABLE records ADD COLUMN note TEXT"),
        write
    );
    assert!(
        first.is_ok() || second.is_ok(),
        "both migrations failed: {first:?}; {second:?}"
    );
    assert_rows(
        &cluster,
        &[0, 1, 2],
        "SELECT name FROM records ORDER BY id",
        &expected,
    )
    .await;
    for i in 0..3 {
        let columns = cluster
            .node(i)
            .simple("SELECT name FROM pragma_table_info('records') WHERE name='note'")
            .await
            .unwrap();
        assert_eq!(
            columns,
            vec![vec!["note".to_string()]],
            "duplicate or absent migration column"
        );
        cluster
            .node(i)
            .execute(
                "UPDATE records SET note=$1 WHERE id=$2",
                &[&format!("node-{i}"), &(i as i64)],
            )
            .await
            .unwrap();
        cluster.node(i).flush().await.unwrap();
    }
    assert_rows(
        &cluster,
        &[0, 1, 2],
        "SELECT note FROM records WHERE id<3 ORDER BY id",
        &[
            vec!["node-0".into()],
            vec!["node-1".into()],
            vec!["node-2".into()],
        ],
    )
    .await;
}
