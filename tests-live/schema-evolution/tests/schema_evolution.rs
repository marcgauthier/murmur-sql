//! Additive SQL schema changes replicate; destructive replicated DDL is rejected.
use murmur_live_harness::{assert_rows, Cluster, ClusterOptions};

#[tokio::test(flavor = "multi_thread")]
async fn additive_sql_schema_migration_preserves_rows_and_replication_settings() {
    let mut cluster = Cluster::new(ClusterOptions {
        name: "schema-evolution".into(),
        num_nodes: 3,
        ..Default::default()
    })
    .await
    .unwrap();
    for i in 0..3 {
        cluster.node(i).simple("CREATE TABLE IF NOT EXISTS records(name TEXT PRIMARY KEY NOT NULL, score INTEGER NOT NULL)").await.unwrap();
        cluster
            .node(i)
            .simple("MURMUR SET TABLE records REPLICATED true")
            .await
            .unwrap();
    }
    cluster
        .node(0)
        .simple("INSERT INTO records(name,score) VALUES('seed',1)")
        .await
        .unwrap();
    cluster.node(0).simple("MURMUR FLUSH").await.unwrap();
    assert_rows(
        &cluster,
        &[0, 1, 2],
        "SELECT name,score FROM records ORDER BY name",
        &[vec!["seed".into(), "1".into()]],
    )
    .await;
    cluster.node(2).simple("MURMUR FLUSH").await.unwrap();
    cluster.stop_node(2);
    cluster
        .node(0)
        .simple("ALTER TABLE records ADD COLUMN note TEXT")
        .await
        .unwrap();
    cluster.node(0).simple("UPDATE records SET note='new-field' WHERE name='seed'; INSERT INTO records(name,score,note) VALUES('new',2,'written-after-ddl')").await.unwrap();
    cluster.node(0).simple("MURMUR FLUSH").await.unwrap();
    let expected = vec![
        vec!["new".into(), "2".into(), "written-after-ddl".into()],
        vec!["seed".into(), "1".into(), "new-field".into()],
    ];
    // The offline peer must learn the new schema and values on reconnect.
    cluster.start_node(2).await.unwrap();
    assert_rows(
        &cluster,
        &[0, 1, 2],
        "SELECT name,score,note FROM records ORDER BY name",
        &expected,
    )
    .await;
    for sql in [
        "DROP TABLE records",
        "ALTER TABLE records RENAME TO renamed",
        "ALTER TABLE records RENAME COLUMN score TO points",
        "ALTER TABLE records DROP COLUMN note",
    ] {
        assert!(
            cluster.node(0).simple(sql).await.is_err(),
            "replicated destructive DDL accepted: {sql}"
        );
    }
    for i in 0..3 {
        assert!(cluster
            .node(i)
            .simple("MURMUR SHOW TABLES REPLICATION")
            .await
            .unwrap()
            .iter()
            .any(|f| f[0] == "records" && f[1] == "true"));
    }
    cluster.node(0).simple("CREATE TABLE local_only(id INTEGER PRIMARY KEY, old TEXT); ALTER TABLE local_only RENAME COLUMN old TO renamed; ALTER TABLE local_only RENAME TO renamed_local; DROP TABLE renamed_local").await.unwrap();
    assert_rows(
        &cluster,
        &[0, 1, 2],
        "SELECT name,score,note FROM records ORDER BY name",
        &expected,
    )
    .await;
}
