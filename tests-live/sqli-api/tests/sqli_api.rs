//! SQL is public now: bound values stay data and errors leave the connection usable.
use murmur_live_harness::{assert_rows, Cluster, ClusterOptions};

#[tokio::test(flavor = "multi_thread")]
async fn sql_parameters_do_not_execute_injected_statements() {
    let cluster = Cluster::new(ClusterOptions {
        name: "sqli-api".into(),
        num_nodes: 2,
        ..Default::default()
    })
    .await
    .unwrap();
    for i in 0..2 {
        cluster
            .node(i)
            .simple(
                "CREATE TABLE IF NOT EXISTS records(id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
            )
            .await
            .unwrap();
        cluster
            .node(i)
            .simple("MURMUR SET TABLE records REPLICATED true")
            .await
            .unwrap();
    }
    let attacks = [
        "'; DROP TABLE records; --",
        "x' OR 1=1 --",
        "'); DELETE FROM records; --",
        "SELECT * FROM sqlite_master",
        "quote' slash\\ newline\n unicode 日本語",
        "/v1/query?sql=DROP%20TABLE%20records",
    ];
    let mut expected = Vec::new();
    for (i, body) in attacks.iter().enumerate() {
        let id = i as i64;
        assert_eq!(
            cluster
                .node(0)
                .execute("INSERT INTO records(id,body) VALUES($1,$2)", &[&id, body])
                .await
                .unwrap(),
            1
        );
        expected.push(vec![body.to_string()]);
    }
    cluster.node(0).simple("MURMUR FLUSH").await.unwrap();
    assert_rows(
        &cluster,
        &[0, 1],
        "SELECT body FROM records ORDER BY id",
        &expected,
    )
    .await;
    let error = cluster
        .node(0)
        .execute(
            "INSERT INTO records(id,body) VALUES($1,$2)",
            &[&0i64, &"duplicate"],
        )
        .await
        .unwrap_err();
    assert_eq!(error.code(), Some("23505"));
    assert!(cluster
        .node(0)
        .simple("SELECT definitely_missing_column FROM records")
        .await
        .is_err());
    assert_eq!(
        cluster
            .node(0)
            .simple("SELECT count(*) FROM records")
            .await
            .unwrap(),
        vec![vec![attacks.len().to_string()]]
    );
    for i in 0..2 {
        let tables = cluster
            .node(i)
            .simple("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            .await
            .unwrap();
        assert_eq!(tables, vec![vec!["records".to_string()]]);
    }
}
