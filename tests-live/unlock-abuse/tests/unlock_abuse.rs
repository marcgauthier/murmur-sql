//! Exercise real wire authentication and per-connection unlock denial.
use murmur_client::Client;
use murmur_live_harness::{Cluster, ClusterOptions};

#[tokio::test(flavor = "multi_thread")]
async fn bad_credentials_and_unlock_keys_do_not_disclose_secrets_or_damage_sql() {
    let mut cluster = Cluster::new(ClusterOptions {
        name: "unlock-abuse".into(),
        num_nodes: 1,
        ..Default::default()
    })
    .await
    .unwrap();
    cluster.node(0).simple("CREATE TABLE records(id INTEGER PRIMARY KEY, body TEXT); INSERT INTO records VALUES(1,'kept')").await.unwrap();
    cluster.node(0).simple("MURMUR FLUSH").await.unwrap();
    for attempt in 0..12 {
        let mut config = cluster.config(0);
        let secret = format!("unlock-attack-secret-{attempt}");
        match attempt % 3 {
            0 => config.password = secret.clone(),
            1 => config.user = secret.clone(),
            _ => config.key = secret.clone(),
        }
        let error = match Client::connect(&config).await {
            Err(e) => e,
            Ok(_) => panic!("bad credentials accepted"),
        };
        assert_eq!(
            error.code(),
            Some(if attempt % 3 == 2 { "28000" } else { "28P01" })
        );
        assert!(
            !error.to_string().contains(&secret),
            "credential leaked: {error}"
        );
        assert_eq!(
            cluster
                .node(0)
                .simple("SELECT body FROM records")
                .await
                .unwrap(),
            vec![vec!["kept".to_string()]]
        );
    }
    cluster.stop_node(0);
    cluster.start_node(0).await.unwrap();
    assert_eq!(
        cluster
            .node(0)
            .simple("SELECT body FROM records")
            .await
            .unwrap(),
        vec![vec!["kept".to_string()]]
    );
}
