//! Corrupt archives are sent through the public client and must fail closed.
use murmur_live_harness::{Cluster, ClusterOptions, KEY};

#[tokio::test(flavor = "multi_thread")]
async fn tampered_backup_streams_leave_live_database_untouched() {
    let cluster = Cluster::new(ClusterOptions {
        name: "tampered-backup".into(),
        num_nodes: 1,
        local_only: true,
        ..Default::default()
    })
    .await
    .unwrap();
    cluster
        .node(0)
        .simple("CREATE TABLE records(id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        .await
        .unwrap();
    for id in 0..30i64 {
        cluster
            .node(0)
            .execute(
                "INSERT INTO records(id,name) VALUES($1,$2)",
                &[&id, &format!("kept-{id:03}")],
            )
            .await
            .unwrap();
    }
    let expected = cluster
        .node(0)
        .simple("SELECT id,name FROM records ORDER BY id")
        .await
        .unwrap();
    let saved = cluster.node(0).backup_save().await.unwrap();
    let mut original = Vec::new();
    cluster
        .node(0)
        .backup_download(&saved.name, &mut original)
        .await
        .unwrap();
    assert!(original.len() > 100);
    let mut bad_magic = original.clone();
    bad_magic[0] ^= 0xff;
    let mut payload_flip = original.clone();
    let middle = payload_flip.len() / 2;
    payload_flip[middle] ^= 0x80;
    let truncated = original[..original.len() / 2].to_vec();
    for (name, data) in [
        ("bad-magic.murmur-backup", bad_magic),
        ("bad-payload.murmur-backup", payload_flip),
        ("truncated.murmur-backup", truncated),
    ] {
        // Upload may reject a malformed envelope; otherwise restore must reject it.
        if cluster.node(0).backup_upload(name, &data[..]).await.is_ok() {
            assert!(
                cluster.node(0).restore(name, KEY).await.is_err(),
                "accepted corrupt archive {name}"
            );
        }
        let fresh = cluster.connect(0).await.unwrap();
        assert_eq!(
            fresh
                .simple("SELECT id,name FROM records ORDER BY id")
                .await
                .unwrap(),
            expected,
            "corrupt restore modified live rows: {name}"
        );
    }
    cluster.node(0).simple("DELETE FROM records").await.unwrap();
    cluster.node(0).restore(&saved.name, KEY).await.unwrap();
    assert_eq!(
        cluster
            .connect(0)
            .await
            .unwrap()
            .simple("SELECT id,name FROM records ORDER BY id")
            .await
            .unwrap(),
        expected
    );
}
