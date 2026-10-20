//! Typed application rows now travel through the Rust SQL client.
use murmur_live_harness::{assert_rows, toml_edit, Cluster, ClusterOptions};
use std::time::{Duration, Instant};

async fn schema(cluster: &Cluster) {
    for i in 0..cluster.len() {
        cluster.node(i).simple("CREATE TABLE IF NOT EXISTS records(id INTEGER PRIMARY KEY, title TEXT NOT NULL, score REAL, payload BLOB, optional TEXT)").await.unwrap();
        cluster
            .node(i)
            .simple("MURMUR SET TABLE records REPLICATED true")
            .await
            .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn sql_typed_records_replicate_update_delete_and_rebuild_after_restart() {
    let mut cluster = Cluster::new(ClusterOptions {
        name: "typed-records-roundtrip".into(),
        num_nodes: 3,
        ..Default::default()
    })
    .await
    .unwrap();
    schema(&cluster).await;
    let title = "quotes ' and unicode 日本語";
    let payload = vec![0u8, 1, 127, 128, 255];
    cluster
        .node(0)
        .execute(
            "INSERT INTO records(id,title,score,payload,optional) VALUES($1,$2,$3,$4,$5)",
            &[&1i64, &title, &1.25f64, &payload, &Option::<String>::None],
        )
        .await
        .unwrap();
    cluster.node(1).simple("INSERT INTO records(id,title,score,payload,optional) VALUES(2,'second',-2.5,x'', 'present')").await.unwrap();
    for i in 0..3 {
        cluster.node(i).simple("MURMUR FLUSH").await.unwrap();
    }
    let projection = "SELECT id,title,score,hex(payload),optional FROM records ORDER BY id";
    let expected = vec![
        vec![
            "1".into(),
            title.into(),
            "1.25".into(),
            "00017F80FF".into(),
            "".into(),
        ],
        vec![
            "2".into(),
            "second".into(),
            "-2.5".into(),
            "".into(),
            "present".into(),
        ],
    ];
    assert_rows(&cluster, &[0, 1, 2], projection, &expected).await;
    let keys = cluster
        .node(0)
        .simple("SELECT id,id_murmur_primary_key FROM records ORDER BY id")
        .await
        .unwrap();
    for i in 1..3 {
        assert_eq!(
            cluster
                .node(i)
                .simple("SELECT id,id_murmur_primary_key FROM records ORDER BY id")
                .await
                .unwrap(),
            keys
        );
    }
    let rows = cluster
        .node(2)
        .query(
            "SELECT title,score,payload,optional FROM records WHERE id=$1",
            &[&1i64],
        )
        .await
        .unwrap();
    assert_eq!(rows[0].get::<_, String>(0), title);
    assert_eq!(rows[0].get::<_, f64>(1), 1.25);
    assert_eq!(rows[0].get::<_, Vec<u8>>(2), payload);
    assert_eq!(rows[0].get::<_, Option<String>>(3), None);
    cluster
        .node(2)
        .execute(
            "UPDATE records SET title=$1 WHERE id=$2",
            &[&"updated remotely", &1i64],
        )
        .await
        .unwrap();
    cluster
        .node(1)
        .execute("DELETE FROM records WHERE id=$1", &[&2i64])
        .await
        .unwrap();
    for i in 0..3 {
        cluster.node(i).simple("MURMUR FLUSH").await.unwrap();
    }
    let final_rows = vec![vec![
        "1".into(),
        "updated remotely".into(),
        "1.25".into(),
        "00017F80FF".into(),
        "".into(),
    ]];
    assert_rows(&cluster, &[0, 1, 2], projection, &final_rows).await;
    cluster.node(2).simple("MURMUR FLUSH").await.unwrap();
    cluster.stop_node(2);
    cluster.start_node(2).await.unwrap();
    assert_rows(&cluster, &[2], projection, &final_rows).await;
    assert_eq!(
        cluster
            .node(2)
            .simple("SELECT id,id_murmur_primary_key FROM records ORDER BY id")
            .await
            .unwrap(),
        vec![keys[0].clone()]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn sql_transaction_words_are_noops_and_each_statement_is_atomic() {
    let cluster = Cluster::new(ClusterOptions {
        name: "typed-records-rollback".into(),
        num_nodes: 2,
        ..Default::default()
    })
    .await
    .unwrap();
    schema(&cluster).await;
    cluster
        .node(0)
        .simple("BEGIN; INSERT INTO records(id,title) VALUES(1,'autocommitted'); ROLLBACK")
        .await
        .unwrap();
    cluster.node(0).simple("BEGIN; INSERT INTO records(id,title) VALUES(2,'kept'); INSERT INTO records(id,title) VALUES(3,'also-kept'); COMMIT").await.unwrap();
    cluster.node(0).simple("MURMUR FLUSH").await.unwrap();
    // The first row of a failed multi-row statement must not leak into RAM,
    // persistence, or replication when the second row violates a unique key.
    assert!(cluster
        .node(0)
        .simple("INSERT INTO records(id,title) VALUES(4,'must-rollback'),(2,'duplicate')")
        .await
        .is_err());
    assert_rows(
        &cluster,
        &[0, 1],
        "SELECT id,title FROM records ORDER BY id",
        &[
            vec!["1".into(), "autocommitted".into()],
            vec!["2".into(), "kept".into()],
            vec!["3".into(), "also-kept".into()],
        ],
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn sql_node_local_records_stay_local_and_persist_after_restart() {
    let mut cluster = Cluster::new(ClusterOptions {
        name: "typed-records-local".into(),
        num_nodes: 2,
        ..Default::default()
    })
    .await
    .unwrap();
    schema(&cluster).await;
    for i in 0..2 {
        cluster
            .node(i)
            .simple("CREATE TABLE private_records(id INTEGER PRIMARY KEY, body TEXT)")
            .await
            .unwrap();
        let id = i as i64;
        let body = format!("private-to-node-{i}");
        cluster
            .node(i)
            .execute(
                "INSERT INTO private_records(id,body) VALUES($1,$2)",
                &[&id, &body],
            )
            .await
            .unwrap();
        assert!(cluster
            .node(i)
            .simple("MURMUR SHOW TABLES REPLICATION")
            .await
            .unwrap()
            .iter()
            .any(|f| f[0] == "private_records" && f[1] == "false"));
        cluster.node(i).simple("MURMUR FLUSH").await.unwrap();
    }
    cluster
        .node(0)
        .simple("INSERT INTO records(id,title) VALUES(10,'replicated-control')")
        .await
        .unwrap();
    cluster.node(0).simple("MURMUR FLUSH").await.unwrap();
    assert_rows(
        &cluster,
        &[0, 1],
        "SELECT title FROM records",
        &[vec!["replicated-control".into()]],
    )
    .await;
    for i in 0..2 {
        cluster.stop_node(i);
        cluster.start_node(i).await.unwrap();
        assert_eq!(
            cluster
                .node(i)
                .simple("SELECT id,body FROM private_records")
                .await
                .unwrap(),
            vec![vec![i.to_string(), format!("private-to-node-{i}")]]
        );
        let columns = cluster
            .node(i)
            .simple("SELECT name FROM pragma_table_info('private_records') WHERE pk=1")
            .await
            .unwrap();
        assert_eq!(columns, vec![vec!["id".to_string()]]);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn sql_replication_rebuilds_populated_keys_and_enforces_immutable_application_keys() {
    let mut cluster = Cluster::new(ClusterOptions {
        name: "typed-records-primary-key".into(),
        num_nodes: 1,
        ..Default::default()
    })
    .await
    .unwrap();
    cluster.node(0).simple("CREATE TABLE records(tenant TEXT NOT NULL, code INTEGER NOT NULL, body TEXT, PRIMARY KEY(tenant,code)); INSERT INTO records VALUES('one',7,'preserved')").await.unwrap();
    cluster
        .node(0)
        .simple("MURMUR SET TABLE records REPLICATED true")
        .await
        .unwrap();
    let first = cluster
        .node(0)
        .simple("SELECT tenant,code,body,id_murmur_primary_key FROM records")
        .await
        .unwrap();
    assert_eq!(first.len(), 1);
    assert!(!first[0][3].is_empty());
    assert_eq!(
        cluster
            .node(0)
            .simple("SELECT name FROM pragma_table_info('records') WHERE pk>0")
            .await
            .unwrap(),
        vec![vec!["id_murmur_primary_key".to_string()]]
    );
    for sql in [
        "UPDATE records SET tenant='other' WHERE code=7",
        "UPDATE records SET code=8 WHERE tenant='one'",
        "UPDATE records SET id_murmur_primary_key='forged'",
        "INSERT INTO records(tenant,code) VALUES(NULL,8)",
        "INSERT INTO records(tenant,code) VALUES('two',NULL)",
        "INSERT INTO records(tenant,code) VALUES('one',7)",
    ] {
        assert!(
            cluster.node(0).simple(sql).await.is_err(),
            "invalid replicated key write accepted: {sql}"
        );
    }
    assert_eq!(
        cluster
            .node(0)
            .simple("SELECT tenant,code,body,id_murmur_primary_key FROM records")
            .await
            .unwrap(),
        first
    );
    cluster
        .node(0)
        .simple("MURMUR SET TABLE records REPLICATED false")
        .await
        .unwrap();
    assert_eq!(
        cluster
            .node(0)
            .simple("SELECT name FROM pragma_table_info('records') WHERE pk>0 ORDER BY pk")
            .await
            .unwrap(),
        vec![vec!["tenant".to_string()], vec!["code".to_string()]]
    );
    cluster
        .node(0)
        .simple("MURMUR SET TABLE records REPLICATED true")
        .await
        .unwrap();
    assert_eq!(
        cluster
            .node(0)
            .simple("SELECT tenant,code,body,id_murmur_primary_key FROM records")
            .await
            .unwrap(),
        first
    );
    cluster.node(0).simple("MURMUR FLUSH").await.unwrap();
    cluster.stop_node(0);
    cluster.start_node(0).await.unwrap();
    assert_eq!(
        cluster
            .node(0)
            .simple("SELECT tenant,code,body,id_murmur_primary_key FROM records")
            .await
            .unwrap(),
        first
    );
    assert!(cluster
        .node(0)
        .simple("MURMUR SHOW TABLES REPLICATION")
        .await
        .unwrap()
        .iter()
        .any(|f| f[0] == "records" && f[1] == "true"));
}

#[tokio::test(flavor = "multi_thread")]
async fn sql_files_replicate_and_download_through_the_rust_client() {
    let mut cluster = Cluster::new(ClusterOptions {
        name: "typed-records-files".into(),
        num_nodes: 2,
        files: true,
        ..Default::default()
    })
    .await
    .unwrap();
    let payload: Vec<u8> = (0..150_000)
        .map(|i| ((i * 131 + i / 251) % 256) as u8)
        .collect();
    let name = "typed/file ' unicode 日本語.bin";
    let client = cluster.node(0);
    let upload = client.query("MURMUR FILE UPLOAD BEGIN $1", &[&name]).await.unwrap();
    let token: String = upload[0].get(0);
    for chunk in payload.chunks(65536) {
        client.execute("MURMUR FILE UPLOAD APPEND $1 $2", &[&token, &chunk]).await.unwrap();
    }
    let metadata = client.query("MURMUR FILE UPLOAD FINISH $1", &[&token]).await.unwrap();
    assert_eq!(metadata[0].get::<_, i64>(1), payload.len() as i64);
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let cursor = cluster.node(1).query("MURMUR FILE LIST", &[]).await.unwrap();
        let token: String = cursor[0].get(0);
        let mut found = false;
        loop {
            let page = cluster.node(1).query("MURMUR FILE NEXT $1 LIMIT 100", &[&token]).await.unwrap();
            if page.is_empty() { break; }
            found |= page.iter().any(|row| row.get::<_, &str>(0) == name);
        }
        cluster.node(1).execute("MURMUR FILE CLOSE $1", &[&token]).await.unwrap();
        if found {
            match file_bytes(cluster.node(1), name).await {
                Ok(got) => { assert_eq!(got, payload); break; }
                Err(error) if error.code() == Some("55000") => {}
                Err(error) => panic!("SQL file download: {error}"),
            }
        }
        assert!(Instant::now() < deadline, "replicated file did not fetch");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    cluster.node(1).simple("MURMUR FLUSH").await.unwrap();
    cluster.stop_node(1);
    cluster.start_node(1).await.unwrap();
    assert_eq!(file_bytes(cluster.node(1), name).await.unwrap(), payload);

}

#[tokio::test(flavor = "multi_thread")]
async fn sql_concurrent_additive_schema_branches_preserve_both_values() {
    let mut cluster = Cluster::new(ClusterOptions {
        name: "typed-records-schema-branches".into(),
        num_nodes: 2,
        replication_toml: "membership = false".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    schema(&cluster).await;
    cluster
        .node(0)
        .simple("INSERT INTO records(id,title) VALUES(1,'branched-record')")
        .await
        .unwrap();
    cluster.node(0).simple("MURMUR FLUSH").await.unwrap();
    assert_rows(
        &cluster,
        &[0, 1],
        "SELECT title FROM records",
        &[vec!["branched-record".into()]],
    )
    .await;
    for i in 0..2 {
        cluster.node(i).simple("MURMUR FLUSH").await.unwrap();
        cluster.stop_node(i);
        cluster.set_peers(i, &[]).unwrap();
    }
    for i in 0..2 {
        cluster.start_node(i).await.unwrap();
    }
    cluster.node(0).simple("ALTER TABLE records ADD COLUMN note TEXT; UPDATE records SET note='note-from-left' WHERE id=1").await.unwrap();
    cluster.node(1).simple("ALTER TABLE records ADD COLUMN region TEXT; UPDATE records SET region='region-from-right' WHERE id=1").await.unwrap();
    assert!(cluster
        .node(0)
        .simple("SELECT region FROM records")
        .await
        .is_err());
    assert!(cluster
        .node(1)
        .simple("SELECT note FROM records")
        .await
        .is_err());
    for i in 0..2 {
        cluster.node(i).simple("MURMUR FLUSH").await.unwrap();
        cluster.stop_node(i);
        cluster.set_peers(i, &[1 - i]).unwrap();
    }
    for i in 0..2 {
        cluster.start_node(i).await.unwrap();
    }
    assert_rows(
        &cluster,
        &[0, 1],
        "SELECT title,note,region FROM records",
        &[vec![
            "branched-record".into(),
            "note-from-left".into(),
            "region-from-right".into(),
        ]],
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn sql_crdt_operations_converge_across_offline_partition() {
    let mut cluster = Cluster::new(ClusterOptions {
        name: "typed-records-crdt".into(), num_nodes: 2,
        replication_toml: "membership = false\nmerge_policies = { \"policies.counter\" = \"counter\", \"policies.tags\" = \"set\", \"policies.peak\" = \"max\", \"policies.floor\" = \"min\" }".into(),
        ..Default::default()
    }).await.unwrap();
    for i in 0..2 {
        cluster.node(i).simple("CREATE TABLE IF NOT EXISTS policies(name TEXT PRIMARY KEY NOT NULL, counter INTEGER NOT NULL, tags BLOB NOT NULL, peak INTEGER NOT NULL, floor REAL NOT NULL)").await.unwrap();
        cluster
            .node(i)
            .simple("MURMUR SET TABLE policies REPLICATED true")
            .await
            .unwrap();
    }
    cluster
        .node(0)
        .simple(
            "INSERT INTO policies(name,counter,tags,peak,floor) VALUES('shared',0,x'00',0,100.0)",
        )
        .await
        .unwrap();
    cluster
        .node(0)
        .simple("MURMUR POLICY SET_ADD policies tags 'shared' 'seed'")
        .await
        .unwrap();
    cluster.node(0).simple("MURMUR FLUSH").await.unwrap();
    let initial = cluster
        .node(0)
        .simple("SELECT name,counter,hex(tags),peak,floor FROM policies")
        .await
        .unwrap();
    assert_rows(
        &cluster,
        &[0, 1],
        "SELECT name,counter,hex(tags),peak,floor FROM policies",
        &initial,
    )
    .await;
    for i in 0..2 {
        cluster.node(i).simple("MURMUR FLUSH").await.unwrap();
        cluster.stop_node(i);
        cluster.set_peers(i, &[]).unwrap();
    }
    for i in 0..2 {
        cluster.start_node(i).await.unwrap();
    }
    for sql in [
        "MURMUR POLICY COUNTER_ADD policies counter 'shared' 4",
        "MURMUR POLICY SET_ADD policies tags 'shared' 'left'",
        "MURMUR POLICY SET_REMOVE policies tags 'shared' 'seed'",
        "MURMUR POLICY MAX policies peak 'shared' 40",
        "MURMUR POLICY MIN policies floor 'shared' 5.0",
    ] {
        cluster.node(0).simple(sql).await.unwrap();
    }
    for sql in [
        "MURMUR POLICY COUNTER_ADD policies counter 'shared' 7",
        "MURMUR POLICY COUNTER_ADD policies counter 'shared' -2",
        "MURMUR POLICY SET_ADD policies tags 'shared' 'right'",
        "MURMUR POLICY SET_ADD policies tags 'shared' 'right'",
        "MURMUR POLICY MAX policies peak 'shared' 99",
        "MURMUR POLICY MIN policies floor 'shared' 0.5",
    ] {
        cluster.node(1).simple(sql).await.unwrap();
    }
    for i in 0..2 {
        cluster.node(i).simple("MURMUR FLUSH").await.unwrap();
        cluster.stop_node(i);
        cluster.set_peers(i, &[1 - i]).unwrap();
    }
    for i in 0..2 {
        cluster.start_node(i).await.unwrap();
    }
    let values = vec![vec!["9".into(), "99".into(), "0.5".into()]];
    assert_rows(
        &cluster,
        &[0, 1],
        "SELECT counter,peak,floor FROM policies WHERE name='shared'",
        &values,
    )
    .await;
    // Build the expected materialized set through public operations, without
    // reimplementing the engine's postcard/CRDT representation in this client.
    cluster.node(0).simple("INSERT INTO policies(name,counter,tags,peak,floor) VALUES('control',0,x'00',0,100.0); MURMUR POLICY SET_ADD policies tags 'control' 'left'; MURMUR POLICY SET_ADD policies tags 'control' 'right'").await.unwrap();
    let expected_set = cluster
        .node(0)
        .simple("SELECT hex(tags) FROM policies WHERE name='control'")
        .await
        .unwrap();
    assert_rows(
        &cluster,
        &[0, 1],
        "SELECT hex(tags) FROM policies WHERE name='shared'",
        &expected_set,
    )
    .await;
    assert!(
        cluster
            .node(0)
            .simple("UPDATE policies SET counter=100 WHERE name='shared'")
            .await
            .is_err(),
        "direct assignment bypassed CRDT policy"
    );
    cluster.node(1).simple("MURMUR FLUSH").await.unwrap();
    cluster.stop_node(1);
    cluster.start_node(1).await.unwrap();
    assert_rows(
        &cluster,
        &[1],
        "SELECT counter,peak,floor FROM policies WHERE name='shared'",
        &values,
    )
    .await;
    assert_rows(
        &cluster,
        &[1],
        "SELECT hex(tags) FROM policies WHERE name='shared'",
        &expected_set,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn sql_empty_peer_recovers_typed_rows_from_a_retention_snapshot() {
    let mut cluster=Cluster::new(ClusterOptions {
        name:"typed-records-snapshot".into(),num_nodes:2,manual_peers:true,
        replication_toml:"membership = false\nretention = \"1ms\"\nretention_interval = \"60s\"\nminimum_retained = 1".into(),
        ..Default::default()
    }).await.unwrap();
    schema(&cluster).await;
    // Authorize only this source to send snapshots to the empty receiver.
    let source = cluster.node_info(0).node_id.clone();
    cluster.node(1).simple("MURMUR FLUSH").await.unwrap();
    cluster.stop_node(1);
    cluster
        .edit_config(1, |doc| {
            let mut ids = toml_edit::Array::new();
            ids.push(source);
            doc["replication"]["trusted_snapshot_sources"] = toml_edit::value(ids);
        })
        .unwrap();
    cluster.start_node(1).await.unwrap();
    let mut expected = Vec::new();
    for id in 0..12i64 {
        let title = format!("snapshot-row-{id:02}");
        cluster
            .node(0)
            .execute(
                "INSERT INTO records(id,title,score) VALUES($1,$2,$3)",
                &[&id, &title, &(id as f64 / 2.0)],
            )
            .await
            .unwrap();
        cluster.node(0).simple("MURMUR FLUSH").await.unwrap();
        expected.push(vec![id.to_string(), title]);
    }
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let collected = cluster
        .node(0)
        .simple("MURMUR RETENTION COLLECT")
        .await
        .unwrap();
    assert!(
        collected[0][0].parse::<u64>().unwrap() > 0,
        "retention did not prune source log history"
    );
    let before = cluster
        .node(1)
        .simple("MURMUR SHOW REPLICATION")
        .await
        .unwrap();
    // Bootstrap discovery is config-owned, so reopen the receiver with a donor.
    cluster.stop_node(1);
    cluster.set_peers(1, &[0]).unwrap();
    cluster.start_node(1).await.unwrap();
    assert_rows(
        &cluster,
        &[1],
        "SELECT id,title FROM records ORDER BY id",
        &expected,
    )
    .await;
    let after = cluster
        .node(1)
        .simple("MURMUR SHOW REPLICATION")
        .await
        .unwrap();
    assert_eq!(
        before[0][5], after[0][5],
        "receiver used log replay rather than the retained snapshot"
    );
    let keys = cluster
        .node(0)
        .simple("SELECT id,id_murmur_primary_key FROM records ORDER BY id")
        .await
        .unwrap();
    assert_eq!(
        cluster
            .node(1)
            .simple("SELECT id,id_murmur_primary_key FROM records ORDER BY id")
            .await
            .unwrap(),
        keys
    );
    cluster.node(1).simple("MURMUR FLUSH").await.unwrap();
    cluster.stop_node(1);
    cluster.start_node(1).await.unwrap();
    assert_rows(
        &cluster,
        &[1],
        "SELECT id,title FROM records ORDER BY id",
        &expected,
    )
    .await;
}

// File handles remain on this Client's one physical SQL connection.
async fn file_bytes(client: &murmur_client::Client, name: &str) -> murmur_client::Result<Vec<u8>> {
    let opened = client.query("MURMUR FILE DOWNLOAD $1", &[&name]).await?;
    let token: String = opened[0].get(0);
    let result = async {
        let mut data = Vec::new();
        loop {
            let page = client.query("MURMUR FILE DOWNLOAD NEXT $1 LIMIT 65536", &[&token]).await?;
            let offset: i64 = page[0].get(0);
            let total: i64 = page[0].get(1);
            let chunk: &[u8] = page[0].get(2);
            assert_eq!(offset as usize, data.len());
            data.extend_from_slice(chunk);
            let eof: bool = page[0].get(3);
            if eof { assert_eq!(data.len(), total as usize); return Ok(data); }
            assert!(!chunk.is_empty());
        }
    }.await;
    client.execute("MURMUR FILE CLOSE $1", &[&token]).await?;
    result
}
