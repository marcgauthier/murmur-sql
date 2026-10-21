//! Managed search (FTS + vectors) and JSON stay correct across the mesh:
//! declarations travel with replicated tables, MATCH/KNN/JSON queries work
//! on a replica, refresh tracks replicated updates, disable/enable round
//! trips, and indexes rebuild from rows after a restart.
use murmur_live_harness::{Cluster, ClusterOptions};
use std::time::{Duration, Instant};

const WAIT: Duration = Duration::from_secs(60);
const POLL: Duration = Duration::from_millis(100);

fn f32le(values: &[f32]) -> Vec<u8> {
    values.iter().flat_map(|v| v.to_le_bytes()).collect()
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Ready FTS index table name for `column`, or None while not indexed.
async fn fts_index(cluster: &Cluster, node: usize, column: &str) -> Option<String> {
    let rows = cluster
        .node(node)
        .query("MURMUR SHOW COLUMNS FTS", &[])
        .await
        .ok()?;
    for row in &rows {
        let table: String = row.try_get(0).ok()?;
        let col: String = row.try_get(1).ok()?;
        let enabled: String = row.try_get(2).ok()?;
        let name: Option<String> = row.try_get(3).ok()?;
        let ready: Option<String> = row.try_get(4).ok()?;
        if table == "docs" && col == column && enabled == "true" && ready.as_deref() == Some("true") {
            return name;
        }
    }
    None
}

/// Ready vector index table name for `column`, or None while not indexed.
async fn vector_index(cluster: &Cluster, node: usize, column: &str) -> Option<String> {
    let rows = cluster
        .node(node)
        .query("MURMUR SHOW COLUMNS VECTOR", &[])
        .await
        .ok()?;
    for row in &rows {
        let table: String = row.try_get(0).ok()?;
        let col: String = row.try_get(1).ok()?;
        let enabled: String = row.try_get(2).ok()?;
        let name: Option<String> = row.try_get(5).ok()?;
        let ready: Option<String> = row.try_get(6).ok()?;
        if table == "docs" && col == column && enabled == "true" && ready.as_deref() == Some("true") {
            return name;
        }
    }
    None
}

async fn row_count(cluster: &Cluster, node: usize) -> i64 {
    cluster
        .node(node)
        .query("SELECT count(*) FROM docs", &[])
        .await
        .ok()
        .and_then(|rows| rows.first()?.try_get::<_, i64>(0).ok())
        .unwrap_or(-1)
}

async fn wait_rows(cluster: &Cluster, node: usize, want: i64, what: &str) {
    let deadline = Instant::now() + WAIT;
    while row_count(cluster, node).await != want {
        assert!(Instant::now() < deadline, "{what} did not converge");
        tokio::time::sleep(POLL).await;
    }
}

async fn wait_index(
    cluster: &Cluster,
    node: usize,
    column: &str,
    vector: bool,
    what: &str,
) -> String {
    let deadline = Instant::now() + WAIT;
    loop {
        let found = if vector {
            vector_index(cluster, node, column).await
        } else {
            fts_index(cluster, node, column).await
        };
        if let Some(name) = found {
            return name;
        }
        assert!(Instant::now() < deadline, "{what} never became ready");
        tokio::time::sleep(POLL).await;
    }
}

/// Wait until `column`'s FTS declaration reads back as disabled.
async fn wait_disabled(cluster: &Cluster, node: usize, column: &str, what: &str) {
    let deadline = Instant::now() + WAIT;
    loop {
        let rows = cluster
            .node(node)
            .query("MURMUR SHOW COLUMNS FTS", &[])
            .await
            .unwrap_or_default();
        let disabled = rows.iter().any(|row| {
            row.try_get::<_, String>(0).ok().as_deref() == Some("docs")
                && row.try_get::<_, String>(1).ok().as_deref() == Some(column)
                && row.try_get::<_, String>(2).ok().as_deref() == Some("false")
        });
        if disabled {
            return;
        }
        assert!(Instant::now() < deadline, "{what} did not converge");
        tokio::time::sleep(POLL).await;
    }
}

/// MATCH row ids, or None while the index rejects reads. Search is
/// eventually consistent: an unavailable or rebuilding index may reject
/// reads, so callers must retry (see docs/USAGE.md "Full-text search").
async fn match_ids(
    cluster: &Cluster,
    node: usize,
    index: &str,
    query: &str,
) -> Option<Vec<i64>> {
    let ident = quote_ident(index);
    let rows = cluster
        .node(node)
        .query(
            &format!("SELECT d.id FROM docs d JOIN temp.{ident} f ON f.rowid=d.id WHERE {ident} MATCH $1 ORDER BY d.id"),
            &[&query],
        )
        .await
        .ok()?;
    rows.iter()
        .map(|row| row.try_get::<_, i64>(0).ok())
        .collect::<Option<Vec<_>>>()
}

async fn wait_match(
    cluster: &Cluster,
    node: usize,
    index: &str,
    query: &str,
    want: &[i64],
    what: &str,
) {
    let deadline = Instant::now() + WAIT;
    loop {
        if match_ids(cluster, node, index, query).await.as_deref() == Some(want) {
            return;
        }
        assert!(Instant::now() < deadline, "{what} did not converge");
        tokio::time::sleep(POLL).await;
    }
}

/// KNN `(rowid, distance)` hits, or None while the index rejects reads.
async fn knn_hits(
    cluster: &Cluster,
    node: usize,
    index: &str,
    query: &str,
    k: i64,
) -> Option<Vec<(i64, f64)>> {
    let ident = quote_ident(index);
    let rows = cluster
        .node(node)
        .query(
            &format!("SELECT rowid,distance FROM temp.{ident} WHERE embedding MATCH $1 AND k=$2 ORDER BY distance"),
            &[&query, &k],
        )
        .await
        .ok()?;
    rows.iter()
        .map(|row| Some((row.try_get::<_, i64>(0).ok()?, row.try_get::<_, f64>(1).ok()?)))
        .collect::<Option<Vec<_>>>()
}

async fn wait_knn(
    cluster: &Cluster,
    node: usize,
    index: &str,
    query: &str,
    k: i64,
    want_first: i64,
    want_len: usize,
    what: &str,
) -> Vec<(i64, f64)> {
    let deadline = Instant::now() + WAIT;
    loop {
        if let Some(hits) = knn_hits(cluster, node, index, query, k).await {
            if hits.len() == want_len && hits.first().map(|h| h.0) == Some(want_first) {
                return hits;
            }
        }
        assert!(Instant::now() < deadline, "{what} did not converge");
        tokio::time::sleep(POLL).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn managed_search_and_json_hold_across_the_mesh() {
    let mut cluster = Cluster::new(ClusterOptions {
        name: "search-mesh".into(),
        num_nodes: 2,
        ..Default::default()
    })
    .await
    .unwrap();
    // Appoint node 0 the schema master before any schema exists: without a
    // single schema authority, peers adopt each other's manifests in arrival
    // order and a declaration change can be wiped back by the replica's
    // stale manifest.
    let master = cluster.node_info(0).node_id.clone();
    cluster.stop_node(0);
    cluster.stop_node(1);
    cluster
        .edit_config(0, |doc| {
            doc["replication"]["master"] = toml_edit::value(true);
            doc["replication"]["schema_master"] = toml_edit::value(master.as_str());
        })
        .unwrap();
    cluster
        .edit_config(1, |doc| {
            doc["replication"]["schema_master"] = toml_edit::value(master.as_str());
        })
        .unwrap();
    cluster.start_node(0).await.unwrap();
    cluster.start_node(1).await.unwrap();
    // Single schema writer: node 0 creates the replicated table and all
    // search declarations. Creating the same table on both nodes makes two
    // schema writers whose manifests overwrite each other's column flags in
    // arrival order, so the replica adopts the table (and its declarations)
    // instead of creating a competing one. Declaration changes propagate
    // when subsequent row batches carry the new schema digest, so every
    // declaration change below is followed by a row write.
    cluster.node(0).simple("CREATE TABLE IF NOT EXISTS docs(id INTEGER PRIMARY KEY, title TEXT, body TEXT, embedding BLOB, meta TEXT)").await.unwrap();
    cluster.node(0).simple("MURMUR SET TABLE docs REPLICATED true").await.unwrap();
    cluster.node(0).simple("MURMUR SET COLUMN 'docs' 'body' FTS true").await.unwrap();
    cluster
        .node(0)
        .simple("MURMUR SET COLUMN 'docs' 'embedding' VECTOR true DIMENSIONS 2 DISTANCE l2")
        .await
        .unwrap();
    cluster.node(0).simple("MURMUR FLUSH").await.unwrap();
    cluster.node(0).simple("MURMUR REFRESH INDEXES").await.unwrap();
    wait_index(&cluster, 0, "body", false, "node0 FTS declaration").await;

    let first = f32le(&[1.0, 0.0]);
    let second = f32le(&[0.0, 1.0]);
    cluster
        .node(0)
        .execute(
            "INSERT INTO docs VALUES($1,$2,$3,$4,$5)",
            &[&1i64, &"SQLite", &"fast search", &first, &r#"{"lang":"en","tags":["db"]}"#],
        )
        .await
        .unwrap();
    cluster
        .node(0)
        .execute(
            "INSERT INTO docs VALUES($1,$2,$3,$4,$5)",
            &[&2i64, &"Notes", &"other text", &second, &r#"{"lang":"en","tags":["misc"]}"#],
        )
        .await
        .unwrap();
    cluster.node(0).simple("MURMUR FLUSH").await.unwrap();
    // Row sync proves the replica adopted the table; the requested schema
    // manifest carries the search declarations with it.
    wait_rows(&cluster, 1, 2, "replica row sync").await;
    wait_index(&cluster, 1, "body", false, "replica FTS declaration").await;
    wait_index(&cluster, 1, "embedding", true, "replica vector declaration").await;
    cluster.node(1).simple("MURMUR REFRESH INDEXES").await.unwrap();

    // FTS MATCH works on the replica over replicated rows.
    let fts = wait_index(&cluster, 1, "body", false, "replica FTS index").await;
    wait_match(&cluster, 1, &fts, "fast AND search", &[1], "replica MATCH").await;

    // Vector KNN on the replica: exact hit first, sqrt(2) neighbor second.
    let vectors = wait_index(&cluster, 1, "embedding", true, "replica vector index").await;
    let hits = wait_knn(&cluster, 1, &vectors, "[1,0]", 2, 1, 2, "replica KNN").await;
    assert_eq!(hits[0].1, 0.0);
    assert_eq!(hits[1].0, 2);
    assert!((hits[1].1 - 2f64.sqrt()).abs() < 1e-5, "neighbor distance {}", hits[1].1);

    // JSON operators read replicated JSONB-shaped text on the replica.
    let rows = cluster
        .node(1)
        .query("SELECT json_extract(meta,'$.lang'), meta ->> '$.tags[0]' FROM docs WHERE id = 1", &[])
        .await
        .unwrap();
    assert_eq!(rows[0].try_get::<_, String>(0).unwrap(), "en");
    assert_eq!(rows[0].try_get::<_, String>(1).unwrap(), "db");

    // Refresh tracks a replicated update on the replica's index.
    cluster.node(0).simple("UPDATE docs SET body='updated' WHERE id=1").await.unwrap();
    cluster.node(0).simple("MURMUR FLUSH").await.unwrap();
    // Confirm the replicated row content landed before querying the index.
    let deadline = Instant::now() + WAIT;
    loop {
        let rows = cluster.node(1).query("SELECT body FROM docs WHERE id=1", &[]).await.unwrap_or_default();
        if rows.first().and_then(|r| r.try_get::<_, String>(0).ok()).as_deref() == Some("updated") {
            break;
        }
        assert!(Instant::now() < deadline, "replicated UPDATE did not converge");
        tokio::time::sleep(POLL).await;
    }
    cluster.node(1).simple("MURMUR REFRESH INDEXES").await.unwrap();
    let fts = wait_index(&cluster, 1, "body", false, "post-update FTS index").await;
    wait_match(&cluster, 1, &fts, "updated", &[1], "MATCH after replicated update").await;

    // Disable on the schema writer drops the replica index once the
    // declaration propagates; re-enable rebuilds it. The meta updates only
    // carry the new schema digest to the replica.
    cluster.node(0).simple("MURMUR SET COLUMN 'docs' 'body' FTS false").await.unwrap();
    cluster.node(0).simple("UPDATE docs SET meta='{}' WHERE id=1").await.unwrap();
    cluster.node(0).simple("MURMUR FLUSH").await.unwrap();
    // Confirm the piggyback row arrived before waiting on the declaration.
    let deadline = Instant::now() + WAIT;
    loop {
        let rows = cluster.node(1).query("SELECT meta FROM docs WHERE id=1", &[]).await.unwrap_or_default();
        if rows.first().and_then(|r| r.try_get::<_, String>(0).ok()).as_deref() == Some("{}") {
            break;
        }
        assert!(Instant::now() < deadline, "disable piggyback UPDATE did not converge");
        tokio::time::sleep(POLL).await;
    }
    wait_disabled(&cluster, 1, "body", "replica FTS disable").await;
    assert!(match_ids(&cluster, 1, &fts, "updated").await.unwrap_or_default().is_empty());
    cluster.node(0).simple("MURMUR SET COLUMN 'docs' 'body' FTS true").await.unwrap();
    cluster.node(0).simple("UPDATE docs SET meta='{}' WHERE id=2").await.unwrap();
    cluster.node(0).simple("MURMUR FLUSH").await.unwrap();
    let deadline = Instant::now() + WAIT;
    loop {
        let rows = cluster.node(1).query("SELECT meta FROM docs WHERE id=2", &[]).await.unwrap_or_default();
        if rows.first().and_then(|r| r.try_get::<_, String>(0).ok()).as_deref() == Some("{}") {
            break;
        }
        assert!(Instant::now() < deadline, "re-enable piggyback UPDATE did not converge");
        tokio::time::sleep(POLL).await;
    }
    cluster.node(1).simple("MURMUR REFRESH INDEXES").await.unwrap();
    let fts = wait_index(&cluster, 1, "body", false, "re-enabled FTS index").await;
    wait_match(&cluster, 1, &fts, "updated", &[1], "MATCH after re-enable").await;

    // Indexes rebuild from rows across a replica restart.
    cluster.stop_node(1);
    cluster.start_node(1).await.unwrap();
    wait_rows(&cluster, 1, 2, "post-restart rows").await;
    cluster.node(1).simple("MURMUR REFRESH INDEXES").await.unwrap();
    let fts = wait_index(&cluster, 1, "body", false, "post-restart FTS index").await;
    wait_match(&cluster, 1, &fts, "updated", &[1], "MATCH after restart").await;
    let vectors = wait_index(&cluster, 1, "embedding", true, "post-restart vector index").await;
    let hits = wait_knn(&cluster, 1, &vectors, "[1,0]", 1, 1, 1, "KNN after restart").await;
    assert_eq!(hits[0].0, 1);
}
