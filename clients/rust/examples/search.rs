#[path = "../support/mod.rs"]
mod support;
use murmur_client::{Client, VectorDistance, VectorOptions};

#[tokio::main]
pub async fn main() {
    let server = support::start_default();
    let db = Client::connect(&support::config(&server, "search-key"))
        .await
        .unwrap();
    db.simple("CREATE TABLE documents(id INTEGER PRIMARY KEY, body TEXT, embedding BLOB)")
        .await
        .unwrap();
    let vector: Vec<u8> = [1.0f32, 0.0]
        .into_iter()
        .flat_map(f32::to_le_bytes)
        .collect();
    db.execute(
        "INSERT INTO documents VALUES($1,$2,$3)",
        &[&1i64, &"hello SQLite", &vector],
    )
    .await
    .unwrap();
    db.set_column_fts("documents", "body", true).await.unwrap();
    db.set_column_vector(
        "documents",
        "embedding",
        Some(VectorOptions {
            dimensions: 2,
            distance: VectorDistance::L2,
        }),
    )
    .await
    .unwrap();
    db.refresh_indexes().await.unwrap();
    let fts = db
        .column_fts()
        .await
        .unwrap()
        .into_iter()
        .find(|c| c.enabled)
        .unwrap()
        .status
        .unwrap();
    assert!(fts.ready && fts.error.is_none());
    let ident = format!("\"{}\"", fts.index_name.replace('"', "\"\""));
    let rows = db
        .query(
            &format!("SELECT rowid FROM temp.{ident} WHERE {ident} MATCH $1"),
            &[&"SQLite"],
        )
        .await
        .unwrap();
    assert_eq!(rows[0].get::<_, i64>(0), 1);
    let vector = db
        .column_vectors()
        .await
        .unwrap()
        .into_iter()
        .find(|c| c.options.is_some())
        .unwrap();
    assert_eq!(vector.options.unwrap().dimensions, 2);
    let status = vector.status.unwrap();
    assert!(status.ready);
    let ident = format!("\"{}\"", status.index_name.replace('"', "\"\""));
    let rows = db.query(&format!("SELECT rowid,distance FROM temp.{ident} WHERE embedding MATCH $1 AND k=$2 ORDER BY distance"), &[&"[1,0]", &1i64]).await.unwrap();
    assert_eq!(rows[0].get::<_, i64>(0), 1);
    assert_eq!(rows[0].get::<_, f64>(1), 0.0);
    db.set_column_fts("documents", "body", false).await.unwrap();
    db.set_column_vector("documents", "embedding", None)
        .await
        .unwrap();
    assert!(
        db.column_fts()
            .await
            .unwrap()
            .iter()
            .all(|c| c.status.is_none())
    );
    assert!(
        db.column_vectors()
            .await
            .unwrap()
            .iter()
            .all(|c| c.options.is_none() && c.status.is_none())
    );
    assert!(
        db.set_column_vector(
            "documents",
            "embedding",
            Some(VectorOptions {
                dimensions: 0,
                distance: VectorDistance::L2
            })
        )
        .await
        .is_err()
    );
    db.flush().await.unwrap();
}
