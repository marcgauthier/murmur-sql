//! Replication flags, UUIDv5 keys, populated conversions, and flush.
#[path = "../support/mod.rs"]
mod support;

use murmur_client::Client;

#[tokio::main]
async fn main() {
    let server = support::start_default();
    let db = Client::connect(&support::config(&server, "example-key"))
        .await
        .unwrap();
    db.execute(
        "CREATE TABLE orders(id INTEGER PRIMARY KEY, v TEXT NOT NULL)",
        &[],
    )
    .await
    .unwrap();
    let flags = db.table_replication().await.unwrap();
    assert!(
        !flags
            .iter()
            .find(|f| f.table == "orders")
            .unwrap()
            .replicated
    );
    db.set_table_replicated("orders", true).await.unwrap();
    let first: String = db
        .query(
            "INSERT INTO orders(id, v) VALUES($1, $2) RETURNING id_murmur_primary_key",
            &[&1i64, &"replicated"],
        )
        .await
        .unwrap()[0]
        .get(0);
    assert!(first.len() == 36 && first.as_bytes()[14] == b'5', "{first}");
    db.set_table_replicated("orders", false).await.unwrap();
    db.set_table_replicated("orders", true).await.unwrap();
    let again: String = db
        .query(
            "SELECT id_murmur_primary_key FROM orders WHERE id = $1",
            &[&1i64],
        )
        .await
        .unwrap()[0]
        .get(0);
    assert_eq!(first, again);
    db.flush().await.unwrap();
    println!("tables: verified flags, UUIDv5 keys, populated conversions and flush");
}
