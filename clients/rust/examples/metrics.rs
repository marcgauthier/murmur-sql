//! SQLite and Spool operational metrics.
#[path = "../support/mod.rs"]
mod support;

use murmur_client::Client;

#[tokio::main]
async fn main() {
    let server = support::start_default();
    let db = Client::connect(&support::config(&server, "example-key"))
        .await
        .unwrap();
    let all = db.metrics("").await.unwrap();
    let sqlite = db.metrics("sqlite").await.unwrap();
    assert!(!sqlite.is_empty() && sqlite.len() <= all.len());
    println!("metrics: {} total, {} sqlite", all.len(), sqlite.len());
}
