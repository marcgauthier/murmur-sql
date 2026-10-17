#[path = "../support/mod.rs"]
mod support;
use murmur_client::Client;

#[tokio::main]
async fn main() {
    let server = support::start_default();
    let db = Client::connect(&support::config(&server, "old-key"))
        .await
        .unwrap();
    db.simple("CREATE TABLE records(id INTEGER PRIMARY KEY); INSERT INTO records VALUES(1)")
        .await
        .unwrap();
    db.rotate_key("old-key", "new-key").await.unwrap();
    let fresh = Client::connect(&support::config(&server, "new-key"))
        .await
        .unwrap();
    assert_eq!(
        fresh.simple("SELECT id FROM records").await.unwrap()[0][0],
        "1"
    );
    assert_eq!(
        Client::connect(&support::config(&server, "old-key"))
            .await
            .err()
            .unwrap()
            .code(),
        Some("28000")
    );
}
