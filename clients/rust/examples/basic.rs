//! Connect, unlock, and run CRUD with parameters.
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
        "CREATE TABLE greetings(id INTEGER PRIMARY KEY, lang TEXT NOT NULL, text TEXT NOT NULL)",
        &[],
    )
    .await
    .unwrap();
    for (i, (lang, text)) in [("en", "hello"), ("fr", "bonjour"), ("es", "hola")]
        .iter()
        .enumerate()
    {
        db.execute(
            "INSERT INTO greetings VALUES ($1, $2, $3)",
            &[&(i as i64 + 1), lang, text],
        )
        .await
        .unwrap();
    }
    let rows = db
        .query(
            "SELECT lang, text FROM greetings WHERE id >= $1 ORDER BY id",
            &[&1i64],
        )
        .await
        .unwrap();
    for row in &rows {
        println!("{}: {}", row.get::<_, String>(0), row.get::<_, String>(1));
    }
    assert_eq!(rows.len(), 3);
    println!("basic: verified 3 rows");
}
