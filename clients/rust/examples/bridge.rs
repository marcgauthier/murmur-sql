#[path = "../support/mod.rs"]
mod support;
use murmur_client::Client;

#[tokio::main]
async fn main() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = support::replication_options(dir.path(), "");
    // Scratch high-receiver: no bundles are exchanged, so trust values are
    // fixed test constants; domain must equal the node's own database id.
    let domain = opts.database_id.clone();
    opts.extra_toml.push_str(&format!(
        "[replication.bridge]\nrole = \"high-receiver\"\ndomain = \"{domain}\"\n\
         trusted_domains = [\"{}\"]\ndirectory = \"{}\"\nstream = \"test-stream\"\n\
         recipient = \"{}\"\nreceiver_secret_hex = \"{}\"\n",
        "11".repeat(16),
        dir.path().join("staging").display(),
        "ee".repeat(32),
        "dd".repeat(32),
    ));
    let server = support::start(dir, opts);
    let db = Client::connect(&support::config(&server, "bridge-key"))
        .await
        .unwrap();
    db.execute(
        "CREATE TABLE notes(name TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL)",
        &[],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO notes VALUES ($1, $2)", &[&"r1", &"v1"])
        .await
        .unwrap();
    db.flush().await.unwrap();
    // The High write above claimed ownership of `title`: the first release
    // reports true and a second finds nothing owned.
    assert!(
        db.bridge_release("notes", "r1", Some("title"))
            .await
            .unwrap()
    );
    assert_eq!(
        db.bridge_release("notes", "r1", Some("title"))
            .await
            .unwrap_err()
            .code(),
        Some("22023")
    );
    assert_eq!(
        db.bridge_release("missing", "r1", None)
            .await
            .unwrap_err()
            .code(),
        Some("22023")
    );
}
