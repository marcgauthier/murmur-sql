//! Save, list, download, upload, and restore a backup.
#[path = "../support/mod.rs"]
mod support;

use murmur_client::Client;

#[tokio::main]
async fn main() {
    let server = support::start_default();
    let cfg = support::config(&server, "example-key");
    let db = Client::connect(&cfg).await.unwrap();
    db.execute("CREATE TABLE notes(id INTEGER PRIMARY KEY, body TEXT)", &[])
        .await
        .unwrap();
    db.execute("INSERT INTO notes VALUES (1, 'kept')", &[])
        .await
        .unwrap();
    let saved = db.backup_save().await.unwrap();
    let mut buf = Vec::new();
    db.backup_download(&saved.name, &mut buf).await.unwrap();
    assert_eq!(buf.len() as i64, saved.size);
    db.backup_upload("copy.murmur-backup", &buf[..])
        .await
        .unwrap();
    assert!(
        db.backup_list()
            .await
            .unwrap()
            .iter()
            .any(|f| f.name == "copy.murmur-backup")
    );
    db.execute("DELETE FROM notes", &[]).await.unwrap();
    db.restore(&saved.name, &cfg.key).await.unwrap();
    let fresh = Client::connect(&cfg).await.unwrap();
    assert_eq!(
        fresh.simple("SELECT body FROM notes").await.unwrap(),
        vec![vec!["kept".to_string()]]
    );
    println!("backup: verified save, transfer, and restore");
}
