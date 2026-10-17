//! Encrypted binary upload/download and LIKE cursor search.
#[path = "../support/mod.rs"]
mod support;

use murmur_client::Client;

#[tokio::main]
async fn main() {
    let dir = tempfile::tempdir().unwrap();
    let opts = support::file_options(dir.path());
    let server = support::start(dir, opts);
    let db = Client::connect(&support::config(&server, "files-example"))
        .await
        .unwrap();
    let payload: Vec<u8> = [0u8, 1, 255, 128].repeat(40_000);
    let info = db
        .upload_file("reports/sample.bin", &payload[..])
        .await
        .unwrap();
    let found = db
        .search_files("reports/%.bin")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(found.len(), 1);
    assert!(found[0].name == info.name && found[0].size == payload.len() as i64 && found[0].local);
    let mut out = Vec::new();
    db.download_file(&info.name, &mut out).await.unwrap();
    assert_eq!(out, payload);
    println!(
        "Verified {}: {} bytes, SHA256 {}",
        info.name, info.size, info.sha256
    );
}
