//! Verified TLS against a minted test PKI.
#[path = "../support/mod.rs"]
mod support;

use murmur_client::{Client, TlsConfig, TlsMode};

#[tokio::main]
async fn main() {
    let dir = tempfile::tempdir().unwrap();
    let tls = support::write_tls(dir.path());
    let ca_file = tls.ca.to_string_lossy().to_string();
    let server = support::start(
        dir,
        support::Options {
            tls: Some(tls),
            ..Default::default()
        },
    );
    let mut cfg = support::config(&server, "example-key");
    cfg.tls = TlsConfig {
        mode: TlsMode::VerifyFull,
        ca_file,
        ..Default::default()
    };
    let db = Client::connect(&cfg).await.unwrap();
    assert_eq!(
        db.simple("SELECT 1").await.unwrap(),
        vec![vec!["1".to_string()]]
    );
    println!("tls: verified verify-full connection");
}
