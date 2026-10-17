//! Production PostgreSQL-wire endpoints require a trusted client certificate.
use murmur_client::{Client, TlsConfig, TlsMode};
use murmur_live_harness::{toml_edit, Cluster, ClusterOptions};

#[tokio::test(flavor = "multi_thread")]
async fn sql_mtls_rejects_missing_and_untrusted_certificates_and_keeps_valid_clients_working() {
    let mut cluster = Cluster::new(ClusterOptions {
        name: "api-mtls".into(),
        num_nodes: 1,
        ..Default::default()
    })
    .await
    .unwrap();
    cluster.node(0).simple("CREATE TABLE records(id INTEGER PRIMARY KEY,body TEXT); INSERT INTO records VALUES(1,'before-tls')").await.unwrap();
    cluster.node(0).flush().await.unwrap();
    cluster.stop_node(0);
    let dir = cluster.node_info(0).dir.clone();
    let cert = dir.join("cert.pem").to_string_lossy().to_string();
    let key = dir.join("key.pem").to_string_lossy().to_string();
    let ca = dir.join("ca.pem").to_string_lossy().to_string();
    cluster
        .edit_config(0, |doc| {
            doc["server"]["production"] = toml_edit::value(true);
            doc["server"]["tls"]["enabled"] = toml_edit::value(true);
            doc["server"]["tls"]["cert_file"] = toml_edit::value(cert.clone());
            doc["server"]["tls"]["key_file"] = toml_edit::value(key.clone());
            doc["server"]["tls"]["ca_file"] = toml_edit::value(ca.clone());
        })
        .unwrap();
    cluster.set_client_tls(
        0,
        TlsConfig {
            mode: TlsMode::VerifyFull,
            ca_file: ca,
            cert_file: cert,
            key_file: key,
        },
    );
    cluster.start_node(0).await.unwrap();
    let valid = cluster.config(0);
    assert_eq!(
        cluster
            .node(0)
            .simple("SELECT body FROM records")
            .await
            .unwrap(),
        vec![vec!["before-tls".to_string()]]
    );
    let mut plaintext = valid.clone();
    plaintext.tls = TlsConfig::default();
    assert!(
        Client::connect(&plaintext).await.is_err(),
        "production accepted plaintext SQL"
    );
    let mut missing = valid.clone();
    missing.tls.cert_file.clear();
    missing.tls.key_file.clear();
    assert!(
        Client::connect(&missing).await.is_err(),
        "production accepted no client certificate"
    );
    let mut params = rcgen::CertificateParams::new(vec!["rogue-client".into()]).unwrap();
    params.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ClientAuth];
    let rogue_key = rcgen::KeyPair::generate().unwrap();
    let rogue = params.self_signed(&rogue_key).unwrap();
    let rogue_cert = dir.join("untrusted-cert.pem");
    let rogue_key_path = dir.join("untrusted-key.pem");
    std::fs::write(&rogue_cert, rogue.pem()).unwrap();
    std::fs::write(&rogue_key_path, rogue_key.serialize_pem()).unwrap();
    let mut untrusted = valid.clone();
    untrusted.tls.cert_file = rogue_cert.to_string_lossy().into();
    untrusted.tls.key_file = rogue_key_path.to_string_lossy().into();
    assert!(
        Client::connect(&untrusted).await.is_err(),
        "untrusted client certificate accepted"
    );
    let mut bad_password = valid.clone();
    bad_password.password = "wrong-mtls-password".into();
    let error = match Client::connect(&bad_password).await {
        Err(e) => e,
        Ok(_) => panic!("certificate bypassed password auth"),
    };
    assert_eq!(error.code(), Some("28P01"));
    let mut bad_key = valid.clone();
    bad_key.key = "wrong-mtls-unlock-key".into();
    let error = match Client::connect(&bad_key).await {
        Err(e) => e,
        Ok(_) => panic!("certificate bypassed unlock"),
    };
    assert_eq!(error.code(), Some("28000"));
    let fresh = Client::connect(&valid).await.unwrap();
    fresh
        .simple("INSERT INTO records VALUES(2,'after-rejections')")
        .await
        .unwrap();
    fresh.flush().await.unwrap();
    drop(fresh);
    cluster.stop_node(0);
    cluster.start_node(0).await.unwrap();
    assert_eq!(
        cluster
            .node(0)
            .simple("SELECT body FROM records ORDER BY id")
            .await
            .unwrap(),
        vec![
            vec!["before-tls".to_string()],
            vec!["after-rejections".to_string()]
        ]
    );
}
