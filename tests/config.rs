//! `config.toml` loading against the shipped example and temp copies.
use murmur_sql::config::{Config, save_table_flag};

#[test]
fn example_config_loads_with_documented_defaults() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("config.example.toml");
    let config = Config::load(&path).unwrap();
    assert_eq!(config.server.bind, "127.0.0.1:5432");
    assert_eq!(config.storage.data_dir, "./data");
    assert_eq!(config.logging.retention_days, 14);
    assert_eq!(config.logging.rotation, "daily");
    assert!(!config.replication.enabled);
    assert_eq!(config.backups.dir, "./backups");
    let options = config.engine.to_options().unwrap();
    assert_eq!(options.queue_bytes, 64 << 20);
}

#[test]
fn bridge_example_roles_validate() {
    use murmur_sql::config::ReplicationBridgeConfig;
    // Mirrors the low-exporter / high-receiver blocks in config.example.toml.
    let low = ReplicationBridgeConfig {
        role: "low-exporter".into(),
        domain: "11".repeat(16),
        directory: "./low-outbox".into(),
        stream: "main".into(),
        recipient: "ee".repeat(32),
        signer_hex: Some("bb".repeat(32)),
        ..ReplicationBridgeConfig::default()
    };
    low.validate().unwrap();
    let mut high = ReplicationBridgeConfig {
        role: "high-receiver".into(),
        domain: "22".repeat(16),
        trusted_domains: vec!["11".repeat(16)],
        directory: "./high-inbox".into(),
        stream: "main".into(),
        recipient: "ee".repeat(32),
        receiver_secret_hex: Some("dd".repeat(32)),
        ..ReplicationBridgeConfig::default()
    };
    high.trusted_signers.insert("aa".repeat(16), "cc".repeat(32));
    high.trusted_origins.insert("aa".repeat(16), "cc".repeat(32));
    high.validate().unwrap();
    // Each role's requirements are enforced.
    let mut broken = high.clone();
    broken.receiver_secret_hex = None;
    assert!(broken.validate().is_err());
    broken = high.clone();
    broken.trusted_domains.clear();
    assert!(broken.validate().is_err());
    let mut broken = low.clone();
    broken.signer_hex = None;
    assert!(broken.validate().is_err());
}

#[test]
fn backups_dir_defaults_and_rejects_blank() {
    let config = Config::default();
    assert_eq!(config.backups.dir, "./backups");
    config.validate().unwrap();
    let mut blank = Config::default();
    blank.backups.dir = "  ".into();
    assert!(blank.validate().is_err());
}

#[test]
fn missing_file_and_bad_toml_fail_loudly() {
    let dir = tempfile::tempdir().unwrap();
    assert!(Config::load(dir.path().join("absent.toml")).is_err());
    let bad = dir.path().join("bad.toml");
    std::fs::write(&bad, "[server\nbind = ").unwrap();
    assert!(Config::load(&bad).is_err());
    let unknown = dir.path().join("unknown.toml");
    std::fs::write(&unknown, "[server]\nbind = \"127.0.0.1:1\"\nnope = 1\n").unwrap();
    assert!(Config::load(&unknown).is_err());
}

#[cfg(feature = "replication")]
#[test]
fn replication_builder_loads_credentials_and_maps_fields() {
    use murmur_sql::config::ReplicationConfig;
    let dir = tempfile::tempdir().unwrap();

    let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca_key = rcgen::KeyPair::generate().unwrap();
    let ca = ca_params.self_signed(&ca_key).unwrap();

    let node = [9u8; 16];
    let key = rcgen::KeyPair::generate().unwrap();
    let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    params.subject_alt_names = vec![rcgen::SanType::URI(
        format!("replicateddb://node/{}", murmur::replication::hex(&node)).try_into().unwrap(),
    )];
    let cert = params.signed_by(&key, &ca, &ca_key).unwrap();

    let chain_path = dir.path().join("chain.pem");
    let key_path = dir.path().join("key.pem");
    let ca_path = dir.path().join("ca.pem");
    std::fs::write(&chain_path, cert.pem()).unwrap();
    std::fs::write(&key_path, key.serialize_pem()).unwrap();
    std::fs::write(&ca_path, ca.pem()).unwrap();

    let mut replication = ReplicationConfig::default();
    replication.enabled = true;
    replication.bind = "127.0.0.1:0".into();
    replication.bootstrap = vec!["127.0.0.1:9002".into()];
    replication.cert_chain_file = Some(chain_path.display().to_string());
    replication.private_key_file = Some(key_path.display().to_string());
    replication.ca_file = Some(ca_path.display().to_string());
    replication.signing_key_hex = Some("ab".repeat(32));
    replication.trusted_origins.insert("01".repeat(16), "cd".repeat(32));
    replication.allowed_peers = vec!["01".repeat(16)];
    replication.allowed_networks = vec!["10.0.0.0/8".into()];
    replication.merge_policies.insert("items.count".into(), "counter".into());
    replication.validate().unwrap();

    let built = replication.to_replication_config().unwrap();
    assert_eq!(built.bootstrap.len(), 1);
    assert_eq!(built.credentials.chain.len(), 1);
    assert_eq!(built.credentials.roots.len(), 1);
    assert!(!built.credentials.private_key.is_empty());
    assert_eq!(*built.signing_key, [0xab; 32]);
    assert!(built.trusted_origins.contains_key(&[0x01; 16]));
    assert!(built.allowed_peers.contains(&[0x01; 16]));
    assert_eq!(built.allowed_networks.len(), 1);
    assert_eq!(
        built.merge_policies.get(&("items".to_owned(), "count".to_owned())),
        Some(&murmur::replication::policies::Policy::Counter)
    );

    // Missing credential files fail the build, not validation.
    replication.cert_chain_file = Some(dir.path().join("absent.pem").display().to_string());
    assert!(replication.to_replication_config().is_err());
}

#[test]
fn table_flag_round_trip_on_example_copy() {
    let dir = tempfile::tempdir().unwrap();
    let example = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("config.example.toml");
    let path = dir.path().join("config.toml");
    std::fs::copy(&example, &path).unwrap();
    save_table_flag(&path, "audit", false).unwrap();
    let config = Config::load(&path).unwrap();
    assert_eq!(config.replication.tables.get("audit"), Some(&false));
    // Unrelated sections survive the rewrite.
    assert_eq!(config.server.bind, "127.0.0.1:5432");
    assert_eq!(config.engine.snapshot_deltas, 128);
}
