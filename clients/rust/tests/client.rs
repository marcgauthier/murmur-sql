#![allow(clippy::duplicate_mod)]
#[path = "../support/mod.rs"]
mod support;

use murmur_client::{Client, Config, TlsConfig, TlsMode};
use tokio_postgres::error::SqlState;

#[test]
fn config_validation() {
    let mut c = Config::new("", "p", "k");
    assert!(matches!(
        tokio_rt(Client::connect(&c)),
        Err(murmur_client::Error::Invalid(_))
    ));
    c = Config::new("u", "p", "k");
    c.tls = TlsConfig {
        mode: TlsMode::VerifyFull,
        ..Default::default()
    };
    assert!(matches!(
        tokio_rt(Client::connect(&c)),
        Err(murmur_client::Error::Invalid(_))
    ));
    c.tls = TlsConfig {
        cert_file: "x".into(),
        ..Default::default()
    };
    assert!(matches!(
        tokio_rt(Client::connect(&c)),
        Err(murmur_client::Error::Invalid(_))
    ));
}

fn tokio_rt<F: std::future::Future>(f: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(f)
}

#[tokio::test]
async fn crud_and_transaction_noops() {
    let server = support::start_default();
    let db = Client::connect(&support::config(&server, "test-key"))
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
    let got: Vec<(String, String)> = rows.iter().map(|r| (r.get(0), r.get(1))).collect();
    assert_eq!(got[0], ("en".into(), "hello".into()));
    assert_eq!(got.len(), 3);

    let err = db
        .execute(
            "INSERT INTO greetings VALUES ($1, $2, $3)",
            &[&1i64, &"de", &"hallo"],
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("23505"));
    assert_eq!(err.code(), Some(SqlState::UNIQUE_VIOLATION.code()));

    // BEGIN/COMMIT/ROLLBACK are accepted no-ops: each statement autocommits.
    db.simple("BEGIN").await.unwrap();
    db.execute("DELETE FROM greetings WHERE id = $1", &[&3i64])
        .await
        .unwrap();
    db.simple("ROLLBACK").await.unwrap();
    let n = db.simple("SELECT count(*) FROM greetings").await.unwrap();
    assert_eq!(n, vec![vec!["2".to_string()]]);
    db.flush().await.unwrap();
}

#[tokio::test]
async fn wrong_key_is_rejected() {
    let server = support::start_default();
    drop(
        Client::connect(&support::config(&server, "right"))
            .await
            .unwrap(),
    );
    let err = Client::connect(&support::config(&server, "wrong"))
        .await
        .err()
        .unwrap();
    assert_eq!(err.code(), Some("28000"));
}

#[tokio::test]
async fn table_replication_and_metrics() {
    let server = support::start_default();
    let db = Client::connect(&support::config(&server, "k"))
        .await
        .unwrap();
    db.execute(
        "CREATE TABLE orders(id INTEGER PRIMARY KEY, v TEXT NOT NULL)",
        &[],
    )
    .await
    .unwrap();
    let flag = |flags: Vec<murmur_client::TableFlag>| {
        flags
            .into_iter()
            .find(|f| f.table == "orders")
            .unwrap()
            .replicated
    };
    assert!(!flag(db.table_replication().await.unwrap()));
    db.set_table_replicated("orders", true).await.unwrap();
    assert!(flag(db.table_replication().await.unwrap()));
    let first: String = db
        .query(
            "INSERT INTO orders(id, v) VALUES($1, $2) RETURNING id_murmur_primary_key",
            &[&1i64, &"x"],
        )
        .await
        .unwrap()[0]
        .get(0);
    assert_eq!(first.len(), 36);
    assert_eq!(first.as_bytes()[14], b'5');
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
    let all = db.metrics("").await.unwrap();
    let sqlite = db.metrics("sqlite").await.unwrap();
    assert!(!sqlite.is_empty() && sqlite.len() <= all.len());
}

#[tokio::test]
async fn backup_roundtrip() {
    let server = support::start_default();
    let cfg = support::config(&server, "test-key");
    let db = Client::connect(&cfg).await.unwrap();
    db.execute("CREATE TABLE b(id INTEGER PRIMARY KEY, v TEXT)", &[])
        .await
        .unwrap();
    db.execute("INSERT INTO b VALUES (1, 'kept')", &[])
        .await
        .unwrap();
    let saved = db.backup_save().await.unwrap();
    assert!(saved.size > 0);
    assert!(
        db.backup_list()
            .await
            .unwrap()
            .iter()
            .any(|f| f.name == saved.name)
    );
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
    db.execute("DELETE FROM b", &[]).await.unwrap();
    db.restore(&saved.name, "test-key").await.unwrap();
    let fresh = Client::connect(&cfg).await.unwrap();
    assert_eq!(
        fresh.simple("SELECT v FROM b").await.unwrap(),
        vec![vec!["kept".to_string()]]
    );
    assert!(db.backup_download("../evil", Vec::new()).await.is_err());
}

#[tokio::test]
async fn encrypted_files() {
    let dir = tempfile::tempdir().unwrap();
    let opts = support::file_options(dir.path());
    let server = support::start(dir, opts);
    let db = Client::connect(&support::config(&server, "files-key"))
        .await
        .unwrap();
    let payload: Vec<u8> = [0u8, 1, 255, 128].repeat(40_000);
    let info = db
        .upload_file("reports/sample.bin", &payload[..])
        .await
        .unwrap();
    assert_eq!(info.size, payload.len() as i64);
    assert!(info.local);
    let found = db
        .search_files("reports/%.bin")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(found, vec![info.clone()]);
    let mut cursor = db.list_files().await.unwrap();
    assert_eq!(
        cursor.next().await.unwrap().unwrap().name,
        "reports/sample.bin"
    );
    assert!(cursor.next().await.unwrap().is_none());
    let mut out = Vec::new();
    db.download_file(&info.name, &mut out).await.unwrap();
    assert_eq!(out, payload);
}

#[tokio::test]
async fn tls_modes() {
    let dir = tempfile::tempdir().unwrap();
    let tls = support::write_tls(dir.path());
    let ca = tls.ca.to_string_lossy().to_string();
    let server = support::start(
        dir,
        support::Options {
            tls: Some(tls),
            ..Default::default()
        },
    );
    for mode in [TlsMode::Require, TlsMode::VerifyCa, TlsMode::VerifyFull] {
        let mut cfg = support::config(&server, "k");
        cfg.tls = TlsConfig {
            mode,
            ca_file: ca.clone(),
            ..Default::default()
        };
        let db = Client::connect(&cfg)
            .await
            .unwrap_or_else(|e| panic!("{mode:?}: {e}"));
        assert_eq!(
            db.simple("SELECT 1").await.unwrap(),
            vec![vec!["1".to_string()]]
        );
    }
}

#[tokio::test]
async fn replication_commands_need_enabled_replication() {
    let server = support::start_default();
    let db = Client::connect(&support::config(&server, "k"))
        .await
        .unwrap();
    for err in [
        db.peer_sync().await.unwrap_err(),
        db.peer_add("127.0.0.1:9").await.unwrap_err(),
        db.show_peers().await.unwrap_err(),
        db.show_replication().await.unwrap_err(),
        db.show_retention().await.unwrap_err(),
        db.retention_collect().await.unwrap_err(),
        db.policy_counter_add("t", "c", "k", 1).await.unwrap_err(),
        db.bridge_release("t", "k", None).await.unwrap_err(),
        db.bridge_release("t", "k", Some("c")).await.unwrap_err(),
    ] {
        assert_eq!(err.code(), Some("55000"));
    }
    // Client-side validation fails before any SQL is sent.
    assert!(matches!(
        db.peer_add("not-an-addr").await.unwrap_err(),
        murmur_client::Error::Invalid(_)
    ));
    assert!(matches!(
        db.peer_remove("short").await.unwrap_err(),
        murmur_client::Error::Invalid(_)
    ));
    assert!(matches!(
        db.peer_retire("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz")
            .await
            .unwrap_err(),
        murmur_client::Error::Invalid(_)
    ));
    assert!(matches!(
        db.policy_min(
            "stats",
            "lowest",
            "row-1",
            murmur_client::PolicyValue::Real(f64::NAN)
        )
        .await
        .unwrap_err(),
        murmur_client::Error::Invalid(_)
    ));
    assert!(matches!(
        db.bridge_release("", "k", None).await.unwrap_err(),
        murmur_client::Error::Invalid(_)
    ));
    assert!(matches!(
        db.bridge_release("t", "k", Some("  ")).await.unwrap_err(),
        murmur_client::Error::Invalid(_)
    ));
    // Server-side usage errors surface before the replication gate, even disabled.
    let err = db.simple("MURMUR PEER ADD 'bogus'").await.unwrap_err();
    assert_eq!(err.code(), Some("22023"));
}

#[tokio::test]
async fn peer_mesh_show_and_retention() {
    let dir0 = tempfile::tempdir().unwrap();
    let dir1 = tempfile::tempdir().unwrap();
    let mut opts = support::replication_mesh_options(&[dir0.path(), dir1.path()], "");
    let o1 = opts.pop().unwrap();
    let o0 = opts.pop().unwrap();
    let (node0, node1) = (o0.node_id.clone(), o1.node_id.clone());
    let s0 = support::start(dir0, o0);
    let s1 = support::start(dir1, o1);
    let db0 = Client::connect(&support::config(&s0, "mesh-key"))
        .await
        .unwrap();
    let db1 = Client::connect(&support::config(&s1, "mesh-key"))
        .await
        .unwrap();
    let st0 = db0.show_replication().await.unwrap();
    assert!(st0.running && st0.peers == 0);
    let addr1 = db1
        .show_replication()
        .await
        .unwrap()
        .address
        .expect("node1 repl addr");
    assert!(db0.show_peers().await.unwrap().is_empty());

    db0.peer_add(&addr1).await.unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    let peer = loop {
        let peers = db0.show_peers().await.unwrap();
        if let Some(p) = peers.iter().find(|p| p.node_id == node1 && p.connected) {
            break p.clone();
        }
        assert!(
            std::time::Instant::now() < deadline,
            "peer never connected: {peers:?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    };
    assert!(peer.sent_watermark >= 0);
    db0.peer_sync().await.unwrap();
    assert!(db0.show_replication().await.unwrap().peers >= 1);

    let collected = db0.retention_collect().await.unwrap();
    assert!(collected >= 0);
    for row in db0.show_retention().await.unwrap() {
        assert_eq!(row.node_id.len(), 32);
        assert!(row.collected_transactions >= 0);
    }

    db0.peer_remove(&node1).await.unwrap();
    db0.peer_retire(&node1).await.unwrap();
    let retention = db0.show_retention().await.unwrap();
    assert!(
        retention.iter().any(|r| r.node_id == node1 && r.retired),
        "retired peer missing from retention: {retention:?}"
    );
    // Retiring the local node is rejected.
    assert_eq!(
        db0.peer_retire(&node0).await.unwrap_err().code(),
        Some("22023")
    );
}

#[tokio::test]
async fn policy_crdt_operations() {
    let dir = tempfile::tempdir().unwrap();
    let policies = "merge_policies = { \"stats.count\" = \"counter\", \"stats.tags\" = \"set\", \
        \"stats.highest\" = \"max\", \"stats.lowest\" = \"min\" }\n";
    let opts = support::replication_options(dir.path(), policies);
    let server = support::start(dir, opts);
    let db = Client::connect(&support::config(&server, "policy-key"))
        .await
        .unwrap();
    db.execute(
        "CREATE TABLE stats(key TEXT NOT NULL PRIMARY KEY, count INTEGER NOT NULL, tags BLOB, highest INTEGER NOT NULL, lowest REAL NOT NULL)",
        &[],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO stats VALUES ($1, $2, NULL, $3, $4)",
        &[&"row-1", &10i64, &5i64, &2.5f64],
    )
    .await
    .unwrap();
    db.policy_counter_add("stats", "count", "row-1", 5)
        .await
        .unwrap();
    db.policy_set_add("stats", "tags", "row-1", "blue")
        .await
        .unwrap();
    db.policy_set_remove("stats", "tags", "row-1", "blue")
        .await
        .unwrap();
    db.policy_max("stats", "highest", "row-1", 42i64.into())
        .await
        .unwrap();
    db.policy_max("stats", "highest", "row-1", 1i64.into())
        .await
        .unwrap();
    db.policy_min(
        "stats",
        "lowest",
        "row-1",
        murmur_client::PolicyValue::Real(-1.5),
    )
    .await
    .unwrap();
    let row = db
        .query(
            "SELECT count, highest, lowest FROM stats WHERE key = $1",
            &[&"row-1"],
        )
        .await
        .unwrap();
    assert_eq!(row[0].get::<_, i64>(0), 15);
    assert_eq!(row[0].get::<_, i64>(1), 42);
    assert!((row[0].get::<_, f64>(2) - -1.5).abs() < f64::EPSILON);
    // Counter op on a max column is a policy mismatch; unknown row is 02000.
    assert_eq!(
        db.policy_counter_add("stats", "highest", "row-1", 1)
            .await
            .unwrap_err()
            .code(),
        Some("22023")
    );
    assert_eq!(
        db.policy_counter_add("stats", "count", "missing", 1)
            .await
            .unwrap_err()
            .code(),
        Some("02000")
    );
}

#[tokio::test]
async fn bridge_release_field() {
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

#[test]
fn coverage_manifest_matches_examples() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("examples");
    let manifest = std::fs::read_to_string(dir.join("coverage.json")).unwrap();
    let readme = std::fs::read_to_string(dir.join("../README.md")).unwrap();
    for line in readme
        .lines()
        .filter(|l| l.starts_with("| ") && !l.starts_with("| Feature"))
    {
        let feature = line.split('|').nth(1).unwrap().trim();
        assert!(
            manifest.contains(&format!("\"{feature}\"")),
            "feature {feature} missing from coverage.json"
        );
    }
    let mut seen = 0;
    for entry in std::fs::read_dir(&dir).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_some_and(|e| e == "rs") {
            let name = path.file_stem().unwrap().to_string_lossy().to_string();
            assert!(
                manifest.contains(&format!("\"{name}\"")),
                "example {name} missing from coverage.json"
            );
            seen += 1;
        }
    }
    assert_eq!(seen, 12);
}

#[test]
fn managed_search_example() {
    // Run the same checked scenario used in the public example.
    search_example::main();
}

#[path = "../examples/search.rs"]
mod search_example;
