//! Wire administration: `MURMUR PEER`, `MURMUR SHOW PEERS|REPLICATION|
//! RETENTION`, `MURMUR RETENTION COLLECT`, `MURMUR POLICY`, and
//! `MURMUR BRIDGE RELEASE`.
//!
//! Without replication (or without the `replication` Cargo feature) every
//! command fails with `55000` after argument validation, so usage (`42601`)
//! and value (`22023`) errors are observable on any build. The
//! replication-enabled tests only compile with the feature.
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};
use tokio_postgres::{Client, NoTls};

const PASSWORD: &str = "s3cret-password";
const KEY: &str = "test-encryption-key";

struct Server {
    child: Child,
    port: u16,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

fn write_config(
    dir: &tempfile::TempDir,
    port: u16,
    storage_extra: &str,
    extra: &str,
) -> std::path::PathBuf {
    let hash = sqlite_session::server::auth::hash_password(PASSWORD).unwrap();
    let config = dir.path().join("config.toml");
    std::fs::write(
        &config,
        format!(
            "[server]\nbind = \"127.0.0.1:{port}\"\n\
             [storage]\ndata_dir = \"{}\"\n{storage_extra}\
             [[auth.users]]\nname = \"admin\"\npassword_hash = \"{hash}\"\n\
             [logging]\ndir = \"{}\"\n{extra}",
            dir.path().join("data").display(),
            dir.path().join("logs").display(),
        ),
    )
    .unwrap();
    config
}

fn spawn_server(config: &std::path::Path, port: u16) -> Server {
    let mut child = Command::new(env!("CARGO_BIN_EXE_murmur-server"))
        .arg("--config")
        .arg(config)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            break;
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            panic!("server on port {port} did not start in time");
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    Server { child, port }
}

async fn connect(server: &Server) -> Client {
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=admin password={PASSWORD} dbname=murmur",
            server.port
        ),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

fn code(error: &tokio_postgres::Error) -> String {
    error.code().map(|c| c.code().to_owned()).unwrap_or_default()
}

#[cfg(feature = "replication")]
fn message(error: &tokio_postgres::Error) -> String {
    error
        .as_db_error()
        .map(|e| e.message().to_owned())
        .unwrap_or_else(|| error.to_string())
}

async fn unlock(client: &Client) {
    client.simple_query(&format!("MURMUR UNLOCK KEY '{KEY}'")).await.unwrap();
}

#[tokio::test]
async fn admin_commands_need_unlock_first() {
    let dir = tempfile::tempdir().unwrap();
    let port = free_port();
    let config = write_config(&dir, port, "", "");
    let server = spawn_server(&config, port);
    let client = connect(&server).await;
    for sql in [
        "MURMUR PEER SYNC",
        "MURMUR SHOW PEERS",
        "MURMUR SHOW REPLICATION",
        "MURMUR SHOW RETENTION",
        "MURMUR RETENTION COLLECT",
        "MURMUR POLICY COUNTER_ADD t c 'k' 1",
        "MURMUR BRIDGE RELEASE t 'k'",
        "MURMUR BRIDGE RELEASE t 'k' COLUMN c",
    ] {
        let error = client.simple_query(sql).await.unwrap_err();
        assert_eq!(code(&error), "28000", "{sql}: {error}");
    }
}

#[tokio::test]
async fn admin_commands_fail_without_replication() {
    let dir = tempfile::tempdir().unwrap();
    let port = free_port();
    let config = write_config(&dir, port, "", "");
    let server = spawn_server(&config, port);
    let client = connect(&server).await;
    unlock(&client).await;
    // Well-formed commands reach the replication gate on any build.
    for sql in [
        "MURMUR PEER ADD '127.0.0.1:9001'",
        "MURMUR PEER REMOVE '00112233445566778899aabbccddeeff'",
        "MURMUR PEER RETIRE '00112233445566778899aabbccddeeff'",
        "MURMUR PEER SYNC",
        "MURMUR SHOW PEERS",
        "MURMUR SHOW REPLICATION",
        "MURMUR SHOW RETENTION",
        "MURMUR RETENTION COLLECT",
        "MURMUR POLICY COUNTER_ADD t c 'k' 1",
        "MURMUR POLICY SET_ADD t c 'k' 'v'",
        "MURMUR POLICY SET_REMOVE t c 'k' 'v'",
        "MURMUR POLICY MIN t c 'k' 1",
        "MURMUR POLICY MAX t c 'k' 1.5",
        "MURMUR BRIDGE RELEASE t 'k'",
        "MURMUR BRIDGE RELEASE t 'k' COLUMN c",
    ] {
        let error = client.simple_query(sql).await.unwrap_err();
        assert_eq!(code(&error), "55000", "{sql}: {error}");
    }
}

#[tokio::test]
async fn admin_usage_and_value_errors_precede_replication_gate() {
    let dir = tempfile::tempdir().unwrap();
    let port = free_port();
    let config = write_config(&dir, port, "", "");
    let server = spawn_server(&config, port);
    let client = connect(&server).await;
    unlock(&client).await;
    for sql in [
        "MURMUR PEER",
        "MURMUR PEER BOGUS",
        "MURMUR PEER ADD",
        "MURMUR PEER ADD '127.0.0.1:9001' extra",
        "MURMUR PEER SYNC extra",
        "MURMUR SHOW PEERS extra",
        "MURMUR SHOW REPLICATION extra",
        "MURMUR SHOW RETENTION extra",
        "MURMUR RETENTION",
        "MURMUR RETENTION PURGE",
        "MURMUR POLICY",
        "MURMUR POLICY BOGUS t c 'k' 1",
        "MURMUR POLICY MIN t c 'k'",
        "MURMUR POLICY MIN t c 'k' 1 extra",
        "MURMUR BRIDGE",
        "MURMUR BRIDGE BOGUS t 'k'",
        "MURMUR BRIDGE RELEASE",
        "MURMUR BRIDGE RELEASE t",
        "MURMUR BRIDGE RELEASE t 'k' extra",
        "MURMUR BRIDGE RELEASE t 'k' COLUMN",
        "MURMUR BRIDGE RELEASE t 'k' COLUMN c extra",
    ] {
        let error = client.simple_query(sql).await.unwrap_err();
        assert_eq!(code(&error), "42601", "{sql}: {error}");
    }
    for sql in [
        "MURMUR PEER ADD 'not-an-address'",
        "MURMUR PEER ADD '127.0.0.1'",
        "MURMUR PEER REMOVE 'xyz'",
        "MURMUR PEER REMOVE '00112233'",
        "MURMUR PEER RETIRE 'zz112233445566778899aabbccddeeff'",
        "MURMUR POLICY COUNTER_ADD t c 'k' 'not-an-int'",
        "MURMUR POLICY COUNTER_ADD t c 'k' 1.5",
        "MURMUR POLICY MIN t c 'k' NaN",
        "MURMUR POLICY MAX t c 'k' inf",
        "MURMUR BRIDGE RELEASE '' 'k'",
        "MURMUR BRIDGE RELEASE t 'k' COLUMN ''",
    ] {
        let error = client.simple_query(sql).await.unwrap_err();
        assert_eq!(code(&error), "22023", "{sql}: {error}");
    }
}

/// Self-signed replication credentials: node `[9; 16]`, seed `[0xab; 32]`.
/// Returns the `[replication]` TOML fragment for the server config;
/// `policies` is appended inside the section (for example `merge_policies`).
#[cfg(feature = "replication")]
fn replication_config_toml(dir: &tempfile::TempDir, policies: &str) -> String {
    use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair, SanType};
    let node = [9u8; 16];
    let seed = [0xabu8; 32];
    let public = sqlite_session::replication::protocol::public_key(&seed);

    let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca_key = KeyPair::generate().unwrap();
    let ca = ca_params.self_signed(&ca_key).unwrap();

    let key = KeyPair::generate().unwrap();
    let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
    params.subject_alt_names = vec![SanType::URI(
        format!("replicateddb://node/{}", sqlite_session::replication::hex(&node))
            .try_into()
            .unwrap(),
    )];
    params.extended_key_usages = vec![
        rcgen::ExtendedKeyUsagePurpose::ServerAuth,
        rcgen::ExtendedKeyUsagePurpose::ClientAuth,
    ];
    let cert = params.signed_by(&key, &ca, &ca_key).unwrap();

    let chain_path = dir.path().join("chain.pem");
    let key_path = dir.path().join("key.pem");
    let ca_path = dir.path().join("ca.pem");
    std::fs::write(&chain_path, cert.pem()).unwrap();
    std::fs::write(&key_path, key.serialize_pem()).unwrap();
    std::fs::write(&ca_path, ca.pem()).unwrap();

    let hex = sqlite_session::replication::hex;
    format!(
        "[replication]\nenabled = true\nbind = \"127.0.0.1:0\"\n\
         cert_chain_file = \"{}\"\nprivate_key_file = \"{}\"\nca_file = \"{}\"\n\
         signing_key_hex = \"{}\"\n\
         trusted_origins = {{ \"{}\" = \"{}\" }}\n\
         {policies}",
        chain_path.display(),
        key_path.display(),
        ca_path.display(),
        hex(&seed),
        hex(&node),
        hex(&public),
    )
}

#[cfg(feature = "replication")]
#[tokio::test]
async fn peer_and_retention_commands_with_replication() {
    let dir = tempfile::tempdir().unwrap();
    let port = free_port();
    let extra = replication_config_toml(&dir, "");
    let config = write_config(&dir, port, "", &extra);
    let server = spawn_server(&config, port);
    let client = connect(&server).await;
    unlock(&client).await;

    // Local summary: one row, eight columns; the first seven indices are stable.
    let rows = client.query("MURMUR SHOW REPLICATION", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].len(), 8);
    assert_eq!(rows[0].get::<_, String>(0), "true");
    assert_eq!(rows[0].get::<_, i64>(2), 0);
    assert_eq!(rows[0].get::<_, Option<String>>(6), None);
    assert!(rows[0].get::<_, i64>(5) >= 0);
    assert!(rows[0].get::<_, i64>(7) >= 0);
    let described = client.prepare("MURMUR SHOW REPLICATION").await.unwrap();
    assert_eq!(described.columns().len(), 8);

    // No peers yet: no rows, but Describe still reports seven columns.
    let rows = client.query("MURMUR SHOW PEERS", &[]).await.unwrap();
    assert!(rows.is_empty());
    let described = client.prepare("MURMUR SHOW PEERS").await.unwrap();
    assert_eq!(described.columns().len(), 7);
    let described = client.prepare("MURMUR SHOW RETENTION").await.unwrap();
    assert_eq!(described.columns().len(), 7);
    let described = client.prepare("MURMUR RETENTION COLLECT").await.unwrap();
    assert_eq!(described.columns().len(), 1);

    // Queue a dial (it fails asynchronously; the command still succeeds),
    // remove an unknown peer, and retire another.
    client.simple_query("MURMUR PEER ADD '127.0.0.1:1'").await.unwrap();
    client.simple_query("MURMUR PEER SYNC").await.unwrap();
    client.simple_query("MURMUR PEER REMOVE '00112233445566778899aabbccddeeff'").await.unwrap();
    let retired = "ee112233445566778899aabbccddeeff";
    client.simple_query(&format!("MURMUR PEER RETIRE '{retired}'")).await.unwrap();

    // The retired peer is visible in retention with NULL acknowledgements.
    let rows = client.query("MURMUR SHOW RETENTION", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].len(), 7);
    assert_eq!(rows[0].get::<_, String>(0), retired);
    assert_eq!(rows[0].get::<_, String>(2), "true");
    assert_eq!(rows[0].get::<_, Option<String>>(3), None);
    assert_eq!(rows[0].get::<_, Option<i64>>(4), None);

    // A bounded collection pass reports its count.
    let rows = client.query("MURMUR RETENTION COLLECT", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].get::<_, i64>(0) >= 0);

    // Retiring this node is rejected.
    let node = "09090909090909090909090909090909";
    let error = client.simple_query(&format!("MURMUR PEER RETIRE '{node}'")).await.unwrap_err();
    assert_eq!(code(&error), "22023", "{error}");
}

#[cfg(feature = "replication")]
#[tokio::test]
async fn policy_commands_cover_single_text_key_usage() {
    let dir = tempfile::tempdir().unwrap();
    let port = free_port();
    let extra = replication_config_toml(
        &dir,
        "merge_policies = { \"live.n\" = \"counter\", \"live.tags\" = \"set\", \
         \"live.peak\" = \"max\", \"live.floor\" = \"min\", \"live.nick\" = \"max\" }\n",
    );
    let config = write_config(&dir, port, "", &extra);
    let server = spawn_server(&config, port);
    let client = connect(&server).await;
    unlock(&client).await;

    // Policies apply at CREATE TABLE from `replication.merge_policies`.
    client
        .simple_query(
            "CREATE TABLE live(name TEXT NOT NULL PRIMARY KEY, n INTEGER NOT NULL, \
             tags BLOB NOT NULL, peak INTEGER NOT NULL, floor REAL NOT NULL, nick TEXT NOT NULL)",
        )
        .await
        .unwrap();
    client.simple_query("INSERT INTO live VALUES('r1', 0, x'00', 0, 0.0, 'm')").await.unwrap();

    client.simple_query("MURMUR POLICY COUNTER_ADD live n 'r1' 7").await.unwrap();
    client.simple_query("MURMUR POLICY COUNTER_ADD live n 'r1' -2").await.unwrap();
    let rows = client.query("SELECT n FROM live WHERE name = 'r1'", &[]).await.unwrap();
    assert_eq!(rows[0].get::<_, i64>(0), 5);

    client.simple_query("MURMUR POLICY SET_ADD live tags 'r1' 'blue'").await.unwrap();
    let after_add = client.query("SELECT tags FROM live WHERE name = 'r1'", &[]).await.unwrap()[0].get::<_, Vec<u8>>(0);
    assert_ne!(after_add, vec![0u8]);
    client.simple_query("MURMUR POLICY SET_REMOVE live tags 'r1' 'blue'").await.unwrap();
    let after_remove = client.query("SELECT tags FROM live WHERE name = 'r1'", &[]).await.unwrap()[0].get::<_, Vec<u8>>(0);
    assert_eq!(after_remove, vec![0u8]);

    client.simple_query("MURMUR POLICY MAX live peak 'r1' 42").await.unwrap();
    client.simple_query("MURMUR POLICY MAX live peak 'r1' 10").await.unwrap();
    client.simple_query("MURMUR POLICY MIN live floor 'r1' -1.5").await.unwrap();
    client.simple_query("MURMUR POLICY MIN live floor 'r1' 9.25").await.unwrap();
    let rows = client.query("SELECT peak, floor FROM live WHERE name = 'r1'", &[]).await.unwrap();
    assert_eq!(rows[0].get::<_, i64>(0), 42);
    assert_eq!(rows[0].get::<_, f64>(1), -1.5);

    // TEXT extrema literals compare as text.
    client.simple_query("MURMUR POLICY MAX live nick 'r1' zebra").await.unwrap();
    client.simple_query("MURMUR POLICY MAX live nick 'r1' apple").await.unwrap();
    let rows = client.query("SELECT nick FROM live WHERE name = 'r1'", &[]).await.unwrap();
    assert_eq!(rows[0].get::<_, String>(0), "zebra");

    // An operation on a column with another policy is rejected by the engine.
    let error = client.simple_query("MURMUR POLICY COUNTER_ADD live peak 'r1' 1").await.unwrap_err();
    assert_eq!(code(&error), "22023", "{error}");
    // A missing row surfaces SQLite's no-data code.
    let error = client.simple_query("MURMUR POLICY COUNTER_ADD live n 'missing' 1").await.unwrap_err();
    assert_eq!(code(&error), "02000", "{error}");
}

/// The `[replication.bridge]` TOML fragment: `role` is `high-receiver` or
/// `low-exporter`, `keys` carries the role's key lines. No bundles are
/// exchanged, so trust values are fixed test constants; `domain` must equal
/// the node's own `storage.database_id`.
#[cfg(feature = "replication")]
fn bridge_config_toml(dir: &tempfile::TempDir, role: &str, domain: &str, keys: &str) -> String {
    format!(
        "[replication.bridge]\nrole = \"{role}\"\ndomain = \"{domain}\"\n\
         trusted_domains = [\"{}\"]\ndirectory = \"{}\"\nstream = \"test-stream\"\n{keys}",
        "11".repeat(16),
        dir.path().join("staging").display(),
    )
}

#[cfg(feature = "replication")]
#[tokio::test]
async fn bridge_release_with_high_receiver() {
    let dir = tempfile::tempdir().unwrap();
    let port = free_port();
    let domain = "22".repeat(16);
    let mut extra = replication_config_toml(&dir, "");
    extra.push_str(&bridge_config_toml(
        &dir,
        "high-receiver",
        &domain,
        &format!(
            "recipient = \"{}\"\nreceiver_secret_hex = \"{}\"\n",
            "ee".repeat(32),
            "dd".repeat(32)
        ),
    ));
    let config = write_config(&dir, port, &format!("database_id = \"{domain}\"\n"), &extra);
    let server = spawn_server(&config, port);
    let client = connect(&server).await;
    unlock(&client).await;

    client
        .simple_query("CREATE TABLE notes(name TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL)")
        .await
        .unwrap();
    client.simple_query("INSERT INTO notes VALUES('r1', 'v1')").await.unwrap();
    client.simple_query("MURMUR FLUSH").await.unwrap();

    // The High write above claimed ownership of `title`: the first release
    // reports true through the extended protocol, and Describe names it.
    let rows = client.query("MURMUR BRIDGE RELEASE notes 'r1' COLUMN title", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>(0), "true");
    let described = client.prepare("MURMUR BRIDGE RELEASE notes 'r1' COLUMN title").await.unwrap();
    assert_eq!(described.columns().len(), 1);
    assert_eq!(described.columns()[0].name(), "released");

    // A second release finds nothing owned.
    let error = client.query("MURMUR BRIDGE RELEASE notes 'r1' COLUMN title", &[]).await.unwrap_err();
    assert_eq!(code(&error), "22023", "{error}");
    assert!(message(&error).contains("field is not High-owned"), "{error}");

    // Row release without COLUMN addresses whole-row ownership, which a
    // plain insert never creates; unknown names are rejected too.
    for (sql, expected) in [
        ("MURMUR BRIDGE RELEASE notes 'r1'", "field is not High-owned"),
        ("MURMUR BRIDGE RELEASE missing 'r1' COLUMN title", "unknown table: missing"),
        ("MURMUR BRIDGE RELEASE notes 'r1' COLUMN missing", "unknown column: notes.missing"),
    ] {
        let error = client.simple_query(sql).await.unwrap_err();
        assert_eq!(code(&error), "22023", "{sql}: {error}");
        assert!(message(&error).contains(expected), "{sql}: {error}");
    }

    // Composite and non-TEXT keys are not addressable.
    client
        .simple_query("CREATE TABLE pair(a TEXT NOT NULL, b TEXT NOT NULL, PRIMARY KEY (a, b))")
        .await
        .unwrap();
    client
        .simple_query("CREATE TABLE seq(id INTEGER NOT NULL PRIMARY KEY, v TEXT NOT NULL)")
        .await
        .unwrap();
    for sql in ["MURMUR BRIDGE RELEASE pair 'r1'", "MURMUR BRIDGE RELEASE seq 'r1'"] {
        let error = client.simple_query(sql).await.unwrap_err();
        assert_eq!(code(&error), "22023", "{sql}: {error}");
        assert!(message(&error).contains("single-TEXT-key"), "{sql}: {error}");
    }
}

#[cfg(feature = "replication")]
#[tokio::test]
async fn bridge_release_needs_configured_bridge() {
    let dir = tempfile::tempdir().unwrap();
    let port = free_port();
    let extra = replication_config_toml(&dir, "");
    let config = write_config(&dir, port, "", &extra);
    let server = spawn_server(&config, port);
    let client = connect(&server).await;
    unlock(&client).await;
    let error = client.simple_query("MURMUR BRIDGE RELEASE t 'k'").await.unwrap_err();
    assert_eq!(code(&error), "55000", "{error}");
    assert!(message(&error).contains("bridge is not enabled"), "{error}");
}

#[cfg(feature = "replication")]
#[tokio::test]
async fn bridge_release_rejected_on_exporter() {
    let dir = tempfile::tempdir().unwrap();
    let port = free_port();
    let domain = "22".repeat(16);
    let mut extra = replication_config_toml(&dir, "");
    extra.push_str(&bridge_config_toml(
        &dir,
        "low-exporter",
        &domain,
        &format!("recipient = \"{}\"\nsigner_hex = \"{}\"\n", "ee".repeat(32), "bb".repeat(32)),
    ));
    let config = write_config(&dir, port, &format!("database_id = \"{domain}\"\n"), &extra);
    let server = spawn_server(&config, port);
    let client = connect(&server).await;
    unlock(&client).await;
    client
        .simple_query("CREATE TABLE notes(name TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL)")
        .await
        .unwrap();
    let error = client.simple_query("MURMUR BRIDGE RELEASE notes 'r1' COLUMN title").await.unwrap_err();
    assert_eq!(code(&error), "22023", "{error}");
    assert!(message(&error).contains("bridge direction"), "{error}");
}
