//! End-to-end Postgres-wire tests through `tokio-postgres`: auth, lockout,
//! unlock/create/reopen, DDL+DML round-trips on both query protocols,
//! parameters, version interceptions, and SQLSTATE mapping.
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

fn write_fresh_config(dir: &tempfile::TempDir, port: u16) -> std::path::PathBuf {
    let hash = murmur_sql::server::auth::hash_password(PASSWORD).unwrap();
    let config = dir.path().join("config.toml");
    std::fs::write(
        &config,
        format!(
            "[server]\nbind = \"127.0.0.1:{port}\"\n\
             [storage]\ndata_dir = \"{}\"\n\
             [[auth.users]]\nname = \"admin\"\npassword_hash = \"{hash}\"\n\
             [logging]\ndir = \"{}\"\n",
            dir.path().join("data").display(),
            dir.path().join("logs").display(),
        ),
    )
    .unwrap();
    config
}

/// Change only the bind address, preserving server-managed sections like
/// `[replication.tables]`.
fn rebind(config: &std::path::Path, port: u16) {
    let mut loaded = murmur_sql::config::Config::load(config).unwrap();
    loaded.server.bind = format!("127.0.0.1:{port}");
    std::fs::write(config, toml::to_string(&loaded).unwrap()).unwrap();
    murmur_sql::config::Config::load(config).unwrap();
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

fn start_server(dir: &tempfile::TempDir, port: u16) -> Server {
    let config = write_fresh_config(dir, port);
    spawn_server(&config, port)
}

async fn connect(server: &Server, user: &str, password: &str) -> Result<Client, tokio_postgres::Error> {
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user={user} password={password} dbname=murmur", server.port),
        NoTls,
    )
    .await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(client)
}

fn code(error: &tokio_postgres::Error) -> String {
    error.code().map(|c| c.code().to_owned()).unwrap_or_default()
}

#[tokio::test]
async fn auth_accepts_user_and_rejects_others() {
    let dir = tempfile::tempdir().unwrap();
    let server = start_server(&dir, free_port());
    assert!(connect(&server, "admin", PASSWORD).await.is_ok());
    assert!(connect(&server, "admin", "wrong").await.is_err());
    assert!(connect(&server, "nobody", PASSWORD).await.is_err());
}

#[tokio::test]
async fn locked_until_unlock_and_key_verified_per_connection() {
    let dir = tempfile::tempdir().unwrap();
    let server = start_server(&dir, free_port());
    let client = connect(&server, "admin", PASSWORD).await.unwrap();
    let error = client.simple_query("SELECT 1").await.unwrap_err();
    assert_eq!(code(&error), "28000", "{error}");
    client.simple_query(&format!("MURMUR UNLOCK KEY '{KEY}'")).await.unwrap();
    // RowDescription + DataRow + CommandComplete.
    assert_eq!(client.simple_query("SELECT 1").await.unwrap().len(), 3);

    // A second connection starts locked even though the database is open.
    let other = connect(&server, "admin", PASSWORD).await.unwrap();
    let error = other.simple_query("SELECT 1").await.unwrap_err();
    assert_eq!(code(&error), "28000", "{error}");
    let error = other.simple_query("MURMUR UNLOCK KEY 'wrong-key'").await.unwrap_err();
    assert_eq!(code(&error), "28000", "{error}");
    let db = error.as_db_error().unwrap();
    assert!(db.message().contains("incorrect key"), "{db:?}");
    // The rejected key is not echoed back.
    assert!(!db.message().contains("wrong-key"), "{db:?}");
    other.simple_query(&format!("MURMUR UNLOCK KEY '{KEY}'")).await.unwrap();
    assert_eq!(other.simple_query("SELECT 2").await.unwrap().len(), 3);
}

#[tokio::test]
async fn crud_round_trip_both_protocols() {
    let dir = tempfile::tempdir().unwrap();
    let server = start_server(&dir, free_port());
    let client = connect(&server, "admin", PASSWORD).await.unwrap();
    client.simple_query(&format!("MURMUR UNLOCK KEY '{KEY}'")).await.unwrap();

    client
        .simple_query("CREATE TABLE items(id INTEGER PRIMARY KEY, name TEXT NOT NULL, score REAL, payload BLOB)")
        .await
        .unwrap();
    // Extended protocol with explicit parameter types (the server reports
    // UNKNOWN for uninferred parameters, like other lightweight servers).
    use tokio_postgres::types::Type;
    let payload = vec![1u8, 2, 3];
    let inserted = client
        .execute_typed(
            "INSERT INTO items VALUES ($1, $2, $3, $4)",
            &[(&1i64 as _, Type::INT8), (&"one" as _, Type::TEXT), (&1.5f64 as _, Type::FLOAT8), (&payload as _, Type::BYTEA)],
        )
        .await
        .unwrap();
    assert_eq!(inserted, 1);
    let none_real: Option<f64> = None;
    let none_blob: Option<Vec<u8>> = None;
    client
        .execute_typed(
            "INSERT INTO items VALUES ($1, $2, $3, $4)",
            &[(&2i64 as _, Type::INT8), (&"two" as _, Type::TEXT), (&none_real as _, Type::FLOAT8), (&none_blob as _, Type::BYTEA)],
        )
        .await
        .unwrap();
    // Simple protocol multi-statement.
    client
        .simple_query("INSERT INTO items VALUES (3, 'three', 3.25, NULL); SELECT count(*) FROM items")
        .await
        .unwrap();

    let rows = client.query("SELECT id, name, score, payload FROM items ORDER BY id", &[]).await.unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get::<_, i64>(0), 1);
    assert_eq!(rows[0].get::<_, String>(1), "one");
    assert_eq!(rows[0].get::<_, f64>(2), 1.5);
    assert_eq!(rows[0].get::<_, Vec<u8>>(3), vec![1u8, 2, 3]);
    assert_eq!(rows[1].get::<_, Option<f64>>(2), None);
    assert_eq!(rows[1].get::<_, Option<Vec<u8>>>(3), None);

    let rows = client
        .query_typed("SELECT name FROM items WHERE id = $1", &[(&3i64 as _, Type::INT8)])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>(0), "three");

    // Untyped expressions agree between describe and execute on the extended
    // protocol, so typed decoding works.
    let rows = client.query("SELECT 1, count(*) FROM items", &[]).await.unwrap();
    assert_eq!(rows[0].get::<_, i64>(0), 1);
    assert_eq!(rows[0].get::<_, i64>(1), 3);

    // Interceptions and transaction no-ops.
    let rows = client.query("SELECT version()", &[]).await.unwrap();
    assert!(rows[0].get::<_, String>(0).starts_with("PostgreSQL 16.6-murmur"));
    let rows = client.query("SHOW server_version", &[]).await.unwrap();
    assert!(rows[0].get::<_, String>(0).contains("murmur"));
    client.simple_query("BEGIN").await.unwrap();
    client.simple_query("COMMIT").await.unwrap();

    // SQLSTATE mapping.
    let error = client.simple_query("SELECT * FROM missing").await.unwrap_err();
    assert_eq!(code(&error), "42P01", "{error}");
    let error = client
        .query_typed(
            "INSERT INTO items VALUES ($1, 'dup', NULL, NULL)",
            &[(&1i64 as _, Type::INT8)],
        )
        .await
        .unwrap_err();
    assert_eq!(code(&error), "23505", "{error}");
    let error = client.simple_query("SELEC oops").await.unwrap_err();
    assert_eq!(code(&error), "42601", "{error}");
}

#[tokio::test]
async fn set_and_show_table_flags_persist_to_config() {
    let dir = tempfile::tempdir().unwrap();
    let server = start_server(&dir, free_port());
    let client = connect(&server, "admin", PASSWORD).await.unwrap();
    client.simple_query(&format!("MURMUR UNLOCK KEY '{KEY}'")).await.unwrap();
    client.simple_query("CREATE TABLE shared(id INTEGER PRIMARY KEY, v TEXT NOT NULL)").await.unwrap();
    client.simple_query("CREATE TABLE scratch(id INTEGER PRIMARY KEY, v TEXT NOT NULL)").await.unwrap();

    let rows = client.query("MURMUR SHOW TABLES REPLICATION", &[]).await.unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, String>(0), "scratch");
    assert_eq!(rows[0].get::<_, String>(1), "false");
    client.simple_query("MURMUR SET TABLE shared REPLICATED true").await.unwrap();

    client.simple_query("MURMUR SET TABLE scratch REPLICATED false").await.unwrap();
    let rows = client.query("MURMUR SHOW TABLES REPLICATION", &[]).await.unwrap();
    let scratch = rows.iter().find(|row| row.get::<_, String>(0) == "scratch").unwrap();
    assert_eq!(scratch.get::<_, String>(1), "false");
    let shared = rows.iter().find(|row| row.get::<_, String>(0) == "shared").unwrap();
    assert_eq!(shared.get::<_, String>(1), "true");

    // Unknown tables and bad values are rejected.
    assert!(client.simple_query("MURMUR SET TABLE missing REPLICATED false").await.is_err());
    assert!(client.simple_query("MURMUR SET TABLE scratch REPLICATED maybe").await.is_err());
    assert!(client.simple_query("MURMUR SET TABLE scratch").await.is_err());

    // The flag survives a restart through config.toml.
    let config_path = dir.path().join("config.toml");
    let before = murmur_sql::config::Config::load(&config_path).unwrap();
    assert_eq!(before.replication.tables.values().filter(|v| !**v).count(), 1);
    drop(server);
    let port = free_port();
    rebind(&config_path, port);
    let server = spawn_server(&config_path, port);
    let client = connect(&server, "admin", PASSWORD).await.unwrap();
    client.simple_query(&format!("MURMUR UNLOCK KEY '{KEY}'")).await.unwrap();
    let rows = client.query("MURMUR SHOW TABLES REPLICATION", &[]).await.unwrap();
    let scratch = rows.iter().find(|row| row.get::<_, String>(0) == "scratch").unwrap();
    assert_eq!(scratch.get::<_, String>(1), "false");
}

// Uses the embedded API to seed a legacy store; step 8 reworks this seeding
// after the embedded API is removed.
#[tokio::test]
async fn legacy_spool_flags_import_into_config_once() {
    use murmur::{Database, Migration, OpenMode, Options};
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("data");
    let key: [u8; 32] = *blake3::hash(KEY.as_bytes()).as_bytes();
    {
        let db = Database::open(&data_dir, key, Options { mode: OpenMode::CreateNew, ..Options::default() }).unwrap();
        db.migrate(Migration::new("CREATE TABLE legacy(id INTEGER PRIMARY KEY, v TEXT NOT NULL)")).unwrap();
        db.set_replication("legacy", false).unwrap();
        db.flush().unwrap();
        db.close().unwrap();
    }
    let server = start_server(&dir, free_port());
    let client = connect(&server, "admin", PASSWORD).await.unwrap();
    client.simple_query(&format!("MURMUR UNLOCK KEY '{KEY}'")).await.unwrap();
    let rows = client.query("MURMUR SHOW TABLES REPLICATION", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>(0), "legacy");
    assert_eq!(rows[0].get::<_, String>(1), "false");
    let config = murmur_sql::config::Config::load(dir.path().join("config.toml")).unwrap();
    assert_eq!(config.replication.tables.values().filter(|v| !**v).count(), 1);
}

#[tokio::test]
async fn restart_reopens_with_same_key_and_keeps_data() {
    let dir = tempfile::tempdir().unwrap();
    let port = free_port();
    {
        let server = start_server(&dir, port);
        let client = connect(&server, "admin", PASSWORD).await.unwrap();
        client.simple_query(&format!("MURMUR UNLOCK KEY '{KEY}'")).await.unwrap();
        client.simple_query("CREATE TABLE kept(id INTEGER PRIMARY KEY, v TEXT NOT NULL)").await.unwrap();
        client.simple_query("INSERT INTO kept VALUES (1, 'durable')").await.unwrap();
        client.simple_query("MURMUR FLUSH").await.unwrap();
    }
    // Restart on a fresh port against the same data directory.
    let server = start_server(&dir, free_port());
    let client = connect(&server, "admin", PASSWORD).await.unwrap();
    let error = client.simple_query("MURMUR UNLOCK KEY 'wrong-key'").await.unwrap_err();
    assert_eq!(code(&error), "28000", "{error}");
    client.simple_query(&format!("MURMUR UNLOCK KEY '{KEY}'")).await.unwrap();
    let rows = client.query("SELECT v FROM kept WHERE id = 1", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>(0), "durable");
}

#[tokio::test]
async fn show_metrics_wire_query() {
    let dir = tempfile::tempdir().unwrap();
    let server = start_server(&dir, free_port());
    let client = connect(&server, "admin", PASSWORD).await.unwrap();
    client.simple_query(&format!("MURMUR UNLOCK KEY '{KEY}'")).await.unwrap();

    let rows = client.query("MURMUR SHOW METRICS", &[]).await.unwrap();
    assert!(!rows.is_empty());
    let subsystems: std::collections::HashSet<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
    assert!(subsystems.contains("sqlite"));
    assert!(subsystems.contains("spool"));

    let sqlite_rows = client.query("MURMUR SHOW METRICS SQLITE", &[]).await.unwrap();
    assert!(!sqlite_rows.is_empty());
    for r in &sqlite_rows {
        assert_eq!(r.get::<_, String>(0), "sqlite");
    }

    let spool_rows = client.query("MURMUR SHOW METRICS SPOOL", &[]).await.unwrap();
    assert!(!spool_rows.is_empty());
    for r in &spool_rows {
        assert_eq!(r.get::<_, String>(0), "spool");
    }

    let err = client.query("MURMUR SHOW METRICS INVALID", &[]).await.unwrap_err();
    assert_eq!(code(&err), "42601");
}

#[tokio::test]
async fn engine_created_tables_remain_local_on_first_unlock() {
    use murmur::{Database, Migration, OpenMode, Options};
    // An absent flag always means local-only, including when the engine
    // created the store before its first server unlock.
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("data");
    let key: [u8; 32] = *blake3::hash(KEY.as_bytes()).as_bytes();
    {
        let db = Database::open(&data_dir, key, Options { mode: OpenMode::CreateNew, ..Options::default() }).unwrap();
        db.migrate(Migration::new("CREATE TABLE kept(id INTEGER PRIMARY KEY, v TEXT NOT NULL)")).unwrap();
        db.flush().unwrap();
        db.close().unwrap();
    }
    let server = start_server(&dir, free_port());
    let client = connect(&server, "admin", PASSWORD).await.unwrap();
    client.simple_query(&format!("MURMUR UNLOCK KEY '{KEY}'")).await.unwrap();
    let rows = client.query("MURMUR SHOW TABLES REPLICATION", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>(0), "kept");
    assert_eq!(rows[0].get::<_, String>(1), "false");
    let config = murmur_sql::config::Config::load(dir.path().join("config.toml")).unwrap();
    assert!(config.replication.tables.is_empty());
}

#[tokio::test]
async fn diagnostic_status_is_read_only_typed_json_over_both_protocols() {
    let dir = tempfile::tempdir().unwrap();
    let server = start_server(&dir, free_port());
    let client = connect(&server, "admin", PASSWORD).await.unwrap();
    let locked = client.simple_query("MURMUR SHOW STATUS").await.unwrap_err();
    assert_eq!(code(&locked), "28000");
    client.simple_query(&format!("MURMUR UNLOCK KEY '{KEY}'")).await.unwrap();
    client.simple_query("CREATE TABLE status_rows(id INTEGER PRIMARY KEY); INSERT INTO status_rows VALUES(1); MURMUR FLUSH").await.unwrap();
    let statement = client.prepare("MURMUR SHOW STATUS").await.unwrap();
    assert_eq!(statement.columns()[0].name(), "status");
    assert_eq!(statement.columns()[0].type_(), &tokio_postgres::types::Type::TEXT);
    let rows = client.query(&statement, &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    let snapshot: serde_json::Value = serde_json::from_str(rows[0].get::<_, &str>(0)).unwrap();
    assert_eq!(snapshot["closed"], false);
    assert_eq!(snapshot["pending_bytes"], 0);
    assert!(snapshot["scheduler"]["local"]["acquisitions"].is_u64());
    assert!(snapshot["spool"]["disk_bytes"].is_u64());
    let simple = client.simple_query("MURMUR SHOW STATUS").await.unwrap();
    let row = simple.iter().find_map(|message| match message { tokio_postgres::SimpleQueryMessage::Row(row) => Some(row), _ => None }).unwrap();
    let _: serde_json::Value = serde_json::from_str(row.get(0).unwrap()).unwrap();
    let invalid = client.simple_query("MURMUR SHOW STATUS extra").await.unwrap_err();
    assert_eq!(code(&invalid), "42601");
    let count: i64 = client.query_one("SELECT count(*) FROM status_rows", &[]).await.unwrap().get(0);
    assert_eq!(count, 1);
}
