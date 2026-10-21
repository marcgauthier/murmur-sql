//! End-to-end Postgres-wire tests through `tokio-postgres`: auth, lockout,
//! unlock/create/reopen, DDL+DML round-trips on both query protocols,
//! parameters, version interceptions, and SQLSTATE mapping.
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
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
    // Wrong passwords and unknown users are indistinguishable: both run
    // the Argon2 round-trip and fail with PostgreSQL invalid-password.
    let wrong = connect(&server, "admin", "wrong").await.unwrap_err();
    assert_eq!(code(&wrong), "28P01");
    let unknown = connect(&server, "nobody", PASSWORD).await.unwrap_err();
    assert_eq!(code(&unknown), "28P01");
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

#[tokio::test]
async fn bool_columns_coerce_numbers_and_words() {
    let dir = tempfile::tempdir().unwrap();
    let server = start_server(&dir, free_port());
    let client = connect(&server, "admin", PASSWORD).await.unwrap();
    client.simple_query(&format!("MURMUR UNLOCK KEY '{KEY}'")).await.unwrap();
    client.simple_query("CREATE TABLE flags(id INTEGER PRIMARY KEY, on_off MURMUR_BOOL)").await.unwrap();
    client.simple_query("INSERT INTO flags VALUES (1, 1), (2, 0), (3, 1.0), (4, 'true'), (5, 'off')").await.unwrap();
    let rows = client.query("SELECT on_off FROM flags ORDER BY id", &[]).await.unwrap();
    let got: Vec<bool> = rows.iter().map(|row| row.get(0)).collect();
    assert_eq!(got, vec![true, false, true, true, false]);
    client.simple_query("INSERT INTO flags VALUES (6, 'bogus')").await.unwrap();
    let error = client.query("SELECT on_off FROM flags WHERE id = 6", &[]).await.unwrap_err();
    assert_eq!(code(&error), "42846", "{error}");
}

#[tokio::test]
async fn connection_cap_refuses_overflow_and_releases_on_close() {
    let dir = tempfile::tempdir().unwrap();
    let port = free_port();
    let config = write_fresh_config(&dir, port);
    let text = std::fs::read_to_string(&config).unwrap();
    std::fs::write(&config, text.replacen("[server]\n", "[server]\nmax_connections = 3\n", 1)).unwrap();
    let server = spawn_server(&config, port);
    let c1 = connect(&server, "admin", PASSWORD).await.unwrap();
    let c2 = connect(&server, "admin", PASSWORD).await.unwrap();
    let c3 = connect(&server, "admin", PASSWORD).await.unwrap();
    assert!(connect(&server, "admin", PASSWORD).await.is_err());
    // Held connections stay healthy under the cap.
    c1.simple_query(&format!("MURMUR UNLOCK KEY '{KEY}'")).await.unwrap();
    assert_eq!(c1.query_one("SELECT 1", &[]).await.unwrap().get::<_, i64>(0), 1);
    drop(c1);
    drop(c2);
    drop(c3);
    // A freed permit admits again.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if connect(&server, "admin", PASSWORD).await.is_ok() {
            break;
        }
        assert!(Instant::now() < deadline, "permit never released");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Minimal extended-protocol driver: named statements are re-parsed and
/// executed without a fresh Describe, which high-level clients never do.
struct Raw {
    stream: tokio::net::TcpStream,
}

impl Raw {
    async fn connect(port: u16) -> Self {
        let stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .unwrap();
        let mut raw = Self { stream };
        let mut body = vec![];
        body.extend_from_slice(b"user\0admin\0database\0murmur\0\0");
        let mut startup = (8 + body.len() as u32).to_be_bytes().to_vec();
        startup.extend_from_slice(&196608u32.to_be_bytes());
        startup.extend_from_slice(&body);
        raw.stream.write_all(&startup).await.unwrap();
        loop {
            let (tag, body) = raw.message().await;
            match tag {
                // Cleartext-password request.
                b'R' if u32::from_be_bytes(body[0..4].try_into().unwrap()) == 3 => {
                    let mut msg = vec![b'p'];
                    let len = 4 + PASSWORD.len() as u32 + 1;
                    msg.extend_from_slice(&len.to_be_bytes());
                    msg.extend_from_slice(PASSWORD.as_bytes());
                    msg.push(0);
                    raw.stream.write_all(&msg).await.unwrap();
                }
                b'Z' => break,
                b'E' => panic!("startup error: {body:?}"),
                _ => {}
            }
        }
        raw
    }

    async fn message(&mut self) -> (u8, Vec<u8>) {
        let tag = self.stream.read_u8().await.unwrap();
        let len = self.stream.read_u32().await.unwrap() as usize;
        let mut body = vec![0u8; len - 4];
        self.stream.read_exact(&mut body).await.unwrap();
        (tag, body)
    }

    async fn send(&mut self, tag: u8, body: &[u8]) {
        self.stream.write_u8(tag).await.unwrap();
        self.stream
            .write_u32(4 + body.len() as u32)
            .await
            .unwrap();
        self.stream.write_all(body).await.unwrap();
    }

    fn cstr(out: &mut Vec<u8>, s: &str) {
        out.extend_from_slice(s.as_bytes());
        out.push(0);
    }

    /// Simple-protocol query; fails on any server error.
    async fn simple(&mut self, sql: &str) {
        let mut body = vec![];
        Self::cstr(&mut body, sql);
        self.send(b'Q', &body).await;
        loop {
            let (tag, body) = self.message().await;
            match tag {
                b'Z' => break,
                b'E' => panic!("simple query failed: {body:?}"),
                _ => {}
            }
        }
    }
}

#[tokio::test]
async fn redefined_statement_without_describe_encodes_fresh_types() {
    let dir = tempfile::tempdir().unwrap();
    let server = start_server(&dir, free_port());
    let mut raw = Raw::connect(server.port).await;
    raw.simple(&format!("MURMUR UNLOCK KEY '{KEY}'")).await;

    // Describe under one SQL text: the server stashes INT8 for "S".
    let mut parse = vec![];
    Raw::cstr(&mut parse, "S");
    Raw::cstr(&mut parse, "SELECT 1");
    parse.extend_from_slice(&0i16.to_be_bytes());
    raw.send(b'P', &parse).await;
    raw.send(b'D', b"SS\0").await;
    raw.send(b'S', &[]).await;
    let mut saw_int8_description = false;
    loop {
        let (tag, body) = raw.message().await;
        match tag {
            b'Z' => break,
            b'E' => panic!("describe failed: {body:?}"),
            // RowDescription: one field, INT8 (OID 20).
            b'T' => {
                assert_eq!(u16::from_be_bytes(body[0..2].try_into().unwrap()), 1);
                let end = body[2..].iter().position(|b| *b == 0).unwrap() + 2;
                let oid = u32::from_be_bytes(body[end + 1 + 4 + 2..end + 1 + 4 + 2 + 4].try_into().unwrap());
                assert_eq!(oid, 20, "SELECT 1 must describe as INT8, got OID {oid}");
                saw_int8_description = true;
            }
            _ => {}
        }
    }
    assert!(saw_int8_description);

    // Re-parse the same name with different SQL and execute WITHOUT a
    // fresh Describe: rows must encode as the new statement's TEXT.
    let mut parse = vec![];
    Raw::cstr(&mut parse, "S");
    Raw::cstr(&mut parse, "SELECT 'x'");
    parse.extend_from_slice(&0i16.to_be_bytes());
    raw.send(b'P', &parse).await;
    let mut bind = vec![];
    Raw::cstr(&mut bind, "");
    Raw::cstr(&mut bind, "S");
    bind.extend_from_slice(&0i16.to_be_bytes()); // no parameters
    bind.extend_from_slice(&0i16.to_be_bytes());
    bind.extend_from_slice(&1i16.to_be_bytes()); // one binary result column
    bind.extend_from_slice(&1i16.to_be_bytes());
    raw.send(b'B', &bind).await;
    let mut execute = vec![];
    Raw::cstr(&mut execute, "");
    execute.extend_from_slice(&0i32.to_be_bytes());
    raw.send(b'E', &execute).await;
    raw.send(b'S', &[]).await;
    let mut rows: Vec<Vec<u8>> = vec![];
    loop {
        let (tag, body) = raw.message().await;
        match tag {
            b'Z' => break,
            b'E' => panic!("execute failed: {body:?}"),
            b'T' => panic!("unexpected RowDescription without Describe"),
            b'D' => {
                assert_eq!(u16::from_be_bytes(body[0..2].try_into().unwrap()), 1);
                let len = i32::from_be_bytes(body[2..6].try_into().unwrap());
                assert!(len >= 0);
                rows.push(body[6..6 + len as usize].to_vec());
            }
            _ => {}
        }
    }
    // TEXT 'x' is one byte; a stale INT8 stash would emit 8 zero bytes.
    assert_eq!(rows, vec![b"x".to_vec()]);
}
