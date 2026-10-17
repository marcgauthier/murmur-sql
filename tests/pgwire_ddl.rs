//! DDL rules over the wire: additive schema changes work; drops and
//! renames need a local-only table; the unsupported-definition block list is
//! rejected; primary-key updates are denied by the authorizer.
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

fn start_server(dir: &tempfile::TempDir, port: u16) -> Server {
    let hash = sqlite_session::server::auth::hash_password(PASSWORD).unwrap();
    let config = dir.path().join("config.toml");
    std::fs::write(
        &config,
        format!(
            "[server]\nbind = \"127.0.0.1:{port}\"\n\
             [storage]\ndata_dir = \"{}\"\n\
             [[auth.users]]\nname = \"admin\"\npassword_hash = \"{hash}\"\n\
             [logging]\ndir = \"{}\"\n\
             [backups]\ndir = \"{}\"\n",
            dir.path().join("data").display(),
            dir.path().join("logs").display(),
            dir.path().join("backups").display(),
        ),
    )
    .unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_murmur-server"))
        .arg("--config")
        .arg(&config)
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

async fn unlocked(server: &Server) -> Client {
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
        .simple_query(&format!("MURMUR UNLOCK KEY '{KEY}'"))
        .await
        .unwrap();
    client
}

fn code(error: &tokio_postgres::Error) -> String {
    error.code().map(|c| c.code().to_owned()).unwrap_or_default()
}

async fn expect_code(client: &Client, sql: &str, want: &str) {
    let error = client.simple_query(sql).await.unwrap_err();
    assert_eq!(code(&error), want, "{sql}: {error}");
}

#[tokio::test]
async fn additive_ddl_works_and_drops_need_local_tables() {
    let dir = tempfile::tempdir().unwrap();
    let server = start_server(&dir, free_port());
    let client = unlocked(&server).await;

    client
        .simple_query("CREATE TABLE docs(id INTEGER PRIMARY KEY, title TEXT)")
        .await
        .unwrap();
    client.simple_query("CREATE INDEX docs_title ON docs(title)").await.unwrap();
    client.simple_query("ALTER TABLE docs ADD COLUMN body TEXT").await.unwrap();
    client.simple_query("INSERT INTO docs VALUES (1, 'a', 'b')").await.unwrap();

    client.simple_query("MURMUR SET TABLE docs REPLICATED true").await.unwrap();
    // Drops and renames fail while the table replicates ...
    for ddl in [
        "DROP TABLE docs",
        "ALTER TABLE docs RENAME TO moved",
        "ALTER TABLE docs RENAME COLUMN title TO heading",
        "ALTER TABLE docs DROP COLUMN body",
    ] {
        expect_code(&client, ddl, "22023").await;
    }
    // ... and the data is untouched.
    let count: i64 = client
        .query_one("SELECT count(*) FROM docs", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 1);

    // Disable replication: the same operations succeed.
    client
        .simple_query("MURMUR SET TABLE docs REPLICATED false")
        .await
        .unwrap();
    client.simple_query("ALTER TABLE docs RENAME COLUMN title TO heading").await.unwrap();
    client.simple_query("ALTER TABLE docs DROP COLUMN body").await.unwrap();
    client.simple_query("ALTER TABLE docs RENAME TO moved").await.unwrap();
    // Renamed tables remain local-only.
    client.simple_query("DROP TABLE moved").await.unwrap();
    expect_code(&client, "SELECT * FROM moved", "42P01").await;
}

#[tokio::test]
async fn unsupported_definitions_are_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let server = start_server(&dir, free_port());
    let client = unlocked(&server).await;

    // Schema inspection rejections surface as 22023 ...
    for ddl in [
        "CREATE TABLE u1(id INTEGER PRIMARY KEY, v TEXT UNIQUE)",
        "CREATE TABLE n1(id TEXT PRIMARY KEY, v TEXT)",
        "CREATE TABLE f1(id INTEGER PRIMARY KEY, o INTEGER REFERENCES other(id))",
        "CREATE TABLE c1(id INTEGER PRIMARY KEY, v INTEGER CHECK(v > 0))",
        "CREATE TABLE a1(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)",
        "CREATE TABLE g1(id INTEGER PRIMARY KEY, v TEXT GENERATED ALWAYS AS ('x'))",
    ] {
        expect_code(&client, ddl, "22023").await;
    }
    // ... unique indexes too, whether or not the table is new ...
    client.simple_query("CREATE TABLE u3(id INTEGER PRIMARY KEY, v TEXT)").await.unwrap();
    expect_code(&client, "CREATE UNIQUE INDEX u3_v ON u3(v)", "22023").await;
    // ... while triggers, views, and PK updates are denied by the authorizer.
    client.simple_query("CREATE TABLE t1(id INTEGER PRIMARY KEY)").await.unwrap();
    expect_code(
        &client,
        "CREATE TRIGGER t1_tr AFTER INSERT ON t1 BEGIN SELECT 1; END",
        "42501",
    )
    .await;
    expect_code(&client, "CREATE VIEW vv AS SELECT * FROM t1", "42501").await;
    client.simple_query("INSERT INTO t1 VALUES (1)").await.unwrap();
    client.simple_query("MURMUR SET TABLE t1 REPLICATED true").await.unwrap();
    expect_code(&client, "UPDATE t1 SET id = 2 WHERE id = 1", "42501").await;
    // Ordinary updates to non-key columns still work.
    client.simple_query("ALTER TABLE t1 ADD COLUMN v TEXT").await.unwrap();
    client.simple_query("UPDATE t1 SET v = 'x' WHERE id = 1").await.unwrap();
}

#[tokio::test]
async fn column_fts_simple_and_extended_queries() {
    let dir = tempfile::tempdir().unwrap();
    let server = start_server(&dir, free_port());
    let client = unlocked(&server).await;
    client.simple_query("CREATE TABLE docs(id INTEGER PRIMARY KEY,body TEXT); INSERT INTO docs VALUES(1,'hello world'); MURMUR SET COLUMN docs body FTS true; MURMUR REFRESH INDEXES").await.unwrap();
    let rows = client.simple_query("MURMUR SHOW COLUMNS FTS").await.unwrap();
    let name = rows.iter().find_map(|r| match r {
        tokio_postgres::SimpleQueryMessage::Row(r) if r.get("column") == Some("body") => {
            assert_eq!(r.get("enabled"), Some("true"));
            r.get("index_name").map(str::to_owned)
        }
        _ => None,
    }).unwrap();
    let query = format!("SELECT d.id FROM docs d JOIN temp.\"{name}\" f ON f.rowid=d.id WHERE \"{name}\" MATCH $1");
    assert_eq!(client.query_one(&query, &[&"hello"]).await.unwrap().get::<_, i64>(0), 1);
    let sql = format!("SELECT count(*) FROM temp.\"{name}\" WHERE \"{name}\" MATCH 'world'");
    let rows = client.simple_query(&sql).await.unwrap();
    assert!(rows.iter().any(|r| matches!(r,tokio_postgres::SimpleQueryMessage::Row(r) if r.get(0)==Some("1"))));
    expect_code(&client,"MURMUR SET COLUMN docs id FTS true","22023").await;
    expect_code(&client,"MURMUR SET COLUMN docs body FTS maybe","22023").await;
    expect_code(&client,"MURMUR SET COLUMN docs body FTS","42601").await;
    client.simple_query("MURMUR SET COLUMN docs body FTS false").await.unwrap();
    let rows=client.query("MURMUR SHOW COLUMNS FTS", &[]).await.unwrap();
    assert!(rows.iter().all(|r| r.get::<_,String>("enabled")=="false" && r.get::<_,Option<String>>("index_name").is_none()));
}

#[cfg(feature="vector")]
#[tokio::test]
async fn vector_queries_describe_numeric_empty_results_and_accept_binary_parameters() {
    use tokio_postgres::types::Type;
    let dir=tempfile::tempdir().unwrap();let server=start_server(&dir,free_port());let client=unlocked(&server).await;
    client.simple_query("CREATE TABLE docs(id INTEGER PRIMARY KEY,embedding BLOB); MURMUR SET COLUMN docs embedding VECTOR true DIMENSIONS 2").await.unwrap();
    let rows=client.query("MURMUR SHOW COLUMNS VECTOR", &[]).await.unwrap();
    let index=rows.iter().find(|r|r.get::<_,String>("column")=="embedding").unwrap().get::<_,String>("index_name");
    let query=format!("SELECT rowid,distance AS score FROM temp.\"{index}\" v WHERE v.embedding MATCH $1 AND v.k=$2 ORDER BY distance");
    let statement=client.prepare(&query).await.unwrap();
    assert_eq!(statement.params(), &[Type::TEXT,Type::INT8]);
    assert_eq!(statement.columns()[0].type_(), &Type::INT8);
    assert_eq!(statement.columns()[1].type_(), &Type::FLOAT8);
    assert!(client.query(&statement,&[&"[1,0]",&1i64]).await.unwrap().is_empty());
    let embedding=[1f32,0.].into_iter().flat_map(f32::to_le_bytes).collect::<Vec<_>>();
    client.execute("INSERT INTO docs VALUES($1,$2)",&[&1i64,&embedding]).await.unwrap();
    client.simple_query("MURMUR REFRESH INDEXES").await.unwrap();
    let row=client.query_one(&statement,&[&"[1,0]",&1i64]).await.unwrap();
    assert_eq!(row.get::<_,i64>(0),1);assert_eq!(row.get::<_,f64>(1),0.);
    let binary=client.prepare_typed(&query,&[Type::BYTEA,Type::INT8]).await.unwrap();
    let row=client.query_one(&binary,&[&embedding,&1i64]).await.unwrap();
    assert_eq!(row.get::<_,f64>(1),0.);
    let simple=client.simple_query(&format!("SELECT rowid,distance FROM temp.\"{index}\" WHERE embedding MATCH '[1,0]' AND k=1 ORDER BY distance")).await.unwrap();
    assert!(simple.iter().any(|r|matches!(r,tokio_postgres::SimpleQueryMessage::Row(r) if r.get(0)==Some("1"))));
}
