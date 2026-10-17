//! Remote backup over the wire: SAVE/LIST/DOWNLOAD round-trips, chunked
//! UPLOAD, live RESTORE, and the locked/error gates for all of them.
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

fn write_config(dir: &tempfile::TempDir, port: u16) -> std::path::PathBuf {
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
    config
}

fn start_server(dir: &tempfile::TempDir, port: u16) -> Server {
    let config = write_config(dir, port);
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

async fn unlocked(server: &Server) -> Client {
    let client = connect(server).await;
    client
        .simple_query(&format!("MURMUR UNLOCK KEY '{KEY}'"))
        .await
        .unwrap();
    client
}

fn code(error: &tokio_postgres::Error) -> String {
    error.code().map(|c| c.code().to_owned()).unwrap_or_default()
}

#[tokio::test]
async fn backup_save_list_download_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let server = start_server(&dir, free_port());
    let client = unlocked(&server).await;
    client
        .simple_query("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT NOT NULL)")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO t VALUES (1, 'one'), (2, 'two')")
        .await
        .unwrap();

    // SAVE over the extended protocol exercises the Describe shape.
    let rows = client.query("MURMUR BACKUP SAVE", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    let name: String = rows[0].get("name");
    let size: i64 = rows[0].get("size");
    assert!(name.starts_with("murmur-"), "{name}");
    assert!(name.ends_with(".murmur-backup"), "{name}");
    assert!(size > 0);
    let path = dir.path().join("backups").join(&name);
    assert_eq!(std::fs::metadata(&path).unwrap().len(), size as u64);

    let rows = client.query("MURMUR BACKUP LIST", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>("name"), name);
    assert_eq!(rows[0].get::<_, i64>("size"), size);
    let modified: String = rows[0].get("modified");
    assert!(modified.starts_with("20") && modified.ends_with('Z'), "{modified}");

    // Chunked download reassembles the exact file bytes.
    let file_bytes = std::fs::read(&path).unwrap();
    let mut fetched = Vec::new();
    let mut offset = 0i64;
    loop {
        let rows = client
            .query(
                &format!("MURMUR BACKUP DOWNLOAD '{name}' OFFSET {offset} LIMIT 100"),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get::<_, i64>("offset"), offset);
        let total: i64 = rows[0].get("total");
        assert_eq!(total as usize, file_bytes.len());
        let chunk: Vec<u8> = rows[0].get("chunk");
        if chunk.is_empty() {
            break;
        }
        fetched.extend_from_slice(&chunk);
        offset += chunk.len() as i64;
        assert!(offset <= total);
    }
    assert_eq!(fetched, file_bytes);

    // Past-EOF reads return an empty chunk, not an error.
    let rows = client
        .query(
            &format!("MURMUR BACKUP DOWNLOAD '{name}' OFFSET {} LIMIT 10", size + 100),
            &[],
        )
        .await
        .unwrap();
    assert!(rows[0].get::<_, Vec<u8>>("chunk").is_empty());

    // Missing files fail loudly; the name is echoed but nothing leaks.
    let error = client
        .query("MURMUR BACKUP DOWNLOAD 'absent.murmur-backup'", &[])
        .await
        .unwrap_err();
    assert_eq!(code(&error), "58P01", "{error}");
}

#[tokio::test]
async fn backup_upload_chunks_finish_and_abort() {
    use base64::Engine;
    let dir = tempfile::tempdir().unwrap();
    let server = start_server(&dir, free_port());
    let client = unlocked(&server).await;
    let engine = base64::engine::general_purpose::STANDARD;

    client
        .simple_query("MURMUR BACKUP UPLOAD BEGIN 'copy.murmur-backup'")
        .await
        .unwrap();
    // Partial uploads stay hidden from LIST.
    assert!(client.query("MURMUR BACKUP LIST", &[]).await.unwrap().is_empty());
    // Bad base64 and missing sessions fail without touching the session.
    let error = client
        .simple_query("MURMUR BACKUP UPLOAD APPEND 'copy.murmur-backup' '!!!'")
        .await
        .unwrap_err();
    assert_eq!(code(&error), "22023", "{error}");
    let error = client
        .simple_query("MURMUR BACKUP UPLOAD APPEND 'other.murmur-backup' 'eA=='")
        .await
        .unwrap_err();
    assert_eq!(code(&error), "22023", "{error}");

    let payload: Vec<u8> = (0..5000u32).map(|i| (i % 251) as u8).collect();
    for chunk in payload.chunks(1000) {
        client
            .simple_query(&format!(
                "MURMUR BACKUP UPLOAD APPEND 'copy.murmur-backup' '{}'",
                engine.encode(chunk)
            ))
            .await
            .unwrap();
    }
    let rows = client
        .query("MURMUR BACKUP UPLOAD FINISH 'copy.murmur-backup'", &[])
        .await
        .unwrap();
    assert_eq!(rows[0].get::<_, String>("name"), "copy.murmur-backup");
    assert_eq!(rows[0].get::<_, i64>("size"), payload.len() as i64);
    assert_eq!(
        std::fs::read(dir.path().join("backups").join("copy.murmur-backup")).unwrap(),
        payload
    );

    // FINISH refuses to overwrite; ABORT discards the retry cleanly.
    client
        .simple_query("MURMUR BACKUP UPLOAD BEGIN 'copy.murmur-backup'")
        .await
        .unwrap();
    client
        .simple_query("MURMUR BACKUP UPLOAD APPEND 'copy.murmur-backup' 'eA=='")
        .await
        .unwrap();
    let error = client
        .query("MURMUR BACKUP UPLOAD FINISH 'copy.murmur-backup'", &[])
        .await
        .unwrap_err();
    assert_eq!(code(&error), "23505", "{error}");
    client
        .simple_query("MURMUR BACKUP UPLOAD ABORT 'copy.murmur-backup'")
        .await
        .unwrap();
    client
        .simple_query("MURMUR BACKUP UPLOAD ABORT 'copy.murmur-backup'")
        .await
        .unwrap();
    assert_eq!(
        std::fs::read(dir.path().join("backups").join("copy.murmur-backup")).unwrap(),
        payload
    );

    // Traversal and hidden names never reach the filesystem.
    for bad in ["../evil", "a/b", ".hidden", "a b"] {
        let error = client
            .simple_query(&format!("MURMUR BACKUP UPLOAD BEGIN '{bad}'"))
            .await
            .unwrap_err();
        assert_eq!(code(&error), "22023", "{bad}: {error}");
    }
}

#[tokio::test]
async fn restore_cycle_swaps_data_and_keeps_serving() {
    let dir = tempfile::tempdir().unwrap();
    let server = start_server(&dir, free_port());
    let client = unlocked(&server).await;
    client
        .simple_query("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT NOT NULL)")
        .await
        .unwrap();
    client.simple_query("INSERT INTO t VALUES (1, 'one')").await.unwrap();
    let rows = client.query("MURMUR BACKUP SAVE", &[]).await.unwrap();
    let first: String = rows[0].get("name");
    client.simple_query("INSERT INTO t VALUES (2, 'two')").await.unwrap();
    let rows = client.query("MURMUR BACKUP SAVE", &[]).await.unwrap();
    let second: String = rows[0].get("name");
    assert_ne!(first, second, "same-second saves must not collide");

    client
        .simple_query(&format!("MURMUR RESTORE FROM '{first}' KEY '{KEY}'"))
        .await
        .unwrap();
    let rows = client.query("SELECT v FROM t ORDER BY id", &[]).await.unwrap();
    let values: Vec<String> = rows.iter().map(|r| r.get(0)).collect();
    assert_eq!(values, ["one"], "restore must roll back to the backup");

    // The server keeps serving on the same unlocked connection.
    client.simple_query("INSERT INTO t VALUES (3, 'three')").await.unwrap();
    let count: i64 = client.query_one("SELECT count(*) FROM t", &[]).await.unwrap().get(0);
    assert_eq!(count, 2);

    // The pre-restore data dir is preserved for the operator.
    let preserved: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().contains(".replaced-"))
        .collect();
    assert_eq!(preserved.len(), 1);

    // A fresh connection unlocks against the restored database.
    let other = unlocked(&server).await;
    let count: i64 = other.query_one("SELECT count(*) FROM t", &[]).await.unwrap().get(0);
    assert_eq!(count, 2);
}

#[tokio::test]
async fn restore_rejects_wrong_key_garbage_and_bad_usage() {
    use base64::Engine;
    let dir = tempfile::tempdir().unwrap();
    let server = start_server(&dir, free_port());
    let client = unlocked(&server).await;
    client
        .simple_query("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT NOT NULL)")
        .await
        .unwrap();
    client.simple_query("INSERT INTO t VALUES (1, 'one')").await.unwrap();
    let rows = client.query("MURMUR BACKUP SAVE", &[]).await.unwrap();
    let name: String = rows[0].get("name");

    // Wrong key: nothing is unpacked, closed, or swapped.
    let error = client
        .simple_query(&format!("MURMUR RESTORE FROM '{name}' KEY 'wrong-key'"))
        .await
        .unwrap_err();
    assert_eq!(code(&error), "28000", "{error}");
    assert!(!error.to_string().contains("wrong-key"));

    // Garbage file: unpack fails before the live database is touched.
    let engine = base64::engine::general_purpose::STANDARD;
    client
        .simple_query("MURMUR BACKUP UPLOAD BEGIN 'garbage.murmur-backup'")
        .await
        .unwrap();
    client
        .simple_query(&format!(
            "MURMUR BACKUP UPLOAD APPEND 'garbage.murmur-backup' '{}'",
            engine.encode(b"definitely not an archive")
        ))
        .await
        .unwrap();
    client
        .simple_query("MURMUR BACKUP UPLOAD FINISH 'garbage.murmur-backup'")
        .await
        .unwrap();
    let error = client
        .simple_query(&format!("MURMUR RESTORE FROM 'garbage.murmur-backup' KEY '{KEY}'"))
        .await
        .unwrap_err();
    assert_eq!(code(&error), "XX001", "{error}");

    // Missing file and malformed commands.
    let error = client
        .simple_query(&format!("MURMUR RESTORE FROM 'absent.murmur-backup' KEY '{KEY}'"))
        .await
        .unwrap_err();
    assert_eq!(code(&error), "58P01", "{error}");
    for bad in [
        format!("MURMUR RESTORE FROM '{name}'"),
        "MURMUR RESTORE FROM".to_string(),
        format!("MURMUR RESTORE '{name}' KEY '{KEY}'"),
    ] {
        let error = client.simple_query(&bad).await.unwrap_err();
        assert_eq!(code(&error), "42601", "{bad}: {error}");
    }

    // The live data survived every rejected restore.
    let count: i64 = client.query_one("SELECT count(*) FROM t", &[]).await.unwrap().get(0);
    assert_eq!(count, 1);
}

#[tokio::test]
async fn backup_commands_require_unlock() {
    let dir = tempfile::tempdir().unwrap();
    let server = start_server(&dir, free_port());
    let client = connect(&server).await;
    for command in [
        "MURMUR BACKUP SAVE".to_string(),
        "MURMUR BACKUP LIST".to_string(),
        "MURMUR BACKUP UPLOAD BEGIN 'x.murmur-backup'".to_string(),
        "MURMUR BACKUP DOWNLOAD 'x.murmur-backup'".to_string(),
        format!("MURMUR RESTORE FROM 'x.murmur-backup' KEY '{KEY}'"),
    ] {
        let error = client.simple_query(&command).await.unwrap_err();
        assert_eq!(code(&error), "28000", "{command}: {error}");
    }
}
