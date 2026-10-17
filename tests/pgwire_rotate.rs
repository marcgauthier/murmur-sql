//! Master-key rotation over the wire: `MURMUR ROTATE KEY` rewraps the
//! registry, retires the old key immediately, and persists across restarts.
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};
use tokio_postgres::{Client, NoTls};

const PASSWORD: &str = "s3cret-password";
const KEY_ONE: &str = "first-encryption-key";
const KEY_TWO: &str = "second-encryption-key";

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

async fn unlock_with(client: &Client, key: &str) -> Result<(), tokio_postgres::Error> {
    client.simple_query(&format!("MURMUR UNLOCK KEY '{key}'")).await.map(|_| ())
}

fn code(error: &tokio_postgres::Error) -> String {
    error.code().map(|c| c.code().to_owned()).unwrap_or_default()
}

#[tokio::test]
async fn rotate_cycle_retires_old_key_and_persists_restart() {
    let dir = tempfile::tempdir().unwrap();
    let port = free_port();
    let config = write_config(&dir, port);
    let server = spawn_server(&config, port);
    let client = connect(&server).await;
    unlock_with(&client, KEY_ONE).await.unwrap();
    client
        .simple_query("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT NOT NULL)")
        .await
        .unwrap();
    client.simple_query("INSERT INTO t VALUES (1, 'kept')").await.unwrap();

    client
        .simple_query(&format!("MURMUR ROTATE KEY '{KEY_ONE}' '{KEY_TWO}'"))
        .await
        .unwrap();

    // Data is intact on the still-unlocked connection.
    let value: String = client
        .query_one("SELECT v FROM t WHERE id = 1", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(value, "kept");

    // New connections require the new key; the old key is dead immediately.
    let rotated = connect(&server).await;
    unlock_with(&rotated, KEY_TWO).await.unwrap();
    let stale = connect(&server).await;
    let error = unlock_with(&stale, KEY_ONE).await.unwrap_err();
    assert_eq!(code(&error), "28000", "{error}");

    // Rotation persists across restarts: only the new key reopens the store.
    drop(server);
    let port = free_port();
    let mut loaded = sqlite_session::config::Config::load(&config).unwrap();
    loaded.server.bind = format!("127.0.0.1:{port}");
    std::fs::write(&config, toml::to_string(&loaded).unwrap()).unwrap();
    let server = spawn_server(&config, port);
    let fresh = connect(&server).await;
    let error = unlock_with(&fresh, KEY_ONE).await.unwrap_err();
    assert_eq!(code(&error), "28000", "{error}");
    unlock_with(&fresh, KEY_TWO).await.unwrap();
    let value: String = fresh
        .query_one("SELECT v FROM t WHERE id = 1", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(value, "kept");
}

#[tokio::test]
async fn rotate_comma_form_and_rejections() {
    let dir = tempfile::tempdir().unwrap();
    let port = free_port();
    let server = spawn_server(&write_config(&dir, port), port);
    let client = connect(&server).await;
    unlock_with(&client, KEY_ONE).await.unwrap();
    client
        .simple_query("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT NOT NULL)")
        .await
        .unwrap();
    client.simple_query("INSERT INTO t VALUES (1, 'kept')").await.unwrap();

    // Wrong previous key: nothing rotates and the key is never echoed.
    let error = client
        .simple_query(&format!("MURMUR ROTATE KEY 'wrong' '{KEY_TWO}'"))
        .await
        .unwrap_err();
    assert_eq!(code(&error), "28000", "{error}");
    assert!(!error.to_string().contains("wrong"));
    let probe = connect(&server).await;
    unlock_with(&probe, KEY_ONE).await.unwrap();

    // Identical keys and malformed commands are usage errors.
    for (bad, expected) in [
        (format!("MURMUR ROTATE KEY '{KEY_ONE}' '{KEY_ONE}'"), "22023"),
        (format!("MURMUR ROTATE KEY '{KEY_ONE}'"), "42601"),
        ("MURMUR ROTATE KEY".to_string(), "42601"),
        (format!("MURMUR ROTATE '{KEY_ONE}' '{KEY_TWO}'"), "42601"),
    ] {
        let error = client.simple_query(&bad).await.unwrap_err();
        assert_eq!(code(&error), expected, "{bad}: {error}");
    }

    // The comma form from the request rotates exactly like the plain form.
    client
        .simple_query(&format!("MURMUR ROTATE KEY '{KEY_ONE}', '{KEY_TWO}'"))
        .await
        .unwrap();
    let rotated = connect(&server).await;
    unlock_with(&rotated, KEY_TWO).await.unwrap();
    let count: i64 = rotated
        .query_one("SELECT count(*) FROM t", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 1);
}

#[tokio::test]
async fn rotate_requires_unlock() {
    let dir = tempfile::tempdir().unwrap();
    let port = free_port();
    let server = spawn_server(&write_config(&dir, port), port);
    let client = connect(&server).await;
    let error = client
        .simple_query(&format!("MURMUR ROTATE KEY '{KEY_ONE}' '{KEY_TWO}'"))
        .await
        .unwrap_err();
    assert_eq!(code(&error), "28000", "{error}");
}
