//! `murmur-server` lifecycle: validate-only mode, startup, accept, bind
//! conflicts, and file-log output.
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

fn server() -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_murmur-server"));
    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
    cmd
}

fn write_config(dir: &std::path::Path, bind: &str) -> std::path::PathBuf {
    let path = dir.join("config.toml");
    std::fs::write(
        &path,
        format!(
            "[server]\nbind = \"{bind}\"\n[storage]\ndata_dir = \"{}\"\n[logging]\ndir = \"{}\"\n",
            dir.join("data").display(),
            dir.join("logs").display(),
        ),
    )
    .unwrap();
    path
}

struct Running {
    child: Child,
}

impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

fn wait_for_port(port: u16) {
    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("server did not open port {port} in time");
}

#[test]
fn validate_config_accepts_good_and_rejects_bad() {
    let dir = tempfile::tempdir().unwrap();
    let good = write_config(dir.path(), "127.0.0.1:5432");
    let out = server().arg("--config").arg(&good).arg("--validate-config").output().unwrap();
    assert!(out.status.success(), "stderr: {}", String::from_utf8_lossy(&out.stderr));
    assert!(String::from_utf8_lossy(&out.stdout).contains("config OK"));
    let bad = dir.path().join("bad.toml");
    std::fs::write(&bad, "[server]\nbind = \"nope\"\n").unwrap();
    let out = server().arg("--config").arg(&bad).arg("--validate-config").output().unwrap();
    assert_eq!(out.status.code(), Some(1));
}

#[test]
fn starts_accepts_and_logs_to_file() {
    let dir = tempfile::tempdir().unwrap();
    let port = free_port();
    let config = write_config(dir.path(), &format!("127.0.0.1:{port}"));
    let running = Running {
        child: server().arg("--config").arg(&config).spawn().unwrap(),
    };
    wait_for_port(port);
    // Second connection proves the accept loop keeps serving.
    assert!(std::net::TcpStream::connect(("127.0.0.1", port)).is_ok());
    // The file writer is asynchronous; wait for the accept line before kill.
    let deadline = Instant::now() + Duration::from_secs(10);
    let log_path = loop {
        let logs: Vec<_> = std::fs::read_dir(dir.path().join("logs")).unwrap().collect();
        assert_eq!(logs.len(), 1, "one daily log file");
        let path = logs[0].as_ref().unwrap().path();
        let content = std::fs::read_to_string(&path).unwrap();
        if content.contains("connection accepted") {
            break path;
        }
        assert!(Instant::now() < deadline, "accept line never logged:\n{content}");
        std::thread::sleep(Duration::from_millis(50));
    };
    drop(running);
    let content = std::fs::read_to_string(&log_path).unwrap();
    assert!(content.contains("listening"), "log:\n{content}");
    assert!(content.contains("connection accepted"), "log:\n{content}");
}

#[test]
fn occupied_port_exits_2() {
    let dir = tempfile::tempdir().unwrap();
    let port = free_port();
    let _held = TcpListener::bind(("127.0.0.1", port)).unwrap();
    let config = write_config(dir.path(), &format!("127.0.0.1:{port}"));
    let out = server().arg("--config").arg(&config).output().unwrap();
    assert_eq!(out.status.code(), Some(2), "stdout: {}", String::from_utf8_lossy(&out.stdout));
}
