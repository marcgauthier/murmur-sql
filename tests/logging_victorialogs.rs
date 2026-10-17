//! VictoriaLogs sink pushes Loki-compatible batches to the configured URL.
use sqlite_session::config::LoggingConfig;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::time::Duration;

#[test]
fn pushes_loki_batch_to_configured_url() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        socket.set_read_timeout(Some(Duration::from_secs(20))).unwrap();
        let mut reader = BufReader::new(socket.try_clone().unwrap());
        let mut content_length = 0usize;
        let mut path = String::new();
        loop {
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            if path.is_empty() {
                path = line.trim().to_owned();
            }
            let lower = line.to_ascii_lowercase();
            if let Some(value) = lower.strip_prefix("content-length:") {
                content_length = value.trim().parse().unwrap();
            }
            if line == "\r\n" || line == "\n" {
                break;
            }
        }
        let mut body = vec![0u8; content_length];
        reader.read_exact(&mut body).unwrap();
        tx.send((path, String::from_utf8(body).unwrap())).unwrap();
        socket
            .write_all(b"HTTP/1.1 204 No Content\r\ncontent-length: 0\r\nconnection: close\r\n\r\n")
            .unwrap();
    });

    let dir = tempfile::tempdir().unwrap();
    let mut logging = LoggingConfig::default();
    logging.dir = dir.path().join("logs").display().to_string();
    logging.victorialogs.enabled = true;
    logging.victorialogs.url = format!("http://127.0.0.1:{port}");
    logging.victorialogs.labels.insert("service".into(), "murmur-test".into());

    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(async {
        let (subscriber, _guard) = sqlite_session::logging::build(&logging, runtime.handle()).unwrap();
        tracing::subscriber::with_default(subscriber, || {
            tracing::info!("hello victorialogs");
        });
        // Dropping the subscriber closes the sink; give the push time to land.
        tokio::time::sleep(Duration::from_secs(5)).await;
    });

    let (path, body) = rx.recv_timeout(Duration::from_secs(10)).unwrap();
    assert!(path.contains("/loki/api/v1/push"), "request: {path}");
    assert!(body.contains("murmur-test"), "body: {body}");
    assert!(body.contains("hello victorialogs"), "body: {body}");
}
