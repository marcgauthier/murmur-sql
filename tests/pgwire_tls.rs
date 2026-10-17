//! Wire TLS: dev-mode TLS + plaintext, production mTLS enforcement, and
//! fail-fast credential errors.
use std::net::{IpAddr, Ipv4Addr, TcpListener};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_postgres::NoTls;

const PASSWORD: &str = "tls-test-password";
const KEY: &str = "tls-test-key";

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

struct Pki {
    ca_pem: String,
    ca_der: Vec<u8>,
    server_cert_pem: String,
    server_key_pem: String,
    client_chain_der: Vec<u8>,
    client_key_der: Vec<u8>,
}

fn make_pki() -> Pki {
    let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![rcgen::KeyUsagePurpose::KeyCertSign, rcgen::KeyUsagePurpose::DigitalSignature];
    let ca_key = rcgen::KeyPair::generate().unwrap();
    let ca = ca_params.self_signed(&ca_key).unwrap();

    let mut srv_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    srv_params.subject_alt_names = vec![rcgen::SanType::IpAddress(IpAddr::V4(Ipv4Addr::LOCALHOST))];
    srv_params.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ServerAuth];
    let srv_key = rcgen::KeyPair::generate().unwrap();
    let server = srv_params.signed_by(&srv_key, &ca, &ca_key).unwrap();

    let mut cli_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    cli_params.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ClientAuth];
    let cli_key = rcgen::KeyPair::generate().unwrap();
    let client = cli_params.signed_by(&cli_key, &ca, &ca_key).unwrap();

    Pki {
        ca_pem: ca.pem(),
        ca_der: ca.der().to_vec(),
        server_cert_pem: server.pem(),
        server_key_pem: srv_key.serialize_pem(),
        client_chain_der: client.der().to_vec(),
        client_key_der: cli_key.serialize_der(),
    }
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

fn write_config(
    dir: &tempfile::TempDir,
    port: u16,
    pki: &Pki,
    production: bool,
    with_ca: bool,
) -> std::path::PathBuf {
    let hash = sqlite_session::server::auth::hash_password(PASSWORD).unwrap();
    let cert_path = dir.path().join("server.crt");
    let key_path = dir.path().join("server.key");
    let ca_path = dir.path().join("ca.crt");
    std::fs::write(&cert_path, &pki.server_cert_pem).unwrap();
    std::fs::write(&key_path, &pki.server_key_pem).unwrap();
    std::fs::write(&ca_path, &pki.ca_pem).unwrap();
    let config = dir.path().join("config.toml");
    let ca_line = if with_ca {
        format!("ca_file = \"{}\"\n", ca_path.display())
    } else {
        String::new()
    };
    std::fs::write(
        &config,
        format!(
            "[server]\nbind = \"127.0.0.1:{port}\"\nproduction = {production}\n\
             [server.tls]\nenabled = true\ncert_file = \"{}\"\nkey_file = \"{}\"\n{ca_line}\
             [storage]\ndata_dir = \"{}\"\n\
             [[auth.users]]\nname = \"admin\"\npassword_hash = \"{hash}\"\n\
             [logging]\ndir = \"{}\"\n",
            cert_path.display(),
            key_path.display(),
            dir.path().join("data").display(),
            dir.path().join("logs").display(),
        ),
    )
    .unwrap();
    config
}

fn spawn_server(config: &std::path::Path, port: u16) -> Server {
    let child = Command::new(env!("CARGO_BIN_EXE_murmur-server"))
        .arg("--config")
        .arg(config)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut server = Server { child, port };
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return server;
        }
        match server.child.try_wait().unwrap() {
            Some(status) => panic!("server exited early: {status}"),
            None => {}
        }
        assert!(Instant::now() < deadline, "server did not start in time");
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn client_config(pki: &Pki, with_client_cert: bool) -> rustls::ClientConfig {
    let provider: std::sync::Arc<rustls::crypto::CryptoProvider> =
        rustls::crypto::ring::default_provider().into();
    let mut roots = rustls::RootCertStore::empty();
    roots.add(rustls::pki_types::CertificateDer::from(pki.ca_der.clone())).unwrap();
    let builder = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
        .unwrap()
        .with_root_certificates(roots);
    if with_client_cert {
        builder
            .with_client_auth_cert(
                vec![rustls::pki_types::CertificateDer::from(pki.client_chain_der.clone())],
                rustls::pki_types::PrivateKeyDer::Pkcs8(
                    rustls::pki_types::PrivatePkcs8KeyDer::from(pki.client_key_der.clone()),
                ),
            )
            .unwrap()
    } else {
        builder.with_no_client_auth()
    }
}

/// Minimal TLS Postgres client: SSLRequest, startup, cleartext auth, and the
/// simple-query protocol. Avoids a TLS-capable driver dependency; the message
/// shapes asserted here are the stable documented wire format.
struct TlsClient {
    stream: tokio_rustls::client::TlsStream<tokio::net::TcpStream>,
}

impl TlsClient {
    async fn connect(
        server: &Server,
        config: rustls::ClientConfig,
    ) -> Result<Self, String> {
        let tcp = tokio::net::TcpStream::connect(("127.0.0.1", server.port))
            .await
            .map_err(|e| e.to_string())?;
        let (mut reader, mut writer) = tcp.into_split();
        let mut request = Vec::new();
        request.extend_from_slice(&8i32.to_be_bytes());
        request.extend_from_slice(&80877103i32.to_be_bytes());
        writer.write_all(&request).await.map_err(|e| e.to_string())?;
        let mut reply = [0u8; 1];
        reader.read_exact(&mut reply).await.map_err(|e| e.to_string())?;
        if reply[0] != b'S' {
            return Err("server refused TLS".into());
        }
        let tcp = reader.reunite(writer).map_err(|e| e.to_string())?;
        let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
        let server_name: rustls::pki_types::ServerName<'static> =
            "127.0.0.1".try_into().map_err(|e| format!("bad server name: {e}"))?;
        let mut stream = connector.connect(server_name, tcp).await.map_err(|e| e.to_string())?;

        let mut startup = Vec::new();
        startup.extend_from_slice(&196608i32.to_be_bytes());
        for (key, value) in [("user", "admin"), ("database", "murmur")] {
            startup.extend_from_slice(key.as_bytes());
            startup.push(0);
            startup.extend_from_slice(value.as_bytes());
            startup.push(0);
        }
        startup.push(0);
        let mut frame = (startup.len() as i32 + 4).to_be_bytes().to_vec();
        frame.extend_from_slice(&startup);
        stream.write_all(&frame).await.map_err(|e| e.to_string())?;

        let mut client = Self { stream };
        loop {
            let (tag, body) = client.message().await?;
            match tag {
                b'R' => {
                    let kind = i32::from_be_bytes(body[0..4].try_into().unwrap());
                    if kind == 0 {
                        continue;
                    }
                    if kind == 3 {
                        let mut password = PASSWORD.as_bytes().to_vec();
                        password.push(0);
                        client.send(b'p', &password).await?;
                        continue;
                    }
                    return Err(format!("unexpected auth kind {kind}"));
                }
                b'S' | b'K' => continue,
                b'Z' => return Ok(client),
                b'E' => return Err(format!("startup error: {}", error_field(&body, b'M'))),
                other => return Err(format!("unexpected startup tag {other}")),
            }
        }
    }

    async fn send(&mut self, tag: u8, body: &[u8]) -> Result<(), String> {
        let mut frame = vec![tag];
        frame.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
        frame.extend_from_slice(body);
        self.stream.write_all(&frame).await.map_err(|e| e.to_string())
    }

    async fn message(&mut self) -> Result<(u8, Vec<u8>), String> {
        let mut tag = [0u8; 1];
        self.stream.read_exact(&mut tag).await.map_err(|e| e.to_string())?;
        let mut len = [0u8; 4];
        self.stream.read_exact(&mut len).await.map_err(|e| e.to_string())?;
        let len = u32::from_be_bytes(len) as usize;
        if len < 4 {
            return Err("short message".into());
        }
        let mut body = vec![0u8; len - 4];
        self.stream.read_exact(&mut body).await.map_err(|e| e.to_string())?;
        Ok((tag[0], body))
    }

    /// Run simple-protocol statements, returning one row set per statement.
    async fn simple_query(&mut self, sql: &str) -> Result<Vec<Vec<Vec<Option<String>>>>, String> {
        let mut body = sql.as_bytes().to_vec();
        body.push(0);
        self.send(b'Q', &body).await?;
        let mut sets = Vec::new();
        let mut rows = Vec::new();
        loop {
            let (tag, body) = self.message().await?;
            match tag {
                b'T' => {
                    rows = Vec::new();
                }
                b'D' => {
                    let mut columns = u16::from_be_bytes(body[0..2].try_into().unwrap()) as usize;
                    let mut offset = 2;
                    let mut row = Vec::new();
                    while columns > 0 {
                        let len = i32::from_be_bytes(body[offset..offset + 4].try_into().unwrap());
                        offset += 4;
                        if len < 0 {
                            row.push(None);
                        } else {
                            let end = offset + len as usize;
                            row.push(Some(String::from_utf8_lossy(&body[offset..end]).into_owned()));
                            offset = end;
                        }
                        columns -= 1;
                    }
                    rows.push(row);
                }
                b'C' => {
                    sets.push(std::mem::take(&mut rows));
                }
                b'E' => {
                    return Err(format!(
                        "query error {}: {}",
                        error_field(&body, b'C'),
                        error_field(&body, b'M')
                    ));
                }
                b'Z' | b'I' => return Ok(sets),
                other => return Err(format!("unexpected tag {other}")),
            }
        }
    }
}

fn error_field(body: &[u8], wanted: u8) -> String {
    let mut offset = 0;
    while offset < body.len() && body[offset] != 0 {
        let code = body[offset];
        let start = offset + 1;
        let end = body[start..].iter().position(|b| *b == 0).map(|i| start + i).unwrap_or(body.len());
        if code == wanted {
            return String::from_utf8_lossy(&body[start..end]).into_owned();
        }
        offset = end + 1;
    }
    String::new()
}

async fn connect_plain(server: &Server) -> Result<tokio_postgres::Client, tokio_postgres::Error> {
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=admin password={PASSWORD} dbname=murmur",
            server.port
        ),
        NoTls,
    )
    .await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(client)
}

#[tokio::test]
async fn dev_tls_accepts_tls_and_plaintext() {
    let dir = tempfile::tempdir().unwrap();
    let pki = make_pki();
    let port = free_port();
    let config = write_config(&dir, port, &pki, false, false);
    let server = spawn_server(&config, port);

    let mut tls_client = TlsClient::connect(&server, client_config(&pki, false)).await.unwrap();
    tls_client.simple_query(&format!("MURMUR UNLOCK KEY '{KEY}'")).await.unwrap();
    let sets = tls_client.simple_query("SELECT 1").await.unwrap();
    assert_eq!(sets, vec![vec![vec![Some("1".to_owned())]]]);

    let plain_client = connect_plain(&server).await.unwrap();
    plain_client.simple_query(&format!("MURMUR UNLOCK KEY '{KEY}'")).await.unwrap();
    assert_eq!(plain_client.simple_query("SELECT 2").await.unwrap().len(), 3);
}

#[tokio::test]
async fn production_enforces_mutual_tls() {
    let dir = tempfile::tempdir().unwrap();
    let pki = make_pki();
    let port = free_port();
    let config = write_config(&dir, port, &pki, true, true);
    let server = spawn_server(&config, port);

    // Plaintext is rejected with a clear error, not a hang.
    let error = connect_plain(&server).await.unwrap_err();
    let db = error.as_db_error().unwrap();
    assert_eq!(db.code().code(), "28000");
    assert!(db.message().contains("TLS"), "{db:?}");

    // TLS without a client certificate fails the handshake.
    assert!(TlsClient::connect(&server, client_config(&pki, false)).await.is_err());

    // Full mTLS works end to end.
    let mut client = TlsClient::connect(&server, client_config(&pki, true)).await.unwrap();
    client.simple_query(&format!("MURMUR UNLOCK KEY '{KEY}'")).await.unwrap();
    client.simple_query("CREATE TABLE mtls(id INTEGER PRIMARY KEY, v TEXT NOT NULL)").await.unwrap();
    client.simple_query("INSERT INTO mtls VALUES (1, 'secure')").await.unwrap();
    let sets = client.simple_query("SELECT v FROM mtls WHERE id = 1").await.unwrap();
    assert_eq!(sets, vec![vec![vec![Some("secure".to_owned())]]]);
}

#[test]
fn missing_credentials_fail_fast() {
    let dir = tempfile::tempdir().unwrap();
    let pki = make_pki();
    let port = free_port();
    let config = write_config(&dir, port, &pki, false, false);
    // Point at a missing certificate after writing a valid config.
    let text = std::fs::read_to_string(&config).unwrap();
    let broken = text.replace("server.crt", "absent.crt");
    std::fs::write(&config, broken).unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_murmur-server"))
        .arg("--config")
        .arg(&config)
        .arg("--validate-config")
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));

    // Startup itself refuses to listen with bad credentials.
    let mut child = Command::new(env!("CARGO_BIN_EXE_murmur-server"))
        .arg("--config")
        .arg(&config)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        assert!(Instant::now() < deadline, "server should exit, not listen");
        std::thread::sleep(Duration::from_millis(50));
    };
    assert_eq!(status.code(), Some(1));
}
