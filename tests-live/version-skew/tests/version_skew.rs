//! Live SQL test: A peer advertising an unknown protocol version (version 99)
//! must be refused fail-closed by live SQL nodes; no sessions or data can form,
//! while honest nodes continue replicating and isolated nodes remain writable.

use murmur_live_harness::{assert_rows, Cluster, ClusterOptions};
use murmur_replication::{
    encode,
    protocol::{self, Message},
    Dissemination,
};
use rustls::client::danger::{
    HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier,
};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

const SKEWED_VERSION: u16 = 99;
const HELLO_KIND: u16 = 1;
const DIAL_TIMEOUT: Duration = Duration::from_secs(10);
const WELCOME_TIMEOUT: Duration = Duration::from_secs(5);
const WINDOW: Duration = Duration::from_secs(8);
const POLL: Duration = Duration::from_millis(400);

#[derive(Debug)]
struct TestServerVerifier {
    roots: rustls::RootCertStore,
    provider: Arc<rustls::crypto::CryptoProvider>,
}

impl ServerCertVerifier for TestServerVerifier {
    fn verify_server_cert(
        &self,
        cert: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _name: &ServerName<'_>,
        _ocsp: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let parsed = rustls::server::ParsedCertificate::try_from(cert)?;
        rustls::client::verify_server_cert_signed_by_trust_anchor(
            &parsed,
            &self.roots,
            intermediates,
            now,
            self.provider.signature_verification_algorithms.all,
        )?;
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

async fn write_versioned(
    send: &mut quinn::SendStream,
    message: &Message,
    frame_version: u16,
) -> Result<(), String> {
    let data = encode(message).map_err(|e| e.to_string())?;
    let mut h = [0u8; 12];
    h[..2].copy_from_slice(b"RD");
    h[2..4].copy_from_slice(&frame_version.to_be_bytes());
    h[4..6].copy_from_slice(&HELLO_KIND.to_be_bytes());
    h[6..8].copy_from_slice(&0u16.to_be_bytes());
    h[8..12].copy_from_slice(&(data.len() as u32).to_be_bytes());
    send.write_all(&h).await.map_err(|e| e.to_string())?;
    send.write_all(&data).await.map_err(|e| e.to_string())?;
    Ok(())
}

struct SkewedPeer {
    endpoint: quinn::Endpoint,
    db: [u8; 16],
    node_id: [u8; 16],
}

impl SkewedPeer {
    fn new(ca_pem: &Path, cert_pem: &Path, key_pem: &Path, db: [u8; 16], node_id: [u8; 16]) -> Self {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let mut ca_reader = std::io::BufReader::new(std::fs::File::open(ca_pem).unwrap());
        let mut cert_reader = std::io::BufReader::new(std::fs::File::open(cert_pem).unwrap());
        let mut key_reader = std::io::BufReader::new(std::fs::File::open(key_pem).unwrap());

        let ca_certs: Vec<_> = rustls_pemfile::certs(&mut ca_reader)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let node_certs: Vec<_> = rustls_pemfile::certs(&mut cert_reader)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let key = rustls_pemfile::private_key(&mut key_reader)
            .unwrap()
            .unwrap();

        let mut roots = rustls::RootCertStore::empty();
        for c in ca_certs {
            roots.add(c).unwrap();
        }

        let mut client = rustls::ClientConfig::builder_with_provider(provider.clone())
            .with_protocol_versions(&[&rustls::version::TLS13])
            .unwrap()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(TestServerVerifier { roots: roots.clone(), provider: provider.clone() }))
            .with_client_auth_cert(node_certs.clone(), key.clone_key())
            .unwrap();
        client.alpn_protocols = vec![b"murmur-sqlite/1".to_vec()];

        let client_verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider.clone())
            .build()
            .unwrap();
        let mut server = rustls::ServerConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])
            .unwrap()
            .with_client_cert_verifier(client_verifier)
            .with_single_cert(node_certs, key)
            .unwrap();
        server.alpn_protocols = vec![b"murmur-sqlite/1".to_vec()];

        let quic_client = quinn::crypto::rustls::QuicClientConfig::try_from(client).unwrap();

        let server_crypto = quinn::crypto::rustls::QuicServerConfig::try_from(server).unwrap();
        let server_cfg = quinn::ServerConfig::with_crypto(Arc::new(server_crypto));

        let mut endpoint = quinn::Endpoint::server(server_cfg, "127.0.0.1:0".parse().unwrap()).unwrap();
        endpoint.set_default_client_config(quinn::ClientConfig::new(Arc::new(quic_client)));

        Self { endpoint, db, node_id }
    }

    fn local_addr(&self) -> SocketAddr {
        self.endpoint.local_addr().unwrap()
    }

    fn hello(&self, version: u16) -> Message {
        Message::Hello {
            capabilities: protocol::CAPABILITIES,
            required: protocol::REQUIRED,
            version,
            database: self.db,
            node: self.node_id,
            fetch: None,
            schema: Vec::new(),
            mode: Dissemination::Gossip,
            max_transaction: 64 << 20,
        }
    }

    async fn assert_refused(&self, addr: SocketAddr, frame_version: u16, hello_version: u16) {
        let connecting = match self.endpoint.connect(addr, "localhost") {
            Ok(c) => c,
            Err(e) => panic!("connect build error to {addr}: {e}"),
        };
        let connection = match tokio::time::timeout(DIAL_TIMEOUT, connecting).await {
            Ok(Ok(c)) => c,
            Ok(Err(e)) => {
                eprintln!("dial to {addr} failed at TLS/connect: {e:?}");
                return;
            }
            Err(_) => panic!("dial timeout to {addr}"),
        };
        let (mut send, mut recv) = match tokio::time::timeout(DIAL_TIMEOUT, connection.open_bi()).await {
            Ok(Ok(s)) => s,
            Ok(Err(e)) => {
                eprintln!("open_bi error: {e:?}");
                return;
            }
            Err(_) => panic!("open_bi timeout"),
        };
        let _ = write_versioned(&mut send, &self.hello(hello_version), frame_version).await;
        match tokio::time::timeout(WELCOME_TIMEOUT, protocol::read(&mut recv)).await {
            Ok(Ok(Message::Welcome { .. })) => {
                panic!("server at {addr} sent Welcome to version {hello_version}!");
            }
            _ => {
                // Connection died, timed out or stream was reset: correct fail-closed refusal!
            }
        }
    }

    async fn accept_without_welcome(&self, slice: Duration) -> Option<Message> {
        let incoming = match tokio::time::timeout(slice, self.endpoint.accept()).await {
            Ok(Some(inc)) => inc,
            _ => return None,
        };
        let connection = incoming.await.ok()?;
        let (_send, mut recv) = tokio::time::timeout(slice, connection.accept_bi()).await.ok()?.ok()?;
        let msg = tokio::time::timeout(slice, protocol::read(&mut recv)).await.ok()?.ok()?;
        connection.close(0u32.into(), b"close without welcome");
        Some(msg)
    }
}

async fn replication_last_error(cluster: &Cluster, node: usize) -> Option<String> {
    let rows = cluster.node(node).simple("MURMUR SHOW REPLICATION").await.ok()?;
    if let Some(row) = rows.first() {
        if row.len() >= 7 && !row[6].is_empty() {
            return Some(row[6].clone());
        }
    }
    None
}

#[tokio::test(flavor = "multi_thread")]
async fn unknown_protocol_version_refused() {
    let cluster = Cluster::new(ClusterOptions {
        name: "version-skew".into(),
        num_nodes: 3,
        manual_peers: true,
        ..Default::default()
    })
    .await
    .expect("cluster setup");

    // Mesh only node 0 and node 1; node 2 remains isolated
    cluster
        .node(0)
        .simple(&format!("MURMUR PEER ADD '{}'", cluster.node_info(1).repl_addr))
        .await
        .unwrap();
    cluster
        .node(1)
        .simple(&format!("MURMUR PEER ADD '{}'", cluster.node_info(0).repl_addr))
        .await
        .unwrap();

    // Create table records and enable replication on all nodes
    for i in 0..cluster.len() {
        let client = cluster.node(i);
        client
            .simple("CREATE TABLE records(id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
            .await
            .unwrap();
        client.set_table_replicated("records", true).await.unwrap();
        client.flush().await.unwrap();
    }

    // Baseline: write 10 rows on node 0
    let mut expected_pair = Vec::new();
    for i in 0..10i64 {
        let name = format!("row-{i:02}");
        cluster
            .node(0)
            .execute("INSERT INTO records(id, name) VALUES($1, $2)", &[&i, &name])
            .await
            .unwrap();
        expected_pair.push(vec![name]);
    }
    cluster.node(0).flush().await.unwrap();

    // Nodes 0 and 1 converge on the 10 rows
    assert_rows(&cluster, &[0, 1], "SELECT name FROM records ORDER BY id", &expected_pair).await;

    // Node 2 must remain completely empty
    let n2_rows = cluster.node(2).simple("SELECT name FROM records").await.unwrap();
    assert!(n2_rows.is_empty(), "Isolated node 2 must have 0 rows");

    // Setup SkewedPeer with CA and cert from node 2
    let ca_pem = cluster.node_info(0).dir.join("ca.pem");
    let cert_pem = cluster.node_info(2).dir.join("cert.pem");
    let key_pem = cluster.node_info(2).dir.join("key.pem");

    // Parse database_id from config
    let cfg_str = std::fs::read_to_string(&cluster.node_info(0).config_path).unwrap();
    let doc = cfg_str.parse::<toml_edit::DocumentMut>().unwrap();
    let db_hex = doc["storage"]["database_id"].as_str().unwrap();
    let mut db_bytes = [0u8; 16];
    for (i, b) in db_bytes.iter_mut().enumerate() {
        *b = u8::from_str_radix(&db_hex[2 * i..2 * i + 2], 16).unwrap();
    }

    let node2_id_hex = &cluster.node_info(2).node_id;
    let mut node2_id = [0u8; 16];
    for (i, b) in node2_id.iter_mut().enumerate() {
        *b = u8::from_str_radix(&node2_id_hex[2 * i..2 * i + 2], 16).unwrap();
    }

    let skewed = SkewedPeer::new(&ca_pem, &cert_pem, &key_pem, db_bytes, node2_id);

    // Skew window: repeated version 99 dials to node 0 and node 1 must be refused
    let deadline = Instant::now() + WINDOW;
    let mut round = 0u32;
    let mut saw_frame_gate = [false, false];
    let mut saw_hello_gate = [false, false];

    while Instant::now() < deadline {
        let (frame_v, hello_v) = if round % 2 == 0 {
            (SKEWED_VERSION, SKEWED_VERSION)
        } else {
            (protocol::VERSION, SKEWED_VERSION)
        };

        for (k, idx) in [0, 1].into_iter().enumerate() {
            let addr = cluster.node_info(idx).repl_addr;
            skewed.assert_refused(addr, frame_v, hello_v).await;

            if let Some(e) = replication_last_error(&cluster, idx).await {
                if e.contains("invalid frame header") {
                    saw_frame_gate[k] = true;
                }
                if e.contains("incompatible hello") {
                    saw_hello_gate[k] = true;
                }
            }
        }
        round += 1;
        tokio::time::sleep(POLL).await;
    }

    // Outbound leg: node 0 dials the skewed address
    let skewed_addr = skewed.local_addr();
    let _ = cluster
        .node(0)
        .simple(&format!("MURMUR PEER ADD '{skewed_addr}'"))
        .await;

    // Observe outbound dial from node 0 to skewed peer
    let outbound_deadline = Instant::now() + Duration::from_secs(6);
    let mut observed_outbound = false;
    while Instant::now() < outbound_deadline {
        if let Some(msg) = skewed.accept_without_welcome(Duration::from_millis(400)).await {
            if let Message::Hello { version, .. } = msg {
                assert_eq!(version, protocol::VERSION, "Node 0 sent unexpected version");
                observed_outbound = true;
                break;
            }
        }
    }
    eprintln!("Observed outbound dial without Welcome: {observed_outbound}");

    // Local writes on isolated node 2
    for i in 0..3i64 {
        let name = format!("skewed-local-{i}");
        cluster
            .node(2)
            .execute("INSERT INTO records(id, name) VALUES($1, $2)", &[&(100 + i), &name])
            .await
            .unwrap();
    }
    cluster.node(2).flush().await.unwrap();

    // Verify node 0 and node 1 still hold exactly their 10 baseline rows
    assert_rows(&cluster, &[0, 1], "SELECT name FROM records ORDER BY id", &expected_pair).await;

    // Verify node 2 holds exactly its 3 local rows
    let expected_n2: Vec<Vec<String>> = vec![
        vec!["skewed-local-0".into()],
        vec!["skewed-local-1".into()],
        vec!["skewed-local-2".into()],
    ];
    let n2_got = cluster.node(2).simple("SELECT name FROM records ORDER BY id").await.unwrap();
    assert_eq!(n2_got, expected_n2, "Node 2 data leaked or was corrupted!");
}
