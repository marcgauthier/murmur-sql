//! Live SQL test: Hostile schema manifests quarantined.
//! A malicious authenticated peer (CA-issued QUIC cert, untrusted origin key)
//! serves hostile signed schema manifests at an honest 3-node SQL cluster:
//! a revision with a corrupted signature, a self-consistent revision from an untrusted signer,
//! an oversized revision, and a zero-sequence revision.
//! Every forgery is refused and quarantined (verified via stream death, the error latch,
//! and unchanged schema bytes/rows), and honest writes continue converging throughout.

use murmur_live_harness::{assert_rows, Cluster, ClusterOptions};
use murmur_replication::{
    decode, encode,
    protocol::{self, Message, SignedSchema},
    Dissemination, NodeId,
};
use rustls::client::danger::{
    HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier,
};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use std::collections::BTreeMap;
use std::fs;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::time::{Duration, Instant};

const ATTACKER_ID: [u8; 16] = [0xA0; 16];
const ATTACKER_SEED: [u8; 32] = [0xA1; 32];
const DIVERGENT_SCHEMA: &[u8] = b"attacker-divergent-schema-v9999";
const EVIL_SCHEMA: &[u8] = b"attacker-evil-schema-v9999";

const DIAL_TIMEOUT: Duration = Duration::from_secs(10);
const WELCOME_TIMEOUT: Duration = Duration::from_secs(5);
const MANIFEST_TIMEOUT: Duration = Duration::from_secs(15);
const PONG_TIMEOUT: Duration = Duration::from_secs(10);
const SLICE: Duration = Duration::from_millis(500);
const DEATH_WINDOW: Duration = Duration::from_secs(3);

#[derive(Debug)]
struct AttackerCertVerifier {
    roots: rustls::RootCertStore,
    provider: Arc<rustls::crypto::CryptoProvider>,
}

impl ServerCertVerifier for AttackerCertVerifier {
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

fn parse_id(hex: &str) -> [u8; 16] {
    let mut out = [0u8; 16];
    let digits = hex.replace('-', "");
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&digits[2 * i..2 * i + 2], 16).unwrap();
    }
    out
}

fn msg_name(m: &Message) -> &'static str {
    match m {
        Message::Hello { .. } => "Hello",
        Message::Welcome { .. } => "Welcome",
        Message::Batches(_) => "Batches",
        Message::Ack { .. } => "Ack",
        Message::Need { .. } => "Need",
        Message::SnapshotRequest => "SnapshotRequest",
        Message::SnapshotManifest(_) => "SnapshotManifest",
        Message::SnapshotChunk { .. } => "SnapshotChunk",
        Message::SnapshotDone { .. } => "SnapshotDone",
        Message::SnapshotResume { .. } => "SnapshotResume",
        Message::SnapshotNeed { .. } => "SnapshotNeed",
        Message::SnapshotApplied { .. } => "SnapshotApplied",
        Message::SnapshotUnavailable { .. } => "SnapshotUnavailable",
        Message::SnapshotCancel { .. } => "SnapshotCancel",
        Message::Ping(_) => "Ping",
        Message::Pong(_) => "Pong",
        Message::Error { .. } => "Error",
        Message::SchemaRequest => "SchemaRequest",
        Message::SchemaManifest(_) => "SchemaManifest",
        Message::SchemaAck { .. } => "SchemaAck",
        Message::PlumtreeData(_) => "PlumtreeData",
        Message::IHave { .. } => "IHave",
        Message::Prune => "Prune",
        Message::Graft { .. } => "Graft",
        Message::ProgressRequest { .. } => "ProgressRequest",
        Message::ProgressPage { .. } => "ProgressPage",
        Message::TransactionChunk(_) => "TransactionChunk",
        Message::MembersRequest { .. } => "MembersRequest",
        Message::MembersPage { .. } => "MembersPage",
        Message::ChunkNeed { .. } => "ChunkNeed",
        Message::ChunkAvailabilityRequest { .. } => "ChunkAvailabilityRequest",
        Message::ChunkAvailabilityPage { .. } => "ChunkAvailabilityPage",
    }
}

fn frame_summary(frames: &[Message]) -> String {
    if frames.is_empty() {
        return "<none>".to_string();
    }
    frames
        .iter()
        .map(msg_name)
        .collect::<Vec<_>>()
        .join(",")
}

fn is_pong(nonce: u64) -> impl Fn(&Message) -> bool {
    move |m| matches!(m, Message::Pong(n) if *n == nonce)
}

struct Attacker {
    endpoint: quinn::Endpoint,
    db: [u8; 16],
    node_id: [u8; 16],
}

struct EvilSession {
    connection: quinn::Connection,
    send: quinn::SendStream,
    recv: quinn::RecvStream,
    welcome_schema: Vec<u8>,
}

impl Attacker {
    fn new(
        ca_pem_path: &Path,
        cert_pem_path: &Path,
        key_pem_path: &Path,
        db: [u8; 16],
        node_id: [u8; 16],
    ) -> Self {
        let provider = Arc::new(rustls::crypto::ring::default_provider());

        let ca_file = fs::File::open(ca_pem_path).expect("ca pem");
        let mut ca_reader = std::io::BufReader::new(ca_file);
        let ca_certs: Vec<CertificateDer<'static>> =
            rustls_pemfile::certs(&mut ca_reader).map(Result::unwrap).collect();

        let mut roots = rustls::RootCertStore::empty();
        for cert in &ca_certs {
            roots.add(cert.clone()).unwrap();
        }

        let cert_file = fs::File::open(cert_pem_path).expect("cert pem");
        let mut cert_reader = std::io::BufReader::new(cert_file);
        let node_certs: Vec<CertificateDer<'static>> =
            rustls_pemfile::certs(&mut cert_reader).map(Result::unwrap).collect();

        let key_file = fs::File::open(key_pem_path).expect("key pem");
        let mut key_reader = std::io::BufReader::new(key_file);
        let key = rustls_pemfile::private_key(&mut key_reader)
            .unwrap()
            .expect("attacker private key");

        let client_verifier = Arc::new(AttackerCertVerifier {
            roots: roots.clone(),
            provider: provider.clone(),
        });
        let mut client = rustls::ClientConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .unwrap()
            .dangerous()
            .with_custom_certificate_verifier(client_verifier.clone())
            .with_client_auth_cert(node_certs.clone(), key.clone_key())
            .unwrap();
        client.alpn_protocols = vec![b"murmur-sqlite/1".to_vec()];

        let client_verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
            Arc::new(roots),
            provider.clone(),
        )
        .build()
        .unwrap();
        let mut server = rustls::ServerConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])
            .unwrap()
            .with_client_cert_verifier(client_verifier)
            .with_single_cert(node_certs, key)
            .unwrap();
        server.alpn_protocols = vec![b"murmur-sqlite/1".to_vec()];

        let mut transport = quinn::TransportConfig::default();
        transport.max_idle_timeout(Some(Duration::from_secs(30).try_into().unwrap()));
        let transport = Arc::new(transport);

        let mut quic_server = quinn::ServerConfig::with_crypto(Arc::new(
            quinn::crypto::rustls::QuicServerConfig::try_from(server).unwrap(),
        ));
        quic_server.transport_config(transport.clone());

        let mut quic_client = quinn::ClientConfig::new(Arc::new(
            quinn::crypto::rustls::QuicClientConfig::try_from(client).unwrap(),
        ));
        quic_client.transport_config(transport);

        let mut endpoint =
            quinn::Endpoint::server(quic_server, "127.0.0.1:0".parse().unwrap()).unwrap();
        endpoint.set_default_client_config(quic_client);

        Self {
            endpoint,
            db,
            node_id,
        }
    }

    fn hello(&self, schema: Vec<u8>) -> Message {
        Message::Hello {
            capabilities: protocol::CAPABILITIES,
            required: protocol::REQUIRED,
            version: protocol::VERSION,
            database: self.db,
            node: self.node_id,
            fetch: None,
            schema,
            mode: Dissemination::Gossip,
            max_transaction: 64 << 20,
        }
    }

    async fn dial(&self, addr: SocketAddr, schema: Vec<u8>, what: &str) -> EvilSession {
        let connection = tokio::time::timeout(
            DIAL_TIMEOUT,
            self.endpoint
                .connect(addr, "localhost")
                .expect("attacker connect setup"),
        )
        .await
        .unwrap_or_else(|_| panic!("{what}: attacker dial timeout"))
        .unwrap_or_else(|e| panic!("{what}: attacker connect: {e}"));

        let (mut send, mut recv) = tokio::time::timeout(DIAL_TIMEOUT, connection.open_bi())
            .await
            .unwrap_or_else(|_| panic!("{what}: attacker open stream timeout"))
            .unwrap_or_else(|e| panic!("{what}: attacker open stream: {e}"));

        protocol::write(&mut send, &self.hello(schema))
            .await
            .unwrap_or_else(|e| panic!("{what}: attacker write hello: {e}"));

        let welcome_schema =
            match tokio::time::timeout(WELCOME_TIMEOUT, protocol::read(&mut recv)).await {
                Ok(Ok(Message::Welcome { schema, .. })) => schema,
                other => panic!("{what}: attacker expected Welcome, got {other:?}"),
            };

        EvilSession {
            connection,
            send,
            recv,
            welcome_schema,
        }
    }
}

impl EvilSession {
    async fn send(&mut self, msg: &Message, what: &str) {
        protocol::write(&mut self.send, msg)
            .await
            .unwrap_or_else(|e| panic!("{what}: attacker send {}: {e}", msg_name(msg)));
    }

    async fn collect(&mut self, total: Duration) -> Vec<Message> {
        self.collect_until(total, |_| false).await
    }

    async fn collect_until(
        &mut self,
        total: Duration,
        mut want: impl FnMut(&Message) -> bool,
    ) -> Vec<Message> {
        let mut out = Vec::new();
        let deadline = Instant::now() + total;
        while Instant::now() < deadline {
            match tokio::time::timeout(SLICE, protocol::read(&mut self.recv)).await {
                Ok(Ok(m)) => {
                    let hit = want(&m);
                    out.push(m);
                    if hit {
                        return out;
                    }
                }
                Ok(Err(_)) => return out,
                Err(_) => {}
            }
        }
        out
    }

    async fn fetch_manifest(&mut self, what: &str) -> SignedSchema {
        self.send(&Message::SchemaRequest, what).await;
        let frames = self
            .collect_until(MANIFEST_TIMEOUT, |m| {
                matches!(m, Message::SchemaManifest(_))
            })
            .await;
        for f in frames {
            if let Message::SchemaManifest(s) = f {
                return s;
            }
        }
        panic!("{what}: node served no schema manifest");
    }

    fn close(self, what: &str) {
        self.connection
            .close(0u32.into(), format!("attacker-close-{what}").as_bytes());
    }
}

struct Recorder {
    log: Arc<Mutex<Vec<(Instant, String)>>>,
    stop: Arc<AtomicBool>,
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl Recorder {
    fn start(cfg: murmur_client::Config) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let log = Arc::new(Mutex::new(Vec::new()));
        let log_clone = log.clone();
        let stop_clone = stop.clone();

        let handle = tokio::spawn(async move {
            let mut client = murmur_client::Client::connect(&cfg).await.ok();
            let mut last = String::new();
            while !stop_clone.load(Ordering::Relaxed) {
                if client.is_none() {
                    client = murmur_client::Client::connect(&cfg).await.ok();
                }
                if let Some(ref c) = client {
                    match c.simple("MURMUR SHOW REPLICATION").await {
                        Ok(rows) => {
                            if !rows.is_empty() && rows[0].len() >= 7 {
                                let cur = rows[0][6].clone();
                                if !cur.is_empty() && cur != last {
                                    last = cur.clone();
                                    log_clone.lock().unwrap().push((Instant::now(), cur));
                                }
                            }
                        }
                        Err(_) => {
                            client = None;
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });

        Self {
            log,
            stop,
            handle: Some(handle),
        }
    }

    fn saw_since(&self, since: Instant, needle: &str) -> bool {
        self.log
            .lock()
            .unwrap()
            .iter()
            .any(|(t, s)| *t >= since && s.contains(needle))
    }

    async fn check_or_query(
        &self,
        client: &murmur_client::Client,
        since: Instant,
        needle: &str,
    ) -> bool {
        if self.saw_since(since, needle) {
            return true;
        }
        if let Ok(rows) = client.simple("MURMUR SHOW REPLICATION").await {
            if !rows.is_empty() && rows[0].len() >= 7 && rows[0][6].contains(needle) {
                return true;
            }
        }
        false
    }

    async fn stop(mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.await;
        }
    }
}

async fn assert_stream_dead(sess: &mut EvilSession, nonce: u64, what: &str) {
    let _ = protocol::write(&mut sess.send, &Message::Ping(nonce)).await;
    let frames = sess.collect(DEATH_WINDOW).await;
    for f in &frames {
        if matches!(f, Message::Pong(n) if *n == nonce) {
            panic!("node answered ping after {what} (stream should be dead)");
        }
        if matches!(f, Message::SchemaAck { .. }) {
            panic!(
                "node acked the forged manifest after {what} (saw {})",
                frame_summary(&frames)
            );
        }
    }
    eprintln!("after {what} the node sent only: {}", frame_summary(&frames));
}

async fn expect_pong(sess: &mut EvilSession, nonce: u64, what: &str) {
    sess.send(&Message::Ping(nonce), what).await;
    let frames = sess.collect_until(PONG_TIMEOUT, is_pong(nonce)).await;
    assert!(
        frames.iter().any(is_pong(nonce)),
        "missing pong to {what}; saw {}",
        frame_summary(&frames)
    );
}

fn attacker_manifest(db: [u8; 16], sequence: u64, schema: Vec<u8>) -> SignedSchema {
    let mut signed = SignedSchema {
        database: db,
        signer: ATTACKER_ID,
        sequence,
        schema,
        signature: Vec::new(),
    };
    signed.sign(&ATTACKER_SEED).expect("attacker sign");
    signed
}

fn assert_authorized_shape(signed: &SignedSchema, db: [u8; 16], max: usize, what: &str) {
    let mut trust: BTreeMap<NodeId, [u8; 32]> = BTreeMap::new();
    trust.insert(ATTACKER_ID, protocol::public_key(&ATTACKER_SEED));
    signed
        .verify(db, ATTACKER_ID, &trust, max)
        .unwrap_or_else(|e| panic!("{what} is not even self-consistent: {e}"));
}

#[tokio::test(flavor = "multi_thread")]
async fn hostile_schema_manifests_quarantined() {
    const MAX_TX: usize = 1 << 20; // 1 MiB transaction cap
    let mut cluster = Cluster::new(ClusterOptions {
        name: "hostile-schema".into(),
        num_nodes: 4,
        replication_toml: r#"
max_transaction_bytes = "1MiB"
"#
        .into(),
        ..Default::default()
    })
    .await
    .expect("cluster setup");

    // Node 0 is the target; Nodes 1 & 2 are honest witnesses.
    // Node 3 is stopped and donates CA-issued TLS credentials for the attacker.
    cluster.stop_node(3);

    let target_id = parse_id(&cluster.node_info(0).node_id);
    let target_addr = cluster.node_info(0).repl_addr;
    let honest_indices = [0, 1, 2];

    // Read database_id from Node 0 config
    let config_content = fs::read_to_string(&cluster.node_info(0).config_path).unwrap();
    let doc = config_content.parse::<toml_edit::DocumentMut>().unwrap();
    let dbid_hex = doc["storage"]["database_id"].as_str().unwrap();
    let dbid = parse_id(dbid_hex);

    // Initialize replicated table on honest nodes (Nodes 0, 1, 2)
    for &i in &honest_indices {
        let client = cluster.node(i);
        client
            .simple("CREATE TABLE IF NOT EXISTS records(id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
            .await
            .unwrap();
        client.set_table_replicated("records", true).await.unwrap();
        client.flush().await.unwrap();
    }

    // Positive control (pre-attack): honest writes replicate everywhere
    cluster
        .node(0)
        .execute(
            "INSERT INTO records(id, name) VALUES($1, $2)",
            &[&1i64, &"baseline"],
        )
        .await
        .unwrap();
    cluster.node(0).flush().await.unwrap();

    let pre_attack = vec![vec!["baseline".to_string()]];
    assert_rows(
        &cluster,
        &honest_indices,
        "SELECT name FROM records ORDER BY name",
        &pre_attack,
    )
    .await;

    // Attacker fixture using Node 3's CA-issued cert
    let ca_pem = cluster.node_info(3).dir.join("ca.pem");
    let cert_pem = cluster.node_info(3).dir.join("cert.pem");
    let key_pem = cluster.node_info(3).dir.join("key.pem");
    let n3_id = parse_id(&cluster.node_info(3).node_id);
    let attacker = Attacker::new(&ca_pem, &cert_pem, &key_pem, dbid, n3_id);

    let divergent = DIVERGENT_SCHEMA.to_vec();
    let mut handshakes = 0u32;

    // Start background error latch recorder on Node 0
    let recorder = Recorder::start(cluster.config(0));

    // --- S1: forged revision with a corrupted signature
    let mut bad_digest = attacker.dial(target_addr, divergent.clone(), "digest-forge").await;
    handshakes += 1;
    let served_schema = bad_digest.welcome_schema.clone();
    let served = bad_digest.fetch_manifest("digest-forge").await;
    assert_eq!(
        served.signer, target_id,
        "served manifest is not signed by the target node"
    );
    assert_eq!(
        served.schema, served_schema,
        "served manifest bytes differ from Welcome bytes"
    );
    assert!(served.sequence > 0, "served manifest has zero sequence");

    let mut forged = served.clone();
    forged.signature[0] ^= 0xFF;
    forged.signature[17] ^= 0x01;

    let wire = encode(&Message::SchemaManifest(forged.clone())).expect("forged manifest encodes");
    let back: Message = decode(&wire).expect("forged manifest still decodes");
    assert!(
        matches!(back, Message::SchemaManifest(_)),
        "forged frame did not survive local round-trip"
    );

    let since_s1 = Instant::now();
    bad_digest.send(&Message::SchemaManifest(forged), "digest-forge").await;
    assert_stream_dead(&mut bad_digest, 71, "digest-forged manifest").await;
    assert!(
        recorder
            .check_or_query(cluster.node(0), since_s1, "signature error")
            .await,
        "node never latched the signature refusal for the digest forgery"
    );
    bad_digest.close("digest-forge");

    // The node survives the forgery
    let mut probe = attacker.dial(target_addr, divergent.clone(), "probe-1").await;
    handshakes += 1;
    expect_pong(&mut probe, 72, "probe after digest-forged manifest").await;
    probe.close("probe-1");

    // --- S2: self-consistent revision from untrusted signer
    let evil = attacker_manifest(dbid, 1, EVIL_SCHEMA.to_vec());
    assert_authorized_shape(&evil, dbid, MAX_TX, "evil revision");
    let since_s2 = Instant::now();
    let mut untrusted = attacker.dial(target_addr, divergent.clone(), "untrusted-signer").await;
    handshakes += 1;
    untrusted.send(&Message::SchemaManifest(evil), "untrusted-signer").await;
    assert_stream_dead(&mut untrusted, 73, "untrusted-signer manifest").await;
    assert!(
        recorder
            .check_or_query(cluster.node(0), since_s2, "untrusted schema signer")
            .await,
        "node never latched the untrusted-signer refusal"
    );
    untrusted.close("untrusted-signer");

    // --- S3: oversized revision (2 MiB against 1 MiB cap)
    let big = attacker_manifest(dbid, 1, vec![0x55; 2 << 20]);
    assert_authorized_shape(&big, dbid, 8 << 20, "oversized revision");
    let since_s3 = Instant::now();
    let mut oversized = attacker.dial(target_addr, divergent.clone(), "oversized").await;
    handshakes += 1;
    oversized.send(&Message::SchemaManifest(big), "oversized").await;
    assert_stream_dead(&mut oversized, 74, "oversized manifest").await;
    assert!(
        recorder
            .check_or_query(cluster.node(0), since_s3, "schema envelope identity or size")
            .await,
        "node never latched the envelope refusal for the oversized manifest"
    );
    oversized.close("oversized");

    // --- S4: bad-epoch revision (zero sequence)
    let mut zero_seq = attacker_manifest(dbid, 1, EVIL_SCHEMA.to_vec());
    assert_authorized_shape(&zero_seq, dbid, MAX_TX, "zero-sequence revision");
    zero_seq.sequence = 0;
    let since_s4 = Instant::now();
    let mut bad_epoch = attacker.dial(target_addr, divergent.clone(), "bad-epoch").await;
    handshakes += 1;
    bad_epoch.send(&Message::SchemaManifest(zero_seq), "bad-epoch").await;
    assert_stream_dead(&mut bad_epoch, 75, "zero-sequence manifest").await;
    assert!(
        recorder
            .check_or_query(cluster.node(0), since_s4, "schema envelope identity or size")
            .await,
        "node never latched the envelope refusal for the zero-sequence manifest"
    );
    bad_epoch.close("bad-epoch");

    // Quarantine: no adoption, served schema bytes unchanged, rows unchanged
    assert_rows(
        &cluster,
        &honest_indices,
        "SELECT name FROM records ORDER BY name",
        &pre_attack,
    )
    .await;

    let final_probe = attacker.dial(target_addr, divergent.clone(), "final-schema").await;
    handshakes += 1;
    assert_eq!(
        final_probe.welcome_schema, served_schema,
        "schema bytes moved during the schema attacks"
    );
    final_probe.close("final-schema");

    assert_eq!(handshakes, 6, "handshake count drifted (attach proof)");

    // --- Positive control (post-attack): honest nodes continue replicating
    cluster
        .node(1)
        .execute(
            "INSERT INTO records(id, name) VALUES($1, $2)",
            &[&2i64, &"post-attack-1"],
        )
        .await
        .unwrap();
    cluster.node(1).flush().await.unwrap();

    cluster
        .node(2)
        .execute(
            "INSERT INTO records(id, name) VALUES($1, $2)",
            &[&3i64, &"post-attack-2"],
        )
        .await
        .unwrap();
    cluster.node(2).flush().await.unwrap();

    cluster
        .node(0)
        .execute(
            "INSERT INTO records(id, name) VALUES($1, $2)",
            &[&4i64, &"attacked-node-write"],
        )
        .await
        .unwrap();
    cluster.node(0).flush().await.unwrap();

    let post_attack = vec![
        vec!["attacked-node-write".to_string()],
        vec!["baseline".to_string()],
        vec!["post-attack-1".to_string()],
        vec!["post-attack-2".to_string()],
    ];

    assert_rows(
        &cluster,
        &honest_indices,
        "SELECT name FROM records ORDER BY name",
        &post_attack,
    )
    .await;

    recorder.stop().await;
}
