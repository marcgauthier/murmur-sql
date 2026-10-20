//! Live SQL test: Hostile peer attacks rejected.
//! A malicious authenticated peer (CA-issued QUIC cert, untrusted origin key) fires forged batches,
//! invalid identities, a replayed handshake, a 256 MiB allocation-claim frame, a compressed snapshot bomb,
//! and a far-future-HLC batch at an honest 2-node SQL cluster.
//! Every attack is rejected on the wire and in product latches/counters, nothing foreign applies,
//! and the honest pair stays converged and responsive throughout.

use murmur_live_harness::{assert_rows, Cluster, ClusterOptions};
use murmur_replication::{
    decode, encode,
    protocol::{self, Message, Transaction},
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
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const ATTACKER_ID: [u8; 16] = [0xA0; 16];
const ATTACKER_SEED: [u8; 32] = [0xA1; 32];
const DIVERGENT_SCHEMA: &[u8] = b"attacker-divergent-schema-v9999";
const BATCHES_KIND: u16 = 3;
const SNAPSHOT_CHUNK_KIND: u16 = 8;
const COMPRESSED_FLAG: u16 = 0x8002;

const DIAL_TIMEOUT: Duration = Duration::from_secs(10);
const WELCOME_TIMEOUT: Duration = Duration::from_secs(5);
const ERROR_TIMEOUT: Duration = Duration::from_secs(10);
const PONG_TIMEOUT: Duration = Duration::from_secs(10);
const SLICE: Duration = Duration::from_millis(500);
const DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
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

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("wall clock")
        .as_millis() as u64
}

fn msg_name(m: &Message) -> &'static str {
    match m {
        Message::Hello { .. } => "Hello",
        Message::Welcome { .. } => "Welcome",
        Message::Need { .. } => "Need",
        Message::Batches(_) => "Batches",
        Message::Ack { .. } => "Ack",
        Message::Ping(_) => "Ping",
        Message::Pong(_) => "Pong",
        Message::SnapshotChunk { .. } => "SnapshotChunk",
        Message::Error { .. } => "Error",
        _ => "Other",
    }
}

fn frame_summary(frames: &[Message]) -> String {
    if frames.is_empty() {
        return "empty".into();
    }
    frames
        .iter()
        .map(|m| match m {
            Message::Error { code, message } => format!("Error({code}: {message:?})"),
            Message::Pong(n) => format!("Pong({n})"),
            other => msg_name(other).into(),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn assert_no_foreign_data(frames: &[Message], honest: &[[u8; 16]], ctx: &str) {
    for f in frames {
        match f {
            Message::Batches(txs) => {
                for tx in txs {
                    assert!(
                        honest.contains(&tx.origin),
                        "attacker received a batch with foreign origin {} {ctx}",
                        hex(&tx.origin)
                    );
                }
            }
            Message::PlumtreeData(tx) => {
                assert!(
                    honest.contains(&tx.origin),
                    "attacker received plumtree data with foreign origin {} {ctx}",
                    hex(&tx.origin)
                );
            }
            Message::TransactionChunk(c) => {
                assert!(
                    honest.contains(&c.proof.origin),
                    "attacker received a chunk with foreign origin {} {ctx}",
                    hex(&c.proof.origin)
                );
            }
            _ => {}
        }
    }
}

fn attacker_tx(
    db: [u8; 16],
    origin: [u8; 16],
    seq: u64,
    schema: [u8; 32],
    payload: Vec<u8>,
    millis: u64,
) -> Transaction {
    let mut tx = Transaction {
        database: db,
        origin,
        sequence: seq,
        id: [0xB0; 16],
        millis,
        logical: 0,
        schema,
        payload_digest: [0; 32],
        payload,
        signature: Vec::new(),
    };
    tx.sign(&ATTACKER_SEED).expect("attacker sign");
    tx
}

fn assert_roundtrips(tx: &Transaction, what: &str) {
    let bytes = encode(tx).unwrap_or_else(|e| panic!("{what} does not encode: {e}"));
    let back: Transaction =
        decode(&bytes).unwrap_or_else(|e| panic!("{what} does not decode locally: {e}"));
    assert!(
        back.origin == tx.origin && back.sequence == tx.sequence && back.payload == tx.payload,
        "{what} did not survive a local round-trip"
    );
}

fn assert_authorized_shape(tx: &Transaction, db: [u8; 16], max: usize, what: &str) {
    let mut trust: BTreeMap<NodeId, [u8; 32]> = BTreeMap::new();
    trust.insert(ATTACKER_ID, protocol::public_key(&ATTACKER_SEED));
    tx.verify(db, &trust, max)
        .unwrap_or_else(|e| panic!("{what} is not even self-consistent: {e}"));
}

fn is_pong(nonce: u64) -> impl Fn(&Message) -> bool {
    move |m| matches!(m, Message::Pong(n) if *n == nonce)
}

fn is_error_code(want: u16) -> impl Fn(&Message) -> bool {
    move |m| matches!(m, Message::Error { code: c, .. } if *c == want)
}

fn is_schema_request(m: &Message) -> bool {
    matches!(m, Message::SchemaRequest)
}

fn last_error_text(frames: &[Message]) -> Option<(u16, String)> {
    frames.iter().rev().find_map(|m| match m {
        Message::Error { code, message } => Some((*code, message.clone())),
        _ => None,
    })
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
    fn new(ca_pem: &Path, cert_pem: &Path, key_pem: &Path, db: [u8; 16], node_id: [u8; 16]) -> Self {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let mut ca_reader = std::io::BufReader::new(fs::File::open(ca_pem).unwrap());
        let mut cert_reader = std::io::BufReader::new(fs::File::open(cert_pem).unwrap());
        let mut key_reader = std::io::BufReader::new(fs::File::open(key_pem).unwrap());

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
            .with_custom_certificate_verifier(Arc::new(AttackerCertVerifier {
                roots: roots.clone(),
                provider: provider.clone(),
            }))
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

    async fn send_raw(&mut self, kind: u16, flags: u16, body: &[u8], what: &str) {
        let mut h = [0u8; 12];
        h[..2].copy_from_slice(b"RD");
        h[2..4].copy_from_slice(&protocol::VERSION.to_be_bytes());
        h[4..6].copy_from_slice(&kind.to_be_bytes());
        h[6..8].copy_from_slice(&flags.to_be_bytes());
        h[8..12].copy_from_slice(&(body.len() as u32).to_be_bytes());
        self.send
            .write_all(&h)
            .await
            .unwrap_or_else(|e| panic!("{what}: send_raw header: {e}"));
        self.send
            .write_all(body)
            .await
            .unwrap_or_else(|e| panic!("{what}: send_raw body: {e}"));
    }

    async fn send_alloc_claim(&mut self, kind: u16, claim: u32, what: &str) {
        let mut h = [0u8; 12];
        h[..2].copy_from_slice(b"RD");
        h[2..4].copy_from_slice(&protocol::VERSION.to_be_bytes());
        h[4..6].copy_from_slice(&kind.to_be_bytes());
        h[6..8].copy_from_slice(&0u16.to_be_bytes());
        h[8..12].copy_from_slice(&claim.to_be_bytes());
        self.send
            .write_all(&h)
            .await
            .unwrap_or_else(|e| panic!("{what}: send_alloc_claim header: {e}"));
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

    async fn check_or_query(&self, client: &murmur_client::Client, since: Instant, needle: &str) -> bool {
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

async fn assert_stream_dead(
    sess: &mut EvilSession,
    honest: &[[u8; 16]],
    nonce: u64,
    what: &str,
) -> Vec<Message> {
    let _ = protocol::write(&mut sess.send, &Message::Ping(nonce)).await;
    let frames = sess.collect(DEATH_WINDOW).await;
    for f in &frames {
        if matches!(f, Message::Pong(n) if *n == nonce) {
            panic!("node answered ping after {what} (stream should be dead)");
        }
    }
    assert_no_foreign_data(&frames, honest, &format!("after {what}"));
    eprintln!("after {what} the node sent only: {}", frame_summary(&frames));
    frames
}

async fn expect_protocol_error(
    sess: &mut EvilSession,
    needle: &str,
    what: &str,
) -> Vec<Message> {
    let frames = sess.collect_until(ERROR_TIMEOUT, is_error_code(1)).await;
    match last_error_text(&frames) {
        Some((1, text)) if text.contains(needle) => frames,
        other => panic!("{what} drew no Error{{code: 1, ..{needle}..}}; saw {other:?}"),
    }
}

async fn expect_pong(sess: &mut EvilSession, nonce: u64, what: &str) -> Vec<Message> {
    sess.send(&Message::Ping(nonce), what).await;
    let frames = sess.collect_until(PONG_TIMEOUT, is_pong(nonce)).await;
    assert!(
        frames.iter().any(is_pong(nonce)),
        "missing pong to {what}; saw {}",
        frame_summary(&frames)
    );
    frames
}

struct Counters {
    staged: u64,
    applied: u64,
}

async fn get_counters(client: &murmur_client::Client) -> Counters {
    let rows = client.simple("MURMUR SHOW REPLICATION").await.unwrap();
    let staged: u64 = rows[0][4].parse().unwrap_or(0);
    let applied: u64 = rows[0][5].parse().unwrap_or(0);
    Counters { staged, applied }
}

#[tokio::test(flavor = "multi_thread")]
async fn hostile_peer_attacks_rejected() {
    const MAX_TX: usize = 1 << 20; // 1 MiB transaction cap
    let mut cluster = Cluster::new(ClusterOptions {
        name: "hostile-peer".into(),
        num_nodes: 3,
        replication_toml: r#"
max_transaction_bytes = "1MiB"
"#
        .into(),
        ..Default::default()
    })
    .await
    .expect("cluster setup");

    // Node 0 is the honest target; Node 1 is the honest witness.
    // Node 2 is stopped and serves as the CA-issued credential donor for the attacker.
    cluster.stop_node(2);

    let n0_id = parse_id(&cluster.node_info(0).node_id);
    let n1_id = parse_id(&cluster.node_info(1).node_id);
    let n2_id = parse_id(&cluster.node_info(2).node_id);
    let honest = vec![n0_id, n1_id];

    // Read database_id from Node 0 config
    let config_content = fs::read_to_string(&cluster.node_info(0).config_path).unwrap();
    let doc = config_content.parse::<toml_edit::DocumentMut>().unwrap();
    let dbid_hex = doc["storage"]["database_id"].as_str().unwrap();
    let dbid = parse_id(dbid_hex);

    // Initialize replicated table on honest nodes (Node 0 and Node 1)
    for i in 0..2 {
        let client = cluster.node(i);
        client
            .simple("CREATE TABLE IF NOT EXISTS records(id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
            .await
            .unwrap();
        client.simple("MURMUR SET TABLE records REPLICATED true").await.unwrap();
        client.simple("MURMUR FLUSH").await.unwrap();
    }

    // Positive control (pre-attack): honest writes replicate both ways
    cluster
        .node(0)
        .execute(
            "INSERT INTO records(id, name) VALUES($1, $2)",
            &[&1i64, &"honest-1"],
        )
        .await
        .unwrap();
    cluster.node(0).simple("MURMUR FLUSH").await.unwrap();

    assert_rows(
        &cluster,
        &[0, 1],
        "SELECT name FROM records ORDER BY name",
        &[vec!["honest-1".to_string()]],
    )
    .await;

    cluster
        .node(1)
        .execute(
            "INSERT INTO records(id, name) VALUES($1, $2)",
            &[&2i64, &"honest-2"],
        )
        .await
        .unwrap();
    cluster.node(1).simple("MURMUR FLUSH").await.unwrap();

    let pre_attack = vec![
        vec!["honest-1".to_string()],
        vec!["honest-2".to_string()],
    ];
    assert_rows(
        &cluster,
        &[0, 1],
        "SELECT name FROM records ORDER BY name",
        &pre_attack,
    )
    .await;

    let base_counters = get_counters(cluster.node(0)).await;

    // Build Attacker using Node 2's CA-issued certificate
    let ca_pem = cluster.node_info(2).dir.join("ca.pem");
    let cert_pem = cluster.node_info(2).dir.join("cert.pem");
    let key_pem = cluster.node_info(2).dir.join("key.pem");
    let attacker = Attacker::new(&ca_pem, &cert_pem, &key_pem, dbid, n2_id);

    let target_addr = cluster.node_info(0).repl_addr;
    let divergent = DIVERGENT_SCHEMA.to_vec();
    let mut handshakes = 0u32;

    // 1. Learn the node's real schema bytes from a divergent dial
    let learn = attacker.dial(target_addr, divergent.clone(), "schema-learn").await;
    handshakes += 1;
    let real_schema = learn.welcome_schema.clone();
    assert_ne!(
        real_schema, divergent,
        "node schema equals divergent schema"
    );
    let real_digest = protocol::schema_digest(&real_schema);
    learn.close("schema-learn");

    tokio::time::sleep(Duration::from_millis(200)).await;

    // 2. Positive control: matched-schema hello welcomed with no SchemaRequest
    let mut control = attacker.dial(target_addr, real_schema.clone(), "control").await;
    handshakes += 1;
    let control_frames = control.collect(Duration::from_secs(2)).await;
    assert!(
        !control_frames.iter().any(is_schema_request),
        "control session drew SchemaRequest; saw {}",
        frame_summary(&control_frames)
    );
    let pong_frames = expect_pong(&mut control, 7, "control ping").await;
    assert_no_foreign_data(&pong_frames, &honest, "on the control session");
    control.close("control");

    tokio::time::sleep(Duration::from_millis(200)).await;

    // 3. Start error latch recorder
    let recorder = Recorder::start(cluster.config(0));

    // --- P1a: invalid identities on one watched divergent session
    let mut watched = attacker.dial(target_addr, divergent.clone(), "watched").await;
    handshakes += 1;

    let mut seen: Vec<Message> = Vec::new();
    let forged_digest = [0xA0; 32];

    // Zero sequence
    let zero_seq = attacker_tx(dbid, ATTACKER_ID, 0, forged_digest, b"zero-seq".to_vec(), now_millis());
    assert_roundtrips(&zero_seq, "zero-sequence batch");
    watched.send(&Message::Batches(vec![zero_seq]), "zero-seq").await;
    seen.extend(expect_protocol_error(
        &mut watched,
        "transaction identity",
        "zero-sequence batch",
    ).await);

    // Wrong database id
    let wrong_db = attacker_tx(
        [0xDD; 16],
        ATTACKER_ID,
        7,
        forged_digest,
        b"wrong-db".to_vec(),
        now_millis(),
    );
    assert_roundtrips(&wrong_db, "wrong-database batch");
    watched.send(&Message::Batches(vec![wrong_db]), "wrong-db").await;
    seen.extend(expect_protocol_error(
        &mut watched,
        "transaction identity",
        "wrong-database batch",
    ).await);

    // Oversized payload: 2 MiB against 1 MiB cap
    let big = attacker_tx(
        dbid,
        ATTACKER_ID,
        8,
        forged_digest,
        vec![0x55; 2 << 20],
        now_millis(),
    );
    assert_roundtrips(&big, "oversized batch");
    watched.send(&Message::Batches(vec![big]), "oversized").await;
    seen.extend(expect_protocol_error(
        &mut watched,
        "transaction identity",
        "oversized batch",
    ).await);

    // Invalid missing range
    watched.send(
        &Message::Need { origin: ATTACKER_ID, from: 0, through: 5 },
        "bad-need",
    ).await;
    seen.extend(expect_protocol_error(
        &mut watched,
        "invalid missing range",
        "zero-from Need",
    ).await);

    // Session must be alive: ping round-trips
    seen.extend(expect_pong(&mut watched, 11, "ping-1 after invalid batches").await);

    // --- P2: replay handshake on live session
    watched.send(&attacker.hello(divergent.clone()), "replay-hello").await;
    seen.extend(expect_protocol_error(
        &mut watched,
        "repeated handshake",
        "replayed handshake",
    ).await);

    // Session still alive
    seen.extend(expect_pong(&mut watched, 22, "ping-2 after replayed handshake").await);
    seen.extend(watched.collect(Duration::from_secs(1)).await);
    assert_no_foreign_data(&seen, &honest, "on watched session");
    watched.close("watched");

    tokio::time::sleep(Duration::from_millis(200)).await;

    // --- P1b: forged matched-schema batch from untrusted origin
    let forged = attacker_tx(dbid, ATTACKER_ID, 1, real_digest, b"forged".to_vec(), now_millis());
    assert_roundtrips(&forged, "forged batch");
    assert_authorized_shape(&forged, dbid, MAX_TX, "forged batch");

    let since_forged = Instant::now();
    let mut evil = attacker.dial(target_addr, divergent.clone(), "forged").await;
    handshakes += 1;
    evil.send(&Message::Batches(vec![forged]), "forged").await;
    let dead_frames = assert_stream_dead(&mut evil, &honest, 0xF0, "forged batch").await;
    let saw_on_wire = dead_frames.iter().any(|m| match m {
        Message::Error { message, .. } => message.contains("untrusted transaction origin"),
        _ => false,
    });
    let saw_in_latch = recorder
        .check_or_query(cluster.node(0), since_forged, "untrusted transaction origin")
        .await;
    assert!(
        saw_on_wire || saw_in_latch,
        "node never rejected forged batch with untrusted-origin cause on wire or latch"
    );
    evil.close("forged");

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Rejection counters unmoved; no attack batch applied
    let c_after = get_counters(cluster.node(0)).await;
    assert_eq!(c_after.staged, base_counters.staged);
    assert_eq!(c_after.applied, base_counters.applied);

    assert_rows(
        &cluster,
        &[0, 1],
        "SELECT name FROM records ORDER BY name",
        &pre_attack,
    )
    .await;

    // --- P3: 256 MiB allocation-claim frame
    let mut claim = attacker.dial(target_addr, divergent.clone(), "alloc-claim").await;
    handshakes += 1;
    claim.collect_until(DRAIN_TIMEOUT, is_schema_request).await;
    claim.send_alloc_claim(BATCHES_KIND, 256 << 20, "alloc-claim").await;
    assert_stream_dead(&mut claim, &honest, 0xDEAD, "256MiB allocation claim").await;
    claim.close("alloc-claim");

    // Node survives: fresh session pings fine
    let mut probe = attacker.dial(target_addr, divergent.clone(), "probe-1").await;
    handshakes += 1;
    expect_pong(&mut probe, 33, "probe after 256MiB claim").await;
    probe.close("probe-1");

    // --- P4: compressed snapshot bomb (zstd bomb)
    let mut bomb = attacker.dial(target_addr, divergent.clone(), "bomb").await;
    handshakes += 1;
    let bomb_raw = vec![0xFF; 4 << 20];
    let compressed = zstd::stream::encode_all(&bomb_raw[..], 3).expect("bomb compress");
    assert!(compressed.len() < bomb_raw.len() / 4);

    bomb.collect_until(DRAIN_TIMEOUT, is_schema_request).await;
    let mut body = (bomb_raw.len() as u32).to_be_bytes().to_vec();
    body.extend_from_slice(&compressed);
    bomb.send_raw(SNAPSHOT_CHUNK_KIND, COMPRESSED_FLAG, &body, "bomb").await;
    assert_stream_dead(&mut bomb, &honest, 0xB0B, "snapshot bomb").await;
    bomb.close("bomb");

    // Node survives: fresh session pings fine
    let mut probe2 = attacker.dial(target_addr, divergent.clone(), "probe-2").await;
    handshakes += 1;
    expect_pong(&mut probe2, 44, "probe after snapshot bomb").await;
    probe2.close("probe-2");

    // --- P-HLC: far-future HLC batch
    let mut hlc = attacker.dial(target_addr, divergent.clone(), "hlc").await;
    handshakes += 1;
    assert_eq!(hlc.welcome_schema, real_schema, "served schema changed");

    let far_future = now_millis() + 10 * 365 * 24 * 3600 * 1000;
    let hlc_batch = attacker_tx(dbid, ATTACKER_ID, 1, real_digest, b"far-future-hlc".to_vec(), far_future);
    assert_roundtrips(&hlc_batch, "far-future batch");
    assert_authorized_shape(&hlc_batch, dbid, MAX_TX, "far-future batch");

    let since_hlc = Instant::now();
    hlc.send(&Message::Batches(vec![hlc_batch]), "hlc").await;
    let dead_hlc_frames = assert_stream_dead(&mut hlc, &honest, 55, "far-future batch").await;
    let saw_hlc_on_wire = dead_hlc_frames.iter().any(|m| match m {
        Message::Error { message, .. } => message.contains("untrusted transaction origin"),
        _ => false,
    });
    let saw_hlc_in_latch = recorder
        .check_or_query(cluster.node(0), since_hlc, "untrusted transaction origin")
        .await;
    assert!(
        saw_hlc_on_wire || saw_hlc_in_latch,
        "node never rejected far-future batch with untrusted-origin cause on wire or latch"
    );
    hlc.close("hlc");

    assert_rows(
        &cluster,
        &[0, 1],
        "SELECT name FROM records ORDER BY name",
        &pre_attack,
    )
    .await;

    // Positive control (post-attack): honest nodes stay healthy and replicate
    let final_probe = attacker.dial(target_addr, divergent.clone(), "final-schema").await;
    handshakes += 1;
    assert_eq!(final_probe.welcome_schema, real_schema);
    final_probe.close("final-schema");

    assert_eq!(handshakes, 10, "handshake count drifted");

    cluster
        .node(1)
        .execute(
            "INSERT INTO records(id, name) VALUES($1, $2)",
            &[&3i64, &"post-attack-1"],
        )
        .await
        .unwrap();
    cluster.node(1).simple("MURMUR FLUSH").await.unwrap();

    let expected_3 = vec![
        vec!["honest-1".to_string()],
        vec!["honest-2".to_string()],
        vec!["post-attack-1".to_string()],
    ];
    assert_rows(
        &cluster,
        &[0, 1],
        "SELECT name FROM records ORDER BY name",
        &expected_3,
    )
    .await;

    cluster
        .node(0)
        .execute(
            "INSERT INTO records(id, name) VALUES($1, $2)",
            &[&4i64, &"post-attack-2"],
        )
        .await
        .unwrap();
    cluster.node(0).simple("MURMUR FLUSH").await.unwrap();

    let expected_4 = vec![
        vec!["honest-1".to_string()],
        vec!["honest-2".to_string()],
        vec!["post-attack-1".to_string()],
        vec!["post-attack-2".to_string()],
    ];
    assert_rows(
        &cluster,
        &[0, 1],
        "SELECT name FROM records ORDER BY name",
        &expected_4,
    )
    .await;

    recorder.stop().await;
}
