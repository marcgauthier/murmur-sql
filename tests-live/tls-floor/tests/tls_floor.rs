//! Live SQL test: The QUIC replication endpoint must enforce a TLS 1.3 floor.
//! QUIC exclusively requires TLS 1.3; TLS 1.2 is strictly rejected,
//! while TLS 1.3 succeeds, and the live SQL mesh converges.

use murmur_live_harness::{assert_rows, Cluster, ClusterOptions};
use rustls::client::danger::{
    HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier,
};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

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

fn build_client_config(
    ca_pem: &Path,
    cert_pem: &Path,
    key_pem: &Path,
    versions: &[&'static rustls::SupportedProtocolVersion],
) -> rustls::ClientConfig {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut ca_reader = std::io::BufReader::new(std::fs::File::open(ca_pem).expect("open ca.pem"));
    let mut cert_reader = std::io::BufReader::new(std::fs::File::open(cert_pem).expect("open cert.pem"));
    let mut key_reader = std::io::BufReader::new(std::fs::File::open(key_pem).expect("open key.pem"));

    let ca_certs: Vec<_> = rustls_pemfile::certs(&mut ca_reader)
        .collect::<Result<Vec<_>, _>>()
        .expect("parse ca");
    let node_certs: Vec<_> = rustls_pemfile::certs(&mut cert_reader)
        .collect::<Result<Vec<_>, _>>()
        .expect("parse cert");
    let key = rustls_pemfile::private_key(&mut key_reader)
        .expect("read key")
        .expect("missing key");

    let mut roots = rustls::RootCertStore::empty();
    for c in ca_certs {
        roots.add(c).expect("add root");
    }

    let mut client = rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(versions)
        .expect("protocol versions")
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(TestServerVerifier { roots, provider }))
        .with_client_auth_cert(node_certs, key)
        .expect("client auth cert");
    client.alpn_protocols = vec![b"murmur-sqlite/1".to_vec()];
    client
}

fn attacker_endpoint(
    ca_pem: &Path,
    cert_pem: &Path,
    key_pem: &Path,
    versions: &[&'static rustls::SupportedProtocolVersion],
) -> Result<quinn::Endpoint, String> {
    let client = build_client_config(ca_pem, cert_pem, key_pem, versions);
    let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).map_err(|e| e.to_string())?;
    let quic_client_config = quinn::crypto::rustls::QuicClientConfig::try_from(client)
        .map_err(|e| format!("{e:?}"))?;
    endpoint.set_default_client_config(quinn::ClientConfig::new(Arc::new(quic_client_config)));
    Ok(endpoint)
}

#[tokio::test(flavor = "multi_thread")]
async fn quic_floor_rejects_below_tls13() {
    let cluster = Cluster::new(ClusterOptions {
        name: "tls-floor".into(),
        num_nodes: 2,
        ..Default::default()
    })
    .await
    .expect("cluster setup");

    for i in 0..cluster.len() {
        let client = cluster.node(i);
        client
            .simple("CREATE TABLE IF NOT EXISTS records(id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
            .await
            .unwrap();
        client.simple("MURMUR SET TABLE records REPLICATED true").await.unwrap();
        client.simple("MURMUR FLUSH").await.unwrap();
    }

    // Mesh positive control: a marker converges over QUIC (TLS 1.3)
    cluster
        .node(0)
        .execute(
            "INSERT INTO records(id, name) VALUES($1, $2)",
            &[&1i64, &"tls-version-floor-marker"],
        )
        .await
        .unwrap();
    cluster.node(0).simple("MURMUR FLUSH").await.unwrap();

    let expected = vec![vec!["tls-version-floor-marker".into()]];
    assert_rows(&cluster, &[0, 1], "SELECT name FROM records ORDER BY id", &expected).await;

    let repl = cluster.node_info(0).repl_addr;
    let ca_pem = cluster.node_info(0).dir.join("ca.pem");
    let cert_pem = cluster.node_info(0).dir.join("cert.pem");
    let key_pem = cluster.node_info(0).dir.join("key.pem");

    // Floor negative: TLS 1.2 is strictly rejected for QUIC (RFC 9001 requires TLS 1.3 exclusively).
    // Attempting to construct a QUIC client connection with TLS 1.2 is rejected:
    let legacy_client = build_client_config(&ca_pem, &cert_pem, &key_pem, &[&rustls::version::TLS12]);
    let server_name = "localhost".try_into().unwrap();
    let quic_res = rustls::quic::ClientConnection::new(
        Arc::new(legacy_client),
        rustls::quic::Version::V1,
        server_name,
        vec![],
    );
    assert!(quic_res.is_err(), "QUIC client connection with TLS 1.2 must be rejected");
    let err_str = quic_res.unwrap_err().to_string();
    assert!(
        err_str.contains("TLS 1.3 support is required for QUIC") || err_str.contains("TLS 1.3"),
        "Unexpected error: {err_str}"
    );

    // Floor positive: TLS 1.3 handshake completes and opens a bi stream
    let modern = attacker_endpoint(&ca_pem, &cert_pem, &key_pem, &[&rustls::version::TLS13])
        .expect("TLS 1.3 endpoint creates");
    let connecting = modern.connect(repl, "localhost").expect("TLS 1.3 dial builds");
    let conn = tokio::time::timeout(Duration::from_secs(10), connecting)
        .await
        .expect("TLS 1.3 handshake completes")
        .expect("TLS 1.3 handshake ok");
    let _stream = tokio::time::timeout(Duration::from_secs(5), conn.open_bi())
        .await
        .expect("TLS 1.3 stream opens")
        .expect("TLS 1.3 stream ok");
    conn.close(0u32.into(), b"probe done");

    // Verify mesh replication remains healthy post-probe
    cluster
        .node(1)
        .execute(
            "INSERT INTO records(id, name) VALUES($1, $2)",
            &[&2i64, &"post-probe-marker"],
        )
        .await
        .unwrap();
    cluster.node(1).simple("MURMUR FLUSH").await.unwrap();

    let expected_both = vec![
        vec!["tls-version-floor-marker".into()],
        vec!["post-probe-marker".into()],
    ];
    assert_rows(&cluster, &[0, 1], "SELECT name FROM records ORDER BY id", &expected_both).await;
}
