//! Postgres-wire TLS: server certificates from `[server.tls]`, mutual TLS
//! in production mode.
//!
//! - `tls.enabled` without production: the server accepts TLS (server
//!   certificate only) and plaintext connections.
//! - `server.production`: TLS is mandatory and clients must present a
//!   certificate signed by `tls.ca_file`. The handshake rejects missing or
//!   untrusted client certificates; the startup handler additionally rejects
//!   non-TLS connections, since pgwire otherwise negotiates down.

use crate::config::TlsConfig;
use murmur::{Error, Result};

/// Build the pgwire TLS acceptor, or `None` when TLS is disabled. Fails fast
/// on missing files, unparseable PEM, or a key that does not match the chain.
pub fn build_acceptor(
    tls: &TlsConfig,
    production: bool,
) -> Result<Option<tokio_rustls::TlsAcceptor>> {
    if !tls.enabled {
        return Ok(None);
    }
    let cert_file = tls
        .cert_file
        .as_deref()
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| Error::Invalid("server.tls.enabled requires tls cert_file".into()))?;
    let key_file = tls
        .key_file
        .as_deref()
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| Error::Invalid("server.tls.enabled requires tls key_file".into()))?;
    let chain: Vec<rustls::pki_types::CertificateDer<'static>> = crate::config::load_certs(cert_file)?
        .into_iter()
        .map(rustls::pki_types::CertificateDer::from)
        .collect();
    let key = rustls::pki_types::PrivateKeyDer::try_from(crate::config::load_private_key(key_file)?)
        .map_err(|e| Error::Invalid(format!("cannot parse {key_file}: {e}")))?;
    let provider: std::sync::Arc<rustls::crypto::CryptoProvider> =
        rustls::crypto::ring::default_provider().into();
    let config = if production {
        let ca_file = tls
            .ca_file
            .as_deref()
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| {
                Error::Invalid("server.production requires tls ca_file for client certificates".into())
            })?;
        let mut roots = rustls::RootCertStore::empty();
        for der in crate::config::load_certs(ca_file)? {
            roots
                .add(rustls::pki_types::CertificateDer::from(der))
                .map_err(|e| Error::Invalid(format!("cannot load {ca_file}: {e}")))?;
        }
        let verifier =
            rustls::server::WebPkiClientVerifier::builder_with_provider(roots.into(), provider.clone())
                .build()
                .map_err(|e| Error::Invalid(format!("cannot build client verifier: {e}")))?;
        rustls::ServerConfig::builder_with_provider(provider)
            .with_protocol_versions(&[ &rustls::version::TLS13, &rustls::version::TLS12 ])
            .map_err(|e| Error::Invalid(format!("TLS versions: {e}")))?
            .with_client_cert_verifier(verifier)
            .with_single_cert(chain, key)
            .map_err(|e| Error::Invalid(format!("TLS identity: {e}")))?
    } else {
        rustls::ServerConfig::builder_with_provider(provider)
            .with_protocol_versions(&[ &rustls::version::TLS13, &rustls::version::TLS12 ])
            .map_err(|e| Error::Invalid(format!("TLS versions: {e}")))?
            .with_no_client_auth()
            .with_single_cert(chain, key)
            .map_err(|e| Error::Invalid(format!("TLS identity: {e}")))?
    };
    Ok(Some(tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(config))))
}
