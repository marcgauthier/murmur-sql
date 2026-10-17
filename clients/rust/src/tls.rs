//! rustls configuration mirroring Postgres `sslmode`.

use std::sync::Arc;

use rustls::client::WebPkiServerVerifier;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, ring};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::{
    CertificateError, ClientConfig, DigitallySignedStruct, Error as TlsError, RootCertStore,
    SignatureScheme,
};

use crate::{Error, TlsConfig, TlsMode};

fn provider() -> Arc<CryptoProvider> {
    Arc::new(ring::default_provider())
}

/// `require`: encrypted, certificate not verified.
#[derive(Debug)]
struct NoVerify(Arc<CryptoProvider>);

impl ServerCertVerifier for NoVerify {
    fn verify_server_cert(
        &self,
        _: &CertificateDer<'_>,
        _: &[CertificateDer<'_>],
        _: &ServerName<'_>,
        _: &[u8],
        _: UnixTime,
    ) -> Result<ServerCertVerified, TlsError> {
        Ok(ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(
        &self,
        m: &[u8],
        c: &CertificateDer<'_>,
        d: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        rustls::crypto::verify_tls12_signature(m, c, d, &self.0.signature_verification_algorithms)
    }
    fn verify_tls13_signature(
        &self,
        m: &[u8],
        c: &CertificateDer<'_>,
        d: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        rustls::crypto::verify_tls13_signature(m, c, d, &self.0.signature_verification_algorithms)
    }
    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

/// `verify-ca`: chain must verify against the CA, host name is not checked.
#[derive(Debug)]
struct ChainOnly(Arc<WebPkiServerVerifier>);

impl ServerCertVerifier for ChainOnly {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        name: &ServerName<'_>,
        ocsp: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, TlsError> {
        match self
            .0
            .verify_server_cert(end_entity, intermediates, name, ocsp, now)
        {
            Err(TlsError::InvalidCertificate(CertificateError::NotValidForName))
            | Err(TlsError::InvalidCertificate(CertificateError::NotValidForNameContext {
                ..
            })) => Ok(ServerCertVerified::assertion()),
            other => other,
        }
    }
    fn verify_tls12_signature(
        &self,
        m: &[u8],
        c: &CertificateDer<'_>,
        d: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        self.0.verify_tls12_signature(m, c, d)
    }
    fn verify_tls13_signature(
        &self,
        m: &[u8],
        c: &CertificateDer<'_>,
        d: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        self.0.verify_tls13_signature(m, c, d)
    }
    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.supported_verify_schemes()
    }
}

fn tls_err(e: impl std::fmt::Display) -> Error {
    Error::Tls(e.to_string())
}

fn roots(ca_file: &str) -> Result<RootCertStore, Error> {
    let mut reader = std::io::BufReader::new(std::fs::File::open(ca_file)?);
    let mut store = RootCertStore::empty();
    for cert in rustls_pemfile::certs(&mut reader) {
        store.add(cert?).map_err(tls_err)?;
    }
    if store.is_empty() {
        return Err(Error::Tls(format!("no certificates in {ca_file}")));
    }
    Ok(store)
}

pub(crate) fn client_config(tls: &TlsConfig) -> Result<ClientConfig, Error> {
    let provider = provider();
    let builder = ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(tls_err)?;
    let builder = match tls.mode {
        TlsMode::Require => builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoVerify(provider))),
        TlsMode::VerifyCa | TlsMode::VerifyFull => {
            let roots = Arc::new(roots(&tls.ca_file)?);
            if tls.mode == TlsMode::VerifyFull {
                builder.with_root_certificates(roots)
            } else {
                let inner = WebPkiServerVerifier::builder_with_provider(roots, provider)
                    .build()
                    .map_err(tls_err)?;
                builder
                    .dangerous()
                    .with_custom_certificate_verifier(Arc::new(ChainOnly(inner)))
            }
        }
        TlsMode::Disable => unreachable!("plaintext needs no TLS config"),
    };
    if tls.cert_file.is_empty() {
        return Ok(builder.with_no_client_auth());
    }
    let mut cert_reader = std::io::BufReader::new(std::fs::File::open(&tls.cert_file)?);
    let chain: Vec<CertificateDer<'static>> =
        rustls_pemfile::certs(&mut cert_reader).collect::<Result<_, _>>()?;
    let mut key_reader = std::io::BufReader::new(std::fs::File::open(&tls.key_file)?);
    let key: PrivateKeyDer<'static> = rustls_pemfile::private_key(&mut key_reader)?
        .ok_or_else(|| Error::Tls(format!("no private key in {}", tls.key_file)))?;
    builder.with_client_auth_cert(chain, key).map_err(tls_err)
}
