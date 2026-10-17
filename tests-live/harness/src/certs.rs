//! Test CA + per-node QUIC/TLS credential provisioning (rcgen).
//!
//! One self-signed CA per cluster, one leaf per node with SAN URI
//! `replicateddb://node/<hex>` (the identity the replication transport
//! parses) and server+client EKU. Material is written as PEM under each
//! node's `tls/` dir for the server's `replication.*_file` settings and kept
//! for forensics parity.

use std::path::{Path, PathBuf};
use std::time::Duration;

/// Cluster test certificate authority.
pub struct Ca {
    cert: rcgen::Certificate,
    key: rcgen::KeyPair,
}

/// Issued leaf material for one node (PEM, plus DER for in-process QUIC
/// endpoints: the attacker fixture builds product `Credentials` from the
/// DER without PEM re-parsing).
pub struct Issued {
    pub cert_pem: String,
    pub key_pem: String,
    pub cert_der: Vec<u8>,
    pub key_der: Vec<u8>,
}

/// PEM files under a node's `tls/` dir.
pub struct TlsPaths {
    pub ca: PathBuf,
    pub chain: PathBuf,
    pub key: PathBuf,
}

impl Ca {
    pub fn generate() -> Result<Self, rcgen::Error> {
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new())?;
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let key = rcgen::KeyPair::generate()?;
        let cert = params.self_signed(&key)?;
        Ok(Self { cert, key })
    }

    pub fn ca_pem(&self) -> String {
        self.cert.pem()
    }

    pub fn ca_der(&self) -> Vec<u8> {
        self.cert.der().to_vec()
    }

    /// Issue a leaf for `node_id`. `ttl` overrides the (effectively
    /// unbounded) default validity; used by `cert-lifecycle` for expiry legs.
    pub fn issue(
        &self,
        node_id: &[u8; 16],
        ttl: Option<Duration>,
    ) -> Result<Issued, rcgen::Error> {
        let key = rcgen::KeyPair::generate()?;
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new())?;
        params.subject_alt_names = vec![rcgen::SanType::URI(
            format!("replicateddb://node/{}", crate::hex(node_id)).try_into()?,
        )];
        params.extended_key_usages = vec![
            rcgen::ExtendedKeyUsagePurpose::ServerAuth,
            rcgen::ExtendedKeyUsagePurpose::ClientAuth,
        ];
        if let Some(ttl) = ttl {
            let now = time::OffsetDateTime::now_utc();
            params.not_before = now;
            params.not_after =
                now + time::Duration::try_from(ttl).unwrap_or(time::Duration::MAX);
        }
        let cert = params.signed_by(&key, &self.cert, &self.key)?;
        Ok(Issued {
            cert_pem: cert.pem(),
            key_pem: key.serialize_pem(),
            cert_der: cert.der().to_vec(),
            key_der: key.serialize_der(),
        })
    }

    /// Write `tls/ca.pem`, `tls/node.pem`, `tls/node-key.pem` under `node_dir`.
    pub fn write_tls_dir(
        &self,
        issued: &Issued,
        node_dir: &Path,
    ) -> std::io::Result<TlsPaths> {
        let tls = node_dir.join("tls");
        std::fs::create_dir_all(&tls)?;
        let paths = TlsPaths {
            ca: tls.join("ca.pem"),
            chain: tls.join("node.pem"),
            key: tls.join("node-key.pem"),
        };
        std::fs::write(&paths.ca, self.ca_pem())?;
        std::fs::write(&paths.chain, &issued.cert_pem)?;
        std::fs::write(&paths.key, &issued.key_pem)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&paths.key, std::fs::Permissions::from_mode(0o600))?;
        }
        Ok(paths)
    }
}
