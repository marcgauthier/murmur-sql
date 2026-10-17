//! Raw-QUIC replication-protocol attacker fixture (Go: `attacker.go`).
//!
//! A hostile peer speaking the product wire protocol: postcard `Message`
//! framing, snapshot manifest/chunk protocol, and a QUIC endpoint with the
//! product SAN-URI TLS verifier. Everything wire-visible is reused from
//! `murmur-replication` (path dep, re-exported as
//! [`crate::replication`]); this module only adds test-side dialogue
//! helpers (dial/handshake, patient collection, corrupt-snapshot forging).
//! Scenarios must use this fixture instead of reimplementing the protocol.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use murmur_replication::{
    protocol::{self, Message},
    retention, snapshot, transport, Config as ReplConfig, Credentials, Dissemination,
};

use crate::{Cluster, Error, Result};

/// Dial/handshake deadline (Go: 15 s).
const DIAL_TIMEOUT: Duration = Duration::from_secs(15);
/// Per-send and Welcome deadlines (Go: 10 s).
const IO_TIMEOUT: Duration = Duration::from_secs(10);
/// Largest transaction the attacker advertises (product default).
const MAX_TRANSACTION: u64 = 64 << 20;

/// Malicious peer identity: a QUIC endpoint with a cluster-CA leaf cert for
/// `node_id` and an origin signing key (`seed`) the victim may or may not
/// trust, depending on the leg. Owns everything; no borrow of the cluster.
pub struct Attacker {
    node_id: [u8; 16],
    config: ReplConfig,
    endpoint: quinn::Endpoint,
}

impl Attacker {
    /// New attacker identity `node_id` with origin signing `seed`. Use a
    /// fixed id distinct from every node id, and register
    /// `protocol::public_key(&seed)` in the victim's
    /// `extra_trusted_origins` when the leg needs the victim to accept the
    /// attacker's signatures (e.g. a forged snapshot manifest).
    pub fn new(cluster: &Cluster, node_id: [u8; 16], seed: [u8; 32]) -> Result<Self> {
        let issued = cluster
            .issue_cert(&node_id)
            .map_err(|e| Error::Attack(format!("issue attacker cert: {e}")))?;
        let credentials = Credentials {
            chain: vec![issued.cert_der],
            private_key: zeroize::Zeroizing::new(issued.key_der),
            roots: vec![cluster.ca_der()],
        };
        let bind: SocketAddr = "127.0.0.1:0"
            .parse()
            .map_err(|e| Error::Attack(format!("attacker bind addr: {e}")))?;
        let config = ReplConfig::new(bind, credentials, seed);
        let endpoint =
            transport::endpoint(&config, node_id).map_err(|e| Error::Attack(e.to_string()))?;
        Ok(Self {
            node_id,
            config,
            endpoint,
        })
    }

    /// Relay impersonating cluster node `idx`: a fresh TLS leaf for that
    /// node's id (identity is the SAN URI, so it authenticates as the node)
    /// with origin signing `seed` (pass
    /// [`Cluster::node_origin_seed`] for a signature the victim accepts).
    pub fn impersonate(cluster: &Cluster, idx: usize, seed: [u8; 32]) -> Result<Self> {
        Self::new(cluster, cluster.node_id(idx), seed)
    }

    pub fn node_id(&self) -> [u8; 16] {
        self.node_id
    }

    /// Product replication config (e.g. to shorten
    /// `snapshot_transfer_lifetime` for a forged manifest).
    pub fn config(&self) -> &ReplConfig {
        &self.config
    }

    pub fn config_mut(&mut self) -> &mut ReplConfig {
        &mut self.config
    }

    /// Complete a handshake against the victim and return the session plus
    /// the victim's schema bytes (revealed by its Welcome).
    pub async fn dial(
        &self,
        addr: SocketAddr,
        db: [u8; 16],
        expect: [u8; 16],
    ) -> Result<Session> {
        let connecting = self
            .endpoint
            .connect(addr, "murmur.invalid")
            .map_err(|e| Error::Attack(format!("connect setup: {e}")))?;
        let connection = tokio::time::timeout(DIAL_TIMEOUT, connecting)
            .await
            .map_err(|_| Error::Attack("dial timeout".into()))?
            .map_err(|e| Error::Attack(format!("connect: {e}")))?;
        let (mut send, mut recv) = tokio::time::timeout(DIAL_TIMEOUT, connection.open_bi())
            .await
            .map_err(|_| Error::Attack("open stream timeout".into()))?
            .map_err(|e| Error::Attack(format!("open stream: {e}")))?;
        protocol::write(
            &mut send,
            &Message::Hello {
                capabilities: protocol::CAPABILITIES,
                required: protocol::REQUIRED,
                version: protocol::VERSION,
                database: db,
                node: self.node_id,
                fetch: None,
                schema: Vec::new(),
                mode: Dissemination::Gossip,
                max_transaction: MAX_TRANSACTION,
            },
        )
        .await
        .map_err(|e| Error::Attack(format!("write hello: {e}")))?;
        let welcome = tokio::time::timeout(IO_TIMEOUT, protocol::read(&mut recv))
            .await
            .map_err(|_| Error::Attack("welcome timeout".into()))?
            .map_err(|e| Error::Attack(format!("read welcome: {e}")))?;
        let schema = match welcome {
            Message::Welcome {
                database: db2,
                node,
                schema,
                ..
            } => {
                if db2 != db {
                    return Err(Error::Attack("welcome database mismatch".into()));
                }
                if node != expect {
                    return Err(Error::Attack("welcome node mismatch".into()));
                }
                schema
            }
            other => return Err(Error::Attack(format!("expected welcome, got {other:?}"))),
        };
        Ok(Session {
            connection,
            send,
            recv,
            welcome_schema: schema,
        })
    }
}

/// One attack QUIC session with its handshake stream open. All replication
/// traffic multiplexes over that stream; dropping the session closes the
/// connection.
pub struct Session {
    connection: quinn::Connection,
    send: quinn::SendStream,
    recv: quinn::RecvStream,
    welcome_schema: Vec<u8>,
}

impl Session {
    /// Victim schema bytes from its Welcome (for forging schema-matching
    /// manifests via [`protocol::schema_digest`]).
    pub fn welcome_schema(&self) -> &[u8] {
        &self.welcome_schema
    }

    pub async fn send(&mut self, message: &Message) -> Result<()> {
        tokio::time::timeout(IO_TIMEOUT, protocol::write(&mut self.send, message))
            .await
            .map_err(|_| Error::Attack("send timeout".into()))?
            .map_err(|e| Error::Attack(format!("send: {e}")))?;
        Ok(())
    }

    /// Read until `want` matches, `total` elapses, or the session dies
    /// (Go: `collectUntil`, but patient across quiet slices instead of
    /// returning on the first one).
    pub async fn collect_until(
        &mut self,
        total: Duration,
        want: impl Fn(&Message) -> bool,
    ) -> Vec<Message> {
        let mut out = Vec::new();
        let deadline = Instant::now() + total;
        while Instant::now() < deadline {
            let slice = (deadline - Instant::now()).min(Duration::from_millis(500));
            match tokio::time::timeout(slice, protocol::read(&mut self.recv)).await {
                Ok(Ok(message)) => {
                    let matched = want(&message);
                    out.push(message);
                    if matched {
                        return out;
                    }
                }
                Ok(Err(_)) => return out, // session died; the caller interprets
                Err(_) => {}              // quiet slice; keep waiting
            }
        }
        out
    }

    pub fn close(self) {
        self.connection.close(0u32.into(), b"attack done");
    }
}

/// A manifest plus chunk payloads whose second chunk was bit-flipped after
/// the manifest digests were computed (Go: `corruptSnapshot`).
pub struct ForgedSnapshot {
    pub manifest: snapshot::Manifest,
    pub transfer: [u8; 16],
    pub chunk0: Vec<u8>,
    pub chunk1_bad: Vec<u8>,
}

/// Forge a structurally valid two-chunk snapshot for the victim's schema:
/// the manifest passes every structural check, but the final chunk's bytes
/// differ from what its chunk hash covers. `bytes` must span exactly two
/// 64 KiB wire chunks. The manifest is signed with `config`'s key for donor
/// `donor`, so point `config` at the attacker identity the victim trusts.
pub fn forge_corrupt_snapshot(
    config: &ReplConfig,
    donor: [u8; 16],
    db: [u8; 16],
    victim: [u8; 16],
    victim_schema: &[u8],
    bytes: usize,
) -> Result<ForgedSnapshot> {
    use sha2::{Digest, Sha256};

    let mut data = vec![0u8; bytes];
    for (i, b) in data.iter_mut().enumerate() {
        *b = b'a' + (i % 26) as u8;
    }
    let schema = protocol::schema_digest(victim_schema);
    let (manifest, _records) = snapshot::build(
        (db, donor),
        victim,
        data.clone(),
        Vec::new(),
        schema,
        config,
        retention::now_millis(),
    )
    .map_err(|e| Error::Attack(format!("forge manifest: {e}")))?;
    assert_eq!(
        manifest.chunks.len(),
        2,
        "fixture: forged snapshot must span exactly two chunks"
    );

    // Non-vacuous guards: the uncorrupted bytes verify against the manifest,
    // and the corrupted bytes must not.
    assert_eq!(
        <[u8; 32]>::from(Sha256::digest(&data)),
        manifest.digest,
        "fixture: uncorrupted snapshot does not verify locally"
    );
    let mut parts = data.chunks(protocol::CHUNK_BYTES);
    let chunk0 = parts.next().expect("fixture: first chunk").to_vec();
    let mut chunk1_bad = parts.next().expect("fixture: second chunk").to_vec();
    assert!(parts.next().is_none(), "fixture: unexpected third chunk");
    // Flip a byte mid-chunk. An ASCII-to-ASCII flip preserves length and
    // framing; only the digest differs.
    let mid = chunk1_bad.len() / 2;
    chunk1_bad[mid] ^= 0x01;
    assert_eq!(
        chunk1_bad.len(),
        data.len() - chunk0.len(),
        "fixture: corruption changed the encoded length"
    );
    assert_ne!(
        <[u8; 32]>::from(Sha256::digest(&chunk1_bad)),
        manifest.chunks[1],
        "fixture: corrupted snapshot still verifies locally"
    );
    let transfer = manifest.transfer;
    Ok(ForgedSnapshot {
        manifest,
        transfer,
        chunk0,
        chunk1_bad,
    })
}
