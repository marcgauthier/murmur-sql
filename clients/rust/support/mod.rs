//! Scratch murmur-server harness shared by tests and examples via
//! `#[path = "../support/mod.rs"] mod support;`. Dev-only; not part of the
//! published crate API.
#![allow(dead_code)]

use std::fmt::Write as _;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

pub const PASSWORD: &str = "test-password";

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("repo root")
}

fn exe(name: &str) -> String {
    if cfg!(windows) {
        format!("{name}.exe")
    } else {
        name.to_string()
    }
}

/// `(server, admin)` binaries: `MURMUR_SERVER_BIN` or a Go build once per process.
fn binaries() -> &'static (PathBuf, PathBuf) {
    static BINS: OnceLock<(PathBuf, PathBuf)> = OnceLock::new();
    BINS.get_or_init(|| {
        if let Ok(server) = std::env::var("MURMUR_SERVER_BIN") {
            let server = PathBuf::from(server);
            let admin = server.with_file_name(exe("murmur-admin"));
            return (server, admin);
        }
        let root = root();
        let target = root.join("bin");
        std::fs::create_dir_all(&target).expect("create Go binary directory");
        for name in ["murmur-server", "murmur-admin"] {
            let status=Command::new("go").arg("build").arg("-o").arg(target.join(exe(name))).arg(format!("./cmd/{name}"))
                .current_dir(&root).status().expect("run go build");
            assert!(status.success(), "Go server build failed");
        }
        (target.join(exe("murmur-server")),target.join(exe("murmur-admin")))
    })
}

fn password_hash() -> &'static str {
    static HASH: OnceLock<String> = OnceLock::new();
    HASH.get_or_init(|| {
        let out = Command::new(&binaries().1)
            .args(["hash-password", "--password", PASSWORD])
            .output()
            .expect("murmur-admin hash-password");
        assert!(out.status.success(), "hash-password failed");
        let hash = String::from_utf8(out.stdout).unwrap().trim().to_string();
        assert!(hash.starts_with("$argon2id$"), "unexpected hash {hash}");
        hash
    })
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Forward-slash path for YAML double-quoted config strings.
pub fn yaml_path(p: &Path) -> String {
    p.to_string_lossy().replace('\\', "/")
}

/// TLS files for the `server.tls` section.
pub struct TlsFiles {
    pub cert: PathBuf,
    pub key: PathBuf,
    pub ca: PathBuf,
}

#[derive(Default)]
pub struct Options {
    pub node_id: String,
    pub database_id: String,
    pub tls: Option<TlsFiles>,
    pub extra_yaml: String,
}

/// Scratch directory plus a running server, both removed on drop.
pub struct Scratch {
    pub dir: tempfile::TempDir,
    pub port: u16,
    child: Child,
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Scratch {
    pub fn dir(&self) -> &Path {
        self.dir.path()
    }
}

/// Write a scratch config (user `admin`) and spawn murmur-server.
pub fn start(dir: tempfile::TempDir, opts: Options) -> Scratch {
    let port = free_port();
    let d = dir.path();
    let mut cfg = format!("server:\n  bind: \"127.0.0.1:{port}\"\n  production: false\n");
    if let Some(tls) = &opts.tls {
        writeln!(
            cfg,
            "  tls:\n    enabled: true\n    cert_file: \"{}\"\n    key_file: \"{}\"\n",
            yaml_path(&tls.cert),
            yaml_path(&tls.key)
        )
        .unwrap();
    }
    let node = if opts.node_id.is_empty() {
        String::new()
    } else {
        format!("  node_id: \"{}\"\n", opts.node_id)
    };
    let dbid = if opts.database_id.is_empty() {
        String::new()
    } else {
        format!("  database_id: \"{}\"\n", opts.database_id)
    };
    write!(
        cfg,
        "storage:\n  data_dir: \"{}\"\n{node}{dbid}\nauth:\n  users:\n    - name: \"admin\"\n      password_hash: \"{}\"\n\n\
         logging:\n  dir: \"{}\"\n\nbackups:\n  dir: \"{}\"\n\n{}",
        yaml_path(&d.join("data")),
        password_hash(),
        yaml_path(&d.join("logs")),
        yaml_path(&d.join("backups")),
        opts.extra_yaml
    )
    .unwrap();
    let config = d.join("config.yaml");
    std::fs::write(&config, cfg).unwrap();
    let mut child = Command::new(&binaries().0)
        .arg("--config")
        .arg(&config)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn murmur-server");
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    while TcpStream::connect_timeout(&addr, Duration::from_millis(200)).is_err() {
        if let Ok(Some(status)) = child.try_wait() {
            panic!("server exited early: {status}");
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            panic!("server on port {port} did not start");
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    Scratch { dir, port, child }
}

pub fn start_default() -> Scratch {
    start(tempfile::tempdir().unwrap(), Options::default())
}

pub fn config(server: &Scratch, key: &str) -> murmur_client::Config {
    let mut cfg = murmur_client::Config::new("admin", PASSWORD, key);
    cfg.port = server.port;
    cfg
}

/// Test PKI: a CA and a `127.0.0.1` server leaf (written under `dir`).
pub fn write_tls(dir: &Path) -> TlsFiles {
    let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca_key = rcgen::KeyPair::generate().unwrap();
    let ca = ca_params.self_signed(&ca_key).unwrap();
    let mut leaf_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    leaf_params.subject_alt_names = vec![rcgen::SanType::IpAddress("127.0.0.1".parse().unwrap())];
    leaf_params.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ServerAuth];
    let leaf_key = rcgen::KeyPair::generate().unwrap();
    let leaf = leaf_params.signed_by(&leaf_key, &ca, &ca_key).unwrap();
    let files = TlsFiles {
        cert: dir.join("server.crt"),
        key: dir.join("server.key"),
        ca: dir.join("ca.crt"),
    };
    std::fs::write(&files.cert, leaf.pem()).unwrap();
    std::fs::write(&files.key, leaf_key.serialize_pem()).unwrap();
    std::fs::write(&files.ca, ca.pem()).unwrap();
    files
}

/// Options for a standalone encrypted file store (replication credentials, no peers).
pub fn file_options(dir: &Path) -> Options {
    let node = "09".repeat(16);
    let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca_key = rcgen::KeyPair::generate().unwrap();
    let ca = ca_params.self_signed(&ca_key).unwrap();
    let mut leaf_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    leaf_params.subject_alt_names = vec![rcgen::SanType::URI(
        format!("replicateddb://node/{node}").try_into().unwrap(),
    )];
    leaf_params.extended_key_usages = vec![
        rcgen::ExtendedKeyUsagePurpose::ServerAuth,
        rcgen::ExtendedKeyUsagePurpose::ClientAuth,
    ];
    let leaf_key = rcgen::KeyPair::generate().unwrap();
    let leaf = leaf_params.signed_by(&leaf_key, &ca, &ca_key).unwrap();
    std::fs::write(dir.join("file-ca.pem"), ca.pem()).unwrap();
    std::fs::write(dir.join("file-node.pem"), leaf.pem()).unwrap();
    std::fs::write(dir.join("file-key.pem"), leaf_key.serialize_pem()).unwrap();
    let signing = ed25519_dalek::SigningKey::from_bytes(&[3u8; 32]);
    let public: String = signing
        .verifying_key()
        .to_bytes()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    let extra_yaml = format!(
        "replication:\n  enabled: true\n  bind: \"127.0.0.1:0\"\n  cert_chain_file: \"{}\"\n  private_key_file: \"{}\"\n  \
         ca_file: \"{}\"\n  signing_key_hex: \"{}\"\n  trusted_origins:\n    \"{node}\": \"{public}\"\n  \
         files:\n    directory: \"{}\"\n    key_hex: \"{}\"\n",
        yaml_path(&dir.join("file-node.pem")),
        yaml_path(&dir.join("file-key.pem")),
        yaml_path(&dir.join("file-ca.pem")),
        "03".repeat(32),
        yaml_path(&dir.join("objects")),
        "07".repeat(32),
    );
    Options {
        node_id: node,
        database_id: String::new(),
        tls: None,
        extra_yaml,
    }
}

/// Fresh hex for test identities. A process-wide counter mixed with the clock
/// and pid keeps parallel tests distinct without extra dependencies.
fn fresh_hex(bytes: usize) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let count = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let mut x = (nanos as u64)
        .wrapping_add(count.wrapping_mul(0x9e3779b97f4a7c15))
        .wrapping_add((std::process::id() as u64).wrapping_mul(0xbf58476d1ce4e5b9))
        | 1;
    let mut out = String::with_capacity(bytes * 2);
    for _ in 0..bytes {
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        out.push_str(&format!(
            "{:02x}",
            (x.wrapping_mul(0x2545f4914f6cdd1d) >> 56) as u8
        ));
    }
    out
}

/// Provision `dirs` as one replication mesh: a shared database id, distinct
/// node ids, one CA, and mutual `trusted_origins`. Returns one [`Options`]
/// per directory, in order. `replication_extra` is appended inside each
/// node's `replication` section (for example a `merge_policies` map).
/// Nodes start unmeshed; dial them together with the client's `peer_add`.
pub fn replication_mesh_options(dirs: &[&Path], replication_extra: &str) -> Vec<Options> {
    let database_id = fresh_hex(16);
    let mut nodes = Vec::with_capacity(dirs.len());
    while nodes.len() < dirs.len() {
        let id = fresh_hex(16);
        if !nodes.contains(&id) {
            nodes.push(id);
        }
    }
    let seeds: Vec<String> = (0..dirs.len()).map(|_| fresh_hex(32)).collect();
    let pubs: Vec<String> = seeds
        .iter()
        .map(|seed| {
            let mut raw = [0u8; 32];
            for (i, byte) in raw.iter_mut().enumerate() {
                *byte = u8::from_str_radix(&seed[i * 2..i * 2 + 2], 16).unwrap();
            }
            ed25519_dalek::SigningKey::from_bytes(&raw)
                .verifying_key()
                .to_bytes()
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect()
        })
        .collect();
    let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca_key = rcgen::KeyPair::generate().unwrap();
    let ca = ca_params.self_signed(&ca_key).unwrap();
    let origins = nodes
        .iter()
        .zip(pubs.iter())
        .map(|(node, key)| format!("    \"{node}\": \"{key}\""))
        .collect::<Vec<_>>()
        .join("\n");
    dirs.iter()
        .zip(nodes.iter())
        .zip(seeds.iter())
        .map(|((dir, node), seed)| {
            let mut leaf_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
            leaf_params.subject_alt_names =
                vec![rcgen::SanType::URI(format!("replicateddb://node/{node}").try_into().unwrap())];
            leaf_params.extended_key_usages = vec![
                rcgen::ExtendedKeyUsagePurpose::ServerAuth,
                rcgen::ExtendedKeyUsagePurpose::ClientAuth,
            ];
            let leaf_key = rcgen::KeyPair::generate().unwrap();
            let leaf = leaf_params.signed_by(&leaf_key, &ca, &ca_key).unwrap();
            std::fs::write(dir.join("repl-ca.pem"), ca.pem()).unwrap();
            std::fs::write(dir.join("repl-node.pem"), leaf.pem()).unwrap();
            std::fs::write(dir.join("repl-key.pem"), leaf_key.serialize_pem()).unwrap();
            let extra = if replication_extra.is_empty() {
                String::new()
            } else {
                format!("\n{}\n", replication_extra.trim_end())
            };
            let extra_yaml = format!(
                "replication:\n  enabled: true\n  bind: \"127.0.0.1:0\"\n  cert_chain_file: \"{}\"\n  private_key_file: \"{}\"\n  \
                 ca_file: \"{}\"\n  signing_key_hex: \"{seed}\"\n  trusted_origins:\n{origins}{extra}\n",
                yaml_path(&dir.join("repl-node.pem")),
                yaml_path(&dir.join("repl-key.pem")),
                yaml_path(&dir.join("repl-ca.pem")),
            );
            Options { node_id: node.clone(), database_id: database_id.clone(), tls: None, extra_yaml }
        })
        .collect()
}

/// A single replication-enabled node (one-node mesh). See
/// [`replication_mesh_options`].
pub fn replication_options(dir: &Path, replication_extra: &str) -> Options {
    replication_mesh_options(&[dir], replication_extra)
        .pop()
        .unwrap()
}
