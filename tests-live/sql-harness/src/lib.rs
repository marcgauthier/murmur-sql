//! Process-isolated live SQL tests. This package never links the embedded engine.
pub use murmur_client::{Client, Config};
use rand::RngCore;
use std::{
    fs,
    net::{SocketAddr, TcpListener, TcpStream, UdpSocket},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::OnceLock,
    time::{Duration, Instant},
};
pub use toml_edit;
pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
pub const PASSWORD: &str = "live-sql-test-password";
pub const KEY: &str = "live-sql-encryption-key";

fn repo() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap()
}
fn binary_name(s: &str) -> String {
    if cfg!(windows) {
        format!("{s}.exe")
    } else {
        s.into()
    }
}
fn binaries() -> &'static (PathBuf, PathBuf) {
    static BINS: OnceLock<(PathBuf, PathBuf)> = OnceLock::new();
    BINS.get_or_init(|| {
        if let Some(path) = std::env::var_os("MURMUR_SERVER_BIN") {
            let server = PathBuf::from(path)
                .canonicalize()
                .expect("MURMUR_SERVER_BIN");
            let admin = std::env::var_os("MURMUR_ADMIN_BIN")
                .map(PathBuf::from)
                .unwrap_or_else(|| server.with_file_name(binary_name("murmur-admin")));
            return (server, admin);
        }
        let root = repo();
        let status = Command::new("cargo")
            .args([
                "build",
                "--features",
                "replication",
                "--bin",
                "murmur-server",
                "--bin",
                "murmur-admin",
            ])
            .current_dir(&root)
            .status()
            .expect("build server");
        assert!(status.success(), "server build failed");
        let target = std::env::var_os("CARGO_TARGET_DIR")
            .map(PathBuf::from)
            .map(|p| if p.is_absolute() { p } else { root.join(p) })
            .unwrap_or_else(|| root.join("target"));
        (
            target.join("debug").join(binary_name("murmur-server")),
            target.join("debug").join(binary_name("murmur-admin")),
        )
    })
}
fn hash() -> &'static str {
    static HASH: OnceLock<String> = OnceLock::new();
    HASH.get_or_init(|| {
        let out = Command::new(&binaries().1)
            .args(["hash-password", "--password", PASSWORD])
            .output()
            .expect("hash password");
        assert!(
            out.status.success(),
            "password hash failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8(out.stdout).unwrap().trim().into()
    })
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
fn quoted(s: &str) -> String {
    format!("{s:?}")
}
fn path(p: &Path) -> String {
    quoted(&p.to_string_lossy().replace('\\', "/"))
}
fn tcp_addr() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}
fn udp_addr() -> SocketAddr {
    UdpSocket::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}

#[derive(Default)]
pub struct ClusterOptions {
    pub name: String,
    pub num_nodes: usize,
    /// Additional top-level configuration, such as `[engine]` or `[replication.files]`.
    pub extra_toml: String,
    /// Overrides inserted into each node's `[replication]` section.
    pub replication_toml: String,
    pub files: bool,
    pub manual_peers: bool,
    pub local_only: bool,
}

pub struct Node {
    pub dir: PathBuf,
    pub data_dir: PathBuf,
    pub config_path: PathBuf,
    pub sql_addr: SocketAddr,
    pub repl_addr: SocketAddr,
    pub node_id: String,
    pub label: String,
    child: Option<Child>,
    client: Option<Client>,
    key: String,
    tls: murmur_client::TlsConfig,
}
impl Drop for Node {
    fn drop(&mut self) {
        self.client.take();
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

pub struct Cluster {
    nodes: Vec<Node>,
    dir: PathBuf,
    name: String,
    starting: bool,
}
impl Cluster {
    pub async fn new(opts: ClusterOptions) -> Result<Self> {
        let count = if opts.num_nodes == 0 {
            3
        } else {
            opts.num_nodes
        };
        let runtime = std::env::var_os("MURMUR_LIVE_RUNTIME")
            .map(PathBuf::from)
            .unwrap_or_else(|| repo().join("tests-live/runtime"));
        fs::create_dir_all(&runtime)?;
        let mut random = [0u8; 16];
        rand::thread_rng().fill_bytes(&mut random);
        let dir = runtime.join(format!(
            "{}-{}-{}",
            opts.name,
            std::process::id(),
            hex(&random[..4])
        ));
        fs::create_dir_all(&dir)?;
        let mut cluster = Self {
            nodes: Vec::new(),
            dir,
            name: opts.name,
            starting: true,
        };
        let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new())?;
        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let ca_key = rcgen::KeyPair::generate()?;
        let ca = ca_params.self_signed(&ca_key)?;
        let mut signing = Vec::new();
        for i in 0..count {
            let mut bytes = [0u8; 32];
            rand::thread_rng().fill_bytes(&mut bytes);
            signing.push(ed25519_dalek::SigningKey::from_bytes(&bytes));
            let mut id = random;
            id[15] = i as u8;
            let node_dir = cluster.dir.join(format!("node{}", i + 1));
            fs::create_dir_all(&node_dir)?;
            cluster.nodes.push(Node {
                data_dir: node_dir.join("data"),
                config_path: node_dir.join("config.toml"),
                dir: node_dir,
                sql_addr: tcp_addr(),
                repl_addr: udp_addr(),
                node_id: hex(&id),
                label: format!("node{}", i + 1),
                child: None,
                client: None,
                key: KEY.into(),
                tls: Default::default(),
            });
        }
        let trusted = cluster
            .nodes
            .iter()
            .zip(&signing)
            .map(|(n, s)| {
                format!(
                    "{} = {}",
                    quoted(&n.node_id),
                    quoted(&hex(&s.verifying_key().to_bytes()))
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        for (i, node) in cluster.nodes.iter().enumerate() {
            let mut params = rcgen::CertificateParams::new(vec!["localhost".into()])?;
            params
                .subject_alt_names
                .push(rcgen::SanType::IpAddress("127.0.0.1".parse()?));
            params.subject_alt_names.push(rcgen::SanType::URI(
                format!("replicateddb://node/{}", node.node_id).try_into()?,
            ));
            params.extended_key_usages = vec![
                rcgen::ExtendedKeyUsagePurpose::ServerAuth,
                rcgen::ExtendedKeyUsagePurpose::ClientAuth,
            ];
            let key = rcgen::KeyPair::generate()?;
            let cert = params.signed_by(&key, &ca, &ca_key)?;
            fs::write(node.dir.join("ca.pem"), ca.pem())?;
            fs::write(node.dir.join("cert.pem"), cert.pem())?;
            fs::write(node.dir.join("key.pem"), key.serialize_pem())?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(node.dir.join("key.pem"), fs::Permissions::from_mode(0o600))?;
            }
            let peers = if opts.manual_peers {
                Vec::new()
            } else {
                cluster
                    .nodes
                    .iter()
                    .enumerate()
                    .filter(|(j, _)| *j != i)
                    .map(|(_, n)| quoted(&n.repl_addr.to_string()))
                    .collect::<Vec<_>>()
            };
            let files = if opts.files {
                format!("\n[replication.files]\ndirectory = {}\nkey_hex = {}\nfetch_interval = \"100ms\"\n", path(&node.dir.join("objects")), quoted(&"3d".repeat(32)))
            } else {
                String::new()
            };
            fs::write(&node.config_path, format!(
                "[server]\nbind = {}\nproduction = false\n[storage]\ndata_dir = {}\ndatabase_id = {}\nnode_id = {}\n\
                 [[auth.users]]\nname = \"admin\"\npassword_hash = {}\n[logging]\ndir = {}\nconsole = false\n[backups]\ndir = {}\n\
                 [replication]\nenabled = {}\nbind = {}\nbootstrap = [{}]\ncert_chain_file = {}\nprivate_key_file = {}\nca_file = {}\nsigning_key_hex = {}\ntrusted_origins = {{ {trusted} }}\n{}\n{}\n{}",
                quoted(&node.sql_addr.to_string()), path(&node.data_dir), quoted(&hex(&random)), quoted(&node.node_id), quoted(hash()), path(&node.dir.join("logs")), path(&node.dir.join("backups")), !opts.local_only, quoted(&node.repl_addr.to_string()), peers.join(","), path(&node.dir.join("cert.pem")), path(&node.dir.join("key.pem")), path(&node.dir.join("ca.pem")), quoted(&hex(&signing[i].to_bytes())), opts.replication_toml, files, opts.extra_toml))?;
        }
        for i in 0..count {
            cluster.start_node(i).await?;
        }
        cluster.starting = false;
        Ok(cluster)
    }
    pub fn len(&self) -> usize {
        self.nodes.len()
    }
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }
    pub fn node(&self, i: usize) -> &Client {
        self.nodes[i].client.as_ref().expect("SQL node stopped")
    }
    pub fn node_info(&self, i: usize) -> &Node {
        &self.nodes[i]
    }
    /// PID for scoped signal/network-fault scenarios; the harness retains ownership
    /// and still reaps the process through stop_node or Drop.
    pub fn pid(&self, i: usize) -> Option<u32> {
        self.nodes[i].child.as_ref().map(Child::id)
    }

    pub fn runtime_dir(&self) -> &Path {
        &self.dir
    }
    pub fn config(&self, i: usize) -> Config {
        let mut config = Config::new("admin", PASSWORD, &self.nodes[i].key);
        config.port = self.nodes[i].sql_addr.port();
        config.tls = self.nodes[i].tls.clone();
        config
    }
    pub async fn connect(&self, i: usize) -> murmur_client::Result<Client> {
        Client::connect(&self.config(i)).await
    }
    pub fn stop_node(&mut self, i: usize) {
        let n = &mut self.nodes[i];
        n.client.take();
        if let Some(mut child) = n.child.take() {
            // A signal scenario may already have terminated this process.
            if child.try_wait().expect("check SQL server exit").is_none() {
                if let Err(error) = child.kill() {
                    assert!(
                        child
                            .try_wait()
                            .expect("check signaled SQL server exit")
                            .is_some(),
                        "kill SQL server: {error}"
                    );
                }
            }
            child.wait().expect("reap SQL server");
        }
    }
    pub async fn start_node(&mut self, i: usize) -> Result<()> {
        let node = &mut self.nodes[i];
        assert!(node.child.is_none(), "node already running");
        let log = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(node.dir.join("server.log"))?;
        node.child = Some(
            Command::new(&binaries().0)
                .arg("--config")
                .arg(&node.config_path)
                .env("TOKIO_WORKER_THREADS", "2")
                .stdout(Stdio::from(log.try_clone()?))
                .stderr(Stdio::from(log))
                .spawn()?,
        );
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Some(exit) = node.child.as_mut().unwrap().try_wait()? {
                return Err(format!(
                    "{} exited {exit}: {}",
                    node.label,
                    fs::read_to_string(node.dir.join("server.log")).unwrap_or_default()
                )
                .into());
            }
            if TcpStream::connect_timeout(&node.sql_addr, Duration::from_millis(100)).is_ok() {
                break;
            }
            if Instant::now() >= deadline {
                return Err(format!("SQL listener timeout: {}", node.label).into());
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
        let config = self.config(i);
        self.nodes[i].client = Some(Client::connect(&config).await?);
        Ok(())
    }
    pub fn set_key(&mut self, i: usize, key: &str) {
        self.nodes[i].key = key.into();
    }
    /// Match client-side TLS to a stopped node's configured SQL listener.
    pub fn set_client_tls(&mut self, i: usize, tls: murmur_client::TlsConfig) {
        self.nodes[i].tls = tls;
    }

    /// Change server configuration while stopped; preserves server-written table flags.
    pub fn edit_config(
        &self,
        i: usize,
        edit: impl FnOnce(&mut toml_edit::DocumentMut),
    ) -> Result<()> {
        let p = &self.nodes[i].config_path;
        let mut doc = fs::read_to_string(p)?.parse::<toml_edit::DocumentMut>()?;
        edit(&mut doc);
        fs::write(p, doc.to_string())?;
        Ok(())
    }
    /// Replace bootstrap peers while stopped; restart both sides to close old sessions.
    pub fn set_peers(&self, i: usize, peers: &[usize]) -> Result<()> {
        let mut arr = toml_edit::Array::new();
        for &p in peers {
            arr.push(self.nodes[p].repl_addr.to_string());
        }
        self.edit_config(i, |doc| {
            doc["replication"]["bootstrap"] = toml_edit::value(arr);
            doc["replication"]["membership"] = toml_edit::value(false);
        })
    }
}
impl Drop for Cluster {
    fn drop(&mut self) {
        self.nodes.clear();
        if std::thread::panicking() || self.starting {
            eprintln!(
                "RESULT: FAIL scenario={} artifacts={}",
                self.name,
                self.dir.display()
            );
        } else {
            let _ = fs::remove_dir_all(&self.dir);
            eprintln!("RESULT: PASS scenario={}", self.name);
        }
    }
}

/// Assert exact SQL rows with a bounded convergence deadline.
pub async fn assert_rows(
    cluster: &Cluster,
    nodes: &[usize],
    query: &str,
    expected: &[Vec<String>],
) {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let mut all = true;
        let mut last = Vec::new();
        for &i in nodes {
            let rows = match cluster.node(i).simple(query).await {
                Ok(rows) => rows,
                Err(e) if matches!(e.code(), Some("42703" | "42P01")) => {
                    all = false;
                    continue;
                }
                Err(e) => panic!("poll SQL state: {e}"),
            };
            if rows != expected {
                all = false;
                last.push((i, rows));
            }
        }
        if all {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "SQL convergence timed out: {query}; expected={expected:?}, got={last:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Recursively inspect durable artifacts without opening an embedded database.
pub fn assert_no_plaintext(root: &Path, needles: &[&[u8]]) {
    let mut paths = vec![root.to_path_buf()];
    while let Some(p) = paths.pop() {
        let meta = fs::symlink_metadata(&p).expect("stat audit file");
        if meta.is_dir() {
            paths.extend(fs::read_dir(&p).unwrap().map(|e| e.unwrap().path()));
        } else if meta.is_file() {
            let bytes = fs::read(&p).expect("read audit file");
            for needle in needles {
                assert!(!needle.is_empty());
                assert!(
                    !bytes.windows(needle.len()).any(|b| b == *needle),
                    "plaintext found in {}",
                    p.display()
                );
            }
        }
    }
}
