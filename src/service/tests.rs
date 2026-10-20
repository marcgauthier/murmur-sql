use super::*;
use std::{
    cell::{Cell, RefCell},
    io::Cursor,
    net::TcpListener,
};

struct Fake {
    root: tempfile::TempDir,
    exists: Cell<bool>,
    active: Cell<bool>,
    fail_permissions: Cell<bool>,
    calls: RefCell<Vec<&'static str>>,
    listener: RefCell<Option<TcpListener>>,
}
impl Fake {
    fn new() -> Self {
        Self {
            root: tempfile::tempdir_in(std::env::temp_dir().canonicalize().unwrap()).unwrap(),
            exists: Cell::new(false),
            active: Cell::new(false),
            fail_permissions: Cell::new(false),
            calls: RefCell::new(Vec::new()),
            listener: RefCell::new(None),
        }
    }
    fn source(&self) -> PathBuf {
        let directory = self.root.path().join("source");
        fs::create_dir_all(&directory).unwrap();
        let path = directory.join(format!("murmur-server{}", std::env::consts::EXE_SUFFIX));
        fs::write(&path, b"new executable").unwrap();
        path
    }
    fn install(&self) -> Installation {
        let (install, runtime) = self.defaults();
        Installation {
            install,
            runtime,
            admin: false,
        }
    }
    fn ui(&self, input: &str) -> Console<Cursor<Vec<u8>>, Vec<u8>> {
        Console {
            input: Cursor::new(input.as_bytes().to_vec()),
            output: Vec::new(),
            log: fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(self.root.path().join("actions.log"))
                .unwrap(),
        }
    }
    fn prepared(&self, running: bool) -> Installation {
        let i = self.install();
        fs::create_dir_all(&i.install).unwrap();
        fs::create_dir_all(i.config().parent().unwrap()).unwrap();
        fs::write(i.executable(), b"old executable").unwrap();
        let mut config = Config::default();
        config.storage.data_dir = i.runtime.join("data").to_str().unwrap().into();
        config.logging.dir = i.runtime.join("logs").to_str().unwrap().into();
        config.backups.dir = i.runtime.join("backups").to_str().unwrap().into();
        let path = i.config();
        fs::write(
            &path,
            format!(
                "# preserve this comment\n{}",
                toml::to_string(&config).unwrap()
            ),
        )
        .unwrap();
        write_record(self, &i).unwrap();
        self.exists.set(true);
        self.active.set(running);
        self.calls.borrow_mut().clear();
        i
    }
}
impl Platform for Fake {
    fn preflight(&self) -> Result<()> {
        Ok(())
    }
    fn record(&self) -> PathBuf {
        self.root.path().join("registry/install.json")
    }
    fn defaults(&self) -> (PathBuf, PathBuf) {
        (
            self.root.path().join("application"),
            self.root.path().join("runtime"),
        )
    }
    fn exists(&self) -> Result<bool> {
        Ok(self.exists.get())
    }
    fn running(&self) -> Result<bool> {
        Ok(self.active.get())
    }
    fn stop(&self) -> Result<()> {
        self.calls.borrow_mut().push("stop");
        self.active.set(false);
        self.listener.borrow_mut().take();
        Ok(())
    }
    fn start(&self) -> Result<()> {
        self.calls.borrow_mut().push("start");
        self.active.set(true);
        Ok(())
    }
    fn identity(&self) -> Result<()> {
        self.calls.borrow_mut().push("identity");
        Ok(())
    }
    fn register(&self, _: &Installation) -> Result<()> {
        self.calls.borrow_mut().push("register");
        self.exists.set(true);
        Ok(())
    }
    fn unregister(&self) -> Result<()> {
        self.calls.borrow_mut().push("unregister");
        self.exists.set(false);
        Ok(())
    }
    fn permissions(&self, _: &Installation, _: &Config) -> Result<()> {
        self.calls.borrow_mut().push("permissions");
        if self.fail_permissions.replace(false) {
            Err("injected permission failure".into())
        } else {
            Ok(())
        }
    }
    fn protect_record(&self) -> Result<()> {
        Ok(())
    }
    fn validate_installed(&self, install: &Installation) -> Result<()> {
        self.calls.borrow_mut().push("validate");
        Config::load(install.config())?;
        Ok(())
    }
}

#[test]
fn fresh_install_keeps_database_directory_empty_and_configuration_separate() {
    let p = Fake::new();
    let source = p.source();
    let mut ui = p.ui("\n\n\n\n\n\n\nn\n");
    run(&p, &mut ui, "install", &source).unwrap();
    let i = p.install();
    assert_eq!(fs::read(i.executable()).unwrap(), b"new executable");
    let config = Config::load(i.config()).unwrap();
    assert!(config.auth.users.is_empty());
    assert_eq!(config.server.bind, "127.0.0.1:5432");
    assert_eq!(fs::read_dir(&config.storage.data_dir).unwrap().count(), 0);
    assert!(p.exists.get());
    assert!(!p.active.get());
    assert_eq!(
        *p.calls.borrow(),
        ["identity", "register", "permissions", "validate"]
    );
}
#[test]
fn closed_input_before_a_phase_creates_no_installation() {
    let p = Fake::new();
    let source = p.source();
    let mut ui = p.ui("\n\n\n");
    assert!(run(&p, &mut ui, "install", &source).is_err());
    assert!(!p.install().install.exists());
    assert!(p.calls.borrow().is_empty());
}
#[test]
fn declining_service_stop_does_not_stop_or_replace_the_service() {
    let p = Fake::new();
    let source = p.source();
    let i = p.prepared(true);
    let original = fs::read(i.config()).unwrap();
    let mut ui = p.ui("\n\n\n\nn\n");
    assert!(run(&p, &mut ui, "install", &source).is_err());
    assert_eq!(*p.calls.borrow(), ["identity"]);
    assert!(p.active.get());
    assert_eq!(fs::read(i.executable()).unwrap(), b"old executable");
    assert_eq!(fs::read(i.config()).unwrap(), original);
}
#[test]
fn stopped_upgrade_preserves_configuration_byte_for_byte() {
    let p = Fake::new();
    let source = p.source();
    let i = p.prepared(false);
    let config = fs::read(i.config()).unwrap();
    let mut ui = p.ui("\n\n\n\n\n\nn\n");
    run(&p, &mut ui, "install", &source).unwrap();
    assert_eq!(fs::read(i.config()).unwrap(), config);
    assert_eq!(fs::read(i.executable()).unwrap(), b"new executable");
    assert!(!p.active.get());
    assert!(!p.calls.borrow().contains(&"stop"));
}
#[test]
fn failed_upgrade_restores_previous_binary_registration_and_running_state() {
    let p = Fake::new();
    let source = p.source();
    let i = p.prepared(true);
    let config = fs::read(i.config()).unwrap();
    p.fail_permissions.set(true);
    let mut ui = p.ui("\n\n\n\ny\n\n\n\n");
    let error = run(&p, &mut ui, "install", &source).unwrap_err();
    assert!(error.to_string().contains("permission failure"));
    assert_eq!(fs::read(i.executable()).unwrap(), b"old executable");
    assert_eq!(fs::read(i.config()).unwrap(), config);
    assert!(p.exists.get());
    assert!(p.active.get());
    assert_eq!(
        *p.calls.borrow(),
        [
            "identity",
            "stop",
            "register",
            "permissions",
            "register",
            "permissions",
            "start"
        ]
    );
}
#[test]
fn retained_fresh_setup_can_be_retried_after_cancellation() {
    let p = Fake::new();
    let source = p.source();
    let mut first = p.ui("\n\n\n\n");
    assert!(run(&p, &mut first, "install", &source).is_err());
    assert!(p.record().exists());
    let mut second = p.ui("\n\n\n\n\n\nn\n");
    run(&p, &mut second, "install", &source).unwrap();
    assert!(p.exists.get());
}
#[test]
fn uninstall_default_preserves_binaries_data_configuration_and_metadata() {
    let p = Fake::new();
    let source = p.source();
    let i = p.prepared(true);
    let data = i.runtime.join("data");
    fs::create_dir_all(&data).unwrap();
    fs::write(data.join("sentinel"), b"data").unwrap();
    let mut ui = p.ui("y\n\n\n\n");
    run(&p, &mut ui, "uninstall", &source).unwrap();
    assert_eq!(*p.calls.borrow(), ["stop", "unregister"]);
    assert!(!p.exists.get());
    assert!(i.executable().exists());
    assert!(i.config().exists());
    assert!(data.join("sentinel").exists());
    assert!(p.record().exists());
}
#[test]
fn uninstall_deletion_requires_independent_confirmations_and_preserves_unrelated_files() {
    let p = Fake::new();
    let source = p.source();
    let i = p.prepared(false);
    fs::write(i.install.join("custom.txt"), b"keep").unwrap();
    let mut ui = p.ui("y\n\nDELETE\n\n");
    run(&p, &mut ui, "uninstall", &source).unwrap();
    assert!(!i.executable().exists());
    assert!(i.config().exists());
    assert!(i.install.join("custom.txt").exists());
    assert!(p.record().exists());
}
#[test]
fn confirmed_runtime_deletion_preserves_application_and_external_credentials() {
    let p = Fake::new();
    let source = p.source();
    let i = p.prepared(false);
    let external = p.root.path().join("credentials/key.pem");
    fs::create_dir_all(external.parent().unwrap()).unwrap();
    fs::write(&external, b"key").unwrap();
    let mut config = Config::load(i.config()).unwrap();
    config.server.tls.key_file = Some(external.to_str().unwrap().into());
    fs::write(i.config(), toml::to_string(&config).unwrap()).unwrap();
    let mut ui = p.ui("y\n\n\nDELETE\n");
    run(&p, &mut ui, "uninstall", &source).unwrap();
    assert!(external.exists());
    assert!(!i.config().exists());
    assert!(i.executable().exists());
    // Reinstall after deleting runtime settings recreates only configuration/folders.
    let mut reinstall = p.ui("\n\n\n\n\n\n\nn\n");
    run(&p, &mut reinstall, "install", &source).unwrap();
    assert!(i.config().exists());
}
#[test]
fn unsafe_and_overlapping_runtime_paths_are_rejected() {
    for path in ["/", "/usr", "/usr/bin/app", "/var/../etc/app"] {
        assert!(safe_directory(Path::new(path)).is_err());
    }
    let p = Fake::new();
    let i = p.install();
    let mut config = Config::default();
    config.storage.data_dir = i.install.to_str().unwrap().into();
    assert!(writable_paths(&i, &config).is_err());
}
#[cfg(unix)]
#[test]
fn linked_targets_are_rejected_before_permissions_or_deletion() {
    let p = Fake::new();
    let i = p.install();
    fs::create_dir_all(&i.runtime).unwrap();
    std::os::unix::fs::symlink(p.root.path(), i.runtime.join("data")).unwrap();
    assert!(reject_tree_links(&i.runtime).is_err());
    assert!(safe_directory(&i.runtime.join("data")).is_err());
}
#[test]
fn replacement_restores_old_contents_and_same_file_is_not_replaced() {
    let p = Fake::new();
    let source = p.source();
    let target = p.root.path().join("target");
    fs::write(&target, b"old").unwrap();
    let replacement = Replacement::copy(&source, &target).unwrap();
    assert_eq!(fs::read(&target).unwrap(), b"new executable");
    replacement.restore().unwrap();
    assert_eq!(fs::read(&target).unwrap(), b"old");
    let same = Replacement::copy(&target, &target).unwrap();
    assert!(!same.changed);
}
#[test]
fn readiness_requires_a_running_service_and_reachable_listener() {
    let p = Fake::new();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    assert!(wait_listener(&p, &address).is_err());
    p.active.set(true);
    wait_listener(&p, &address).unwrap();
}

#[test]
fn relative_runtime_paths_resolve_from_service_working_directory() {
    let p = Fake::new();
    let i = p.install();
    fs::create_dir_all(&i.install).unwrap();
    let mut config = Config::default();
    config.storage.data_dir = "../runtime/data".into();
    config.logging.dir = "../runtime/logs".into();
    config.backups.dir = "../runtime/backups".into();
    let paths = writable_paths(&i, &config).unwrap();
    assert!(paths.contains(&i.runtime.join("data")));
    assert!(paths.contains(&i.runtime.join("logs")));
}
#[cfg(windows)]
#[test]
fn windows_path_aliases_cannot_bypass_directory_separation() {
    let i = Installation {
        install: r"C:\Murmur\Application".into(),
        runtime: r"c:\murmur\APPLICATION\data".into(),
        admin: false,
    };
    assert!(i.validate().is_err());
    assert!(path_within(
        Path::new("C:\\Murmur\\Σ\\data"),
        Path::new("c:\\murmur\\ς")
    ));
    assert!(safe_directory(Path::new(r"C:\Murmur\Application.")).is_err());
    assert!(safe_directory(Path::new(r"\\?\C:\Murmur\Application")).is_err());
}

#[test]
fn uninstall_refuses_runtime_deletion_when_credentials_would_be_deleted() {
    let p = Fake::new();
    let source = p.source();
    let i = p.prepared(false);
    let key = i.config().parent().unwrap().join("server.key");
    fs::write(&key, b"private fixture").unwrap();
    let mut config = Config::load(i.config()).unwrap();
    config.server.tls.key_file = Some(key.to_str().unwrap().into());
    fs::write(i.config(), toml::to_string(&config).unwrap()).unwrap();
    let mut ui = p.ui("y\n\nDELETE\nDELETE\n");
    let error = run(&p, &mut ui, "uninstall", &source).unwrap_err();
    assert!(error.to_string().contains("credential"));
    assert!(i.executable().exists());
    assert!(i.config().exists());
    assert!(key.exists());
}
#[test]
fn unreadable_configuration_prevents_destructive_uninstall() {
    let p = Fake::new();
    let source = p.source();
    let i = p.prepared(false);
    fs::write(i.config(), "not valid TOML").unwrap();
    let mut ui = p.ui("y\n\nDELETE\nDELETE\n");
    let error = run(&p, &mut ui, "uninstall", &source).unwrap_err();
    assert!(error.to_string().contains("unreadable configuration"));
    assert!(i.executable().exists());
    assert!(i.config().exists());
}
