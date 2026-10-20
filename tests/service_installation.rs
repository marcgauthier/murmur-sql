//! Elevated end-to-end deployment test. Run ONLY in a disposable OS instance.
#![cfg(any(target_os = "linux", windows))]
use std::{
    fs,
    io::Write,
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

fn invoke(binary: &Path, action: &str, answers: &str, cwd: &Path) -> std::process::Output {
    let mut child = Command::new(binary)
        .arg(action)
        .current_dir(cwd)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(answers.as_bytes())
        .unwrap();
    child.wait_with_output().unwrap()
}
fn ok(output: std::process::Output) {
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
struct Cleanup {
    binary: PathBuf,
    cwd: PathBuf,
}
impl Drop for Cleanup {
    fn drop(&mut self) {
        let output = invoke(
            &self.binary,
            "/uninstall",
            "y\n\nDELETE\nDELETE\n",
            &self.cwd,
        );
        if !output.status.success() {
            eprintln!(
                "Installer cleanup: {} {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
    }
}

#[test]
#[ignore = "requires explicit opt-in and Administrator/root in a disposable Windows/systemd machine"]
fn install_upgrade_restart_and_uninstall_without_creating_a_database() {
    assert_eq!(
        std::env::var("MURMUR_SERVICE_TESTS").as_deref(),
        Ok("1"),
        "set MURMUR_SERVICE_TESTS=1 only inside a disposable OS instance"
    );
    #[cfg(target_os = "linux")]
    {
        assert_eq!(
            Command::new("id").arg("-u").output().unwrap().stdout,
            b"0\n"
        );
        assert!(
            !Path::new("/etc/murmur-sql/install.json").exists(),
            "never replace a pre-existing installation in this test"
        );
        let output = Command::new("systemctl")
            .args(["show", "murmur-sql.service", "-p", "LoadState", "--value"])
            .output()
            .unwrap();
        assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), "not-found");
    }
    #[cfg(windows)]
    {
        let record = PathBuf::from(std::env::var_os("ProgramData").unwrap())
            .join("MurmurSQL-Installer/install.json");
        assert!(
            !record.exists(),
            "never replace a pre-existing installation in this test"
        );
        let output = Command::new("sc.exe")
            .args(["query", "MurmurSQL"])
            .output()
            .unwrap();
        assert!(
            String::from_utf8_lossy(&output.stdout).contains("1060"),
            "service must not already exist"
        );
    }
    // Linux PrivateTmp isolates /tmp and /var/tmp; use a disposable /opt subtree.
    #[cfg(target_os = "linux")]
    let scratch = tempfile::Builder::new()
        .prefix("murmur-install-test-")
        .tempdir_in("/opt")
        .unwrap();
    #[cfg(windows)]
    let scratch = tempfile::Builder::new()
        .prefix("MurmurInstallTest-")
        .tempdir_in(std::env::var_os("ProgramData").unwrap())
        .unwrap();
    let app = scratch.path().join("application with spaces");
    let state = scratch.path().join("runtime with spaces");
    let cwd = scratch.path().join("launcher");
    fs::create_dir(&cwd).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(scratch.path(), fs::Permissions::from_mode(0o755)).unwrap();
    }
    let binary = PathBuf::from(env!("CARGO_BIN_EXE_murmur-server"));
    let port = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let answers = format!(
        "{}\n{}\n127.0.0.1:{port}\n\n\n\n\ny\n\n",
        app.display(),
        state.display()
    );
    // Install, start, and validate without an authenticated database session.
    let result = invoke(&binary, "/install", &answers, &cwd);
    let _cleanup = Cleanup {
        binary: binary.clone(),
        cwd: cwd.clone(),
    };
    ok(result);
    assert!(TcpStream::connect(("127.0.0.1", port)).is_ok());
    #[cfg(target_os = "linux")]
    {
        let identity = Command::new("systemctl")
            .args(["show", "murmur-sql.service", "-p", "User", "--value"])
            .output()
            .unwrap();
        assert_eq!(String::from_utf8_lossy(&identity.stdout).trim(), "murmur");
    }
    #[cfg(windows)]
    {
        let identity = Command::new("sc.exe")
            .args(["qc", "MurmurSQL"])
            .output()
            .unwrap();
        assert!(String::from_utf8_lossy(&identity.stdout).contains("NT SERVICE\\MurmurSQL"));
    }
    let config = state.join("config/config.toml");
    assert_eq!(
        fs::read_dir(state.join("data")).unwrap().count(),
        0,
        "installation/startup must not create a database"
    );
    let original = fs::read(&config).unwrap();
    let installed = app.join(format!("murmur-server{}", std::env::consts::EXE_SUFFIX));
    // Running upgrade from a separate source; automatically restart the prior service.
    ok(invoke(&binary, "/install", "\n\n\n\ny\n\n\n\n\n", &cwd));
    assert_eq!(fs::read(&config).unwrap(), original);
    assert!(TcpStream::connect(("127.0.0.1", port)).is_ok());
    // Cancellation cannot stop the running service or replace its files.
    let denied = invoke(&binary, "/install", "\n\n\n\nn\n", &cwd);
    assert!(!denied.status.success());
    assert!(TcpStream::connect(("127.0.0.1", port)).is_ok());
    // First uninstall preserves everything and allows a stopped reinstall.
    ok(invoke(&binary, "/uninstall", "y\n\n\n\n", &cwd));
    assert!(installed.is_file());
    assert!(config.is_file());
    assert!(TcpStream::connect(("127.0.0.1", port)).is_err());
    ok(invoke(&binary, "/install", "\n\n\n\n\n\nn\n", &cwd));
    assert!(TcpStream::connect(("127.0.0.1", port)).is_err());
    assert_eq!(fs::read(&config).unwrap(), original);
    // Explicitly confirm each deletion group.
    ok(invoke(
        &installed,
        "/uninstall",
        "y\n\nDELETE\nDELETE\n",
        &cwd,
    ));
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    while installed.exists() && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    assert!(!installed.exists());
    assert!(!config.exists());
    assert!(cwd.join("installation.log").is_file());
    assert!(cwd.join("uninstallation.log").is_file());
    std::mem::forget(_cleanup); // All managed files/services were removed above.
}
