//! `murmur-admin` CLI hardening: clean failures, never panics.
use std::process::Command;

fn admin() -> Command {
    Command::new(env!("CARGO_BIN_EXE_murmur-admin"))
}

#[test]
fn dangling_flag_values_fail_with_usage_not_unknown_command() {
    for flag in ["--passphrase", "--key-hex", "--password"] {
        let result = admin().arg(flag).output().unwrap();
        assert_eq!(result.status.code(), Some(1), "{flag}");
        let error = String::from_utf8_lossy(&result.stderr);
        assert!(error.contains("needs a value"), "{flag}: {error}");
        assert!(!error.contains("unknown command"), "{flag}: {error}");
    }
}

#[test]
fn non_ascii_key_hex_is_rejected_not_a_panic() {
    let result = admin()
        .args(["status", ".", "--key-hex", "€€"])
        .output()
        .unwrap();
    assert_eq!(result.status.code(), Some(1));
    let error = String::from_utf8_lossy(&result.stderr);
    assert!(error.contains("hex"), "{error}");
    assert!(!error.contains("panicked"), "{error}");
}

#[test]
fn version_matches_package_version() {
    let result = admin().arg("--version").output().unwrap();
    assert!(result.status.success());
    let output = String::from_utf8_lossy(&result.stdout);
    assert!(
        output.contains(env!("CARGO_PKG_VERSION")),
        "version drift: {output}"
    );
}

#[cfg(unix)]
#[test]
fn non_utf8_argv_exits_cleanly() {
    use std::os::unix::ffi::OsStrExt;
    let result = admin()
        .arg("status")
        .arg(std::ffi::OsStr::from_bytes(b"\xff\xfe"))
        .output()
        .unwrap();
    assert_eq!(result.status.code(), Some(1));
    let error = String::from_utf8_lossy(&result.stderr);
    assert!(!error.contains("panicked"), "{error}");
}
