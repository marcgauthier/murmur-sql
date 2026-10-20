//! Installer entry points must be independent of normal configuration loading.
use std::process::Command;

#[test]
fn help_lists_interactive_service_commands() {
    let result = Command::new(env!("CARGO_BIN_EXE_murmur-server"))
        .arg("--help")
        .output()
        .unwrap();
    assert!(result.status.success());
    let output = String::from_utf8(result.stdout).unwrap();
    assert!(output.contains("/install"));
    assert!(output.contains("/uninstall"));
}

#[test]
fn installer_commands_reject_server_options_before_touching_the_system() {
    for args in [
        ["/install", "--validate-config"],
        ["--config", "/uninstall"],
        ["/install", "/uninstall"],
    ] {
        let result = Command::new(env!("CARGO_BIN_EXE_murmur-server"))
            .args(args)
            .output()
            .unwrap();
        assert_eq!(result.status.code(), Some(1));
        let error = String::from_utf8_lossy(&result.stderr);
        assert!(error.contains("must be used alone"), "{error}");
        assert!(!error.contains("cannot load"));
    }
}
