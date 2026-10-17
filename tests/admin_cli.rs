use sqlite_session::{Database, Migration, OpenMode, Options};
use std::process::Command;
use tempfile::tempdir;

fn admin_bin() -> std::path::PathBuf {
    let mut path = std::env::current_exe().expect("test binary path");
    path.pop(); // remove test exe name
    if path.ends_with("deps") {
        path.pop();
    }
    path.push("murmur-admin");
    path
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        use std::fmt::Write;
        let _ = write!(s, "{:02x}", b);
    }
    s
}

fn create_sample_db(path: &std::path::Path, key: [u8; 32]) {
    let opts = Options {
        mode: OpenMode::CreateNew,
        ..Options::default()
    };
    let db = Database::open(path, key, opts).expect("create sample db");
    db.migrate(Migration::new(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, score INTEGER);"
    )).expect("migrate");
    db.transaction(|s| {
        s.execute("INSERT INTO users (id, name, score) VALUES (1, 'alice', 100);", [])?;
        s.execute("INSERT INTO users (id, name, score) VALUES (2, 'bob', 250);", [])?;
        Ok(())
    }).expect("transaction");
    db.flush().expect("flush");
    db.close().expect("close db");
}

#[test]
fn test_cli_help_and_version() {
    let bin = admin_bin();
    let out = Command::new(&bin).arg("--help").output().expect("run --help");
    assert!(out.status.success());
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("Murmur Operational Admin CLI"));
    assert!(stdout.contains("status"));
    assert!(stdout.contains("inspect"));
    assert!(stdout.contains("verify"));
    assert!(stdout.contains("backup"));
    assert!(stdout.contains("restore"));
    assert!(stdout.contains("keys"));

    let out_v = Command::new(&bin).arg("--version").output().expect("run --version");
    assert!(out_v.status.success());
    assert!(String::from_utf8_lossy(&out_v.stdout).contains("0.1.0"));
}

#[test]
fn test_cli_lifecycle_status_inspect_keys_verify_backup_restore() {
    let bin = admin_bin();
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("main_db");
    let key = [42u8; 32];
    let key_hex = hex_encode(&key);

    create_sample_db(&db_path, key);

    // 1. Status command (text and json)
    let out = Command::new(&bin)
        .args(["status", db_path.to_str().unwrap(), &format!("--key-hex={key_hex}")])
        .output()
        .expect("status");
    assert!(out.status.success(), "status failed: {}", String::from_utf8_lossy(&out.stderr));
    let txt = String::from_utf8_lossy(&out.stdout);
    assert!(txt.contains("Murmur Database Status"));
    assert!(txt.contains("SQLite In-Memory Engine"));
    assert!(txt.contains("Spool Persistence Engine"));

    let out_json = Command::new(&bin)
        .args(["status", db_path.to_str().unwrap(), &format!("--key-hex={key_hex}"), "--json"])
        .output()
        .expect("status json");
    assert!(out_json.status.success());
    let json_val: serde_json::Value = serde_json::from_slice(&out_json.stdout).expect("parse status json");
    assert_eq!(json_val["data_dir"], db_path.to_str().unwrap());
    assert!(json_val["sqlite"]["total_rows"].as_u64().unwrap() >= 2);

    // 2. Inspect command
    let out_inspect = Command::new(&bin)
        .args(["inspect", db_path.to_str().unwrap(), &format!("--key-hex={key_hex}"), "--json"])
        .output()
        .expect("inspect");
    assert!(out_inspect.status.success(), "inspect failed: {}", String::from_utf8_lossy(&out_inspect.stderr));
    let inspect_val: serde_json::Value = serde_json::from_slice(&out_inspect.stdout).expect("parse inspect json");
    assert_eq!(inspect_val["table_count"], 1);
    assert_eq!(inspect_val["tables"][0]["name"], "users");

    // 3. Keys command
    let out_keys = Command::new(&bin)
        .args(["keys", db_path.to_str().unwrap(), &format!("--key-hex={key_hex}"), "--json"])
        .output()
        .expect("keys");
    assert!(out_keys.status.success(), "keys failed: {}", String::from_utf8_lossy(&out_keys.stderr));
    let keys_val: serde_json::Value = serde_json::from_slice(&out_keys.stdout).expect("parse keys json");
    assert_eq!(keys_val["encrypted"], true);
    assert!(keys_val["data_keys_count"].as_u64().unwrap() >= 1);

    // 4. Verify database directory
    let out_verify = Command::new(&bin)
        .args(["verify", db_path.to_str().unwrap(), &format!("--key-hex={key_hex}"), "--json"])
        .output()
        .expect("verify db");
    assert!(out_verify.status.success(), "verify db failed: {}", String::from_utf8_lossy(&out_verify.stderr));
    let verify_val: serde_json::Value = serde_json::from_slice(&out_verify.stdout).expect("parse verify json");
    assert_eq!(verify_val["valid"], true);

    // 5. Backup database
    let backup_path = dir.path().join("backup_dir");
    let out_backup = Command::new(&bin)
        .args([
            "backup",
            db_path.to_str().unwrap(),
            backup_path.to_str().unwrap(),
            &format!("--key-hex={key_hex}"),
            "--json",
        ])
        .output()
        .expect("backup");
    assert!(out_backup.status.success(), "backup failed: {}", String::from_utf8_lossy(&out_backup.stderr));
    let backup_val: serde_json::Value = serde_json::from_slice(&out_backup.stdout).expect("parse backup json");
    assert_eq!(backup_val["success"], true);

    // 6. Verify backup directory
    let out_verify_bkp = Command::new(&bin)
        .args([
            "verify",
            backup_path.to_str().unwrap(),
            &format!("--key-hex={key_hex}"),
            "--backup",
            "--json",
        ])
        .output()
        .expect("verify backup");
    assert!(out_verify_bkp.status.success(), "verify bkp failed: {}", String::from_utf8_lossy(&out_verify_bkp.stderr));
    let verify_bkp_val: serde_json::Value = serde_json::from_slice(&out_verify_bkp.stdout).expect("parse verify bkp json");
    assert_eq!(verify_bkp_val["valid"], true);
    assert_eq!(verify_bkp_val["mode"], "backup");

    // 7. Restore database
    let restored_path = dir.path().join("restored_db");
    let out_restore = Command::new(&bin)
        .args([
            "restore",
            backup_path.to_str().unwrap(),
            restored_path.to_str().unwrap(),
            &format!("--key-hex={key_hex}"),
            "--fresh-node-id",
            "--json",
        ])
        .output()
        .expect("restore");
    assert!(out_restore.status.success(), "restore failed: {}", String::from_utf8_lossy(&out_restore.stderr));
    let restore_val: serde_json::Value = serde_json::from_slice(&out_restore.stdout).expect("parse restore json");
    assert_eq!(restore_val["success"], true);
    assert!(restore_val["new_node_id"].is_string());

    // 8. Verify the restored database opens and queries correctly
    let db_restored = Database::open(&restored_path, key, Options {
        mode: OpenMode::Existing,
        ..Options::default()
    }).expect("open restored db");
    let count: i64 = db_restored.read(|s| {
        s.query_row("SELECT count(*) FROM users", [], |r| r.get(0))
            .map_err(sqlite_session::Error::Sql)
    }).expect("query count");
    assert_eq!(count, 2);
    db_restored.close().expect("close restored db");
}

#[test]
fn hash_password_prints_argon2_hash() {
    let out = Command::new(admin_bin())
        .args(["hash-password", "--password", "s3cret"])
        .output()
        .expect("hash-password");
    assert!(out.status.success(), "stderr: {}", String::from_utf8_lossy(&out.stderr));
    let hash = String::from_utf8_lossy(&out.stdout);
    assert!(hash.trim_start().starts_with("$argon2id$"), "output: {hash}");
    assert!(sqlite_session::server::auth::verify_password(hash.trim(), b"s3cret"));
    assert!(!sqlite_session::server::auth::verify_password(hash.trim(), b"other"));

    // Empty passwords are rejected rather than hashed.
    let out = Command::new(admin_bin())
        .args(["hash-password", "--password", ""])
        .output()
        .expect("hash-password empty");
    assert!(!out.status.success());
}
