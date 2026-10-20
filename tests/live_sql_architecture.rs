//! Guard the live-test boundary: processes and SQL, never embedded data access.
use std::{collections::BTreeSet, fs, path::Path};

#[test]
fn every_live_scenario_uses_a_wire_harness_without_embedded_database_apis() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests-live");
    let runner = fs::read_to_string(root.join("run.sh")).unwrap();
    let catalog = runner
        .split("ALL_SCENARIOS=\"")
        .nth(1)
        .unwrap()
        .split('"')
        .next()
        .unwrap();
    let listed: BTreeSet<_> = catalog.split_whitespace().map(str::to_owned).collect();
    let mut found = BTreeSet::new();
    for entry in fs::read_dir(&root).unwrap() {
        let directory = entry.unwrap().path();
        let manifest = directory.join("Cargo.toml");
        if !directory.join("tests").is_dir() || !manifest.is_file() {
            continue;
        }
        let name = directory.file_name().unwrap().to_str().unwrap().to_owned();
        found.insert(name.clone());
        let package: toml::Value = toml::from_str(&fs::read_to_string(&manifest).unwrap()).unwrap();
        assert!(
            package["dependencies"].get("murmur-live-harness").is_some(),
            "{name} lacks a live SQL harness"
        );
        for section in ["dependencies", "dev-dependencies", "build-dependencies"] {
            if let Some(dependencies) = package.get(section).and_then(toml::Value::as_table) {
                for (alias, declaration) in dependencies {
                    let dependency = declaration
                        .get("package")
                        .and_then(toml::Value::as_str)
                        .unwrap_or(alias);
                    assert!(
                        !["sqlite-session", "murmur", "murmur-spool", "rusqlite"]
                            .contains(&dependency),
                        "{name} links embedded dependency {dependency}"
                    );
                }
            }
        }
        for source in fs::read_dir(directory.join("tests")).unwrap() {
            let source = source.unwrap().path();
            if source.extension().and_then(|s| s.to_str()) != Some("rs") {
                continue;
            }
            let text = fs::read_to_string(&source).unwrap();
            for forbidden in [
                "sqlite_session",
                "rusqlite::",
                ".transaction(",
                ".migrate(",
                "Database::open",
                "open_in_memory(",
                ".replication(",
                ".scheduler(",
                "tokio_postgres::connect(",
                ".set_table_replicated(",
                ".flush(",
                ".backup_save(",
                ".backup_list(",
                ".backup_upload(",
                ".backup_download(",
                ".restore(",
                ".rotate_key(",
                ".upload_file(",
                ".download_file(",
                ".list_files(",
            ] {
                assert!(
                    !text.contains(forbidden),
                    "{} still uses {forbidden}",
                    source.display()
                );
            }
        }
    }
    assert_eq!(
        found, listed,
        "runner must cover every live scenario and fail on missing packages"
    );
    for harness in ["harness", "sql-harness"] {
        let manifest: toml::Value =
            toml::from_str(&fs::read_to_string(root.join(harness).join("Cargo.toml")).unwrap())
                .unwrap();
        assert!(
            manifest["dependencies"].get("murmur-client").is_some(),
            "{harness} bypasses clients/rust"
        );
        assert!(
            manifest["dependencies"].get("tokio-postgres").is_none(),
            "{harness} declares a second driver connection path"
        );
    }
}
