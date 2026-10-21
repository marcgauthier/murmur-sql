//! Murmur Operational Admin CLI (`murmur-admin`).
//!
//! Subcommands:
//!   status  <data-dir>                      Database status and storage/SQLite metrics
//!   inspect <data-dir>                      Low-level storage inspector (segments, schema, disk size)
//!   verify  <data-dir>                      Replay & verify durable state or backup directory
//!   backup  <data-dir> <dest-dir>           Consistent online hot backup
//!   restore <backup-dir> <dest-dir>         Restore backup into clean destination directory
//!   keys    <data-dir>                      Inspect key registry and active data keys
//!   hash-password                           Argon2 hash for auth.users (reads stdin without --password)
//!
//! Options:
//!   --json                                  Output structured JSON format
//!   --passphrase=<str>                      Passphrase for database encryption
//!   --password=<str>                        Password for hash-password (stdin when absent)
//!   --key-hex=<hex>                         32-byte (64 hex char) raw encryption key
//!   --backup                                Verify backup directory mode for `verify`
//!   --fresh-node-id[=<hex>]                 Assign a new node identity on restore
//!   --fresh-db-id[=<hex>]                   Assign a new database identity on restore
//!   -v, --version                           Print version
//!   -h, --help                              Print help

use serde::Serialize;
use murmur::{Database, OpenMode, Options, RestoreOptions};
use std::env;
use std::fs;
use std::path::Path;
use std::process::ExitCode;
use std::time::UNIX_EPOCH;

const VERSION: &str = env!("CARGO_PKG_VERSION");
const DEFAULT_KEY: &[u8; 32] = b"0123456789abcdef0123456789abcdef";

#[derive(Debug, Clone, Default)]
struct GlobalOpts {
    json: bool,
    passphrase: Option<String>,
    password: Option<String>,
    key_hex: Option<String>,
    is_backup: bool,
    fresh_node_id: Option<[u8; 16]>,
    fresh_db_id: Option<[u8; 16]>,
}

impl GlobalOpts {
    fn resolve_key(&self) -> Result<[u8; 32], String> {
        if let Some(ref hex_str) = self.key_hex {
            let bytes = hex_decode(hex_str)?;
            if bytes.len() != 32 {
                return Err("key-hex must be exactly 32 bytes (64 hex characters)".into());
            }
            let mut key = [0u8; 32];
            key.copy_from_slice(&bytes);
            return Ok(key);
        }
        if let Some(ref pass) = self.passphrase {
            return Ok(*blake3::hash(pass.as_bytes()).as_bytes());
        }
        eprintln!(
            "warning: no --key-hex or --passphrase given; using the built-in development key"
        );
        Ok(*DEFAULT_KEY)
    }
}

fn hex_decode(s: &str) -> Result<Vec<u8>, String> {
    let s = s.trim();
    if s.len() % 2 != 0 {
        return Err("invalid hex string length".into());
    }
    // Byte-slicing below is only safe on ASCII: a multi-byte character
    // with even total length (e.g. "€€") would panic on a non-boundary.
    if !s.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err("invalid hex string: expected hex digits".into());
    }
    let mut bytes = Vec::with_capacity(s.len() / 2);
    for i in (0..s.len()).step_by(2) {
        let byte = u8::from_str_radix(&s[i..i + 2], 16)
            .map_err(|e| format!("invalid hex byte {}: {}", &s[i..i + 2], e))?;
        bytes.push(byte);
    }
    Ok(bytes)
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        use std::fmt::Write;
        let _ = write!(s, "{:02x}", b);
    }
    s
}

fn parse_id_16(s: &str) -> Result<[u8; 16], String> {
    let bytes = hex_decode(s)?;
    if bytes.len() != 16 {
        return Err(format!("expected 16-byte hex (32 chars), got {} bytes", bytes.len()));
    }
    let mut id = [0u8; 16];
    id.copy_from_slice(&bytes);
    Ok(id)
}

fn print_usage() {
    println!(
        r#"Murmur Operational Admin CLI (v{})

Usage:
  murmur-admin <command> [options] [arguments]

Commands:
  status   <data-dir>                      Database status and storage/SQLite metrics
  inspect  <data-dir>                      Low-level storage inspector (segments, schema, disk size)
  verify   <data-dir>                      Replay & verify durable state or backup directory
  backup   <data-dir> <dest-dir>           Consistent online hot backup
  restore  <backup-dir> <dest-dir>         Restore backup into clean destination directory
  keys     <data-dir>                      Inspect key registry and active data keys
  hash-password                           Argon2 hash for auth.users (reads stdin without --password)

Options:
  --json                                  Output structured JSON format
  --passphrase=<str>                      Passphrase for database encryption
  --password=<str>                        Password for hash-password (stdin when absent)
  --key-hex=<hex>                         32-byte (64 hex char) raw encryption key
  --backup                                Treat target directory as backup archive in `verify`
  --fresh-node-id[=<hex>]                 Assign new node identity on restore
  --fresh-db-id[=<hex>]                   Assign new database identity on restore
  -v, --version                           Print version
  -h, --help                              Print help
"#,
        VERSION
    );
}

fn calculate_dir_size(path: &Path) -> (u64, usize) {
    let mut total_bytes = 0;
    let mut file_count = 0;
    if let Ok(entries) = fs::read_dir(path) {
        for entry in entries.flatten() {
            let p = entry.path();
            if p.is_dir() {
                let (sub_bytes, sub_count) = calculate_dir_size(&p);
                total_bytes += sub_bytes;
                file_count += sub_count;
            } else if let Ok(meta) = p.metadata() {
                total_bytes += meta.len();
                file_count += 1;
            }
        }
    }
    (total_bytes, file_count)
}

fn open_existing_db(path: &Path, key: [u8; 32]) -> Result<Database, String> {
    let opts = Options {
        mode: OpenMode::Existing,
        ..Options::default()
    };
    Database::open(path, key, opts).map_err(|e| format!("failed to open database: {e}"))
}

// ---------------------------------------------------------------------------
// status command
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct StatusReport {
    data_dir: String,
    database_id: String,
    node_id: String,
    committed_seq: u64,
    durable_seq: u64,
    pending_bytes: usize,
    checkpoint_pause_ms: u64,
    closed: bool,
    storage_error: Option<String>,
    sqlite: Option<SqliteReport>,
    spool: Option<SpoolReport>,
}

#[derive(Serialize)]
struct SqliteReport {
    memory_used_bytes: i64,
    memory_highwater_bytes: i64,
    page_size: i64,
    page_count: i64,
    database_size_bytes: i64,
    table_count: usize,
    index_count: usize,
    total_rows: u64,
    session_changeset_bytes: usize,
}

#[derive(Serialize)]
struct SpoolReport {
    keys: u64,
    segments: u64,
    disk_bytes: u64,
    live_bytes: u64,
    live_records: u64,
    dead_records: u64,
    blocks_written: u64,
    compression_ratio: f64,
    compactions: u64,
    commits: u64,
}

fn cmd_status(data_dir: &str, opts: &GlobalOpts) -> Result<(), String> {
    let path = Path::new(data_dir);
    if !path.exists() {
        return Err(format!("directory does not exist: {data_dir}"));
    }
    let key = opts.resolve_key()?;
    let db = open_existing_db(path, key)?;

    let status = db.status();
    let sqlite_metrics = db.sqlite_metrics().ok();
    let spool_metrics = db.spool_metrics();

    let db_id = hex_encode(&db.database_id());
    let node_id = hex_encode(&db.node_id());

    let report = StatusReport {
        data_dir: data_dir.to_string(),
        database_id: db_id.clone(),
        node_id: node_id.clone(),
        committed_seq: status.committed,
        durable_seq: status.durable,
        pending_bytes: status.pending_bytes,
        checkpoint_pause_ms: status.checkpoint_pause.as_millis() as u64,
        closed: status.closed,
        storage_error: status.storage_error,
        sqlite: sqlite_metrics.map(|s| SqliteReport {
            memory_used_bytes: s.memory_used_bytes,
            memory_highwater_bytes: s.memory_highwater_bytes,
            page_size: s.page_size,
            page_count: s.page_count,
            database_size_bytes: s.database_size_bytes,
            table_count: s.table_count,
            index_count: s.index_count,
            total_rows: s.total_rows,
            session_changeset_bytes: s.session_changeset_bytes,
        }),
        spool: Some(SpoolReport {
            keys: spool_metrics.keys,
            segments: spool_metrics.segments,
            disk_bytes: spool_metrics.disk_bytes,
            live_bytes: spool_metrics.live_bytes,
            live_records: spool_metrics.live_records,
            dead_records: spool_metrics.dead_records,
            blocks_written: spool_metrics.blocks_written,
            compression_ratio: spool_metrics.compression_ratio,
            compactions: spool_metrics.compactions,
            commits: spool_metrics.commits,
        }),
    };

    let _ = db.close();

    if opts.json {
        println!("{}", serde_json::to_string_pretty(&report).map_err(|e| e.to_string())?);
        return Ok(());
    }

    println!("=== Murmur Database Status ===");
    println!("Data Directory:      {}", report.data_dir);
    println!("Database ID:         {}", report.database_id);
    println!("Node ID:             {}", report.node_id);
    println!("Committed Sequence:  {}", report.committed_seq);
    println!("Durable Sequence:    {}", report.durable_seq);
    println!("Pending Bytes:       {}", report.pending_bytes);
    if let Some(ref err) = report.storage_error {
        println!("Storage Error:       {}", err);
    }

    if let Some(ref s) = report.sqlite {
        println!("\n--- SQLite In-Memory Engine ---");
        println!("Memory Used:         {} bytes (highwater: {} bytes)", s.memory_used_bytes, s.memory_highwater_bytes);
        println!("Page Count / Size:   {} pages @ {} bytes", s.page_count, s.page_size);
        println!("Database Size:       {} bytes", s.database_size_bytes);
        println!("Tables / Indexes:    {} tables, {} indexes", s.table_count, s.index_count);
        println!("Total Rows:          {}", s.total_rows);
        println!("Session Changeset:   {} bytes", s.session_changeset_bytes);
    }

    if let Some(ref sp) = report.spool {
        println!("\n--- Spool Persistence Engine ---");
        println!("Keys Tracked:        {}", sp.keys);
        println!("Segments / Disk:     {} segments ({} bytes)", sp.segments, sp.disk_bytes);
        println!("Live Records / Bytes:{} records ({} bytes)", sp.live_records, sp.live_bytes);
        println!("Dead Records:        {}", sp.dead_records);
        println!("Blocks Written:      {}", sp.blocks_written);
        println!("Compression Ratio:   {:.2}x", sp.compression_ratio);
        println!("Compactions / Commits:{} compactions, {} commits", sp.compactions, sp.commits);
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// inspect command
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct InspectReport {
    data_dir: String,
    database_id: String,
    node_id: String,
    storage_files: usize,
    storage_disk_bytes: u64,
    wrapping_key_id: String,
    keyring_seq: u64,
    table_count: usize,
    index_count: usize,
    tables: Vec<TableSummary>,
}

#[derive(Serialize)]
struct TableSummary {
    id: String,
    name: String,
    columns_count: usize,
    primary_key: usize,
}

fn cmd_inspect(data_dir: &str, opts: &GlobalOpts) -> Result<(), String> {
    let path = Path::new(data_dir);
    if !path.exists() {
        return Err(format!("directory does not exist: {data_dir}"));
    }
    let (disk_bytes, file_count) = calculate_dir_size(path);
    let key_hint = murmur::read_key_hint(path).ok();

    let key = opts.resolve_key()?;
    let db = open_existing_db(path, key)?;
    let db_id = hex_encode(&db.database_id());
    let node_id = hex_encode(&db.node_id());
    let schema = db.schema().unwrap_or_default();
    let _ = db.close();

    let tables: Vec<TableSummary> = schema
        .tables
        .iter()
        .map(|t| TableSummary {
            id: t.id.clone(),
            name: t.name.clone(),
            columns_count: t.columns.len(),
            primary_key: t.columns.iter().find(|c| c.primary_key > 0).map(|c| c.primary_key).unwrap_or(0),
        })
        .collect();

    let report = InspectReport {
        data_dir: data_dir.to_string(),
        database_id: db_id,
        node_id,
        storage_files: file_count,
        storage_disk_bytes: disk_bytes,
        wrapping_key_id: key_hint.as_ref().map(|h| h.wrapping_key_id.clone()).unwrap_or_default(),
        keyring_seq: key_hint.as_ref().map(|h| h.keyring_seq).unwrap_or(0),
        table_count: schema.tables.len(),
        index_count: schema.indexes.len(),
        tables,
    };

    if opts.json {
        println!("{}", serde_json::to_string_pretty(&report).map_err(|e| e.to_string())?);
        return Ok(());
    }

    println!("=== Murmur Storage Inspection ===");
    println!("Data Directory:      {}", report.data_dir);
    println!("Database ID:         {}", report.database_id);
    println!("Node ID:             {}", report.node_id);
    println!("Files On Disk:       {} files ({} bytes / {:.2} MB)",
             report.storage_files,
             report.storage_disk_bytes,
             report.storage_disk_bytes as f64 / (1024.0 * 1024.0));
    println!("Wrapping Key ID:     {}", report.wrapping_key_id);
    println!("Keyring Sequence:    {}", report.keyring_seq);
    println!("Tables Registered:   {}", report.table_count);
    println!("Indexes Registered:  {}", report.index_count);

    if !report.tables.is_empty() {
        println!("\n--- Tables ---");
        for t in &report.tables {
            println!("  • {:<20} (columns: {}, id: {})", t.name, t.columns_count, t.id);
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// verify command
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct VerifyReport {
    target_path: String,
    valid: bool,
    mode: String,
    database_id: Option<String>,
    node_id: Option<String>,
    tables_checked: usize,
    errors: Vec<String>,
}

fn cmd_verify(target: &str, opts: &GlobalOpts) -> Result<(), String> {
    let path = Path::new(target);
    if !path.exists() {
        return Err(format!("target does not exist: {target}"));
    }
    let key = opts.resolve_key()?;

    if opts.is_backup {
        let res = Database::verify_backup(path, key);
        let mut report = VerifyReport {
            target_path: target.to_string(),
            valid: res.is_ok(),
            mode: "backup".into(),
            database_id: None,
            node_id: None,
            tables_checked: 0,
            errors: Vec::new(),
        };
        if let Err(e) = res {
            report.errors.push(e.to_string());
        }

        if opts.json {
            println!("{}", serde_json::to_string_pretty(&report).map_err(|e| e.to_string())?);
            return if report.valid { Ok(()) } else { Err("verification failed".into()) };
        }

        if report.valid {
            println!("PASS: Backup directory verified successfully at {}", target);
            return Ok(());
        } else {
            println!("FAIL: Backup verification failed at {}:", target);
            for err in &report.errors {
                println!("  - {err}");
            }
            return Err("verification failed".into());
        }
    }

    // Default verify: open database, check tables, run integrity check
    let mut errors = Vec::new();
    let mut db_id = None;
    let mut node_id = None;
    let mut table_count = 0;

    match open_existing_db(path, key) {
        Ok(db) => {
            db_id = Some(hex_encode(&db.database_id()));
            node_id = Some(hex_encode(&db.node_id()));
            if let Ok(schema) = db.schema() {
                table_count = schema.tables.len();
            }
            let _ = db.close();
        }
        Err(e) => {
            errors.push(e);
        }
    }

    let report = VerifyReport {
        target_path: target.to_string(),
        valid: errors.is_empty(),
        mode: "database".into(),
        database_id: db_id,
        node_id,
        tables_checked: table_count,
        errors,
    };

    if opts.json {
        println!("{}", serde_json::to_string_pretty(&report).map_err(|e| e.to_string())?);
        return if report.valid { Ok(()) } else { Err("verification failed".into()) };
    }

    if report.valid {
        println!("PASS: Database storage integrity verified at {}", target);
        println!("Database ID:     {}", report.database_id.unwrap_or_default());
        println!("Node ID:         {}", report.node_id.unwrap_or_default());
        println!("Tables Checked:  {}", report.tables_checked);
        Ok(())
    } else {
        println!("FAIL: Storage verification failed at {}:", target);
        for err in &report.errors {
            println!("  - {err}");
        }
        Err("verification failed".into())
    }
}

// ---------------------------------------------------------------------------
// backup command
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct BackupReport {
    source_dir: String,
    destination_dir: String,
    database_id: String,
    node_id: String,
    success: bool,
}

fn cmd_backup(source: &str, dest: &str, opts: &GlobalOpts) -> Result<(), String> {
    let src_path = Path::new(source);
    let dst_path = Path::new(dest);
    if !src_path.exists() {
        return Err(format!("source directory does not exist: {source}"));
    }
    if dst_path.exists() {
        return Err(format!("destination path already exists: {dest}"));
    }

    let key = opts.resolve_key()?;
    let db = open_existing_db(src_path, key)?;
    let db_id = hex_encode(&db.database_id());
    let node_id = hex_encode(&db.node_id());

    let res = db.backup(dst_path);
    let _ = db.close();

    res.map_err(|e| format!("backup failed: {e}"))?;

    let report = BackupReport {
        source_dir: source.to_string(),
        destination_dir: dest.to_string(),
        database_id: db_id,
        node_id,
        success: true,
    };

    if opts.json {
        println!("{}", serde_json::to_string_pretty(&report).map_err(|e| e.to_string())?);
        return Ok(());
    }

    println!("PASS: Consistent hot backup created successfully.");
    println!("Source:       {}", report.source_dir);
    println!("Destination:  {}", report.destination_dir);
    println!("Database ID:  {}", report.database_id);
    println!("Node ID:      {}", report.node_id);
    Ok(())
}

// ---------------------------------------------------------------------------
// restore command
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct RestoreReport {
    backup_dir: String,
    destination_dir: String,
    success: bool,
    new_database_id: Option<String>,
    new_node_id: Option<String>,
}

fn cmd_restore(backup_dir: &str, dest_dir: &str, opts: &GlobalOpts) -> Result<(), String> {
    let bkp_path = Path::new(backup_dir);
    let dst_path = Path::new(dest_dir);
    if !bkp_path.exists() {
        return Err(format!("backup directory does not exist: {backup_dir}"));
    }
    if dst_path.exists() {
        return Err(format!("destination path already exists: {dest_dir}"));
    }

    let key = opts.resolve_key()?;
    let restore_options = RestoreOptions {
        new_database_id: opts.fresh_db_id,
        new_node_id: opts.fresh_node_id,
    };

    Database::restore_with_options(bkp_path, dst_path, key, restore_options)
        .map_err(|e| format!("restore failed: {e}"))?;

    let report = RestoreReport {
        backup_dir: backup_dir.to_string(),
        destination_dir: dest_dir.to_string(),
        success: true,
        new_database_id: opts.fresh_db_id.map(|id| hex_encode(&id)),
        new_node_id: opts.fresh_node_id.map(|id| hex_encode(&id)),
    };

    if opts.json {
        println!("{}", serde_json::to_string_pretty(&report).map_err(|e| e.to_string())?);
        return Ok(());
    }

    println!("PASS: Database restored successfully.");
    println!("Backup Source:    {}", report.backup_dir);
    println!("Destination:      {}", report.destination_dir);
    if let Some(id) = report.new_database_id {
        println!("New Database ID:  {}", id);
    }
    if let Some(id) = report.new_node_id {
        println!("New Node ID:      {}", id);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// keys command
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct KeysReport {
    data_dir: String,
    encrypted: bool,
    wrapping_key_id: String,
    active_data_key_id: u32,
    keyring_seq: u64,
    manifest_generation: u64,
    data_keys_count: usize,
    data_keys: Vec<KeyEntry>,
}

#[derive(Serialize)]
struct KeyEntry {
    id: u32,
    id_hex: String,
    created_at_epoch_secs: u64,
    status: String,
}

fn cmd_keys(data_dir: &str, opts: &GlobalOpts) -> Result<(), String> {
    let path = Path::new(data_dir);
    if !path.exists() {
        return Err(format!("directory does not exist: {data_dir}"));
    }
    let key = opts.resolve_key()?;
    let db = open_existing_db(path, key)?;

    let inv = db.key_inventory();
    let _ = db.close();

    let data_keys: Vec<KeyEntry> = inv
        .data_keys
        .iter()
        .map(|k| {
            let secs = k
                .created_at
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);
            let status_str = match k.status {
                murmur::KeyStatus::Active => "active",
                murmur::KeyStatus::Retired => "retired",
            };
            KeyEntry {
                id: k.id,
                id_hex: format!("{:08x}", k.id),
                created_at_epoch_secs: secs,
                status: status_str.to_string(),
            }
        })
        .collect();

    let report = KeysReport {
        data_dir: data_dir.to_string(),
        encrypted: inv.encrypted,
        wrapping_key_id: inv.wrapping_key_id.clone(),
        active_data_key_id: inv.active_data_key_id,
        keyring_seq: inv.keyring_seq,
        manifest_generation: inv.manifest_generation,
        data_keys_count: data_keys.len(),
        data_keys,
    };

    if opts.json {
        println!("{}", serde_json::to_string_pretty(&report).map_err(|e| e.to_string())?);
        return Ok(());
    }

    println!("=== Murmur Key Registry & Inventory ===");
    println!("Data Directory:         {}", report.data_dir);
    println!("Encrypted:              {}", report.encrypted);
    println!("Wrapping Key ID:        {}", report.wrapping_key_id);
    println!("Active Data Key ID:     {:08x} (#{})", report.active_data_key_id, report.active_data_key_id);
    println!("Keyring Persist Seq:    {}", report.keyring_seq);
    println!("Manifest Generation:    {}", report.manifest_generation);
    println!("Total Data Keys:        {}", report.data_keys_count);

    if !report.data_keys.is_empty() {
        println!("\n  Key ID    Status    Created At (UNIX secs)");
        println!("  --------  --------  ----------------------");
        for k in &report.data_keys {
            println!("  {:8}  {:<8}  {}", k.id_hex, k.status, k.created_at_epoch_secs);
        }
    }

    Ok(())
}

fn cmd_hash_password(opts: &GlobalOpts) -> Result<(), String> {
    let password = match &opts.password {
        Some(password) => password.clone(),
        None => {
            use std::io::BufRead;
            let mut line = String::new();
            std::io::stdin()
                .lock()
                .read_line(&mut line)
                .map_err(|e| format!("cannot read password from stdin: {e}"))?;
            line.trim_end_matches(['\r', '\n']).to_owned()
        }
    };
    if password.is_empty() {
        return Err("password must not be empty".into());
    }
    let hash = murmur_sql::server::auth::hash_password(&password)
        .map_err(|e| format!("cannot hash password: {e}"))?;
    if opts.json {
        println!("{}", serde_json::json!({ "password_hash": hash }));
    } else {
        println!("{hash}");
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// main dispatch
// ---------------------------------------------------------------------------

fn main() -> ExitCode {
    // `args()` panics on non-UTF8 argv; fail cleanly instead.
    let mut args = Vec::new();
    for arg in env::args_os() {
        match arg.into_string() {
            Ok(s) => args.push(s),
            Err(_) => {
                eprintln!("Error: argument is not valid UTF-8");
                return ExitCode::from(1);
            }
        }
    }
    if args.len() < 2 {
        print_usage();
        return ExitCode::from(0);
    }

    let mut global_opts = GlobalOpts::default();
    let mut positional: Vec<String> = Vec::new();

    let mut i = 1;
    while i < args.len() {
        let arg = &args[i];
        if arg == "-h" || arg == "--help" {
            print_usage();
            return ExitCode::from(0);
        } else if arg == "-v" || arg == "--version" {
            println!("murmur-admin version {VERSION}");
            return ExitCode::from(0);
        } else if arg == "--json" {
            global_opts.json = true;
        } else if arg == "--backup" {
            global_opts.is_backup = true;
        } else if arg.starts_with("--passphrase=") {
            global_opts.passphrase = Some(arg.trim_start_matches("--passphrase=").to_string());
        } else if arg == "--passphrase" && i + 1 < args.len() {
            i += 1;
            global_opts.passphrase = Some(args[i].clone());
        } else if arg.starts_with("--key-hex=") {
            global_opts.key_hex = Some(arg.trim_start_matches("--key-hex=").to_string());
        } else if arg == "--key-hex" && i + 1 < args.len() {
            i += 1;
            global_opts.key_hex = Some(args[i].clone());
        } else if arg.starts_with("--password=") {
            global_opts.password = Some(arg.trim_start_matches("--password=").to_string());
        } else if arg == "--password" && i + 1 < args.len() {
            i += 1;
            global_opts.password = Some(args[i].clone());
        } else if arg == "--passphrase" || arg == "--key-hex" || arg == "--password" {
            // The valued forms above matched when a value followed, so
            // reaching here means the flag was left dangling last.
            eprintln!("Error: {arg} needs a value");
            return ExitCode::from(1);
        } else if arg == "--fresh-node-id" {
            global_opts.fresh_node_id = Some(rand::random());
        } else if arg.starts_with("--fresh-node-id=") {
            let val = arg.trim_start_matches("--fresh-node-id=");
            match parse_id_16(val) {
                Ok(id) => global_opts.fresh_node_id = Some(id),
                Err(e) => {
                    eprintln!("Error: {e}");
                    return ExitCode::from(1);
                }
            }
        } else if arg == "--fresh-db-id" {
            global_opts.fresh_db_id = Some(rand::random());
        } else if arg.starts_with("--fresh-db-id=") {
            let val = arg.trim_start_matches("--fresh-db-id=");
            match parse_id_16(val) {
                Ok(id) => global_opts.fresh_db_id = Some(id),
                Err(e) => {
                    eprintln!("Error: {e}");
                    return ExitCode::from(1);
                }
            }
        } else {
            positional.push(arg.clone());
        }
        i += 1;
    }

    if positional.is_empty() {
        print_usage();
        return ExitCode::from(0);
    }

    let cmd = positional[0].as_str();
    let cmd_args = &positional[1..];

    let result = match cmd {
        "status" => {
            if cmd_args.is_empty() {
                Err("missing <data-dir>. Usage: murmur-admin status <data-dir>".into())
            } else {
                cmd_status(&cmd_args[0], &global_opts)
            }
        }
        "inspect" => {
            if cmd_args.is_empty() {
                Err("missing <data-dir>. Usage: murmur-admin inspect <data-dir>".into())
            } else {
                cmd_inspect(&cmd_args[0], &global_opts)
            }
        }
        "verify" => {
            if cmd_args.is_empty() {
                Err("missing <data-dir>. Usage: murmur-admin verify <data-dir> [--backup]".into())
            } else {
                cmd_verify(&cmd_args[0], &global_opts)
            }
        }
        "backup" => {
            if cmd_args.len() < 2 {
                Err("missing arguments. Usage: murmur-admin backup <source-dir> <dest-dir>".into())
            } else {
                cmd_backup(&cmd_args[0], &cmd_args[1], &global_opts)
            }
        }
        "restore" => {
            if cmd_args.len() < 2 {
                Err("missing arguments. Usage: murmur-admin restore <backup-dir> <dest-dir>".into())
            } else {
                cmd_restore(&cmd_args[0], &cmd_args[1], &global_opts)
            }
        }
        "keys" => {
            if cmd_args.is_empty() {
                Err("missing <data-dir>. Usage: murmur-admin keys <data-dir>".into())
            } else {
                cmd_keys(&cmd_args[0], &global_opts)
            }
        }
        "hash-password" => cmd_hash_password(&global_opts),
        unknown => Err(format!(
            "unknown command: \"{unknown}\". Run 'murmur-admin --help' for usage."
        )),
    };

    match result {
        Ok(()) => ExitCode::from(0),
        Err(e) => {
            if global_opts.json {
                eprintln!(
                    "{}",
                    serde_json::json!({
                        "error": true,
                        "message": e
                    })
                );
            } else {
                eprintln!("Error: {e}");
            }
            ExitCode::from(1)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_decode_accepts_valid_and_rejects_the_rest_without_panicking() {
        assert_eq!(hex_decode("00ffAB").unwrap(), vec![0x00, 0xff, 0xab]);
        assert_eq!(hex_decode("  12  ").unwrap(), vec![0x12]);
        assert!(hex_decode("abc").is_err());
        assert!(hex_decode("zz").is_err());
        assert!(hex_decode("0g").is_err());
        // Even-length non-ASCII used to panic slicing mid-character.
        assert!(hex_decode("€€").is_err());
        assert!(hex_decode("ab€!").is_err());
    }

    #[test]
    fn resolve_key_prefers_explicit_key_material() {
        let hex = "ab".repeat(32);
        let opts = GlobalOpts { key_hex: Some(hex), ..Default::default() };
        assert_eq!(opts.resolve_key().unwrap(), [0xab; 32]);
        let short = GlobalOpts { key_hex: Some("ab".into()), ..Default::default() };
        assert!(short.resolve_key().is_err());
        let pass = GlobalOpts { passphrase: Some("pw".into()), ..Default::default() };
        assert_eq!(
            pass.resolve_key().unwrap(),
            *blake3::hash(b"pw").as_bytes()
        );
        let fallback = GlobalOpts::default();
        assert_eq!(fallback.resolve_key().unwrap(), *DEFAULT_KEY);
    }
}
