//! Interactive deployment only. This module never opens an engine/database.
use crate::config::Config;
use serde::{Deserialize, Serialize};
use std::{
    ffi::{OsStr, OsString},
    fs,
    io::{self, BufRead, Write},
    net::{SocketAddr, TcpStream},
    path::{Component, Path, PathBuf},
    process::{Command, Stdio},
    time::{Duration, Instant},
};

#[cfg(target_os = "linux")]
mod linux;
#[cfg(windows)]
mod windows;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
const WAIT: Duration = Duration::from_secs(120);

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Installation {
    install: PathBuf,
    runtime: PathBuf,
    admin: bool,
}
impl Installation {
    fn executable(&self) -> PathBuf {
        self.install
            .join(format!("murmur-server{}", std::env::consts::EXE_SUFFIX))
    }
    fn admin_executable(&self) -> PathBuf {
        self.install
            .join(format!("murmur-admin{}", std::env::consts::EXE_SUFFIX))
    }
    fn config(&self) -> PathBuf {
        self.runtime.join("config/config.toml")
    }
    fn validate(&self) -> Result<()> {
        safe_directory(&self.install)?;
        safe_directory(&self.runtime)?;
        if path_within(&self.install, &self.runtime) || path_within(&self.runtime, &self.install) {
            return Err("installation and runtime directories must be separate, non-overlapping directories".into());
        }
        Ok(())
    }
}

trait Platform {
    fn preflight(&self) -> Result<()>;
    fn record(&self) -> PathBuf;
    fn defaults(&self) -> (PathBuf, PathBuf);
    fn exists(&self) -> Result<bool>;
    fn running(&self) -> Result<bool>;
    fn stop(&self) -> Result<()>;
    fn start(&self) -> Result<()>;
    fn identity(&self) -> Result<()>;
    fn prepare_filesystem(&self, _install: &Installation) -> Result<()> {
        Ok(())
    }
    fn register(&self, install: &Installation) -> Result<()>;
    fn unregister(&self) -> Result<()>;
    fn permissions(&self, install: &Installation, config: &Config) -> Result<()>;
    fn protect_record(&self) -> Result<()>;
    fn event_source(&self) -> Result<()> {
        Ok(())
    }
    fn validate_installed(&self, install: &Installation) -> Result<()> {
        checked(
            Command::new(install.executable())
                .arg("--config")
                .arg(install.config())
                .arg("--validate-config")
                .current_dir(&install.install),
        )?;
        Ok(())
    }
}

struct Console<R, W> {
    input: R,
    output: W,
    log: fs::File,
}
impl<R: BufRead, W: Write> Console<R, W> {
    fn say(&mut self, message: &str) -> Result<()> {
        writeln!(self.output, "{message}")?;
        writeln!(self.log, "{message}")?;
        self.log.flush()?;
        Ok(())
    }
    fn answer(&mut self, prompt: &str) -> Result<String> {
        self.say(prompt)?;
        self.output.flush()?;
        let mut line = String::new();
        if self.input.read_line(&mut line)? == 0 {
            return Err("input closed; cancelled before the next action".into());
        }
        Ok(line.trim().to_owned())
    }
    fn yes(&mut self, prompt: &str, default: bool) -> Result<bool> {
        loop {
            match self.answer(prompt)?.to_ascii_lowercase().as_str() {
                "" => return Ok(default),
                "y" | "yes" => return Ok(true),
                "n" | "no" => return Ok(false),
                _ => self.say("Enter yes or no.")?,
            }
        }
    }
    fn phase(&mut self, title: &str, details: &str) -> Result<()> {
        self.say(&format!("\n=== {title} ===\n{details}"))?;
        if self
            .answer("Press Enter to continue (or type CANCEL to stop)...")?
            .is_empty()
        {
            Ok(())
        } else {
            Err(format!("cancelled before {title}").into())
        }
    }
    fn directory(&mut self, label: &str, default: &Path) -> Result<PathBuf> {
        loop {
            let value = self.answer(&format!("{label} [{}]:", default.display()))?;
            let path = if value.is_empty() {
                default.to_owned()
            } else {
                PathBuf::from(value)
            };
            match safe_directory(&path) {
                Ok(()) => return Ok(path),
                Err(e) => self.say(&format!("Invalid path: {e}"))?,
            }
        }
    }
}

/// Dispatch installer commands before loading normal server configuration.
/// Returns `None` for ordinary server arguments.
pub fn dispatch(args: &[OsString]) -> Option<std::process::ExitCode> {
    if !args.iter().any(|a| a == "/install" || a == "/uninstall") {
        return None;
    }
    if args.len() != 1 {
        eprintln!("/install and /uninstall must be used alone");
        return Some(std::process::ExitCode::from(1));
    }
    let action = if args[0] == "/install" {
        "install"
    } else {
        "uninstall"
    };
    let result = platform_run(action);
    if let Err(e) = &result {
        eprintln!("{action} failed: {e}");
    }
    Some(if result.is_ok() {
        std::process::ExitCode::SUCCESS
    } else {
        std::process::ExitCode::from(1)
    })
}
fn platform_run(action: &str) -> Result<()> {
    #[cfg(any(target_os = "linux", windows))]
    {
        #[cfg(target_os = "linux")]
        let platform = linux::Linux;
        #[cfg(windows)]
        let platform = windows::Windows;
        platform.preflight()?;
        let log_path = std::env::current_dir()?.join(if action == "install" {
            "installation.log"
        } else {
            "uninstallation.log"
        });
        let mut options = fs::OpenOptions::new();
        options.create(true).append(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600).custom_flags(0x20000);
        } // O_NOFOLLOW on Linux
        reject_links(&log_path)?;
        let log = options.open(&log_path)?;
        let mut console = Console {
            input: io::stdin().lock(),
            output: io::stdout().lock(),
            log,
        };
        console.say(&format!(
            "Murmur-SQL /{action}; log: {}",
            log_path.display()
        ))?;
        let result = run(&platform, &mut console, action, &std::env::current_exe()?);
        match &result {
            Ok(()) => console.say("Completed.")?,
            Err(e) => console.say(&format!(
                "FAILED: {e}. Completed phases and recovery actions are recorded above."
            ))?,
        }
        result
    }
    #[cfg(not(any(target_os = "linux", windows)))]
    {
        let _ = action;
        Err("service installation supports Windows and Linux with systemd only".into())
    }
}

fn run<P: Platform, R: BufRead, W: Write>(
    platform: &P,
    ui: &mut Console<R, W>,
    action: &str,
    source: &Path,
) -> Result<()> {
    let record = platform.record();
    let previous: Option<Installation> = if record.exists() {
        reject_links(&record)?;
        Some(serde_json::from_slice(&fs::read(&record)?)?)
    } else {
        None
    };
    let exists = platform.exists()?;
    if exists && previous.is_none() {
        return Err(
            "service exists without installer metadata; refusing to replace an unmanaged service"
                .into(),
        );
    }
    if let Some(old) = &previous {
        old.validate()?;
    }
    if action == "uninstall" {
        return uninstall(
            platform,
            ui,
            previous.ok_or("no managed installation found")?,
            source,
        );
    }
    let (default_install, default_runtime) = previous
        .as_ref()
        .map(|i| (i.install.clone(), i.runtime.clone()))
        .unwrap_or_else(|| platform.defaults());
    let mut install = Installation {
        install: ui.directory("Installation directory", &default_install)?,
        runtime: ui.directory(
            "Data/runtime directory (config, data, logs, backups)",
            &default_runtime,
        )?,
        admin: false,
    };
    install.validate()?;
    let metadata_dir = record.parent().unwrap();
    for path in [&install.install, &install.runtime] {
        if path_within(path, metadata_dir) || path_within(metadata_dir, path) {
            return Err("installation/runtime paths overlap protected installer metadata".into());
        }
    }
    if let Some(old) = &previous
        && (!same_path(&old.install, &install.install)
            || !same_path(&old.runtime, &install.runtime))
    {
        return Err("upgrades preserve installation and runtime paths; relocation requires a separate manual migration".into());
    }
    let config_path = install.config();
    reject_links(&config_path)?;
    if previous.is_none() {
        // A fresh deployment must not adopt unrelated trees or silently rewrite them.
        for directory in [&install.install, &install.runtime] {
            if directory.exists() && fs::read_dir(directory)?.next().is_some() {
                return Err(format!(
                    "fresh installation requires an empty directory: {}",
                    directory.display()
                )
                .into());
            }
        }
    }
    let fresh = !config_path.exists();
    let config = if fresh {
        let mut config = Config::default();
        loop {
            let value = ui.answer("SQL listen address [127.0.0.1:5432]:")?;
            let value = if value.is_empty() {
                "127.0.0.1:5432".to_owned()
            } else {
                value
            };
            match value.parse::<SocketAddr>() {
                Ok(address) if address.port() != 0 => {
                    config.server.bind = value;
                    break;
                }
                _ => {
                    ui.say("Enter an IP address and a nonzero port (for example 127.0.0.1:5432).")?
                }
            }
        }
        config.storage.data_dir = install.runtime.join("data").to_string_lossy().into_owned();
        config.logging.dir = install.runtime.join("logs").to_string_lossy().into_owned();
        config.backups.dir = install
            .runtime
            .join("backups")
            .to_string_lossy()
            .into_owned();
        config
    } else {
        Config::load(&config_path)?
    };
    config.validate()?;
    writable_paths(&install, &config)?;
    credential_paths(&install, &config)?;
    let was_running = exists && platform.running()?;
    ui.say(&format!("{}\nExecutable: {}\nConfiguration: {}\nSQL listener: {}\nService filesystem permissions will be restricted to these configured paths.", if fresh { "Fresh installation" } else { "Upgrade; existing configuration will be preserved byte-for-byte" }, install.executable().display(), config_path.display(), config.server.bind))?;
    ui.say(&format!(
        "Writable directories: {:?}\nRead-only credential files: {:?}",
        writable_paths(&install, &config)?,
        credential_paths(&install, &config)?
    ))?;
    ui.phase("Prepare filesystem and service identity", "Create the service identity when absent and the dedicated installation/runtime directories.")?;
    platform.identity()?;
    fs::create_dir_all(&install.install)?;
    fs::create_dir_all(config_path.parent().unwrap())?;
    if previous.is_none() {
        platform.prepare_filesystem(&install)?;
    }
    if fresh {
        write_atomic(&config_path, toml::to_string_pretty(&config)?.as_bytes())?;
    }
    for path in writable_paths(&install, &config)? {
        fs::create_dir_all(path)?;
    }
    if previous.is_none() {
        write_record(platform, &install)?;
    }
    ui.say("Prepare filesystem: complete")?;
    let mut stop_attempted = false;
    let mut registration_attempted = false;
    let mut replacements = Vec::new();
    let result = (|| -> Result<()> {
        if was_running {
            ui.phase(
                "Stop existing service",
                "Stop the running service before replacing its executable.",
            )?;
            if !ui.yes("Stop the service? [y/N]:", false)? {
                return Err("service stop declined".into());
            }
            stop_attempted = true;
            platform.stop()?;
            ui.say("Stop existing service: complete")?;
        }
        ui.phase(
            "Install executables and configuration",
            "Stage and replace binaries. Preserve the active configuration on upgrades.",
        )?;
        replacements.push(Replacement::copy(source, &install.executable())?);
        let admin_source =
            source.with_file_name(format!("murmur-admin{}", std::env::consts::EXE_SUFFIX));
        if admin_source.is_file() {
            replacements.push(Replacement::copy(
                &admin_source,
                &install.admin_executable(),
            )?);
            install.admin = true;
        } else {
            install.admin =
                previous.as_ref().is_some_and(|i| i.admin) && install.admin_executable().is_file();
            ui.say("murmur-admin is not beside this executable; existing admin tools are preserved. Service installation can continue.")?;
        }
        ui.say("Install executables and configuration: complete")?;
        ui.phase(
            "Register service",
            "Configure automatic startup, restart-on-failure recovery, and the platform event-log source.",
        )?;
        registration_attempted = true;
        platform.register(&install)?;
        if let Err(e) = platform.event_source() {
            ui.say(&format!("Optional event-log registration failed: {e}"))?;
        }
        write_record(platform, &install)?;
        ui.say("Register service: complete")?;
        ui.phase("Apply service permissions", "Keep binaries read/execute-only. Grant write access to configured runtime paths and the atomic configuration directory.")?;
        platform.permissions(&install, &config)?;
        // Validate the real installed config/TLS without opening the engine.
        platform.validate_installed(&install)?;
        ui.say("Apply service permissions and validate configuration: complete")?;
        ui.say(&format!(
            "Firewall unchanged. SQL TCP listener: {}{}",
            config.server.bind,
            if config.replication.enabled {
                format!(
                    "; replication QUIC/UDP listener: {}",
                    config.replication.bind
                )
            } else {
                String::new()
            }
        ))?;
        if was_running || ui.yes("Start the service now? [Y/n]:", true)? {
            ui.phase(
                "Start and validate service",
                "Wait for the service and its SQL listener to become available.",
            )?;
            platform.start()?;
            wait_listener(platform, &config.server.bind)?;
            ui.say("Start and validate service: complete")?;
        } else {
            ui.say("Service left stopped; automatic startup is enabled.")?;
        }
        Ok(())
    })();
    if let Err(error) = result {
        if !stop_attempted && replacements.is_empty() && !registration_attempted {
            return Err(error);
        }
        ui.say(&format!(
            "Installation interrupted: {error}. Recovering previous service/binaries."
        ))?;
        // Never replace an executable while its service might still be running.
        let recovery = (|| -> Result<()> {
            if platform.exists()? && platform.running()? {
                platform.stop()?;
            }
            for replacement in replacements.iter().rev() {
                replacement.restore()?;
            }
            if let Some(old) = &previous {
                if exists {
                    platform.register(old)?;
                } else if platform.exists()? {
                    platform.unregister()?;
                }
                write_record(platform, old)?;
                platform.permissions(old, &Config::load(old.config())?)?;
                if was_running {
                    platform.start()?;
                }
            } else {
                if platform.exists()? {
                    platform.unregister()?;
                }
                write_record(platform, &install)?;
            }
            Ok(())
        })();
        match recovery {
            Ok(()) => {
                ui.say("Recovery complete. Prepared runtime folders/configuration were retained.")?
            }
            Err(e) => ui.say(&format!(
                "RECOVERY FAILED: {e}; retained recovery binaries for manual repair."
            ))?,
        }
        return Err(error);
    }
    for replacement in replacements {
        replacement.finish()?;
    }
    ui.say(&format!("Installed configuration: {}\nConfigure SQL authentication and TLS separately as required. No database operations were performed.", config_path.display()))?;
    Ok(())
}

fn write_record(platform: &impl Platform, install: &Installation) -> Result<()> {
    let path = platform.record();
    reject_links(&path)?;
    fs::create_dir_all(path.parent().unwrap())?;
    platform.protect_record()?;
    write_atomic(&path, &serde_json::to_vec_pretty(install)?)?;
    platform.protect_record()
}

fn uninstall<P: Platform, R: BufRead, W: Write>(
    platform: &P,
    ui: &mut Console<R, W>,
    install: Installation,
    source: &Path,
) -> Result<()> {
    ui.say(&format!("Remove Murmur-SQL service. Files remain unless separately confirmed.\nApplication: {}\nRuntime/data: {}", install.install.display(), install.runtime.display()))?;
    if !ui.yes("Proceed with uninstall? [y/N]:", false)? {
        return ui.say("Uninstall cancelled; no changes made.");
    }
    ui.phase(
        "Stop and remove service",
        "Stop the service cleanly, then remove its registration and boot startup.",
    )?;
    if platform.exists()? {
        if platform.running()? {
            platform.stop()?;
        }
        platform.unregister()?;
    }
    ui.say("Stop and remove service: complete. Service account retained.")?;
    let application = if install.admin {
        vec![install.executable(), install.admin_executable()]
    } else {
        vec![install.executable()]
    };
    ui.say(&format!(
        "Optional application deletion (unrelated files will remain): {application:?}"
    ))?;
    let delete_app = ui
        .answer("Type DELETE to delete these application files, or Enter to preserve:")?
        == "DELETE";
    let config = Config::load(install.config());
    let mut runtime_targets = vec![
        install.runtime.join("config"),
        install.runtime.join("data"),
        install.runtime.join("logs"),
        install.runtime.join("backups"),
    ];
    // Include custom runtime paths only when inside the managed runtime root.
    if let Ok(config) = &config {
        for path in writable_paths(&install, config)? {
            if path_within(&path, &install.runtime) {
                runtime_targets.push(path);
            } else {
                ui.say(&format!(
                    "External configured runtime path will be preserved: {}",
                    path.display()
                ))?;
            }
        }
    } else {
        ui.say("Configuration unreadable; runtime deletion will be refused because credential locations cannot be verified.")?;
    }
    runtime_targets.sort();
    runtime_targets.dedup();
    ui.say(&format!("Optional runtime/data deletion: {runtime_targets:?}\nExternal credentials and other runtime-root files will be preserved."))?;
    let delete_data = ui
        .answer("Type DELETE to delete these runtime/data folders, or Enter to preserve:")?
        == "DELETE";
    // Validate the entire deletion plan before deleting any target. Never follow links.
    if delete_app {
        for path in &application {
            reject_links(path)?;
        }
    }
    if delete_data {
        if config.is_err() {
            return Err(
                "cannot delete runtime/data with unreadable configuration; files were preserved"
                    .into(),
            );
        }
        for path in &runtime_targets {
            safe_directory(path)?;
            if !path_within(&path, &install.runtime) || same_path(path, &install.runtime) {
                return Err("unsafe runtime deletion plan".into());
            }
            reject_tree_links(path)?;
        }
        // Never delete credential files, including credentials inside managed folders.
        if let Ok(config) = &config {
            for credential in credential_paths(&install, config)? {
                if runtime_targets.iter().any(|p| path_within(&credential, p)) {
                    return Err(format!("credential {} lies in a deletion target; move it before requesting runtime deletion", credential.display()).into());
                }
            }
        }
        for path in &runtime_targets {
            if path.exists() {
                fs::remove_dir_all(path)?;
            }
        }
        let _ = fs::remove_dir(&install.runtime);
        ui.say("Confirmed runtime/data folders removed.")?;
    }
    if delete_app {
        #[cfg(windows)]
        let deferred = windows::defer_application_deletion(&application, source)?;
        #[cfg(not(windows))]
        let deferred = {
            let _ = source;
            false
        };
        if !deferred {
            for path in &application {
                if path.exists() {
                    fs::remove_file(path)?;
                }
            }
            let _ = fs::remove_dir(&install.install);
        }
        ui.say(if deferred {
            "Application deletion scheduled after this installer exits."
        } else {
            "Application files removed."
        })?;
    }
    // Keep metadata when files are retained, so repeated uninstall/reinstall remains possible.
    if delete_app && delete_data {
        let record = platform.record();
        if record.exists() {
            fs::remove_file(record)?;
        }
    }
    Ok(())
}

fn safe_directory(path: &Path) -> Result<()> {
    if path.to_str().is_none() || path.to_string_lossy().chars().any(char::is_control) {
        return Err("deployment paths must be Unicode without control characters".into());
    }
    #[cfg(windows)]
    if !matches!(path.components().next(), Some(Component::Prefix(prefix)) if matches!(prefix.kind(), std::path::Prefix::Disk(_)))
    {
        return Err("use a local drive path; network/UNC deployment paths are unsupported".into());
    }
    #[cfg(windows)]
    for component in path.components() {
        if let Component::Normal(name) = component {
            let name = name.to_string_lossy();
            if name.ends_with(['.', ' ']) || name.contains(':') {
                return Err(
                    "ambiguous Windows directory names and alternate data streams are unsupported"
                        .into(),
                );
            }
        }
    }
    if !path.is_absolute()
        || path
            .components()
            .any(|c| matches!(c, Component::ParentDir | Component::CurDir))
    {
        return Err("use an absolute path without . or .. components".into());
    }
    let normal: Vec<_> = path
        .components()
        .filter_map(|c| {
            if let Component::Normal(c) = c {
                Some(c.to_string_lossy().to_lowercase())
            } else {
                None
            }
        })
        .collect();
    if normal.len() < 2 {
        return Err(
            "choose a dedicated application subdirectory, not a root or top-level directory".into(),
        );
    }
    let first = normal[0].as_str();
    if matches!(
        first,
        "windows" | "boot" | "dev" | "proc" | "sys" | "bin" | "sbin" | "lib" | "lib64"
    ) || (first == "usr"
        && normal
            .get(1)
            .is_some_and(|s| matches!(s.as_str(), "bin" | "sbin" | "lib" | "lib64")))
    {
        return Err("system directories are not installation/runtime targets".into());
    }
    #[cfg(target_os = "linux")]
    if path.to_string_lossy().ends_with('\\')
        || [
            "/var/lib",
            "/var/log",
            "/var/cache",
            "/var/tmp",
            "/usr/local",
            "/etc/ssh",
            "/etc/ssl",
            "/etc/systemd",
        ]
        .contains(&path.to_str().unwrap())
    {
        return Err(
            "choose a dedicated application directory rather than a system/shared directory".into(),
        );
    }
    reject_links(path)
}
fn reject_links(path: &Path) -> Result<()> {
    for ancestor in path.ancestors() {
        match fs::symlink_metadata(ancestor) {
            Ok(meta) => {
                #[cfg(windows)]
                {
                    use std::os::windows::fs::MetadataExt;
                    if meta.file_attributes() & 0x400 != 0 {
                        return Err(
                            format!("reparse point rejected: {}", ancestor.display()).into()
                        );
                    }
                }
                if meta.file_type().is_symlink() {
                    return Err(format!("symlink rejected: {}", ancestor.display()).into());
                }
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => (),
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}
fn reject_tree_links(path: &Path) -> Result<()> {
    reject_links(path)?;
    if path.is_dir() {
        for entry in fs::read_dir(path)? {
            reject_tree_links(&entry?.path())?;
        }
    }
    Ok(())
}
fn path_within(path: &Path, parent: &Path) -> bool {
    #[cfg(windows)]
    {
        let path = windows::comparison_path(path);
        let parent = windows::comparison_path(parent);
        let mut actual = path.components();
        parent.components().all(|expected| {
            actual.next().is_some_and(|part| {
                windows::equal_component(part.as_os_str(), expected.as_os_str())
            })
        })
    }
    #[cfg(not(windows))]
    {
        path.starts_with(parent)
    }
}

fn same_path(a: &Path, b: &Path) -> bool {
    path_within(a, b) && path_within(b, a)
}
fn configured_path(install: &Installation, value: &str) -> Result<PathBuf> {
    let path = Path::new(value);
    let raw = if path.is_absolute() {
        path.to_owned()
    } else {
        install.install.join(path)
    };
    reject_links(&raw)?;
    let mut resolved = PathBuf::new();
    for component in raw.components() {
        match component {
            Component::CurDir => (),
            Component::ParentDir => {
                if !resolved.pop() {
                    return Err("configured path escapes its filesystem root".into());
                }
            }
            component => resolved.push(component.as_os_str()),
        }
    }
    Ok(resolved)
}

fn writable_paths(install: &Installation, config: &Config) -> Result<Vec<PathBuf>> {
    let mut paths = vec![install.config().parent().unwrap().to_owned()];
    for value in [
        &config.storage.data_dir,
        &config.logging.dir,
        &config.backups.dir,
    ] {
        paths.push(configured_path(install, value)?);
    }
    if let Some(files) = &config.replication.files {
        paths.push(configured_path(install, &files.directory)?);
    }
    if let Some(bridge) = &config.replication.bridge {
        paths.push(configured_path(install, &bridge.directory)?);
    }
    for path in &paths {
        safe_directory(path)?;
        if path_within(&install.install, path) || path_within(path, &install.install) {
            return Err(format!(
                "writable path overlaps executable installation: {}",
                path.display()
            )
            .into());
        }
    }
    paths.sort();
    paths.dedup();
    Ok(paths)
}
fn credential_paths(install: &Installation, config: &Config) -> Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    for path in [
        &config.server.tls.cert_file,
        &config.server.tls.key_file,
        &config.server.tls.ca_file,
        &config.replication.cert_chain_file,
        &config.replication.private_key_file,
        &config.replication.ca_file,
        &config.replication.signing_key_file,
    ]
    .into_iter()
    .flatten()
    {
        let path = configured_path(install, path)?;
        reject_links(&path)?;
        paths.push(path);
    }
    Ok(paths)
}
fn checked(command: &mut Command) -> Result<String> {
    let output = command.stdin(Stdio::null()).output()?;
    if !output.status.success() {
        return Err(format!(
            "{:?} failed ({}): {} {}",
            command.get_program(),
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    Ok(String::from_utf8(output.stdout)?)
}
fn wait_listener(platform: &impl Platform, bind: &str) -> Result<()> {
    let mut address: SocketAddr = bind.parse()?;
    if address.ip().is_unspecified() {
        address.set_ip(if address.is_ipv4() {
            "127.0.0.1".parse()?
        } else {
            "::1".parse()?
        });
    }
    let deadline = Instant::now() + WAIT;
    loop {
        if !platform.running()? {
            return Err("service exited before its SQL listener became ready".into());
        }
        if TcpStream::connect_timeout(&address, Duration::from_millis(500)).is_ok() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err("service SQL listener readiness timed out after 120 seconds".into());
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}
fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    reject_links(path)?;
    let temporary = path.with_extension(format!("install-{}.tmp", std::process::id()));
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&temporary)?;
    let result = (|| -> Result<()> {
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&temporary, path)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(temporary);
    }
    result
}
struct Replacement {
    target: PathBuf,
    backup: Option<PathBuf>,
    changed: bool,
}
impl Replacement {
    fn copy(source: &Path, target: &Path) -> Result<Self> {
        reject_links(target)?;
        if target.exists() && fs::canonicalize(source)? == fs::canonicalize(target)? {
            return Ok(Self {
                target: target.into(),
                backup: None,
                changed: false,
            });
        }
        let staged = target.with_extension(format!("install-{}.tmp", std::process::id()));
        let backup = target.with_extension(format!("previous-{}", std::process::id()));
        if staged.exists() || backup.exists() {
            return Err("staging/recovery file already exists; inspect the previous failed installation first".into());
        }
        // create_new prevents overwriting an existing staging file or link.
        let mut output = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&staged)?;
        let copied = (|| -> Result<()> {
            io::copy(&mut fs::File::open(source)?, &mut output)?;
            output.sync_all()?;
            Ok(())
        })();
        drop(output);
        if let Err(e) = copied {
            let _ = fs::remove_file(&staged);
            return Err(e);
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&staged, fs::Permissions::from_mode(0o755))?;
        }
        let existed = target.exists();
        if existed {
            fs::rename(target, &backup)?;
        }
        if let Err(e) = fs::rename(&staged, target) {
            if existed {
                let _ = fs::rename(&backup, target);
            }
            let _ = fs::remove_file(staged);
            return Err(e.into());
        }
        Ok(Self {
            target: target.into(),
            backup: existed.then_some(backup),
            changed: true,
        })
    }
    fn restore(&self) -> Result<()> {
        if self.changed {
            if self.target.exists() {
                fs::remove_file(&self.target)?;
            }
            if let Some(backup) = &self.backup {
                fs::rename(backup, &self.target)?;
            }
        }
        Ok(())
    }
    fn finish(self) -> Result<()> {
        if let Some(backup) = self.backup {
            fs::remove_file(backup)?;
        }
        Ok(())
    }
}

/// Windows SCM hosting; ordinary console mode is unchanged on every platform.
pub fn dispatch_host(args: &[OsString]) -> Option<std::process::ExitCode> {
    #[cfg(windows)]
    if args.first().is_some_and(|a| a == "--service") {
        return Some(windows::host(args));
    }
    let _ = args;
    None
}

#[cfg(test)]
mod tests;

/// Notify systemd only after the listener is bound. Console runs have no socket.
pub(crate) fn notify_ready() -> std::result::Result<(), String> {
    #[cfg(target_os = "linux")]
    if let Some(socket) = std::env::var_os("NOTIFY_SOCKET") {
        use std::os::{
            linux::net::SocketAddrExt,
            unix::{
                ffi::OsStrExt,
                net::{SocketAddr, UnixDatagram},
            },
        };
        let bytes = socket.as_os_str().as_bytes();
        let result = (|| -> io::Result<()> {
            let address = if bytes.first() == Some(&b'@') {
                SocketAddr::from_abstract_name(&bytes[1..])?
            } else {
                SocketAddr::from_pathname(&socket)?
            };
            UnixDatagram::unbound()?.send_to_addr(b"READY=1", &address)?;
            Ok(())
        })();
        result.map_err(|e| format!("systemd readiness notification failed: {e}"))?;
    }
    Ok(())
}
