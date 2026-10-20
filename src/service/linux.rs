use super::*;
use std::os::unix::fs::{MetadataExt, PermissionsExt};

pub(super) struct Linux;
const UNIT: &str = "murmur-sql.service";
const UNIT_PATH: &str = "/etc/systemd/system/murmur-sql.service";

fn systemctl(args: &[&OsStr]) -> Result<String> {
    checked(Command::new("/usr/bin/systemctl").args(args))
}
fn call(args: &[&str]) -> Result<String> {
    systemctl(&args.iter().map(OsStr::new).collect::<Vec<_>>())
}
fn quote(value: &Path) -> Result<String> {
    let value = value.to_str().ok_or("systemd paths must be UTF-8")?;
    if value.chars().any(char::is_control) {
        return Err("control characters in a systemd path".into());
    }
    Ok(format!(
        "\"{}\"",
        value
            .replace('\\', "\\\\")
            .replace('"', "\\\"")
            .replace('%', "%%")
    ))
}
fn unit(installation: &Installation, group: &str) -> Result<String> {
    Ok(format!(
        "[Unit]\nDescription=Murmur SQL server\nAfter=network.target\n\n[Service]\nType=notify\nNotifyAccess=main\nUser=murmur\nGroup={group}\nWorkingDirectory={}\nExecStart=:{} --config {}\nRestart=on-failure\nRestartSec=60s\nTimeoutStartSec=120s\nKillSignal=SIGTERM\nTimeoutStopSec=120s\nUMask=0077\nNoNewPrivileges=true\n\n[Install]\nWantedBy=multi-user.target\n",
        installation
            .install
            .to_str()
            .ok_or("systemd paths must be UTF-8")?
            .replace('%', "%%"),
        quote(&installation.executable())?,
        quote(&installation.config())?
    ))
}
fn wait(active: bool) -> Result<()> {
    let deadline = Instant::now() + WAIT;
    while Linux.running()? != active {
        if Instant::now() >= deadline {
            return Err("systemd service state timed out after 120 seconds".into());
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    Ok(())
}
fn mode_tree(path: &Path) -> Result<()> {
    reject_tree_links(path)?;
    if path.is_dir() {
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
        for entry in fs::read_dir(path)? {
            mode_tree(&entry?.path())?;
        }
    } else {
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}
impl Platform for Linux {
    fn preflight(&self) -> Result<()> {
        if checked(Command::new("/usr/bin/id").arg("-u"))?.trim() != "0" {
            return Err("run /install or /uninstall as root (for example with sudo)".into());
        }
        if !Path::new("/run/systemd/system").is_dir() || !Path::new("/usr/bin/systemctl").is_file()
        {
            return Err(
                "Linux service installation requires a running systemd system manager".into(),
            );
        }
        Ok(())
    }
    fn record(&self) -> PathBuf {
        "/etc/murmur-sql/install.json".into()
    }
    fn defaults(&self) -> (PathBuf, PathBuf) {
        ("/opt/murmur-sql".into(), "/var/lib/murmur-sql".into())
    }
    fn exists(&self) -> Result<bool> {
        let output = Command::new("/usr/bin/systemctl")
            .args(["show", UNIT, "--property=LoadState", "--value"])
            .output()?;
        let value = String::from_utf8(output.stdout)?;
        if value.trim() == "not-found" {
            return Ok(false);
        }
        if !output.status.success() {
            return Err(format!(
                "cannot inspect systemd service: {}",
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }
        Ok(true)
    }
    fn running(&self) -> Result<bool> {
        let state = call(&["show", UNIT, "--property=ActiveState", "--value"])?;
        Ok(matches!(
            state.trim(),
            "active" | "activating" | "reloading" | "deactivating"
        ))
    }
    fn stop(&self) -> Result<()> {
        call(&["stop", UNIT])?;
        wait(false)
    }
    fn start(&self) -> Result<()> {
        call(&["start", UNIT])?;
        wait(true)
    }
    fn identity(&self) -> Result<()> {
        let status = Command::new("/usr/bin/id")
            .args(["-u", "murmur"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()?;
        if !status.success() {
            let shell = ["/usr/sbin/nologin", "/sbin/nologin"]
                .into_iter()
                .find(|p| Path::new(p).is_file())
                .ok_or("nologin executable not found")?;
            checked(Command::new("/usr/sbin/useradd").args([
                "--system",
                "--user-group",
                "--no-create-home",
                "--shell",
                shell,
                "murmur",
            ]))?;
        }
        Ok(())
    }
    fn prepare_filesystem(&self, installation: &Installation) -> Result<()> {
        for path in [&installation.install, &installation.runtime] {
            for parent in path.ancestors() {
                reject_links(parent)?;
                let metadata = fs::metadata(parent)?;
                if metadata.uid() != 0
                    || (metadata.mode() & 0o022 != 0 && metadata.mode() & 0o1000 == 0)
                {
                    return Err(format!("service directory/ancestor must be root-owned and protected from other users' writes: {}", parent.display()).into());
                }
            }
            fs::set_permissions(path, fs::Permissions::from_mode(0o755))?;
        }
        fs::set_permissions(
            installation.config().parent().unwrap(),
            fs::Permissions::from_mode(0o700),
        )?;
        Ok(())
    }
    fn register(&self, installation: &Installation) -> Result<()> {
        let group = checked(Command::new("/usr/bin/id").args(["-gn", "murmur"]))?;
        let group = group.trim();
        if !group
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"_-".contains(&c))
        {
            return Err("unsafe service group name".into());
        }
        reject_links(Path::new(UNIT_PATH))?;
        write_atomic(Path::new(UNIT_PATH), unit(installation, group)?.as_bytes())?;
        fs::set_permissions(UNIT_PATH, fs::Permissions::from_mode(0o644))?;
        call(&["daemon-reload"])?;
        call(&["enable", UNIT])?;
        Ok(())
    }
    fn unregister(&self) -> Result<()> {
        call(&["disable", UNIT])?;
        reject_links(Path::new(UNIT_PATH))?;
        if Path::new(UNIT_PATH).exists() {
            fs::remove_file(UNIT_PATH)?;
        }
        call(&["daemon-reload"])?;
        // A failed unit can remain cached; reset only this unit, never others.
        let _ = call(&["reset-failed", UNIT]);
        Ok(())
    }
    fn permissions(&self, installation: &Installation, config: &Config) -> Result<()> {
        for path in [&installation.install, &installation.executable()] {
            reject_links(path)?;
            checked(Command::new("/usr/bin/chown").arg("root:root").arg(path))?;
            fs::set_permissions(path, fs::Permissions::from_mode(0o755))?;
        }
        if installation.admin {
            let path = installation.admin_executable();
            reject_links(&path)?;
            checked(Command::new("/usr/bin/chown").arg("root:root").arg(&path))?;
            fs::set_permissions(path, fs::Permissions::from_mode(0o755))?;
        }
        // The runtime root must be traversable, but not writable, by the service.
        checked(
            Command::new("/usr/bin/chown")
                .arg("root:root")
                .arg(&installation.runtime),
        )?;
        fs::set_permissions(&installation.runtime, fs::Permissions::from_mode(0o755))?;
        let group = checked(Command::new("/usr/bin/id").args(["-gn", "murmur"]))?;
        for path in writable_paths(installation, config)? {
            mode_tree(&path)?;
            checked(
                Command::new("/usr/bin/chown")
                    .arg("-R")
                    .arg(format!("murmur:{}", group.trim()))
                    .arg(&path),
            )?;
        }
        for path in credential_paths(installation, config)? {
            if !path.is_file() {
                return Err(format!("credential file does not exist: {}", path.display()).into());
            }
            checked(Command::new("/usr/bin/chgrp").arg(group.trim()).arg(&path))?;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o640))?;
        }
        // Verify parent traversal and read access without changing unrelated parent ACLs.
        let runuser = if Path::new("/usr/sbin/runuser").exists() {
            "/usr/sbin/runuser"
        } else {
            "/sbin/runuser"
        };
        for path in credential_paths(installation, config)?
            .into_iter()
            .chain([installation.config(), installation.executable()])
        {
            checked(
                Command::new(runuser)
                    .args(["-u", "murmur", "--", "/usr/bin/test", "-r"])
                    .arg(&path),
            )?;
        }
        Ok(())
    }
    fn protect_record(&self) -> Result<()> {
        let record = self.record();
        reject_links(record.parent().unwrap())?;
        fs::set_permissions(record.parent().unwrap(), fs::Permissions::from_mode(0o700))?;
        checked(
            Command::new("/usr/bin/chown")
                .arg("root:root")
                .arg(record.parent().unwrap()),
        )?;
        if record.exists() {
            fs::set_permissions(record, fs::Permissions::from_mode(0o600))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unit_quotes_paths_and_preserves_systemd_literals() {
        let i = Installation {
            install: "/opt/murmur with \"quotes\" $data%".into(),
            runtime: "/var/lib/murmur app".into(),
            admin: false,
        };
        let text = unit(&i, "murmur").unwrap();
        assert!(text.contains("Type=notify\nNotifyAccess=main\nUser=murmur"));
        assert!(text.contains("WorkingDirectory=/opt/murmur with \"quotes\" $data%%"));
        assert!(text.contains("--config \"/var/lib/murmur app/config/config.toml\""));
        assert!(text.contains("KillSignal=SIGTERM"));
        assert!(quote(Path::new("/opt/bad\nunit")).is_err());
    }
}
