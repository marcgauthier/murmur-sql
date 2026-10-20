use super::*;
use std::sync::{
    Mutex, OnceLock,
    atomic::{AtomicU8, Ordering},
};
use windows_service::{
    service::{
        Service, ServiceAccess, ServiceAction, ServiceActionType, ServiceControl,
        ServiceControlAccept, ServiceErrorControl, ServiceExitCode, ServiceFailureActions,
        ServiceFailureResetPeriod, ServiceInfo, ServiceSidType, ServiceStartType, ServiceState,
        ServiceStatus, ServiceType,
    },
    service_control_handler::{self, ServiceControlHandlerResult},
    service_dispatcher,
    service_manager::{ServiceManager, ServiceManagerAccess},
};

// Match Windows ordinal case comparison, including non-ASCII filename aliases.
#[link(name = "kernel32")]
unsafe extern "system" {
    fn CompareStringOrdinal(
        a: *const u16,
        a_len: i32,
        b: *const u16,
        b_len: i32,
        ignore_case: i32,
    ) -> i32;
}
// Resolve existing ancestors to long filesystem paths, including DOS 8.3 aliases.
pub(super) fn comparison_path(path: &Path) -> PathBuf {
    for ancestor in path.ancestors() {
        if let Ok(base) = fs::canonicalize(ancestor) {
            if let Ok(tail) = path.strip_prefix(ancestor) {
                return base.join(tail);
            }
        }
    }
    path.to_owned()
}
pub(super) fn equal_component(a: &OsStr, b: &OsStr) -> bool {
    use std::os::windows::ffi::OsStrExt;
    let a: Vec<u16> = a.encode_wide().collect();
    let b: Vec<u16> = b.encode_wide().collect();
    let (Ok(a_len), Ok(b_len)) = (i32::try_from(a.len()), i32::try_from(b.len())) else {
        return false;
    };
    // SAFETY: both buffers remain valid for their supplied UTF-16 lengths.
    unsafe { CompareStringOrdinal(a.as_ptr(), a_len, b.as_ptr(), b_len, 1) == 2 }
}

pub(super) struct Windows;
const NAME: &str = "MurmurSQL";
const ACCOUNT: &str = "NT SERVICE\\MurmurSQL";
fn manager() -> Result<ServiceManager> {
    Ok(ServiceManager::local_computer(
        None::<&str>,
        ServiceManagerAccess::CONNECT | ServiceManagerAccess::CREATE_SERVICE,
    )?)
}
fn open() -> Result<Service> {
    Ok(manager()?.open_service(NAME, ServiceAccess::ALL_ACCESS)?)
}
fn missing(e: &windows_service::Error) -> bool {
    matches!(e, windows_service::Error::Winapi(e) if e.raw_os_error() == Some(1060))
}
fn system_tool(name: &str) -> PathBuf {
    PathBuf::from(std::env::var_os("SystemRoot").unwrap_or_else(|| r"C:\Windows".into()))
        .join("System32")
        .join(name)
}
fn powershell_exe() -> PathBuf {
    system_tool(r"WindowsPowerShell\v1.0\powershell.exe")
}
fn powershell(script: &str) -> Result<String> {
    checked(Command::new(powershell_exe()).args([
        "-NoProfile",
        "-NonInteractive",
        "-Command",
        script,
    ]))
}
fn literal(value: &OsStr) -> Result<String> {
    let value = value
        .to_str()
        .ok_or("Windows deployment paths must be Unicode")?;
    if value.chars().any(char::is_control) {
        return Err("control characters in deployment path".into());
    }
    Ok(format!("'{}'", value.replace('\'', "''")))
}
fn wait(desired: ServiceState) -> Result<()> {
    let deadline = Instant::now() + WAIT;
    loop {
        let state = open()?.query_status()?.current_state;
        if state == desired {
            return Ok(());
        }
        if desired == ServiceState::Running && state == ServiceState::Stopped {
            return Err("Windows service stopped during startup; inspect the service log and Application event log".into());
        }
        if Instant::now() >= deadline {
            return Err(
                format!("Windows service did not reach {desired:?} within 120 seconds").into(),
            );
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}
fn acl(
    path: &Path,
    account: &str,
    permission: &str,
    directory: bool,
    recursive: bool,
) -> Result<()> {
    reject_tree_links(path)?;
    let rights = if directory {
        format!("{account}:(OI)(CI){permission}")
    } else {
        format!("{account}:{permission}")
    };
    let mut cmd = Command::new(system_tool("icacls.exe"));
    cmd.arg(path).args(["/grant:r", &rights]);
    if recursive {
        cmd.arg("/T");
    }
    checked(&mut cmd)?;
    Ok(())
}
fn secure_tree(path: &Path, service_permission: Option<&str>) -> Result<()> {
    reject_tree_links(path)?;
    let service_rule = if let Some(permission) = service_permission {
        let permission = match permission {
            "RX" => "ReadAndExecute",
            "M" => "Modify",
            value => value,
        };
        format!(
            "$acl.AddAccessRule([Security.AccessControl.FileSystemAccessRule]::new('{}','{}','ContainerInherit,ObjectInherit','None','Allow'));",
            ACCOUNT, permission
        )
    } else {
        String::new()
    };
    // Build a new protected DACL rather than retaining old explicit Everyone/Users grants.
    powershell(&format!(
        "$ErrorActionPreference='Stop'; $acl=[Security.AccessControl.DirectorySecurity]::new(); $acl.SetAccessRuleProtection($true,$false); $admin=[Security.Principal.SecurityIdentifier]::new('S-1-5-32-544'); $system=[Security.Principal.SecurityIdentifier]::new('S-1-5-18'); $acl.SetOwner($admin); foreach ($sid in @($admin,$system)) {{ $acl.AddAccessRule([Security.AccessControl.FileSystemAccessRule]::new($sid,'FullControl','ContainerInherit,ObjectInherit','None','Allow')) }} {service_rule} Set-Acl -LiteralPath {} -AclObject $acl",
        literal(path.as_os_str())?
    ))?;
    for entry in fs::read_dir(path)? {
        checked(
            Command::new(system_tool("icacls.exe"))
                .arg(entry?.path())
                .args(["/reset", "/T"]),
        )?;
    }
    Ok(())
}
impl Platform for Windows {
    fn preflight(&self) -> Result<()> {
        powershell(
            "$ErrorActionPreference='Stop'; if (-not ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) { throw 'Run /install or /uninstall in an elevated Administrator console' }",
        )?;
        Ok(())
    }
    fn record(&self) -> PathBuf {
        PathBuf::from(std::env::var_os("ProgramData").unwrap_or_else(|| r"C:\ProgramData".into()))
            .join("MurmurSQL-Installer/install.json")
    }
    fn defaults(&self) -> (PathBuf, PathBuf) {
        (
            PathBuf::from(
                std::env::var_os("ProgramFiles").unwrap_or_else(|| r"C:\Program Files".into()),
            )
            .join("MurmurSQL"),
            PathBuf::from(
                std::env::var_os("ProgramData").unwrap_or_else(|| r"C:\ProgramData".into()),
            )
            .join("MurmurSQL"),
        )
    }
    fn exists(&self) -> Result<bool> {
        match manager()?.open_service(NAME, ServiceAccess::QUERY_STATUS) {
            Ok(_) => Ok(true),
            Err(e) if missing(&e) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }
    fn running(&self) -> Result<bool> {
        Ok(open()?.query_status()?.current_state != ServiceState::Stopped)
    }
    fn stop(&self) -> Result<()> {
        let service = open()?;
        let before = service.query_status()?;
        let deadline = Instant::now() + WAIT;
        if before.current_state != ServiceState::StopPending {
            service.stop()?;
        }
        wait(ServiceState::Stopped)?;
        // SCM can report Stopped just before the host exits and releases its image.
        if let Some(pid) = before.process_id.filter(|pid| *pid != 0) {
            let seconds = deadline
                .saturating_duration_since(Instant::now())
                .as_secs()
                .max(1);
            powershell(&format!(
                "$ErrorActionPreference='Stop'; if (Get-Process -Id {pid} -ErrorAction SilentlyContinue) {{ Wait-Process -Id {pid} -Timeout {seconds} }}"
            ))?;
        }
        Ok(())
    }
    fn start(&self) -> Result<()> {
        open()?.start::<&str>(&[])?;
        wait(ServiceState::Running)
    }
    fn identity(&self) -> Result<()> {
        Ok(())
    } // SCM creates the virtual service identity.
    fn prepare_filesystem(&self, installation: &Installation) -> Result<()> {
        secure_tree(&installation.install, None)?;
        secure_tree(&installation.runtime, None)
    }
    fn register(&self, installation: &Installation) -> Result<()> {
        let info = ServiceInfo {
            name: NAME.into(),
            display_name: "Murmur SQL Server".into(),
            service_type: ServiceType::OWN_PROCESS,
            start_type: ServiceStartType::AutoStart,
            error_control: ServiceErrorControl::Normal,
            executable_path: installation.executable(),
            launch_arguments: vec![
                "--service".into(),
                "--config".into(),
                installation.config().into_os_string(),
            ],
            dependencies: vec![],
            account_name: Some(ACCOUNT.into()),
            account_password: None,
        };
        let manager = manager()?;
        let service = match manager.open_service(NAME, ServiceAccess::ALL_ACCESS) {
            Ok(service) => {
                service.change_config(&info)?;
                service
            }
            Err(e) if missing(&e) => manager.create_service(&info, ServiceAccess::ALL_ACCESS)?,
            Err(e) => return Err(e.into()),
        };
        service.set_description("PostgreSQL-wire Murmur SQL server")?;
        service.set_config_service_sid_info(ServiceSidType::Unrestricted)?;
        service.set_delayed_auto_start(true)?;
        service.update_failure_actions(ServiceFailureActions {
            reset_period: ServiceFailureResetPeriod::After(Duration::from_secs(86400)),
            reboot_msg: None,
            command: None,
            actions: Some(vec![
                ServiceAction {
                    action_type: ServiceActionType::Restart,
                    delay: Duration::from_secs(60)
                };
                3
            ]),
        })?;
        service.set_failure_actions_on_non_crash_failures(true)?;
        Ok(())
    }
    fn unregister(&self) -> Result<()> {
        open()?.delete()?;
        let deadline = Instant::now() + WAIT;
        loop {
            match manager()?.open_service(NAME, ServiceAccess::QUERY_STATUS) {
                Err(e) if missing(&e) => return Ok(()),
                Err(windows_service::Error::Winapi(e)) if e.raw_os_error() == Some(1072) => (), // marked for deletion
                Err(e) => return Err(e.into()),
                Ok(_) => (),
            }
            if Instant::now() >= deadline {
                return Err(
                    "service still pending deletion; close Services management windows and retry"
                        .into(),
                );
            }
            std::thread::sleep(Duration::from_millis(200));
        }
    }
    fn permissions(&self, installation: &Installation, config: &Config) -> Result<()> {
        secure_tree(&installation.install, Some("RX"))?;
        secure_tree(&installation.runtime, Some("RX"))?;
        for path in writable_paths(installation, config)? {
            secure_tree(&path, Some("M"))?;
        }
        for path in credential_paths(installation, config)? {
            if !path.is_file() {
                return Err(format!("credential file does not exist: {}", path.display()).into());
            }
            acl(&path, ACCOUNT, "R", false, false)?;
        }
        Ok(())
    }
    fn protect_record(&self) -> Result<()> {
        secure_tree(self.record().parent().unwrap(), None)
    }
    fn event_source(&self) -> Result<()> {
        powershell(
            "$ErrorActionPreference='Stop'; if (-not [Diagnostics.EventLog]::SourceExists('MurmurSQL')) { New-EventLog -LogName Application -Source MurmurSQL }",
        )?;
        Ok(())
    }
}

static HOST_CONFIG: OnceLock<PathBuf> = OnceLock::new();
static HOST_EXIT: AtomicU8 = AtomicU8::new(1);
windows_service::define_windows_service!(service_entry, service_main);
pub(super) fn host(args: &[OsString]) -> std::process::ExitCode {
    let result = (|| -> Result<()> {
        if args.len() != 3 || args[1] != "--config" {
            return Err("internal service mode requires --service --config <absolute path>".into());
        }
        let path = PathBuf::from(&args[2]);
        if !path.is_absolute() {
            return Err("service configuration path must be absolute".into());
        }
        HOST_CONFIG
            .set(path)
            .map_err(|_| "service host already initialized")?;
        service_dispatcher::start(NAME, service_entry)?;
        Ok(())
    })();
    if let Err(e) = result {
        eprintln!("Windows service host: {e}");
        return std::process::ExitCode::from(1);
    }
    std::process::ExitCode::from(HOST_EXIT.load(Ordering::SeqCst))
}
fn status(state: ServiceState, exit: u32) -> ServiceStatus {
    ServiceStatus {
        service_type: ServiceType::OWN_PROCESS,
        current_state: state,
        controls_accepted: if state == ServiceState::Running {
            ServiceControlAccept::STOP | ServiceControlAccept::SHUTDOWN
        } else {
            ServiceControlAccept::empty()
        },
        exit_code: ServiceExitCode::Win32(exit),
        checkpoint: if state == ServiceState::StartPending || state == ServiceState::StopPending {
            1
        } else {
            0
        },
        wait_hint: if state == ServiceState::StartPending || state == ServiceState::StopPending {
            WAIT
        } else {
            Duration::ZERO
        },
        process_id: None,
    }
}
fn service_main(_: Vec<OsString>) {
    let result = run_host();
    let code = match result {
        Ok(code) => {
            if code != 0 {
                event_error(&format!(
                    "MurmurSQL exited with code {code}; inspect the configured server logs."
                ));
            }
            code
        }
        Err(e) => {
            event_error(&e.to_string());
            1
        }
    };
    HOST_EXIT.store(code, Ordering::SeqCst);
}
fn run_host() -> Result<u8> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let tx = Mutex::new(Some(tx));
    let handler = service_control_handler::register(NAME, move |event| match event {
        ServiceControl::Stop | ServiceControl::Shutdown => {
            if let Some(tx) = tx.lock().unwrap().take() {
                let _ = tx.send(());
            }
            ServiceControlHandlerResult::NoError
        }
        ServiceControl::Interrogate => ServiceControlHandlerResult::NoError,
        _ => ServiceControlHandlerResult::NotImplemented,
    })?;
    handler.set_service_status(status(ServiceState::StartPending, 0))?;
    let result = (|| -> Result<u8> {
        let path = HOST_CONFIG.get().ok_or("missing service config")?.clone();
        std::env::set_current_dir(
            std::env::current_exe()?
                .parent()
                .ok_or("missing install directory")?,
        )?;
        let config = Config::load(&path)?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        let _logging = crate::logging::init(&config.logging, runtime.handle())?;
        let ready_handler = handler;
        let stop_handler = handler;
        let code = runtime.block_on(crate::server::serve_managed(
            path,
            config,
            async move {
                let _ = rx.await;
                let _ = stop_handler.set_service_status(status(ServiceState::StopPending, 0));
            },
            move || {
                ready_handler
                    .set_service_status(status(ServiceState::Running, 0))
                    .map_err(|e| e.to_string())
            },
        ));
        // ExitCode has no stable numeric accessor. Server has only 0/1/2.
        Ok(if code == std::process::ExitCode::SUCCESS {
            0
        } else if code == std::process::ExitCode::from(2) {
            2
        } else {
            1
        })
    })();
    let exit = result.as_ref().copied().unwrap_or(1);
    handler.set_service_status(status(ServiceState::Stopped, exit as u32))?;
    result
}
fn event_error(message: &str) {
    if let Ok(message) = literal(OsStr::new(message)) {
        let _ = powershell(&format!(
            "Write-EventLog -LogName Application -Source MurmurSQL -EntryType Error -EventId 1 -Message {message}"
        ));
    }
}

pub(super) fn defer_application_deletion(paths: &[PathBuf], source: &Path) -> Result<bool> {
    if !paths
        .iter()
        .any(|p| fs::canonicalize(p).ok() == fs::canonicalize(source).ok())
    {
        return Ok(false);
    }
    let log = std::env::current_dir()?.join("uninstallation.log");
    let mut script = format!(
        "$ErrorActionPreference='Stop'; Wait-Process -Id {} -ErrorAction SilentlyContinue; ",
        std::process::id()
    );
    for path in paths {
        let path = literal(path.as_os_str())?;
        script.push_str(&format!("$deadline=[DateTime]::UtcNow.AddSeconds(30); while (Test-Path -LiteralPath {path}) {{ try {{ Remove-Item -LiteralPath {path} -Force }} catch {{ if ([DateTime]::UtcNow -ge $deadline) {{ Add-Content -LiteralPath {} -Value 'Deferred application deletion failed; remove the remaining executable manually.'; break }}; Start-Sleep -Milliseconds 200 }} }}; ", literal(log.as_os_str())?));
    }
    let parent = paths[0].parent().unwrap();
    // Non-recursive directory deletion preserves unrelated application files.
    script.push_str(&format!(
        "try {{ [IO.Directory]::Delete({}) }} catch {{}}",
        literal(parent.as_os_str())?
    ));
    use base64::Engine;
    let encoded = base64::engine::general_purpose::STANDARD.encode(
        script
            .encode_utf16()
            .flat_map(u16::to_le_bytes)
            .collect::<Vec<_>>(),
    );
    Command::new(powershell_exe())
        .args(["-NoProfile", "-NonInteractive", "-EncodedCommand", &encoded])
        .current_dir(std::env::temp_dir())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn cleanup_paths_are_literal_powershell_values() {
        assert_eq!(
            literal(OsStr::new("C:\\it's $(literal)")).unwrap(),
            "'C:\\it''s $(literal)'"
        );
        assert!(literal(OsStr::new("C:\\bad\npath")).is_err());
    }
}
