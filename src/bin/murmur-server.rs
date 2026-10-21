//! Murmur server: Postgres-wire access to one Murmur database.
//!
//! Usage:
//!   murmur-server [--config <path>] [--validate-config]
//!
//! Exit codes: 0 ok, 1 configuration error, 2 bind error.

use murmur_sql::config::Config;
use std::process::ExitCode;

const VERSION: &str = env!("CARGO_PKG_VERSION");

fn usage() -> String {
    "usage: murmur-server [--config <path>] [--validate-config]\n\
     \n\
     service commands (run elevated):\n\
     \t/install\tinteractive service installation/upgrade\n\
     \t/uninstall\tinteractive service removal\n\
     \n\
     options:\n\
     \t--config <path>\tconfig.toml path (default: ./config.toml)\n\
     \t--validate-config\tload and validate the config, then exit\n\
     \t-v, --version\tprint version\n\
     \t-h, --help\tprint help\n"
        .into()
}

fn main() -> ExitCode {
    let arguments: Vec<_> = std::env::args_os().skip(1).collect();
    if let Some(code) = murmur_sql::service::dispatch(&arguments) { return code; }
    if let Some(code) = murmur_sql::service::dispatch_host(&arguments) { return code; }
    let mut config_path = "./config.toml".to_string();
    let mut validate_only = false;
    // Reuse the OsString pass: `args()` panics on non-UTF8 argv.
    let mut rest = Vec::with_capacity(arguments.len());
    for arg in &arguments {
        match arg.clone().into_string() {
            Ok(s) => rest.push(s),
            Err(_) => {
                eprintln!("invalid argument: not valid UTF-8\n{}", usage());
                return ExitCode::from(1);
            }
        }
    }
    let mut args = rest.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                let Some(path) = args.next() else {
                    eprintln!("--config needs a path");
                    return ExitCode::from(1);
                };
                config_path = path;
            }
            "--validate-config" => validate_only = true,
            "-v" | "--version" => {
                println!("murmur-server {VERSION}");
                return ExitCode::SUCCESS;
            }
            "-h" | "--help" => {
                print!("{}", usage());
                return ExitCode::SUCCESS;
            }
            other => {
                eprintln!("unknown argument {other:?}\n{}", usage());
                return ExitCode::from(1);
            }
        }
    }

    let config = match Config::load(&config_path) {
        Ok(config) => config,
        Err(e) => {
            eprintln!("cannot load {config_path}: {e}");
            return ExitCode::from(1);
        }
    };
    if validate_only {
        if let Err(e) = murmur_sql::server::tls::build_acceptor(
            &config.server.tls,
            config.server.production,
        ) {
            eprintln!("invalid server TLS configuration: {e}");
            return ExitCode::from(1);
        }
        #[cfg(feature = "replication")]
        if config.replication.enabled {
            if let Err(e) = config.replication.to_replication_config() {
                eprintln!("invalid replication configuration: {e}");
                return ExitCode::from(1);
            }
        }
        println!("config OK (bind {}, data {:?})", config.server.bind, config.storage.data_dir);
        return ExitCode::SUCCESS;
    }

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(e) => {
            eprintln!("cannot start async runtime: {e}");
            return ExitCode::from(1);
        }
    };
    let _log = match murmur_sql::logging::init(&config.logging, runtime.handle()) {
        Ok(guard) => guard,
        Err(e) => {
            eprintln!("cannot initialize logging: {e}");
            return ExitCode::from(1);
        }
    };
    runtime.block_on(murmur_sql::server::serve(config_path.into(), config))
}
