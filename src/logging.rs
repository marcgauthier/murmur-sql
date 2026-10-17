//! Server logging: daily-rotated files with retention, optional console
//! output, and best-effort shipping to VictoriaLogs.
//!
//! Files are named `<prefix>.YYYY-MM-DD` inside `logging.dir`. Rotation is
//! daily; [`enforce_retention`] deletes files older than `retention_days`.
//! VictoriaLogs receives Loki-compatible JSON batches at
//! `{url}/loki/api/v1/push`; the file log stays primary and remote failures
//! never block the server.

use crate::config::{LoggingConfig, VictoriaLogsConfig};
use crate::{Error, Result};
use std::path::Path;
use std::time::Duration;
use tracing_subscriber::layer::{Layer as _, SubscriberExt};

/// How often the VictoriaLogs task flushes a partial batch in production.
pub const VICTORIALOGS_FLUSH_INTERVAL: Duration = Duration::from_secs(2);
const VICTORIALOGS_BATCH_LINES: usize = 100;
const VICTORIALOGS_QUEUE_LINES: usize = 1000;

/// Holds logging resources. Dropping flushes the file writer and stops the
/// VictoriaLogs task.
pub struct LogGuard {
    _file: tracing_appender::non_blocking::WorkerGuard,
    _victorialogs: Option<VictoriaLogsGuard>,
}

/// Build the subscriber and its guard without installing it globally. Tests
/// use this with `tracing::subscriber::with_default`.
pub fn build(
    config: &LoggingConfig,
    runtime: &tokio::runtime::Handle,
) -> Result<(impl tracing::Subscriber + Send + Sync, LogGuard)> {
    config.validate()?;
    std::fs::create_dir_all(&config.dir)
        .map_err(|e| Error::Invalid(format!("cannot create log dir {:?}: {e}", config.dir)))?;
    enforce_retention(&config.dir, &config.file_prefix, config.retention_days)?;
    let level = parse_level(&config.level)?;
    let file_appender =
        tracing_appender::rolling::daily(&config.dir, &config.file_prefix);
    let (file_writer, file_guard) = tracing_appender::non_blocking(file_appender);
    let file_layer = tracing_subscriber::fmt::layer()
        .with_writer(file_writer)
        .with_ansi(false)
        .with_filter(level);
    let console_layer = config.console.then(|| {
        tracing_subscriber::fmt::layer()
            .with_writer(std::io::stderr)
            .with_filter(level)
    });
    let (vl_layer, vl_guard) = if config.victorialogs.enabled {
        let (layer, guard) = VictoriaLogsSink::spawn(
            runtime,
            &config.victorialogs,
            level,
            VICTORIALOGS_FLUSH_INTERVAL,
        );
        (Some(layer), Some(guard))
    } else {
        (None, None)
    };
    let subscriber = tracing_subscriber::registry()
        .with(file_layer)
        .with(console_layer)
        .with(vl_layer);
    Ok((
        subscriber,
        LogGuard {
            _file: file_guard,
            _victorialogs: vl_guard,
        },
    ))
}

/// Build logging and install it as the global default.
pub fn init(
    config: &LoggingConfig,
    runtime: &tokio::runtime::Handle,
) -> Result<LogGuard> {
    let (subscriber, guard) = build(config, runtime)?;
    tracing::subscriber::set_global_default(subscriber)
        .map_err(|e| Error::Invalid(format!("logging is already initialized: {e}")))?;
    Ok(guard)
}

fn parse_level(level: &str) -> Result<tracing::level_filters::LevelFilter> {
    use tracing::level_filters::LevelFilter;
    match level.to_ascii_lowercase().as_str() {
        "error" => Ok(LevelFilter::ERROR),
        "warn" => Ok(LevelFilter::WARN),
        "info" => Ok(LevelFilter::INFO),
        "debug" => Ok(LevelFilter::DEBUG),
        "trace" => Ok(LevelFilter::TRACE),
        other => Err(Error::Invalid(format!("unknown logging.level {other:?}"))),
    }
}

/// Delete `<prefix>.YYYY-MM-DD` files in `dir` older than `retention_days`.
/// Returns the number of files removed. Files that do not match the pattern
/// are left untouched.
pub fn enforce_retention(dir: impl AsRef<Path>, prefix: &str, retention_days: u32) -> Result<usize> {
    let today = unix_days(std::time::SystemTime::now());
    let mut removed = 0;
    let entries = std::fs::read_dir(dir.as_ref())
        .map_err(|e| Error::Invalid(format!("cannot list log dir {:?}: {e}", dir.as_ref())))?;
    for entry in entries {
        let entry = entry.map_err(|e| Error::Invalid(format!("cannot list log dir: {e}")))?;
        let name = entry.file_name().to_string_lossy().into_owned();
        let Some(date) = name.strip_prefix(prefix).and_then(|s| s.strip_prefix('.')) else {
            continue;
        };
        let Some(days) = parse_log_date(date).map(civil_to_days) else {
            continue;
        };
        if today.saturating_sub(days) > retention_days as u64 {
            std::fs::remove_file(entry.path())
                .map_err(|e| Error::Invalid(format!("cannot remove old log {name:?}: {e}")))?;
            removed += 1;
        }
    }
    Ok(removed)
}

fn unix_days(now: std::time::SystemTime) -> u64 {
    now.duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        / 86400
}

fn parse_log_date(date: &str) -> Option<(i64, u32, u32)> {
    let mut parts = date.split('-');
    let (y, m, d) = (parts.next()?, parts.next()?, parts.next()?);
    if parts.next().is_some() || y.len() != 4 || m.len() != 2 || d.len() != 2 {
        return None;
    }
    let (y, m, d): (i64, u32, u32) = (y.parse().ok()?, m.parse().ok()?, d.parse().ok()?);
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return None;
    }
    Some((y, m, d))
}

/// Days since 1970-01-01 (Howard Hinnant's days-from-civil).
fn civil_to_days((y, m, d): (i64, u32, u32)) -> u64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400) as u64;
    let mp = ((m as i64 + 9) % 12) as u64;
    let doy = (153 * mp + 2) / 5 + (d as u64 - 1);
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    (era as u64 * 146097 + doe).saturating_sub(719468)
}

struct VictoriaLogsGuard {
    _task: tokio::task::JoinHandle<()>,
}

struct VictoriaLogsSink;

impl VictoriaLogsSink {
    fn spawn(
        runtime: &tokio::runtime::Handle,
        config: &VictoriaLogsConfig,
        level: tracing::level_filters::LevelFilter,
        flush_interval: Duration,
    ) -> (VictoriaLogsLayer, VictoriaLogsGuard) {
        let (tx, rx) = tokio::sync::mpsc::channel(VICTORIALOGS_QUEUE_LINES);
        let task = runtime.spawn(push_task(
            rx,
            config.clone(),
            flush_interval,
        ));
        (
            VictoriaLogsLayer { tx, level },
            VictoriaLogsGuard { _task: task },
        )
    }
}

struct VictoriaLogsLayer {
    tx: tokio::sync::mpsc::Sender<String>,
    level: tracing::level_filters::LevelFilter,
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for VictoriaLogsLayer {
    fn enabled(&self, metadata: &tracing::Metadata<'_>, _: tracing_subscriber::layer::Context<'_, S>) -> bool {
        self.level >= *metadata.level()
    }

    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut visitor = FieldVisitor::default();
        event.record(&mut visitor);
        let line = format!(
            "{} {}: {}",
            event.metadata().level(),
            event.metadata().target(),
            visitor.message()
        );
        // Best effort: a slow or dead VictoriaLogs must not block logging.
        let _ = self.tx.try_send(line);
    }
}

#[derive(Default)]
struct FieldVisitor {
    message: Option<String>,
    extra: Vec<(String, String)>,
}

impl FieldVisitor {
    fn message(&self) -> String {
        let mut out = self.message.clone().unwrap_or_default();
        for (k, v) in &self.extra {
            if !out.is_empty() {
                out.push(' ');
            }
            out.push_str(k);
            out.push('=');
            out.push_str(v);
        }
        out
    }
}

impl tracing::field::Visit for FieldVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.record_str(field, &format!("{value:?}"));
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "message" {
            self.message = Some(value.to_owned());
        } else {
            self.extra.push((field.name().to_owned(), value.to_owned()));
        }
    }
}

/// Build a Loki push body. `entries` are `(nanosecond timestamp, line)`.
fn loki_body(
    labels: &std::collections::BTreeMap<String, String>,
    entries: &[(String, String)],
) -> serde_json::Value {
    let mut stream = serde_json::Map::new();
    for (k, v) in labels {
        stream.insert(k.clone(), serde_json::Value::String(v.clone()));
    }
    serde_json::json!({
        "streams": [{
            "stream": stream,
            "values": entries.iter().map(|(ts, line)| [ts, line]).collect::<Vec<_>>(),
        }]
    })
}

async fn push_task(
    mut rx: tokio::sync::mpsc::Receiver<String>,
    config: VictoriaLogsConfig,
    flush_interval: Duration,
) {
    let client = match reqwest::Client::builder().build() {
        Ok(client) => client,
        Err(e) => {
            eprintln!("victorialogs sink disabled: cannot build HTTP client: {e}");
            return;
        }
    };
    let url = format!("{}/loki/api/v1/push", config.url.trim_end_matches('/'));
    let mut batch: Vec<(String, String)> = Vec::new();
    let mut last_error = String::new();
    let mut flush = tokio::time::interval(flush_interval);
    flush.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            line = rx.recv() => {
                let Some(line) = line else {
                    if !batch.is_empty() {
                        push_batch(&client, &url, &config, std::mem::take(&mut batch), &mut last_error).await;
                    }
                    return;
                };
                batch.push((unix_nanos(), line));
                if batch.len() >= VICTORIALOGS_BATCH_LINES {
                    push_batch(&client, &url, &config, std::mem::take(&mut batch), &mut last_error).await;
                    flush.reset();
                }
            }
            _ = flush.tick() => {
                if !batch.is_empty() {
                    push_batch(&client, &url, &config, std::mem::take(&mut batch), &mut last_error).await;
                }
            }
        }
    }
}

async fn push_batch(
    client: &reqwest::Client,
    url: &str,
    config: &VictoriaLogsConfig,
    batch: Vec<(String, String)>,
    last_error: &mut String,
) {
    let body = loki_body(&config.labels, &batch);
    let mut request = client.post(url).json(&body);
    if let Some(token) = &config.bearer_token {
        request = request.bearer_auth(token);
    } else if config.username.is_some() || config.password.is_some() {
        request = request.basic_auth(
            config.username.clone().unwrap_or_default(),
            config.password.clone(),
        );
    }
    match request.send().await.and_then(|r| r.error_for_status()) {
        Ok(_) => last_error.clear(),
        Err(e) => {
            let message = e.to_string();
            if message != *last_error {
                eprintln!("victorialogs push failed, retrying in background: {message}");
                *last_error = message;
            }
        }
    }
}

fn unix_nanos() -> String {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retention_removes_only_old_matching_files() {
        let dir = tempfile::tempdir().unwrap();
        // civil_to_days must agree with unix time for known dates.
        assert_eq!(civil_to_days((1970, 1, 1)), 0);
        assert_eq!(civil_to_days((2026, 10, 16)), 20742);
        for name in ["murmur.2020-01-01", "murmur.2030-13-40", "other.2020-01-01", "murmur.notes"] {
            std::fs::write(dir.path().join(name), "x").unwrap();
        }
        // A file dated securely in the past is removed; malformed and foreign
        // names are kept.
        let removed = enforce_retention(dir.path(), "murmur", 14).unwrap();
        assert_eq!(removed, 1);
        assert!(!dir.path().join("murmur.2020-01-01").exists());
        assert!(dir.path().join("murmur.2030-13-40").exists());
        assert!(dir.path().join("other.2020-01-01").exists());
        assert!(dir.path().join("murmur.notes").exists());
    }

    #[test]
    fn loki_body_shape_matches_push_api() {
        let mut labels = std::collections::BTreeMap::new();
        labels.insert("service".into(), "murmur".into());
        let body = loki_body(&labels, &[("123".into(), "INFO m: hi".into())]);
        assert_eq!(body["streams"][0]["stream"]["service"], "murmur");
        assert_eq!(body["streams"][0]["values"][0][0], "123");
        assert_eq!(body["streams"][0]["values"][0][1], "INFO m: hi");
    }

    #[test]
    fn unknown_level_fails() {
        assert!(parse_level("verbose").is_err());
        assert!(parse_level("info").is_ok());
    }
}
