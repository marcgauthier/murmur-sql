//! Wire connect + `MURMUR UNLOCK` under the stall/overall watchdog.
//!
//! Same env knobs as the Go await-unlock
//! (`MURMUR_LIVE_UNLOCK_STALL_SECONDS=120`,
//! `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS=1800`). The watchdog observes redial
//! progress: any change in the last connection error resets the stall timer.
//! One `MURMUR UNLOCK` attempt runs to completion once issued; there is no
//! intra-open progress API over the wire, so a wedged open surfaces as an
//! overall timeout, not a stall.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use tokio_postgres::NoTls;

use crate::cluster::NodeClient;

/// Connect to `sql_addr`, authenticate `user`/`password`, and unlock with the
/// 64-hex-char `key_hex` (used raw by the server). Retries until the overall
/// timeout; fails fast when nothing changes for the stall window.
pub async fn connect_with_watchdog(
    sql_addr: SocketAddr,
    user: &str,
    password: &str,
    key_hex: &str,
) -> crate::Result<NodeClient> {
    let stall = crate::env_seconds("MURMUR_LIVE_UNLOCK_STALL_SECONDS", 120);
    let overall = crate::env_seconds("MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS", 1800);
    let start = Instant::now();
    let mut last_change = start;
    let mut last_err = String::new();
    let mut last_log = start;
    loop {
        match try_connect_once(sql_addr, user, password, key_hex).await {
            Ok(client) => return Ok(client),
            Err(e) => {
                let msg = short_error(&e);
                if msg != last_err {
                    last_change = Instant::now();
                    last_err = msg;
                }
                let elapsed = start.elapsed();
                if last_change.elapsed() >= stall {
                    return Err(crate::Error::UnlockTimeout(
                        elapsed,
                        "no unlock-progress advancement",
                    ));
                }
                if elapsed >= overall {
                    return Err(crate::Error::UnlockTimeout(elapsed, "overall unlock timeout"));
                }
                if last_log.elapsed() >= Duration::from_secs(30) {
                    last_log = Instant::now();
                    eprintln!("unlock waiting after {elapsed:?}: {last_err}");
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
    }
}

async fn try_connect_once(
    sql_addr: SocketAddr,
    user: &str,
    password: &str,
    key_hex: &str,
) -> crate::Result<NodeClient> {
    let mut config = tokio_postgres::Config::new();
    config
        .host(sql_addr.ip().to_string())
        .port(sql_addr.port())
        .user(user)
        .password(password)
        .dbname("murmur")
        .connect_timeout(Duration::from_secs(5));
    let (client, connection) = config.connect(NoTls).await?;
    let task = tokio::spawn(async move {
        let _ = connection.await;
    });
    let node = NodeClient::new(client, task);
    node.simple(&format!("MURMUR UNLOCK KEY '{key_hex}'")).await?;
    Ok(node)
}

fn short_error(error: &crate::Error) -> String {
    let mut msg: String = error
        .to_string()
        .chars()
        .filter(|c| !c.is_control())
        .take(200)
        .collect();
    if msg.is_empty() {
        msg.push_str("<empty error>");
    }
    msg
}
