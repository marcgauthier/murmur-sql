//! Post-failure diagnostics capture (Go `DumpForensics` parity).
//!
//! Status summaries go to the test log (truncated); full states go to
//! `forensics-<reason>-*` files under each node directory (preserved with
//! failure artifacts). Every fetch is infallible: timeouts and errors
//! degrade to marker strings, so forensics survives dead and wedged nodes
//! alike. This module has no panic paths by construction (no unwrap, expect,
//! indexing, or parsing that can fail the test).

use std::path::Path;
use std::time::Duration;

/// Dump diagnostics for one node over its wire connection (or record that it
/// is stopped). Never fails the test.
pub async fn dump_node_forensics(
    label: &str,
    node_dir: &Path,
    client: Option<&crate::cluster::NodeClient>,
    reason: &str,
) {
    let safe_reason: String = reason
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();

    let replication = fetch(client, "MURMUR SHOW REPLICATION").await;
    eprintln!(
        "forensics({reason}): {label} replication: {}",
        truncate(&replication, 2048)
    );
    let peers = fetch(client, "MURMUR SHOW PEERS").await;
    eprintln!(
        "forensics({reason}): {label} peers: {}",
        truncate(&peers, 8192)
    );
    let metrics = fetch(client, "MURMUR SHOW METRICS").await;

    write_file(node_dir, &format!("forensics-{safe_reason}-replication.txt"), &replication);
    write_file(node_dir, &format!("forensics-{safe_reason}-peers.txt"), &peers);
    write_file(node_dir, &format!("forensics-{safe_reason}-metrics.txt"), &metrics);
}

async fn fetch(client: Option<&crate::cluster::NodeClient>, sql: &str) -> String {
    let Some(client) = client else {
        return "<node stopped>".to_string();
    };
    match tokio::time::timeout(Duration::from_secs(10), client.simple(sql)).await {
        Ok(Ok(rows)) if !rows.is_empty() => {
            rows.iter()
                .map(|row| row.join(" | "))
                .collect::<Vec<_>>()
                .join("\n")
        }
        Ok(Ok(_)) => "<empty>".to_string(),
        Ok(Err(error)) => format!("<fetch failed: {}>", one_line(&error.to_string())),
        Err(_) => "<fetch timed out>".to_string(),
    }
}

fn write_file(node_dir: &Path, name: &str, body: &str) {
    if body.is_empty() {
        return;
    }
    if let Err(e) = std::fs::write(node_dir.join(name), body) {
        eprintln!("forensics: {name} write failed: {e}");
    }
}

fn one_line(s: &str) -> String {
    s.chars().filter(|c| !c.is_control()).take(300).collect()
}

fn truncate(s: &str, n: usize) -> String {
    if s.len() > n {
        let n = s.floor_char_boundary(n);
        format!("{}...<truncated>", &s[..n])
    } else {
        s.to_string()
    }
}
