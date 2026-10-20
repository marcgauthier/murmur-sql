//! Byte fixtures for exercising explicit backup SQL, not a client command API.
use base64::Engine as _;
use murmur_client::{Client, Error, Result};

/// Execute the supplied DOWNLOAD SQL with bounded ranges and check offsets.
pub async fn download(client: &Client, statement: &str, output: &mut Vec<u8>) -> Result<()> {
    let mut offset = 0i64;
    loop {
        let rows = client
            .query(&format!("{statement} OFFSET {offset} LIMIT 1048576"), &[])
            .await?;
        if rows.len() != 1 {
            return Err(Error::Protocol("expected one backup range".into()));
        }
        let actual: i64 = rows[0].try_get(0)?;
        let total: i64 = rows[0].try_get(1)?;
        let chunk: Vec<u8> = rows[0].try_get(2)?;
        if actual != offset || total < offset || chunk.len() as i64 > total - offset {
            return Err(Error::Protocol("invalid backup range".into()));
        }
        output.extend_from_slice(&chunk);
        offset += chunk.len() as i64;
        if offset == total {
            return Ok(());
        }
        if chunk.is_empty() {
            return Err(Error::Protocol("empty nonfinal backup range".into()));
        }
    }
}

/// Transfer a byte fixture using explicit backup SQL; abort on failure.
pub async fn upload(client: &Client, name: &str, bytes: &[u8]) -> Result<()> {
    let quoted = crate::sql_literal(name);
    client
        .simple(&format!("MURMUR BACKUP UPLOAD BEGIN {quoted}"))
        .await?;
    let result = async {
        for chunk in bytes.chunks(256 << 10) {
            client
                .simple(&format!(
                    "MURMUR BACKUP UPLOAD APPEND {quoted} '{}'",
                    base64::engine::general_purpose::STANDARD.encode(chunk)
                ))
                .await?;
        }
        client
            .simple(&format!("MURMUR BACKUP UPLOAD FINISH {quoted}"))
            .await?;
        Ok(())
    }
    .await;
    if result.is_err() {
        let _ = client
            .simple(&format!("MURMUR BACKUP UPLOAD ABORT {quoted}"))
            .await;
    }
    result
}
