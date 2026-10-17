//! Managed search declarations; index maintenance stays on the server.
use crate::{Client, Error, Result, quote};

/// Distance used by a managed vector index.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum VectorDistance {
    #[default]
    L2,
    Cosine,
    L1,
}

impl VectorDistance {
    fn sql(self) -> &'static str {
        match self {
            Self::L2 => "l2",
            Self::Cosine => "cosine",
            Self::L1 => "l1",
        }
    }
}

/// Vector values are finite little-endian float32 BLOBs. Dimensions: 1..=8192.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VectorOptions {
    pub dimensions: usize,
    pub distance: VectorDistance,
}

/// Local disposable index status. Disabled columns have no status.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexStatus {
    pub index_name: String,
    pub ready: bool,
    pub rebuilding: bool,
    pub pending_keys: i64,
    pub queued_bytes: i64,
    pub error: Option<String>,
}

/// An application column's FTS declaration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnFts {
    pub table: String,
    pub column: String,
    pub enabled: bool,
    pub status: Option<IndexStatus>,
}

/// An application column's vector declaration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnVector {
    pub table: String,
    pub column: String,
    pub options: Option<VectorOptions>,
    pub status: Option<IndexStatus>,
}

fn names(table: &str, column: &str) -> Result<String> {
    if table.trim().is_empty() || column.trim().is_empty() {
        return Err(Error::Invalid(
            "table and column names must not be empty".into(),
        ));
    }
    Ok(format!("{} {}", quote(table), quote(column)))
}

fn status(row: &tokio_postgres::Row, offset: usize) -> Result<Option<IndexStatus>> {
    let Some(index_name) = row.try_get(offset)? else {
        return Ok(None);
    };
    Ok(Some(IndexStatus {
        index_name,
        ready: row.try_get::<_, String>(offset + 1)? == "true",
        rebuilding: row.try_get::<_, String>(offset + 2)? == "true",
        pending_keys: row.try_get(offset + 3)?,
        queued_bytes: row.try_get(offset + 4)?,
        error: row.try_get(offset + 5)?,
    }))
}

impl Client {
    /// Enable a TEXT column in managed FTS5. Requires one INTEGER application key.
    pub async fn set_column_fts(&self, table: &str, column: &str, enabled: bool) -> Result<()> {
        self.simple(&format!(
            "MURMUR SET COLUMN {} FTS {enabled}",
            names(table, column)?
        ))
        .await?;
        Ok(())
    }

    /// Wait for local indexes to include collected writes; does not wait for peers.
    pub async fn refresh_indexes(&self) -> Result<()> {
        self.simple("MURMUR REFRESH INDEXES").await?;
        Ok(())
    }

    /// List all application columns with nullable local FTS status.
    pub async fn column_fts(&self) -> Result<Vec<ColumnFts>> {
        self.query("MURMUR SHOW COLUMNS FTS", &[])
            .await?
            .iter()
            .map(|r| {
                Ok(ColumnFts {
                    table: r.try_get(0)?,
                    column: r.try_get(1)?,
                    enabled: r.try_get::<_, String>(2)? == "true",
                    status: status(r, 3)?,
                })
            })
            .collect()
    }

    /// Configure a BLOB column's vector index; None disables it.
    /// Requires the server vector feature and one INTEGER application key.
    pub async fn set_column_vector(
        &self,
        table: &str,
        column: &str,
        options: Option<VectorOptions>,
    ) -> Result<()> {
        let target = names(table, column)?;
        let setting = match options {
            None => "false".to_string(),
            Some(o) => {
                if !(1..=8192).contains(&o.dimensions) {
                    return Err(Error::Invalid("vector dimensions must be 1..8192".into()));
                }
                format!(
                    "true DIMENSIONS {} DISTANCE {}",
                    o.dimensions,
                    o.distance.sql()
                )
            }
        };
        self.simple(&format!("MURMUR SET COLUMN {target} VECTOR {setting}"))
            .await?;
        Ok(())
    }

    /// List all application columns with nullable vector configuration and status.
    pub async fn column_vectors(&self) -> Result<Vec<ColumnVector>> {
        self.query("MURMUR SHOW COLUMNS VECTOR", &[])
            .await?
            .iter()
            .map(|r| {
                let dimensions: Option<i64> = r.try_get(3)?;
                let options = match dimensions {
                    None => None,
                    Some(n) => Some(VectorOptions {
                        dimensions: usize::try_from(n).map_err(|_| {
                            Error::Invalid("invalid vector dimensions from server".into())
                        })?,
                        distance: match r.try_get::<_, String>(4)?.as_str() {
                            "l2" => VectorDistance::L2,
                            "cosine" => VectorDistance::Cosine,
                            "l1" => VectorDistance::L1,
                            _ => {
                                return Err(Error::Invalid(
                                    "invalid vector distance from server".into(),
                                ));
                            }
                        },
                    }),
                };
                Ok(ColumnVector {
                    table: r.try_get(0)?,
                    column: r.try_get(1)?,
                    options,
                    status: status(r, 5)?,
                })
            })
            .collect()
    }
}
