//! In-memory SQLite with encrypted background persistence and LWW metadata.
#[cfg(feature="replication")]
mod snapshot_stream;
mod logical_store;
mod backup;
#[cfg(feature = "replication")]
mod files;
#[cfg(feature = "replication")]
pub use murmur_replication::files::{Availability as FileAvailability, Config as FileConfig, KeyRotationStatus as FileKeyRotationStatus, Metadata as FileMetadata};

mod capture;
pub mod config;
pub mod logging;
pub mod server;
mod database;
mod merge;
mod metadata;
mod model;
mod schema;
mod schema_codec;
mod primary_key;
mod search;
mod progress;
mod table_replication;
#[cfg(feature = "replication")]
mod retention;
#[cfg(feature = "replication")]
pub use murmur_replication::retention::{PeerRetention, RetentionStatus};
mod storage;
#[cfg(feature = "replication")]
mod network;
#[cfg(feature = "replication")]
mod policy;
#[cfg(feature = "replication")]
mod high_low;
#[cfg(feature = "replication")]
pub use murmur_replication as replication;

mod subscription;
pub mod scheduler;
pub use scheduler::{
    SchedulerClassStats, SchedulerConfig, SchedulerSnapshot, Ticket, WriterClass, WriterScheduler,
};

pub use database::{Database, Sql};
pub use search::{DistanceMetric, Fts5Index, SearchIndex, SearchIndexStatus, VectorColumnConfig, VectorIndex};
pub use model::{
    ApplicationKey, Cell, Column, MergePolicy, Migration, OpenMode, OpenPhase, OpenProgress, Options, ReplicaState,
    RestoreOptions, RowState, Schema, SqliteMetrics, SpoolMetrics, Status, Table, TableMapping,
    Tombstone, Value, Version,
};
pub use spool::{DataKeyInfo, KeyInventory, KeyStatus};
pub use subscription::{
    ChangeType, RowChange, Subscription, SubscriptionCursor, SubscriptionEvent, SubscriptionOptions,
};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("SQLite: {0}")]
    Sql(#[from] rusqlite::Error),
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("storage: {0}")]
    Storage(String),
    #[error("incorrect encryption key")]
    WrongKey,
    #[error("invalid configuration or operation: {0}")]
    Invalid(String),
    #[error("corrupt or unsupported storage: {0}")]
    Corrupt(String),
    #[error("resource limit: {0}")]
    Limit(String),
    #[error("database identity or schema history mismatch")]
    SchemaMismatch,
    #[error("checkpoint queue full or merge baseline changed")]
    Busy,
    #[error("database is closed")]
    Closed,
    #[error("SQL callback panicked; transaction rolled back")]
    CallbackPanicked,
}
impl From<spool::Error> for Error {
    fn from(e: spool::Error) -> Self {
        Self::Storage(e.to_string())
    }
}
pub type Result<T> = std::result::Result<T, Error>;

use rusqlite::{Connection, session::Session};

/// Open a new in-memory SQLite database.
pub fn open_in_memory() -> rusqlite::Result<Connection> {
    Connection::open_in_memory()
}

/// Start recording changes to every table in the database.
///
/// Tables must have a primary key for SQLite's session extension to capture
/// their changes.
pub fn start_session(connection: &Connection) -> rusqlite::Result<Session<'_>> {
    let mut session = Session::new(connection)?;
    session.attach(None::<&str>)?;
    Ok(session)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::fallible_streaming_iterator::FallibleStreamingIterator;

    #[test]
    fn session_records_changes_in_memory_database() -> Result<()> {
        let connection = open_in_memory()?;
        connection.execute(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
            [],
        )?;

        let mut session = start_session(&connection)?;
        connection.execute("INSERT INTO items (id, value) VALUES (1, 'first')", [])?;

        let changeset = session.changeset()?;
        let mut changes = changeset.iter()?;
        assert!(changes.next()?.is_some());
        assert!(changes.next()?.is_none());
        Ok(())
    }
}
