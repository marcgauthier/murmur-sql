//! Murmur-SQL: PostgreSQL-wire front over the embedded Murmur engine.
//!
//! All storage, replication, and conflict-resolution behavior lives in the
//! `murmur` crate (`../MURMUR`). This crate only maps configuration, SQL
//! sessions, and administration onto that engine.
pub mod config;
pub mod logging;
pub mod server;
