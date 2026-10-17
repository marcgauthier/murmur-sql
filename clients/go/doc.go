// Package murmur connects to a Murmur server over the PostgreSQL wire
// protocol using database/sql. SQL is the SQLite dialect; parameters use $N.
//
// Every physical connection unlocks itself with the configured encryption
// key before use (see MURMUR UNLOCK in the server manual). DDL runs through
// server-side migrations, DML autocommits per statement, and MURMUR FLUSH
// provides a durability barrier. Use SetTableReplicated and TableReplication
// for per-table replication flags stored in the server's config.toml.
// SetColumnFTS changes persistent column schema declarations; ColumnFTS reports
// local index status and RefreshIndexes waits for local search maintenance.
// SetColumnVector and ColumnVectors provide equivalent vector declarations
// and status; embeddings are application-supplied little-endian float32 BLOBs.
package murmur
