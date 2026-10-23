// Package murmur opens standard database/sql pools to Murmur-SQL using the marcgauthier/pq fork of lib/pq.
// SQL uses the SQLite dialect and $N parameters. Open authenticates and unlocks
// every physical connection before use. Execute application and administrative
// SQL directly with ExecContext, QueryContext, and QueryRowContext; the package
// provides no command wrappers or result types. Server-reported errors use
// *Error, discoverable with errors.As, without importing the underlying driver.
package murmur
