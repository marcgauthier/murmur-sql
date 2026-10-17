package murmur

import (
	"context"
	"errors"
	"strconv"
	"strings"
)

// SetColumnFTS enables or disables a TEXT column in its table's managed FTS5
// index. The table needs one INTEGER application primary key. This is a schema
// change: it persists in Spool and follows the existing schema replication rules.
func (db *DB) SetColumnFTS(ctx context.Context, table, column string, enabled bool) error {
	if strings.TrimSpace(table) == "" || strings.TrimSpace(column) == "" {
		return errors.New("murmur: table and column names must not be empty")
	}
	_, err := db.ExecContext(ctx, "MURMUR SET COLUMN "+quoteIdent(table)+" "+quoteIdent(column)+" FTS "+strconv.FormatBool(enabled))
	return err
}

// RefreshIndexes waits for local managed search indexes to include writes
// collected before this call. It does not wait for replication peers.
func (db *DB) RefreshIndexes(ctx context.Context) error {
	_, err := db.ExecContext(ctx, "MURMUR REFRESH INDEXES")
	return err
}

// ColumnFTS describes an application column's declaration and local index
// status. Disabled columns have nil index fields. Multiple enabled columns in
// one table share an index. IndexName is a SQL identifier: quote it in queries.
type ColumnFTS struct {
	Table       string
	Column      string
	Enabled     bool
	IndexName   *string
	Ready       *bool
	Rebuilding  *bool
	PendingKeys *int64
	QueuedBytes *int64
	Error       *string
}

// ColumnFTS lists declarations and local index status for all application columns.
func (db *DB) ColumnFTS(ctx context.Context) ([]ColumnFTS, error) {
	rows, err := db.QueryContext(ctx, "MURMUR SHOW COLUMNS FTS")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var columns []ColumnFTS
	for rows.Next() {
		var c ColumnFTS
		if err := rows.Scan(&c.Table, &c.Column, &c.Enabled, &c.IndexName, &c.Ready, &c.Rebuilding, &c.PendingKeys, &c.QueuedBytes, &c.Error); err != nil {
			return nil, err
		}
		columns = append(columns, c)
	}
	return columns, rows.Err()
}
