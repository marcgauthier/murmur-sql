package murmur

import (
	"context"
	"errors"
	"strconv"
	"strings"
)

// VectorDistance selects the distance used by a managed vector index.
type VectorDistance string

const (
	VectorL2     VectorDistance = "l2"
	VectorCosine VectorDistance = "cosine"
	VectorL1     VectorDistance = "l1"
)

// VectorOptions describes a persistent column vector declaration. Dimensions
// must be 1..8192; an empty Distance defaults to L2. Values are finite float32s
// in little-endian BLOBs. The source table needs one INTEGER application key.
type VectorOptions struct {
	Dimensions int
	Distance   VectorDistance
}

// SetColumnVector configures a managed index; nil disables it. Declarations
// persist in Spool and replicate under existing schema-authority rules. Index
// data is rebuilt locally. Invalid existing vectors make configuration fail.
func (db *DB) SetColumnVector(ctx context.Context, table, column string, options *VectorOptions) error {
	if strings.TrimSpace(table) == "" || strings.TrimSpace(column) == "" {
		return errors.New("murmur: table and column names must not be empty")
	}
	command := "MURMUR SET COLUMN " + quoteIdent(table) + " " + quoteIdent(column) + " VECTOR "
	if options == nil {
		command += "false"
	} else {
		if options.Dimensions < 1 || options.Dimensions > 8192 {
			return errors.New("murmur: vector dimensions must be 1..8192")
		}
		distance := options.Distance
		if distance == "" {
			distance = VectorL2
		}
		switch distance {
		case VectorL2, VectorCosine, VectorL1:
		default:
			return errors.New("murmur: vector distance must be l2, cosine, or l1")
		}
		command += "true DIMENSIONS " + strconv.Itoa(options.Dimensions) + " DISTANCE " + string(distance)
	}
	_, err := db.ExecContext(ctx, command)
	return err
}

// ColumnVector reports a declaration and its local index status. Disabled
// columns have nil configuration/status fields; index names need SQL quoting.
type ColumnVector struct {
	Table       string
	Column      string
	Enabled     bool
	Dimensions  *int64
	Distance    *VectorDistance
	IndexName   *string
	Ready       *bool
	Rebuilding  *bool
	PendingKeys *int64
	QueuedBytes *int64
	Error       *string
}

// ColumnVectors lists every application column with configuration and status.
func (db *DB) ColumnVectors(ctx context.Context) ([]ColumnVector, error) {
	rows, err := db.QueryContext(ctx, "MURMUR SHOW COLUMNS VECTOR")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var columns []ColumnVector
	for rows.Next() {
		var column ColumnVector
		if err := rows.Scan(&column.Table, &column.Column, &column.Enabled, &column.Dimensions, &column.Distance, &column.IndexName, &column.Ready, &column.Rebuilding, &column.PendingKeys, &column.QueuedBytes, &column.Error); err != nil {
			return nil, err
		}
		columns = append(columns, column)
	}
	return columns, rows.Err()
}
