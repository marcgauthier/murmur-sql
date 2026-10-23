// Package engine defines the boundary between the PostgreSQL shell and MURMUR-GO.
// No SQLite, replication, persistence, or conflict implementation belongs here.
package engine

import (
	"context"
	"errors"
	"fmt"

	"github.com/marcgauthier/murmur-sql/config"
)

const (
	Bool        uint32 = 16
	Bytea       uint32 = 17
	Int8        uint32 = 20
	Int2        uint32 = 21
	Int4        uint32 = 23
	Text        uint32 = 25
	Float4      uint32 = 700
	Float8      uint32 = 701
	Varchar     uint32 = 1043
	Unknown     uint32 = 705
	Date        uint32 = 1082
	Timestamp   uint32 = 1114
	TimestampTZ uint32 = 1184
	UUID        uint32 = 2950
	Numeric     uint32 = 1700
	JSON        uint32 = 114
	JSONB       uint32 = 3802
	BPChar      uint32 = 1042
	Name        uint32 = 19
)

// Value is nil, bool, int64, float64, string, or []byte. Nil means SQL NULL.
type Value = any
type Column struct {
	Name string
	OID  uint32
}
type Description struct {
	Parameters []uint32
	Columns    []Column
}
type Result struct {
	Columns []Column
	Rows    [][]Value
	Tag     string
}
type Error struct {
	Code, Message string
	Cause         error
}

func (e *Error) Error() string { return e.Message }
func (e *Error) Unwrap() error { return e.Cause }
func SQLState(err error) string {
	var e *Error
	if errors.As(err, &e) && len(e.Code) == 5 {
		return e.Code
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return "57014"
	}
	return "XX000"
}

var ErrUnavailable = &Error{Code: "0A000", Message: "database engine is intentionally disabled for this host"}

type OpenRequest struct {
	Config         config.Config
	Key            [32]byte
	TableFlagsSink func(map[string]bool) error
}
type OpenOutcome uint8

const (
	OpenedNew OpenOutcome = iota
	OpenedExisting
	AlreadyOpen
)

// Command is a shell-parsed administration request. Arguments are decoded SQL
// literals/parameters, never interpolated SQL. Operation is uppercase, e.g.
// "SHOW STATUS", "SET TABLE", "FILE UPLOAD APPEND".
type Command struct {
	Operation string
	Arguments []Value
}
type AdminRequest struct {
	Operation                    string
	DataDir, Destination         string
	Key                          [32]byte
	Backup                       bool
	FreshNodeID, FreshDatabaseID *[16]byte
}

// Factory owns creation and offline administration. Open must not mutate a
// store when the key is incorrect. It must apply config table flags and install
// the callback before returning a database.
type Factory interface {
	Open(context.Context, OpenRequest) (Database, OpenOutcome, error)
	Admin(context.Context, AdminRequest) (any, error)
}

// ConfigValidator lets factories validate engine-owned options without opening storage.
type ConfigValidator interface{ ValidateConfig(config.Config) error }

// Database operations must be safe for concurrent sessions. Execute is one
// autocommit statement. Migrate owns schema changes; Describe must not execute
// writes. Disconnect releases all connection-owned transfer/cursor handles.
// Unlock verifies a key on an already-open instance without opening storage.
// Command must persist table-flag changes through TableFlagsSink before
// publishing them. Restore/key rotation must invalidate appropriate handles.
type Database interface {
	Unlock(context.Context, [32]byte) error
	Execute(context.Context, string, []Value) (Result, error)
	Describe(context.Context, string, []uint32) (Description, error)
	Migrate(context.Context, string) (Result, error)
	Command(context.Context, uint64, Command) (Result, error)
	Disconnect(uint64)
	Close(context.Context) error
}

// DescribedDatabase accepts the portal's fixed PostgreSQL description so
// expression result types remain identical to Describe for every binding.
type DescribedDatabase interface {
	ExecuteDescribed(context.Context, string, []Value, Description) (Result, error)
}

// Unavailable is an explicit disabled-engine factory for protocol-only hosts.
// It deliberately never falls back to another database or a test implementation.
type Unavailable struct{}

func (Unavailable) Open(context.Context, OpenRequest) (Database, OpenOutcome, error) {
	return nil, 0, ErrUnavailable
}
func (Unavailable) Admin(context.Context, AdminRequest) (any, error) { return nil, ErrUnavailable }
func RequireOID(oid uint32) error {
	switch oid {
	case 0, Bool, Bytea, Int8, Int2, Int4, Text, Float4, Float8, Varchar, Unknown, Date, Timestamp, TimestampTZ, UUID, Numeric, JSON, JSONB, BPChar, Name:
		return nil
	}
	return &Error{Code: "0A000", Message: fmt.Sprintf("unsupported PostgreSQL type OID %d", oid)}
}
