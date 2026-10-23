package murmur

import (
	"errors"
	"strconv"

	"github.com/lib/pq"
)

// Error is a server-reported failure. Code is its five-character SQLSTATE.
// Diagnostic fields may be empty when the server does not supply them.
// Applications should use errors.As to inspect errors returned by Open or its pool.
type Error struct {
	Severity            string
	SeverityUnlocalized string
	Code                string
	Message             string
	Detail              string
	Hint                string
	Position            int32
	InternalPosition    int32
	InternalQuery       string
	Where               string
	SchemaName          string
	TableName           string
	ColumnName          string
	DataTypeName        string
	ConstraintName      string
	File                string
	Line                int32
	Routine             string
	cause               error
}

// Error returns the original diagnostic, including connection or unlock context.
func (e *Error) Error() string {
	if e.cause != nil {
		return e.cause.Error()
	}
	return e.Severity + ": " + e.Message + " (SQLSTATE " + e.Code + ")"
}

// SQLState returns the server's SQLSTATE code.
func (e *Error) SQLState() string { return e.Code }

// Unwrap preserves the original cause. Its concrete driver types are not API guarantees.
func (e *Error) Unwrap() error { return e.cause }

func serverError(err error) error {
	if err == nil {
		return nil
	}
	var own *Error
	if errors.As(err, &own) {
		return err
	}
	var source *pq.Error
	if !errors.As(err, &source) {
		return err
	}
	var pos, intPos, line int
	if source.Position != "" {
		pos, _ = strconv.Atoi(source.Position)
	}
	if source.InternalPosition != "" {
		intPos, _ = strconv.Atoi(source.InternalPosition)
	}
	if source.Line != "" {
		line, _ = strconv.Atoi(source.Line)
	}
	return &Error{
		Severity:            source.Severity,
		SeverityUnlocalized: source.Severity,
		Code:                string(source.Code),
		Message:             source.Message,
		Detail:              source.Detail,
		Hint:                source.Hint,
		Position:            int32(pos),
		InternalPosition:    int32(intPos),
		InternalQuery:       source.InternalQuery,
		Where:               source.Where,
		SchemaName:          source.Schema,
		TableName:           source.Table,
		ColumnName:          source.Column,
		DataTypeName:        source.DataTypeName,
		ConstraintName:      source.Constraint,
		File:                source.File,
		Line:                int32(line),
		Routine:             source.Routine,
		cause:               err,
	}
}
