package murmur

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/lib/pq"
)

func TestServerErrorDiagnostics(t *testing.T) {
	source := &pq.Error{
		Severity:         "ERROR",
		Code:             "23505",
		Message:          "duplicate",
		Detail:           "Detail",
		Hint:             "Hint",
		Position:         "1",
		InternalPosition: "2",
		InternalQuery:    "InternalQuery",
		Where:            "Where",
		Schema:           "SchemaName",
		Table:            "TableName",
		Column:           "ColumnName",
		DataTypeName:     "DataTypeName",
		Constraint:       "ConstraintName",
		File:             "File",
		Line:             "3",
		Routine:          "Routine",
	}
	wrapped := fmt.Errorf("murmur: unlock failed: %w", source)
	got := serverError(wrapped)
	var own *Error
	if !errors.As(got, &own) {
		t.Fatalf("missing murmur.Error: %v", got)
	}
	if own.Code != "23505" || own.Message != "duplicate" || own.Position != 1 || own.InternalPosition != 2 || own.Line != 3 {
		t.Fatalf("unexpected fields: %+v", own)
	}
	if own.SchemaName != "SchemaName" || own.TableName != "TableName" || own.ColumnName != "ColumnName" || own.ConstraintName != "ConstraintName" {
		t.Fatalf("unexpected schema fields: %+v", own)
	}
	if got.Error() != wrapped.Error() || own.SQLState() != string(source.Code) || !errors.Is(got, wrapped) {
		t.Fatalf("lost text, code or cause: %v", got)
	}
	var original *pq.Error
	if !errors.As(got, &original) || original != source {
		t.Fatal("lost underlying driver error")
	}
	if serverError(got) != got {
		t.Fatal("conversion is not idempotent")
	}
	outer := fmt.Errorf("application: %w", got)
	if serverError(outer) != outer {
		t.Fatal("wrapped murmur.Error changed")
	}
	constructed := &Error{Severity: "ERROR", Message: "duplicate", Code: "23505"}
	if constructed.Error() != "ERROR: duplicate (SQLSTATE 23505)" || constructed.Unwrap() != nil {
		t.Fatal(constructed)
	}
}

func TestServerErrorPreservesNonServerErrors(t *testing.T) {
	for _, err := range []error{nil, io.EOF, driver.ErrBadConn, driver.ErrSkip, context.Canceled, context.DeadlineExceeded, errors.New("network or validation error")} {
		if serverError(err) != err {
			t.Fatalf("changed sentinel or unrelated error: %v", err)
		}
	}
}
