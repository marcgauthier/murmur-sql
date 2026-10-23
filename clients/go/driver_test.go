package murmur

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"reflect"
	"testing"

	"github.com/lib/pq"
)

// The fake follows the same optional-interface surface as the driver adapter.
type fakeConnection struct {
	err    error
	rowErr error
}

func (c *fakeConnection) Prepare(string) (driver.Stmt, error) { return &fakeStatement{c}, c.err }
func (c *fakeConnection) PrepareContext(context.Context, string) (driver.Stmt, error) {
	return c.Prepare("")
}
func (c *fakeConnection) Close() error              { return c.err }
func (c *fakeConnection) Begin() (driver.Tx, error) { return fakeTransaction{c.err}, c.err }
func (c *fakeConnection) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return c.Begin()
}
func (c *fakeConnection) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	return driver.RowsAffected(1), c.err
}
func (c *fakeConnection) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return &fakeRows{err: c.rowErr}, c.err
}
func (c *fakeConnection) Ping(context.Context) error               { return c.err }
func (c *fakeConnection) ResetSession(context.Context) error       { return c.err }
func (c *fakeConnection) CheckNamedValue(*driver.NamedValue) error { return c.err }

type fakeStatement struct{ c *fakeConnection }

func (s *fakeStatement) Close() error  { return s.c.err }
func (s *fakeStatement) NumInput() int { return 1 }
func (s *fakeStatement) Exec([]driver.Value) (driver.Result, error) {
	return driver.RowsAffected(1), s.c.err
}
func (s *fakeStatement) ExecContext(context.Context, []driver.NamedValue) (driver.Result, error) {
	return s.Exec(nil)
}
func (s *fakeStatement) Query([]driver.Value) (driver.Rows, error) {
	return &fakeRows{err: s.c.rowErr}, s.c.err
}
func (s *fakeStatement) QueryContext(context.Context, []driver.NamedValue) (driver.Rows, error) {
	return s.Query(nil)
}

type fakeTransaction struct{ err error }

func (t fakeTransaction) Commit() error   { return t.err }
func (t fakeTransaction) Rollback() error { return t.err }

type fakeRows struct{ err error }

func (r *fakeRows) Columns() []string                                 { return []string{"value"} }
func (r *fakeRows) Close() error                                      { return r.err }
func (r *fakeRows) Next([]driver.Value) error                         { return r.err }
func (r *fakeRows) ColumnTypeDatabaseTypeName(int) string             { return "TEXT" }
func (r *fakeRows) ColumnTypeLength(int) (int64, bool)                { return 42, true }
func (r *fakeRows) ColumnTypePrecisionScale(int) (int64, int64, bool) { return 12, 3, true }
func (r *fakeRows) ColumnTypeScanType(int) reflect.Type               { return reflect.TypeOf("") }

type fakeConnector struct {
	c   *fakeConnection
	err error
}

func (c fakeConnector) Connect(context.Context) (driver.Conn, error) { return c.c, c.err }
func (c fakeConnector) Driver() driver.Driver                        { return fakeDriver(c) }

type fakeDriver fakeConnector

func (d fakeDriver) Open(string) (driver.Conn, error)               { return d.c, d.err }
func (d fakeDriver) OpenConnector(string) (driver.Connector, error) { return fakeConnector(d), nil }

func TestDriverErrorBoundaries(t *testing.T) {
	ctx := context.Background()
	for _, source := range []error{&pq.Error{Code: "23505", Message: "duplicate"}, driver.ErrBadConn, driver.ErrSkip, context.Canceled, io.EOF} {
		base := &fakeConnection{err: source, rowErr: source}
		c := errorConn{base}
		s := errorStmt{&fakeStatement{base}}
		r := errorRows{&fakeRows{err: source}}
		tx := errorTx{fakeTransaction{source}}
		connector := errorConnector{fakeConnector{base, source}}
		checks := map[string]func() error{
			"connect":         func() error { _, err := connector.Connect(ctx); return err },
			"driver":          func() error { _, err := connector.Driver().Open(""); return err },
			"prepare":         func() error { _, err := c.Prepare(""); return err },
			"prepare context": func() error { _, err := c.PrepareContext(ctx, ""); return err },
			"begin":           func() error { _, err := c.Begin(); return err },
			"begin context":   func() error { _, err := c.BeginTx(ctx, driver.TxOptions{}); return err },
			"exec":            func() error { _, err := c.ExecContext(ctx, "", nil); return err },
			"query":           func() error { _, err := c.QueryContext(ctx, "", nil); return err },
			"close":           c.Close, "ping": func() error { return c.Ping(ctx) },
			"reset":              func() error { return c.ResetSession(ctx) },
			"named value":        func() error { return c.CheckNamedValue(&driver.NamedValue{}) },
			"stmt close":         s.Close,
			"stmt exec":          func() error { _, err := s.Exec(nil); return err },
			"stmt exec context":  func() error { _, err := s.ExecContext(ctx, nil); return err },
			"stmt query":         func() error { _, err := s.Query(nil); return err },
			"stmt query context": func() error { _, err := s.QueryContext(ctx, nil); return err },
			"commit":             tx.Commit, "rollback": tx.Rollback,
			"rows close": r.Close, "rows next": func() error { return r.Next(nil) },
		}
		for name, call := range checks {
			err := call()
			var own *Error
			if _, isServer := source.(*pq.Error); isServer {
				if !errors.As(err, &own) || own.Code != "23505" || !errors.Is(err, source) {
					t.Fatalf("%s: %v", name, err)
				}
			} else if err != source {
				t.Fatalf("%s changed sentinel: %v", name, err)
			}
		}
	}
}

func TestDriverDeferredErrorsAndMetadata(t *testing.T) {
	source := &pq.Error{Code: "XX000", Message: "late failure"}
	base := &fakeConnection{rowErr: source}
	db := sql.OpenDB(errorConnector{fakeConnector{c: base}})
	defer db.Close()
	rows, err := db.Query("query")
	if err != nil {
		t.Fatal(err)
	}
	columns, err := rows.ColumnTypes()
	if err != nil || len(columns) != 1 {
		t.Fatalf("columns: %v %v", columns, err)
	}
	col := columns[0]
	length, ok := col.Length()
	precision, scale, hasPrecision := col.DecimalSize()
	if col.Name() != "value" || col.DatabaseTypeName() != "TEXT" || col.ScanType() != reflect.TypeOf("") || length != 42 || !ok || precision != 12 || scale != 3 || !hasPrecision {
		t.Fatal("lost metadata")
	}
	if rows.Next() {
		t.Fatal("expected deferred failure")
	}
	var own *Error
	if !errors.As(rows.Err(), &own) || own.Code != "XX000" {
		t.Fatalf("rows.Err: %v", rows.Err())
	}
	if err := db.QueryRow("query").Scan(new(string)); !errors.As(err, &own) {
		t.Fatalf("scan: %v", err)
	}
	stmt, err := db.Prepare("query")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	if err := stmt.QueryRow("arg").Scan(new(string)); !errors.As(err, &own) {
		t.Fatalf("prepared scan: %v", err)
	}
	base.rowErr = io.EOF
	rows, err = db.Query("query")
	if err != nil {
		t.Fatal(err)
	}
	if rows.Next() || rows.Err() != nil {
		t.Fatalf("EOF mishandled: %v", rows.Err())
	}
}

func TestDriverWrapsSuccessfulObjects(t *testing.T) {
	ctx := context.Background()
	base := &fakeConnection{rowErr: io.EOF}
	connector := errorConnector{fakeConnector{c: base}}
	d := connector.Driver().(driver.DriverContext)
	nested, err := d.OpenConnector("")
	if err != nil {
		t.Fatal(err)
	}
	conn, err := nested.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := conn.(errorConn); !ok {
		t.Fatal("connector bypassed error adapter")
	}
	direct, err := connector.Driver().Open("")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := direct.(errorConn); !ok {
		t.Fatal("driver bypassed error adapter")
	}
	stmt, err := conn.Prepare("")
	if err != nil {
		t.Fatal(err)
	}
	if stmt.NumInput() != 1 {
		t.Fatal("lost parameter count")
	}
	result, err := stmt.Exec([]driver.Value{"arg"})
	if err != nil {
		t.Fatal(err)
	}
	if count, _ := result.RowsAffected(); count != 1 {
		t.Fatal("lost affected rows")
	}
	rows, err := stmt.Query(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := rows.Next(nil); err != io.EOF {
		t.Fatalf("EOF: %v", err)
	}
	tx, err := conn.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := tx.(errorTx); !ok {
		t.Fatal("transaction bypassed error adapter")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	base.err = &pq.Error{Code: "08003", Message: "connection lost"}
	_, err = stmt.Exec(nil)
	var own *Error
	if !errors.As(err, &own) {
		t.Fatalf("prepared execution bypassed adapter: %v", err)
	}
}
