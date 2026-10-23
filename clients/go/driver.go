package murmur

import (
	"context"
	"database/sql/driver"
	"reflect"
)

// These interfaces describe the connection implementation. Keeping the adapter
// driver-neutral also makes every error boundary testable without a server.
type connection interface {
	driver.Conn
	driver.ConnPrepareContext
	driver.ConnBeginTx
	driver.ExecerContext
	driver.QueryerContext
	driver.Pinger
	driver.SessionResetter
}
type statement interface {
	driver.Stmt
	driver.StmtExecContext
	driver.StmtQueryContext
}
type resultRows interface {
	driver.Rows
	driver.RowsColumnTypeDatabaseTypeName
	driver.RowsColumnTypeLength
	driver.RowsColumnTypePrecisionScale
	driver.RowsColumnTypeScanType
}

type errorConnector struct{ base driver.Connector }

func (c errorConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.base.Connect(ctx)
	if err != nil {
		return nil, serverError(err)
	}
	return errorConn{conn.(connection)}, nil
}
func (c errorConnector) Driver() driver.Driver { return errorDriver{c.base.Driver()} }

type errorDriver struct{ base driver.Driver }

func (d errorDriver) Open(name string) (driver.Conn, error) {
	conn, err := d.base.Open(name)
	if err != nil {
		return nil, serverError(err)
	}
	return errorConn{conn.(connection)}, nil
}

type errorConn struct{ base connection }

func (c errorConn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}
func (c errorConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	stmt, err := c.base.PrepareContext(ctx, query)
	if err != nil {
		return nil, serverError(err)
	}
	return errorStmt{stmt.(statement)}, nil
}
func (c errorConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}
func (c errorConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	tx, err := c.base.BeginTx(ctx, opts)
	if err != nil {
		return nil, serverError(err)
	}
	return errorTx{tx}, nil
}
func (c errorConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	result, err := c.base.ExecContext(ctx, query, args)
	return result, serverError(err)
}
func (c errorConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	rows, err := c.base.QueryContext(ctx, query, args)
	return wrapRows(rows, err)
}
func (c errorConn) Close() error                   { return serverError(c.base.Close()) }
func (c errorConn) Ping(ctx context.Context) error { return serverError(c.base.Ping(ctx)) }
func (c errorConn) ResetSession(ctx context.Context) error {
	return serverError(c.base.ResetSession(ctx))
}
func (c errorConn) CheckNamedValue(v *driver.NamedValue) error {
	if nvc, ok := c.base.(driver.NamedValueChecker); ok {
		return serverError(nvc.CheckNamedValue(v))
	}
	return driver.ErrSkip
}

type errorStmt struct{ base statement }

func (s errorStmt) NumInput() int { return s.base.NumInput() }
func (s errorStmt) Close() error  { return serverError(s.base.Close()) }
func (s errorStmt) Exec(args []driver.Value) (driver.Result, error) {
	result, err := s.base.Exec(args)
	return result, serverError(err)
}
func (s errorStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	result, err := s.base.ExecContext(ctx, args)
	return result, serverError(err)
}
func (s errorStmt) Query(args []driver.Value) (driver.Rows, error) {
	rows, err := s.base.Query(args)
	return wrapRows(rows, err)
}
func (s errorStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	rows, err := s.base.QueryContext(ctx, args)
	return wrapRows(rows, err)
}

type errorTx struct{ base driver.Tx }

func (t errorTx) Commit() error   { return serverError(t.base.Commit()) }
func (t errorTx) Rollback() error { return serverError(t.base.Rollback()) }

type errorRows struct{ base resultRows }

func wrapRows(rows driver.Rows, err error) (driver.Rows, error) {
	if err != nil {
		return nil, serverError(err)
	}
	return errorRows{rows.(resultRows)}, nil
}
func (r errorRows) Columns() []string              { return r.base.Columns() }
func (r errorRows) Close() error                   { return serverError(r.base.Close()) }
func (r errorRows) Next(dest []driver.Value) error { return serverError(r.base.Next(dest)) }
func (r errorRows) ColumnTypeDatabaseTypeName(i int) string {
	return r.base.ColumnTypeDatabaseTypeName(i)
}
func (r errorRows) ColumnTypeLength(i int) (int64, bool) { return r.base.ColumnTypeLength(i) }
func (r errorRows) ColumnTypePrecisionScale(i int) (int64, int64, bool) {
	return r.base.ColumnTypePrecisionScale(i)
}
func (r errorRows) ColumnTypeScanType(i int) reflect.Type { return r.base.ColumnTypeScanType(i) }

func (d errorDriver) OpenConnector(name string) (driver.Connector, error) {
	if dc, ok := d.base.(driver.DriverContext); ok {
		connector, err := dc.OpenConnector(name)
		if err != nil {
			return nil, serverError(err)
		}
		return errorConnector{connector}, nil
	}
	return errorConnector{dummyConnector{driver: d.base, name: name}}, nil
}

type dummyConnector struct {
	driver driver.Driver
	name   string
}

func (d dummyConnector) Connect(ctx context.Context) (driver.Conn, error) {
	return d.driver.Open(d.name)
}
func (d dummyConnector) Driver() driver.Driver { return d.driver }
