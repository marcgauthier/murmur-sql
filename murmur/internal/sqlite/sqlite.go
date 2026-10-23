// Package sqlite is a minimal cgo binding to the vendored SQLite
// amalgamation (third_party/sqlite), covering exactly what murmur needs:
// in-memory execution, an authorizer, scalar functions, session change
// capture, and changeset iteration. Only the standard library is used
// besides the vendored public-domain C sources.
//
// A Conn is bound to the goroutine that uses it, mirroring the single
// database thread of the Rust implementation; it is not safe for
// concurrent use.
package sqlite

/*
#cgo CFLAGS: -DSQLITE_ENABLE_SESSION -DSQLITE_ENABLE_PREUPDATE_HOOK -DSQLITE_ENABLE_FTS5 -DSQLITE_ENABLE_COLUMN_METADATA -DSQLITE_THREADSAFE=1 -O2
#cgo LDFLAGS: -lm
#include <stdlib.h>
#include "../../third_party/sqlite/sqlite3.h"

int murmurSetAuthorizer(sqlite3 *db, int enable);
int murmurCreateFunction(sqlite3 *db, const char *name, int nArg, int flags, long long handle);
int murmurBindText(sqlite3_stmt *stmt, int i, const char *s, int n);
int murmurBindBlob(sqlite3_stmt *stmt, int i, const void *b, int n);
void murmurResultText(sqlite3_context *ctx, const char *s, int n);
void murmurResultBlob(sqlite3_context *ctx, const void *b, int n);
void murmurResultError(sqlite3_context *ctx, const char *s, int n);
int murmurRegisterVec0(sqlite3 *db);
*/
import "C"

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"unsafe"
)

// Error reports a SQLite failure with its primary and extended codes.
type Error struct {
	Code    int
	ExtCode int
	Msg     string
}

func (e *Error) Error() string {
	if e.Msg == "" {
		return fmt.Sprintf("sqlite: code %d", e.Code)
	}
	return fmt.Sprintf("sqlite: %s (code %d)", e.Msg, e.Code)
}

// ErrNoRows reports a query that returned no row.
var ErrNoRows = errors.New("sqlite: no rows")

// ErrClosed reports use of a closed connection or statement.
var ErrClosed = errors.New("sqlite: closed")

// emptyBindByte is a non-nil address for zero-length text/blob binds and
// results. SQLite treats a NULL pointer with length 0 as NULL, so empties
// must pass a valid pointer to stay empty instead of becoming NULL.
var emptyBindByte byte

func connErr(db *C.sqlite3, code C.int) *Error {
	return &Error{
		Code:    int(code),
		ExtCode: int(C.sqlite3_extended_errcode(db)),
		Msg:     C.GoString(C.sqlite3_errmsg(db)),
	}
}

// ValueType classifies a Value.
type ValueType int

const (
	Null ValueType = iota
	Integer
	Float
	Text
	Blob
)

// Value is a copied column or function-argument value.
type Value struct {
	Type  ValueType
	Int   int64
	Float float64
	Str   string
	Blob  []byte
}

func (v Value) String() string {
	switch v.Type {
	case Null:
		return "NULL"
	case Integer:
		return fmt.Sprintf("%d", v.Int)
	case Float:
		return fmt.Sprintf("%v", v.Float)
	case Text:
		return v.Str
	case Blob:
		return fmt.Sprintf("blob[%d]", len(v.Blob))
	}
	return "?"
}

func valueOf(v *C.sqlite3_value) Value {
	switch C.sqlite3_value_type(v) {
	case C.SQLITE_INTEGER:
		return Value{Type: Integer, Int: int64(C.sqlite3_value_int64(v))}
	case C.SQLITE_FLOAT:
		return Value{Type: Float, Float: float64(C.sqlite3_value_double(v))}
	case C.SQLITE_TEXT:
		n := int(C.sqlite3_value_bytes(v))
		if n == 0 {
			return Value{Type: Text}
		}
		p := C.sqlite3_value_text(v)
		return Value{Type: Text, Str: string(C.GoBytes(unsafe.Pointer(p), C.int(n)))}
	case C.SQLITE_BLOB:
		n := int(C.sqlite3_value_bytes(v))
		if n == 0 {
			return Value{Type: Blob, Blob: []byte{}}
		}
		p := C.sqlite3_value_blob(v)
		return Value{Type: Blob, Blob: C.GoBytes(p, C.int(n))}
	default:
		return Value{Type: Null}
	}
}

// Conn is one SQLite connection.
type Conn struct {
	db       *C.sqlite3
	mu       sync.Mutex // guards authorizer + callbacks below
	closed   bool
	auth     Authorizer
	funcs    map[int64]scalarImpl
	funcLast int64
}

// scalarImpl holds one registered function implementation.
type scalarImpl struct {
	plain ScalarFunc
	raw   RawScalarFunc
}

// Open opens a database; use ":memory:" for a private in-memory database.
func Open(path string) (*Conn, error) {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	var db *C.sqlite3
	if rc := C.sqlite3_open(cpath, &db); rc != C.SQLITE_OK {
		if db != nil {
			err := connErr(db, rc)
			C.sqlite3_close(db)
			return nil, err
		}
		return nil, &Error{Code: int(rc), Msg: "sqlite3_open failed"}
	}
	C.sqlite3_busy_timeout(db, 5000)
	if rc := C.murmurRegisterVec0(db); rc != C.SQLITE_OK {
		err := connErr(db, rc)
		C.sqlite3_close(db)
		return nil, err
	}
	c := &Conn{db: db, funcs: make(map[int64]scalarImpl)}
	authMu.Lock()
	authReg[unsafe.Pointer(db)] = c
	authMu.Unlock()
	return c, nil
}

// OpenMemory opens a private in-memory database.
func OpenMemory() (*Conn, error) { return Open(":memory:") }

func (c *Conn) check() error {
	if c == nil || c.db == nil {
		return ErrClosed
	}
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed {
		return ErrClosed
	}
	return nil
}

// Close closes the connection, running function destructors.
func (c *Conn) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()
	authMu.Lock()
	delete(authReg, unsafe.Pointer(c.db))
	authMu.Unlock()
	if rc := C.sqlite3_close(c.db); rc != C.SQLITE_OK {
		return connErr(c.db, rc)
	}
	c.db = nil
	return nil
}

// ExecBatch runs one or more statements with no bindings or result rows.
func (c *Conn) ExecBatch(sql string) error {
	if err := c.check(); err != nil {
		return err
	}
	csql := C.CString(sql)
	defer C.free(unsafe.Pointer(csql))
	var msg *C.char
	if rc := C.sqlite3_exec(c.db, csql, nil, nil, &msg); rc != C.SQLITE_OK {
		defer C.sqlite3_free(unsafe.Pointer(msg))
		return &Error{Code: int(rc), ExtCode: int(C.sqlite3_extended_errcode(c.db)), Msg: C.GoString(msg)}
	}
	return nil
}

// Exec runs a single DML/DDL statement with positional bindings.
func (c *Conn) Exec(sql string, args ...any) error {
	s, err := c.Prepare(sql)
	if err != nil {
		return err
	}
	defer s.Close()
	if err := s.Bind(args...); err != nil {
		return err
	}
	for {
		row, err := s.Step()
		if err != nil {
			return err
		}
		if !row {
			return nil
		}
		return errors.New("sqlite: statement returned rows in Exec")
	}
}

// QueryRow runs a query and returns the first row.
func (c *Conn) QueryRow(sql string, args ...any) ([]Value, error) {
	s, err := c.Prepare(sql)
	if err != nil {
		return nil, err
	}
	defer s.Close()
	if err := s.Bind(args...); err != nil {
		return nil, err
	}
	row, err := s.Step()
	if err != nil {
		return nil, err
	}
	if !row {
		return nil, ErrNoRows
	}
	return s.Columns(), nil
}

// Query runs a query and returns all rows.
func (c *Conn) Query(sql string, args ...any) ([][]Value, error) {
	s, err := c.Prepare(sql)
	if err != nil {
		return nil, err
	}
	defer s.Close()
	if err := s.Bind(args...); err != nil {
		return nil, err
	}
	var out [][]Value
	for {
		row, err := s.Step()
		if err != nil {
			return nil, err
		}
		if !row {
			return out, nil
		}
		out = append(out, s.Columns())
	}
}

// TotalChanges reports rows modified by recent INSERT/UPDATE/DELETE.
func (c *Conn) TotalChanges() int64 {
	if c.check() != nil {
		return 0
	}
	return int64(C.sqlite3_total_changes(c.db))
}

// Changes reports rows modified by the latest statement.
func (c *Conn) Changes() int64 {
	if c.check() != nil {
		return 0
	}
	return int64(C.sqlite3_changes(c.db))
}

// LastInsertRowid reports the latest successful rowid insert.
func (c *Conn) LastInsertRowid() int64 {
	if c.check() != nil {
		return 0
	}
	return int64(C.sqlite3_last_insert_rowid(c.db))
}

// Stmt is a prepared statement.
type Stmt struct {
	c    *Conn
	stmt *C.sqlite3_stmt
	ncol int
}

// Prepare compiles the first statement in sql.
func (c *Conn) Prepare(sql string) (*Stmt, error) {
	if err := c.check(); err != nil {
		return nil, err
	}
	csql := C.CString(sql)
	defer C.free(unsafe.Pointer(csql))
	var stmt *C.sqlite3_stmt
	if rc := C.sqlite3_prepare_v2(c.db, csql, -1, &stmt, nil); rc != C.SQLITE_OK {
		return nil, connErr(c.db, rc)
	}
	if stmt == nil {
		return nil, errors.New("sqlite: empty statement")
	}
	return &Stmt{c: c, stmt: stmt, ncol: int(C.sqlite3_column_count(stmt))}, nil
}

// Close finalizes the statement.
func (s *Stmt) Close() error {
	if s.stmt == nil {
		return nil
	}
	rc := C.sqlite3_finalize(s.stmt)
	s.stmt = nil
	if rc != C.SQLITE_OK {
		return connErr(s.c.db, rc)
	}
	return nil
}

// Bind binds positional arguments (1-based ? placeholders).
func (s *Stmt) Bind(args ...any) error {
	if s.stmt == nil {
		return ErrClosed
	}
	for i, a := range args {
		if err := s.bindOne(i+1, a); err != nil {
			return err
		}
	}
	return nil
}

func (s *Stmt) bindOne(idx int, a any) error {
	i := C.int(idx)
	var rc C.int
	switch v := a.(type) {
	case nil:
		rc = C.sqlite3_bind_null(s.stmt, i)
	case bool:
		if v {
			rc = C.sqlite3_bind_int64(s.stmt, i, 1)
		} else {
			rc = C.sqlite3_bind_int64(s.stmt, i, 0)
		}
	case int:
		rc = C.sqlite3_bind_int64(s.stmt, i, C.sqlite3_int64(v))
	case int8:
		rc = C.sqlite3_bind_int64(s.stmt, i, C.sqlite3_int64(v))
	case int16:
		rc = C.sqlite3_bind_int64(s.stmt, i, C.sqlite3_int64(v))
	case int32:
		rc = C.sqlite3_bind_int64(s.stmt, i, C.sqlite3_int64(v))
	case int64:
		rc = C.sqlite3_bind_int64(s.stmt, i, C.sqlite3_int64(v))
	case uint:
		rc = C.sqlite3_bind_int64(s.stmt, i, C.sqlite3_int64(v))
	case uint8:
		rc = C.sqlite3_bind_int64(s.stmt, i, C.sqlite3_int64(v))
	case uint16:
		rc = C.sqlite3_bind_int64(s.stmt, i, C.sqlite3_int64(v))
	case uint32:
		rc = C.sqlite3_bind_int64(s.stmt, i, C.sqlite3_int64(v))
	case uint64:
		rc = C.sqlite3_bind_int64(s.stmt, i, C.sqlite3_int64(v))
	case float32:
		rc = C.sqlite3_bind_double(s.stmt, i, C.double(v))
	case float64:
		rc = C.sqlite3_bind_double(s.stmt, i, C.double(v))
	case string:
		if len(v) == 0 {
			// (NULL, 0) binds NULL; a valid pointer with length 0
			// binds empty text.
			rc = C.murmurBindText(s.stmt, i, (*C.char)(unsafe.Pointer(&emptyBindByte)), 0)
		} else {
			rc = C.murmurBindText(s.stmt, i, (*C.char)(unsafe.Pointer(unsafe.StringData(v))), C.int(len(v)))
		}
	case []byte:
		if len(v) == 0 {
			rc = C.murmurBindBlob(s.stmt, i, unsafe.Pointer(&emptyBindByte), 0)
		} else {
			rc = C.murmurBindBlob(s.stmt, i, unsafe.Pointer(&v[0]), C.int(len(v)))
		}
	case Value:
		switch v.Type {
		case Null:
			return s.bindOne(idx, nil)
		case Integer:
			return s.bindOne(idx, v.Int)
		case Float:
			return s.bindOne(idx, v.Float)
		case Text:
			return s.bindOne(idx, v.Str)
		case Blob:
			return s.bindOne(idx, v.Blob)
		default:
			return fmt.Errorf("sqlite: bad value type %d", v.Type)
		}
	default:
		return fmt.Errorf("sqlite: cannot bind %T", a)
	}
	if rc != C.SQLITE_OK {
		return connErr(s.c.db, rc)
	}
	return nil
}

// Step advances the statement; it reports whether a row is available.
func (s *Stmt) Step() (bool, error) {
	if s.stmt == nil {
		return false, ErrClosed
	}
	switch rc := C.sqlite3_step(s.stmt); rc {
	case C.SQLITE_ROW:
		return true, nil
	case C.SQLITE_DONE:
		return false, nil
	default:
		return false, connErr(s.c.db, rc)
	}
}

// Reset resets the statement for re-execution with new bindings.
func (s *Stmt) Reset() error {
	if s.stmt == nil {
		return ErrClosed
	}
	if rc := C.sqlite3_reset(s.stmt); rc != C.SQLITE_OK {
		return connErr(s.c.db, rc)
	}
	C.sqlite3_clear_bindings(s.stmt)
	return nil
}

// ColumnCount reports the number of result columns.
func (s *Stmt) ColumnCount() int { return s.ncol }

// ColumnName reports the name of result column i.
func (s *Stmt) ColumnName(i int) string {
	if s.stmt == nil || i < 0 || i >= s.ncol {
		return ""
	}
	return C.GoString(C.sqlite3_column_name(s.stmt, C.int(i)))
}

// ColumnType reports the type of column i.
func (s *Stmt) ColumnType(i int) ValueType {
	if s.stmt == nil || i < 0 || i >= s.ncol {
		return Null
	}
	switch C.sqlite3_column_type(s.stmt, C.int(i)) {
	case C.SQLITE_INTEGER:
		return Integer
	case C.SQLITE_FLOAT:
		return Float
	case C.SQLITE_TEXT:
		return Text
	case C.SQLITE_BLOB:
		return Blob
	default:
		return Null
	}
}

// ColumnIsNull reports whether column i is NULL.
func (s *Stmt) ColumnIsNull(i int) bool {
	if s.stmt == nil || i < 0 || i >= s.ncol {
		return true
	}
	return C.sqlite3_column_type(s.stmt, C.int(i)) == C.SQLITE_NULL
}

// ColumnInt64 returns the integer value of column i.
func (s *Stmt) ColumnInt64(i int) int64 {
	if s.stmt == nil || i < 0 || i >= s.ncol {
		return 0
	}
	return int64(C.sqlite3_column_int64(s.stmt, C.int(i)))
}

// ColumnInt returns the integer value of column i as an int.
func (s *Stmt) ColumnInt(i int) int {
	return int(s.ColumnInt64(i))
}

// ColumnFloat returns the float value of column i.
func (s *Stmt) ColumnFloat(i int) float64 {
	if s.stmt == nil || i < 0 || i >= s.ncol {
		return 0
	}
	return float64(C.sqlite3_column_double(s.stmt, C.int(i)))
}

// ColumnText returns the string value of column i.
func (s *Stmt) ColumnText(i int) string {
	if s.stmt == nil || i < 0 || i >= s.ncol {
		return ""
	}
	ci := C.int(i)
	p := C.sqlite3_column_text(s.stmt, ci)
	if p == nil {
		return ""
	}
	n := int(C.sqlite3_column_bytes(s.stmt, ci))
	if n == 0 {
		return ""
	}
	return string(unsafe.Slice((*byte)(unsafe.Pointer(p)), n))
}

// ColumnBlob returns a copy of the blob value of column i.
func (s *Stmt) ColumnBlob(i int) []byte {
	if s.stmt == nil || i < 0 || i >= s.ncol {
		return nil
	}
	ci := C.int(i)
	p := C.sqlite3_column_blob(s.stmt, ci)
	if p == nil {
		// Zero-length blobs also report a NULL pointer; only a NULL
		// column type means NULL.
		if C.sqlite3_column_type(s.stmt, ci) == C.SQLITE_BLOB {
			return []byte{}
		}
		return nil
	}
	n := int(C.sqlite3_column_bytes(s.stmt, ci))
	if n == 0 {
		return []byte{}
	}
	return C.GoBytes(p, C.int(n))
}

// ColumnValue copies the value of column i as a typed Value.
func (s *Stmt) ColumnValue(i int) Value {
	if s.stmt == nil || i < 0 || i >= s.ncol {
		return Value{Type: Null}
	}
	return s.column(i)
}

// Columns copies the current row's values.
func (s *Stmt) Columns() []Value {
	out := make([]Value, 0, s.ncol)
	for i := range s.ncol {
		out = append(out, s.column(i))
	}
	return out
}

func (s *Stmt) column(i int) Value {
	ci := C.int(i)
	switch C.sqlite3_column_type(s.stmt, ci) {
	case C.SQLITE_INTEGER:
		return Value{Type: Integer, Int: int64(C.sqlite3_column_int64(s.stmt, ci))}
	case C.SQLITE_FLOAT:
		return Value{Type: Float, Float: float64(C.sqlite3_column_double(s.stmt, ci))}
	case C.SQLITE_TEXT:
		p := C.sqlite3_column_text(s.stmt, ci)
		n := int(C.sqlite3_column_bytes(s.stmt, ci))
		if p == nil || n == 0 {
			// Dispatched on the column type, so this is empty
			// text, never NULL.
			return Value{Type: Text}
		}
		return Value{Type: Text, Str: string(unsafe.Slice((*byte)(unsafe.Pointer(p)), n))}
	case C.SQLITE_BLOB:
		p := C.sqlite3_column_blob(s.stmt, ci)
		n := int(C.sqlite3_column_bytes(s.stmt, ci))
		if p == nil || n == 0 {
			// A zero-length blob reports a NULL pointer; the
			// column type says this is an empty blob, not NULL.
			return Value{Type: Blob, Blob: []byte{}}
		}
		return Value{Type: Blob, Blob: C.GoBytes(p, C.int(n))}
	default:
		return Value{Type: Null}
	}
}

// AuthAction is a sqlite3_set_authorizer action code.
type AuthAction int

const (
	AuthSelect      AuthAction = 21
	AuthRead        AuthAction = 20
	AuthInsert      AuthAction = 18
	AuthUpdate      AuthAction = 23
	AuthDelete      AuthAction = 9
	AuthPragma      AuthAction = 19
	AuthFunction    AuthAction = 31
	AuthReindex     AuthAction = 27
	AuthRecursive   AuthAction = 33
	AuthCreateTable AuthAction = 2
	AuthDropTable   AuthAction = 11
	AuthAlterTable  AuthAction = 26
	AuthCreateIndex AuthAction = 1
	AuthDropIndex   AuthAction = 10
)

// AuthResult is returned by an Authorizer.
type AuthResult int

const (
	AuthOK     AuthResult = 0
	AuthDeny   AuthResult = 1
	AuthIgnore AuthResult = 2
)

// Authorizer approves or rejects SQL operations. Arguments after action
// mirror sqlite3_set_authorizer's detail strings (empty when NULL).
type Authorizer func(action AuthAction, a1, a2, a3, a4 string) AuthResult

var (
	authMu  sync.Mutex
	authReg = make(map[unsafe.Pointer]*Conn)
)

//export murmurGoAuthorizer
func murmurGoAuthorizer(db unsafe.Pointer, action C.int, a1, a2, a3, a4 *C.char) C.int {
	authMu.Lock()
	c := authReg[db]
	authMu.Unlock()
	if c == nil {
		return C.int(AuthDeny)
	}
	c.mu.Lock()
	fn := c.auth
	c.mu.Unlock()
	if fn == nil {
		return C.int(AuthOK)
	}
	str := func(p *C.char) string {
		if p == nil {
			return ""
		}
		return C.GoString(p)
	}
	// Recover a panicking authorizer into denial; SQLite cannot unwind Go.
	res := AuthDeny
	func() {
		defer func() { _ = recover() }()
		res = fn(AuthAction(action), str(a1), str(a2), str(a3), str(a4))
	}()
	return C.int(res)
}

// SetAuthorizer installs (or, with nil, removes) the SQL authorizer.
func (c *Conn) SetAuthorizer(fn Authorizer) error {
	if err := c.check(); err != nil {
		return err
	}
	c.mu.Lock()
	c.auth = fn
	c.mu.Unlock()
	enable := 0
	if fn != nil {
		enable = 1
	}
	if rc := C.murmurSetAuthorizer(c.db, C.int(enable)); rc != C.SQLITE_OK {
		return connErr(c.db, rc)
	}
	return nil
}

// FuncContext sets a scalar function's result.
type FuncContext struct {
	ctx *C.sqlite3_context
}

// ResultNull sets a NULL result.
func (f *FuncContext) ResultNull() { C.sqlite3_result_null(f.ctx) }

// ResultInt sets an integer result.
func (f *FuncContext) ResultInt(v int64) { C.sqlite3_result_int64(f.ctx, C.sqlite3_int64(v)) }

// ResultFloat sets a float result.
func (f *FuncContext) ResultFloat(v float64) { C.sqlite3_result_double(f.ctx, C.double(v)) }

// ResultText sets a text result.
func (f *FuncContext) ResultText(s string) {
	cs := C.CString(s)
	defer C.free(unsafe.Pointer(cs))
	C.murmurResultText(f.ctx, cs, C.int(len(s)))
}

// ResultBlob sets a blob result.
func (f *FuncContext) ResultBlob(b []byte) {
	if len(b) == 0 {
		C.murmurResultBlob(f.ctx, unsafe.Pointer(&emptyBindByte), 0)
		return
	}
	C.murmurResultBlob(f.ctx, unsafe.Pointer(&b[0]), C.int(len(b)))
}

// ResultValue sets a Value result.
func (f *FuncContext) ResultValue(v Value) {
	switch v.Type {
	case Null:
		f.ResultNull()
	case Integer:
		f.ResultInt(v.Int)
	case Float:
		f.ResultFloat(v.Float)
	case Text:
		f.ResultText(v.Str)
	case Blob:
		f.ResultBlob(v.Blob)
	default:
		f.ResultError("bad value type")
	}
}

// ResultError fails the function with a message.
func (f *FuncContext) ResultError(msg string) {
	cs := C.CString(msg)
	defer C.free(unsafe.Pointer(cs))
	C.murmurResultError(f.ctx, cs, C.int(len(msg)))
}

// ResultErrorCode fails the function with an extended error code.
func (f *FuncContext) ResultErrorCode(code int, msg string) {
	f.ResultError(msg)
	C.sqlite3_result_error_code(f.ctx, C.int(code))
}

// ResultRawValue copies a raw argument value to the result.
func (f *FuncContext) ResultRawValue(v RawValue) {
	C.sqlite3_result_value(f.ctx, v.v)
}

// ScalarFunc implements a scalar SQL function. A Go panic becomes a
// function error; result defaults to NULL when unset.
type ScalarFunc func(ctx *FuncContext, args []Value)

// RawValue exposes one function argument without conversion, for
// functions that need SQLite's own type affinities.
type RawValue struct {
	v *C.sqlite3_value
}

// Value type codes, mirroring the C API.
const (
	TypeInteger = 1
	TypeFloat   = 2
	TypeText    = 3
	TypeBlob    = 4
	TypeNull    = 5
)

// Type reports the argument's storage type.
func (r RawValue) Type() int { return int(C.sqlite3_value_type(r.v)) }

// NumericType reports the type after NUMERIC affinity conversion.
func (r RawValue) NumericType() int { return int(C.sqlite3_value_numeric_type(r.v)) }

// Int64 reads the argument as an integer (with conversion).
func (r RawValue) Int64() int64 { return int64(C.sqlite3_value_int64(r.v)) }

// Float64 reads the argument as a float (with conversion).
func (r RawValue) Float64() float64 { return float64(C.sqlite3_value_double(r.v)) }

// Text reads the argument as text (with conversion).
func (r RawValue) Text() []byte {
	n := int(C.sqlite3_value_bytes(r.v))
	if n == 0 {
		return nil
	}
	return C.GoBytes(unsafe.Pointer(C.sqlite3_value_text(r.v)), C.int(n))
}

// Blob reads the argument as a blob.
func (r RawValue) Blob() []byte {
	n := int(C.sqlite3_value_bytes(r.v))
	if n == 0 {
		return []byte{}
	}
	return C.GoBytes(C.sqlite3_value_blob(r.v), C.int(n))
}

// RawScalarFunc implements a scalar SQL function over raw values.
type RawScalarFunc func(ctx *FuncContext, args []RawValue)

//export murmurGoFunc
func murmurGoFunc(ctx unsafe.Pointer, handle int64, argc C.int, argv unsafe.Pointer) {
	fctx := &FuncContext{ctx: (*C.sqlite3_context)(ctx)}
	c := funcConn(handle)
	if c == nil {
		fctx.ResultError("unknown function")
		return
	}
	c.mu.Lock()
	impl := c.funcs[handle]
	c.mu.Unlock()
	if impl.plain == nil && impl.raw == nil {
		fctx.ResultError("unknown function")
		return
	}
	n := int(argc)
	raw := make([]RawValue, 0, n)
	if n > 0 && argv != nil {
		vals := (*[1 << 29]*C.sqlite3_value)(argv)[:n:n]
		for _, v := range vals {
			raw = append(raw, RawValue{v: v})
		}
	}
	func() {
		defer func() {
			if r := recover(); r != nil {
				fctx.ResultError(fmt.Sprintf("panic: %v", r))
			}
		}()
		if impl.raw != nil {
			impl.raw(fctx, raw)
			return
		}
		args := make([]Value, 0, len(raw))
		for _, r := range raw {
			args = append(args, valueOf(r.v))
		}
		impl.plain(fctx, args)
	}()
}

//export murmurGoFuncDestroy
func murmurGoFuncDestroy(p unsafe.Pointer) {
	h := int64(uintptr(p))
	funcMu.Lock()
	c := funcReg[h]
	delete(funcReg, h)
	funcMu.Unlock()
	if c == nil {
		return
	}
	c.mu.Lock()
	delete(c.funcs, h)
	c.mu.Unlock()
}

var (
	funcMu  sync.Mutex
	funcReg = make(map[int64]*Conn)
	funcSeq atomic.Int64
)

func funcConn(h int64) *Conn {
	funcMu.Lock()
	defer funcMu.Unlock()
	return funcReg[h]
}

// CreateScalarFunction registers a deterministic-capable scalar function.
// nArg follows SQLite convention (-1 for variadic).
func (c *Conn) CreateScalarFunction(name string, nArg int, deterministic bool, fn ScalarFunc) error {
	if err := c.check(); err != nil {
		return err
	}
	h := funcSeq.Add(1)
	funcMu.Lock()
	funcReg[h] = c
	funcMu.Unlock()
	c.mu.Lock()
	c.funcs[h] = scalarImpl{plain: fn}
	c.mu.Unlock()
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	flags := int(C.SQLITE_UTF8)
	if deterministic {
		flags |= int(C.SQLITE_DETERMINISTIC)
	}
	if rc := C.murmurCreateFunction(c.db, cname, C.int(nArg), C.int(flags), C.longlong(h)); rc != C.SQLITE_OK {
		funcMu.Lock()
		delete(funcReg, h)
		funcMu.Unlock()
		c.mu.Lock()
		delete(c.funcs, h)
		c.mu.Unlock()
		return connErr(c.db, rc)
	}
	return nil
}

// CreateRawScalarFunction registers a scalar function over raw values.
// nArg follows SQLite convention (-1 for variadic).
func (c *Conn) CreateRawScalarFunction(name string, nArg int, deterministic bool, fn RawScalarFunc) error {
	if err := c.check(); err != nil {
		return err
	}
	h := funcSeq.Add(1)
	funcMu.Lock()
	funcReg[h] = c
	funcMu.Unlock()
	c.mu.Lock()
	c.funcs[h] = scalarImpl{raw: fn}
	c.mu.Unlock()
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	flags := int(C.SQLITE_UTF8)
	if deterministic {
		flags |= int(C.SQLITE_DETERMINISTIC)
	}
	if rc := C.murmurCreateFunction(c.db, cname, C.int(nArg), C.int(flags), C.longlong(h)); rc != C.SQLITE_OK {
		funcMu.Lock()
		delete(funcReg, h)
		funcMu.Unlock()
		c.mu.Lock()
		delete(c.funcs, h)
		c.mu.Unlock()
		return connErr(c.db, rc)
	}
	return nil
}

// Session captures changes on selected tables of one connection.
type Session struct {
	c   *Conn
	ptr *C.sqlite3_session
}

// CreateSession starts a session on the main database with changeset
// size tracking enabled (mirrors capture::Capture::new).
func (c *Conn) CreateSession() (*Session, error) {
	if err := c.check(); err != nil {
		return nil, err
	}
	main := C.CString("main")
	defer C.free(unsafe.Pointer(main))
	var ptr *C.sqlite3_session
	if rc := C.sqlite3session_create(c.db, main, &ptr); rc != C.SQLITE_OK {
		return nil, connErr(c.db, rc)
	}
	enabled := C.int(1)
	if rc := C.sqlite3session_object_config(ptr, C.SQLITE_SESSION_OBJCONFIG_SIZE, unsafe.Pointer(&enabled)); rc != C.SQLITE_OK {
		C.sqlite3session_delete(ptr)
		return nil, connErr(c.db, rc)
	}
	return &Session{c: c, ptr: ptr}, nil
}

// Attach starts capturing a table. Tables need a primary key for capture.
func (s *Session) Attach(table string) error {
	if s.ptr == nil {
		return ErrClosed
	}
	ct := C.CString(table)
	defer C.free(unsafe.Pointer(ct))
	if rc := C.sqlite3session_attach(s.ptr, ct); rc != C.SQLITE_OK {
		return connErr(s.c.db, rc)
	}
	return nil
}

// MemoryUsed estimates session heap usage in bytes.
func (s *Session) MemoryUsed() int64 {
	if s.ptr == nil {
		return 0
	}
	return int64(C.sqlite3session_memory_used(s.ptr))
}

// ChangesetSize reports the pending changeset size in bytes.
func (s *Session) ChangesetSize() int64 {
	if s.ptr == nil {
		return 0
	}
	return int64(C.sqlite3session_changeset_size(s.ptr))
}

// IsEmpty reports whether the session has recorded anything.
func (s *Session) IsEmpty() bool {
	if s.ptr == nil {
		return true
	}
	return C.sqlite3session_isempty(s.ptr) != 0
}

// Changeset returns the session's net-effect changeset: one coalesced op
// per touched row since the session was created, computed against the
// current database contents. It does NOT clear the session; calling it
// again without further writes returns identical bytes. Murmur recreates
// the session at every checkpoint boundary to delimit batches.
func (s *Session) Changeset() ([]byte, error) {
	if s.ptr == nil {
		return nil, ErrClosed
	}
	var n C.int
	var p unsafe.Pointer
	if rc := C.sqlite3session_changeset(s.ptr, &n, &p); rc != C.SQLITE_OK {
		return nil, connErr(s.c.db, rc)
	}
	if n == 0 || p == nil {
		return nil, nil
	}
	defer C.sqlite3_free(p)
	return C.GoBytes(p, n), nil
}

// Close deletes the session.
func (s *Session) Close() {
	if s.ptr != nil {
		C.sqlite3session_delete(s.ptr)
		s.ptr = nil
	}
}

// ChangeOp classifies one changeset operation.
type ChangeOp int

const (
	OpInsert ChangeOp = 18
	OpUpdate ChangeOp = 23
	OpDelete ChangeOp = 9
)

// Change is one decoded changeset operation. Old holds pre-images for
// UPDATE/DELETE, New holds post-images for INSERT/UPDATE; inapplicable
// entries are Null values. Present flags distinguish missing images
// (unchanged UPDATE columns, inapplicable sides) from explicit NULLs.
type Change struct {
	Table      string
	Columns    int
	Op         ChangeOp
	Indirect   bool
	PK         []bool
	Old        []Value
	New        []Value
	OldPresent []bool
	NewPresent []bool
}

// IterateChangeset decodes every operation in buf, which may concatenate
// several changesets exactly as the Rust backend stores them.
func IterateChangeset(buf []byte, fn func(Change) error) error {
	if len(buf) == 0 {
		return nil
	}
	var it *C.sqlite3_changeset_iter
	rc := C.sqlite3changeset_start(&it, C.int(len(buf)), unsafe.Pointer(&buf[0]))
	if rc != C.SQLITE_OK {
		return &Error{Code: int(rc), Msg: "sqlite3changeset_start failed"}
	}
	defer C.sqlite3changeset_finalize(it)
	for {
		rc := C.sqlite3changeset_next(it)
		if rc == C.SQLITE_DONE {
			return nil
		}
		if rc != C.SQLITE_ROW {
			return &Error{Code: int(rc), Msg: "sqlite3changeset_next failed"}
		}
		var ctab *C.char
		var ncol C.int
		var op C.int
		var indirect C.int
		if rc := C.sqlite3changeset_op(it, &ctab, &ncol, &op, &indirect); rc != C.SQLITE_OK {
			return &Error{Code: int(rc), Msg: "sqlite3changeset_op failed"}
		}
		var cpk *C.uchar
		var npk C.int
		if rc := C.sqlite3changeset_pk(it, &cpk, &npk); rc != C.SQLITE_OK {
			return &Error{Code: int(rc), Msg: "sqlite3changeset_pk failed"}
		}
		ch := Change{
			Table:      C.GoString(ctab),
			Columns:    int(ncol),
			Op:         ChangeOp(op),
			Indirect:   indirect != 0,
			PK:         make([]bool, int(npk)),
			Old:        make([]Value, int(ncol)),
			New:        make([]Value, int(ncol)),
			OldPresent: make([]bool, int(ncol)),
			NewPresent: make([]bool, int(ncol)),
		}
		if npk > 0 && cpk != nil {
			pk := C.GoBytes(unsafe.Pointer(cpk), npk)
			for i := range pk {
				ch.PK[i] = pk[i] != 0
			}
		}
		// Old images exist only for UPDATE/DELETE, new images only for
		// INSERT/UPDATE; querying the inapplicable side is MISUSE.
		wantOld := ch.Op == OpUpdate || ch.Op == OpDelete
		wantNew := ch.Op == OpInsert || ch.Op == OpUpdate
		for i := range int(ncol) {
			var v *C.sqlite3_value
			if wantOld {
				if rc := C.sqlite3changeset_old(it, C.int(i), &v); rc != C.SQLITE_OK {
					return &Error{Code: int(rc), Msg: "sqlite3changeset_old failed"}
				}
				if v != nil {
					ch.Old[i] = valueOf(v)
					ch.OldPresent[i] = true
				}
			}
			if wantNew {
				if rc := C.sqlite3changeset_new(it, C.int(i), &v); rc != C.SQLITE_OK {
					return &Error{Code: int(rc), Msg: "sqlite3changeset_new failed"}
				}
				if v != nil {
					ch.New[i] = valueOf(v)
					ch.NewPresent[i] = true
				}
			}
		}
		if err := fn(ch); err != nil {
			return err
		}
	}
}

// MemoryUsed reports SQLite's current heap usage.
func MemoryUsed() int64 { return int64(C.sqlite3_memory_used()) }

// MemoryHighwater reports SQLite's peak heap usage.
func MemoryHighwater() int64 { return int64(C.sqlite3_memory_highwater(0)) }
