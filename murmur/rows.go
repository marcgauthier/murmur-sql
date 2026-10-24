package murmur

import (
	"database/sql"
	"math"
	"strconv"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/internal/sqlite"
)

// Row is a single result row with its column names. It is a snapshot:
// the values stay valid after the callback that produced them returns.
type Row struct {
	columns []string
	values  []sqlite.Value
}

// Row runs a query and returns its first row. A query with no rows
// reports ErrNotFound.
func (s *Sql) Row(sql string, params ...any) (*Row, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	if err := rejectTxnVerbs(sql); err != nil {
		return nil, err
	}
	rewritten, err := Rewrite(sql, s.schema)
	if err != nil {
		return nil, err
	}
	stmt, release, err := s.checkout(sql, rewritten)
	if err != nil {
		return nil, err
	}
	defer release()
	if err := stmt.Bind(params...); err != nil {
		return nil, sqlErr(err)
	}
	has, err := stmt.Step()
	if err != nil {
		return nil, sqlErr(err)
	}
	if !has {
		return nil, notFoundErr("query returned no rows")
	}
	return &Row{columns: columnNames(stmt), values: stmt.Columns()}, nil
}

// Columns returns the result column names.
func (r *Row) Columns() []string {
	out := make([]string, len(r.columns))
	copy(out, r.columns)
	return out
}

// Values returns the row's values in natural Go types: nil for NULL,
// int64, float64, string, and []byte.
func (r *Row) Values() []any {
	out := make([]any, len(r.values))
	for i, v := range r.values {
		out[i] = naturalValue(v)
	}
	return out
}

// Scan assigns the row's columns to dest, one destination per column.
// Destinations follow database/sql conventions:
//
//   - *any receives the natural value: nil, int64, float64, string,
//     or []byte.
//   - sql.Scanner receives the natural value.
//   - *string and *[]byte accept text, blobs, and formatted numbers.
//   - Integer and float pointers accept integers and integral floats
//     within range; *bool accepts 0 and 1.
//   - *time.Time accepts the server's timestamp text ("2006-01-02
//     15:04:05.000000"), dates, RFC3339, and unix-epoch numbers.
//
// NULL assigns nil to *any and *[]byte and scans into sql.Scanner;
// every other NULL destination is an error.
func (r *Row) Scan(dest ...any) error {
	if len(dest) != len(r.values) {
		return invalidErr("scan: query returned %d columns, got %d destinations",
			len(r.values), len(dest))
	}
	for i := range r.values {
		if err := scanValue(r.columnName(i), r.values[i], dest[i]); err != nil {
			return err
		}
	}
	return nil
}

func (r *Row) columnName(i int) string {
	if i < len(r.columns) {
		return r.columns[i]
	}
	return ""
}

// Rows is a streaming result cursor. Rows must be consumed inside the
// Transaction or Read callback that created them: after the callback
// returns, Next reports no rows and Err reports the misuse instead of
// touching the writer connection off-thread.
type Rows struct {
	sql     *Sql
	stmt    *sqlite.Stmt
	release func()
	columns []string
	current []sqlite.Value
	done    bool
	closed  bool
	freed   bool
	err     error
}

// Rows runs a query and returns a cursor over its rows. Call Next to
// advance to the first row, then Scan or Values to read it. Close
// releases the cursor; it is idempotent.
func (s *Sql) Rows(sql string, params ...any) (*Rows, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	if err := rejectTxnVerbs(sql); err != nil {
		return nil, err
	}
	rewritten, err := Rewrite(sql, s.schema)
	if err != nil {
		return nil, err
	}
	stmt, release, err := s.checkout(sql, rewritten)
	if err != nil {
		return nil, err
	}
	if err := stmt.Bind(params...); err != nil {
		release()
		return nil, sqlErr(err)
	}
	return &Rows{sql: s, stmt: stmt, release: release, columns: columnNames(stmt)}, nil
}

// Columns returns the result column names.
func (r *Rows) Columns() []string {
	out := make([]string, len(r.columns))
	copy(out, r.columns)
	return out
}

// Next advances to the next row, reporting whether one is available.
// The first call advances to the first row.
func (r *Rows) Next() bool {
	if r.closed || r.done || r.err != nil {
		return false
	}
	if err := r.sql.checkOpen(); err != nil {
		r.err = err
		r.finish()
		return false
	}
	has, err := r.stmt.Step()
	if err != nil {
		r.err = sqlErr(err)
		r.finish()
		return false
	}
	if !has {
		r.done = true
		r.current = nil
		r.finish()
		return false
	}
	r.current = r.stmt.Columns()
	return true
}

// Scan assigns the current row's columns to dest; see Row.Scan for the
// conversion rules. Scan before the first Next is an error.
func (r *Rows) Scan(dest ...any) error {
	current, err := r.currentRow()
	if err != nil {
		return err
	}
	if len(dest) != len(current) {
		return invalidErr("scan: query returned %d columns, got %d destinations",
			len(current), len(dest))
	}
	for i := range current {
		if err := scanValue(r.columnName(i), current[i], dest[i]); err != nil {
			return err
		}
	}
	return nil
}

// Values returns the current row's values in natural Go types; see
// Row.Values.
func (r *Rows) Values() ([]any, error) {
	current, err := r.currentRow()
	if err != nil {
		return nil, err
	}
	out := make([]any, len(current))
	for i, v := range current {
		out[i] = naturalValue(v)
	}
	return out, nil
}

// Err reports the cursor's terminal error, if any.
func (r *Rows) Err() error { return r.err }

// Close releases the cursor. It is idempotent and reports nil.
func (r *Rows) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	r.current = nil
	r.finish()
	return nil
}

func (r *Rows) currentRow() ([]sqlite.Value, error) {
	if r.current == nil {
		if r.err != nil {
			return nil, r.err
		}
		if r.closed || r.done {
			return nil, invalidErr("scan: no current row")
		}
		return nil, invalidErr("scan: call Next before Scan")
	}
	return r.current, nil
}

func (r *Rows) columnName(i int) string {
	if i < len(r.columns) {
		return r.columns[i]
	}
	return ""
}

func (r *Rows) finish() {
	if r.freed {
		return
	}
	r.freed = true
	r.release()
}

func columnNames(stmt *sqlite.Stmt) []string {
	names := make([]string, stmt.ColumnCount())
	for i := range names {
		names[i] = stmt.ColumnName(i)
	}
	return names
}

// naturalValue maps a storage value to its natural Go type.
func naturalValue(v sqlite.Value) any {
	switch v.Type {
	case sqlite.Integer:
		return v.Int
	case sqlite.Float:
		return v.Float
	case sqlite.Text:
		return v.Str
	case sqlite.Blob:
		return v.Blob
	default:
		return nil
	}
}

func valueKind(t sqlite.ValueType) string {
	switch t {
	case sqlite.Integer:
		return "integer"
	case sqlite.Float:
		return "float"
	case sqlite.Text:
		return "text"
	case sqlite.Blob:
		return "blob"
	default:
		return "NULL"
	}
}

// scanTimeLayouts parses the timestamp texts the server writes
// ("2006-01-02 15:04:05.000000", "2006-01-02") plus RFC3339. Layouts
// without a zone parse as UTC. Fractional seconds parse even where the
// layout omits them.
var scanTimeLayouts = []string{
	"2006-01-02 15:04:05",
	"2006-01-02 15:04:05-07:00",
	time.RFC3339Nano,
	time.RFC3339,
	"2006-01-02",
	"2006-01-02T15:04:05",
}

func scanValue(col string, src sqlite.Value, dest any) error {
	if dest == nil {
		return invalidErr("scan column %q: destination is nil", col)
	}
	if d, ok := dest.(*any); ok {
		*d = naturalValue(src)
		return nil
	}
	if scanner, ok := dest.(sql.Scanner); ok {
		if err := scanner.Scan(naturalValue(src)); err != nil {
			return invalidErr("scan column %q: %v", col, err)
		}
		return nil
	}
	nullErr := func(name string) error {
		return invalidErr("scan column %q: cannot scan NULL into %s (use *any or sql.Null*)", col, name)
	}
	switch d := dest.(type) {
	case *string:
		s, err := scanString(src)
		if err != nil {
			if src.Type == sqlite.Null {
				return nullErr("*string")
			}
			return invalidErr("scan column %q: %v", col, err)
		}
		*d = s
		return nil
	case *[]byte:
		if src.Type == sqlite.Null {
			*d = nil
			return nil
		}
		b, err := scanBytes(src)
		if err != nil {
			return invalidErr("scan column %q: %v", col, err)
		}
		*d = b
		return nil
	case *bool:
		if src.Type != sqlite.Integer || (src.Int != 0 && src.Int != 1) {
			if src.Type == sqlite.Null {
				return nullErr("*bool")
			}
			return invalidErr("scan column %q: cannot scan %s into *bool (want 0 or 1)", col, valueKind(src.Type))
		}
		*d = src.Int == 1
		return nil
	case *int:
		v, err := scanInt(col, src, strconv.IntSize)
		if err != nil {
			return err
		}
		*d = int(v)
		return nil
	case *int8:
		v, err := scanInt(col, src, 8)
		if err != nil {
			return err
		}
		*d = int8(v)
		return nil
	case *int16:
		v, err := scanInt(col, src, 16)
		if err != nil {
			return err
		}
		*d = int16(v)
		return nil
	case *int32:
		v, err := scanInt(col, src, 32)
		if err != nil {
			return err
		}
		*d = int32(v)
		return nil
	case *int64:
		v, err := scanInt(col, src, 64)
		if err != nil {
			return err
		}
		*d = v
		return nil
	case *uint:
		v, err := scanUint(col, src, strconv.IntSize)
		if err != nil {
			return err
		}
		*d = uint(v)
		return nil
	case *uint8:
		v, err := scanUint(col, src, 8)
		if err != nil {
			return err
		}
		*d = uint8(v)
		return nil
	case *uint16:
		v, err := scanUint(col, src, 16)
		if err != nil {
			return err
		}
		*d = uint16(v)
		return nil
	case *uint32:
		v, err := scanUint(col, src, 32)
		if err != nil {
			return err
		}
		*d = uint32(v)
		return nil
	case *uint64:
		v, err := scanUint(col, src, 64)
		if err != nil {
			return err
		}
		*d = v
		return nil
	case *float32:
		v, err := scanFloat(col, src)
		if err != nil {
			return err
		}
		*d = float32(v)
		return nil
	case *float64:
		v, err := scanFloat(col, src)
		if err != nil {
			return err
		}
		*d = v
		return nil
	case *time.Time:
		v, err := scanTime(col, src)
		if err != nil {
			return err
		}
		*d = v
		return nil
	default:
		return invalidErr("scan column %q: cannot scan %s into %T", col, valueKind(src.Type), dest)
	}
}

func scanString(src sqlite.Value) (string, error) {
	switch src.Type {
	case sqlite.Text:
		return src.Str, nil
	case sqlite.Integer:
		return strconv.FormatInt(src.Int, 10), nil
	case sqlite.Float:
		return strconv.FormatFloat(src.Float, 'g', -1, 64), nil
	case sqlite.Blob:
		return string(src.Blob), nil
	default:
		return "", invalidErr("cannot scan %s into *string", valueKind(src.Type))
	}
}

func scanBytes(src sqlite.Value) ([]byte, error) {
	switch src.Type {
	case sqlite.Blob:
		out := make([]byte, len(src.Blob))
		copy(out, src.Blob)
		return out, nil
	case sqlite.Text:
		return []byte(src.Str), nil
	case sqlite.Integer:
		return []byte(strconv.FormatInt(src.Int, 10)), nil
	case sqlite.Float:
		return []byte(strconv.FormatFloat(src.Float, 'g', -1, 64)), nil
	default:
		return nil, invalidErr("cannot scan %s into *[]byte", valueKind(src.Type))
	}
}

func scanInt(col string, src sqlite.Value, bits int) (int64, error) {
	name := "int"
	if bits != strconv.IntSize {
		name = "int" + strconv.Itoa(bits)
	}
	switch src.Type {
	case sqlite.Integer:
		if bits < 64 {
			limit := int64(1) << (bits - 1)
			if src.Int < -limit || src.Int >= limit {
				return 0, invalidErr("scan column %q: %d overflows *%s", col, src.Int, name)
			}
		}
		return src.Int, nil
	case sqlite.Float:
		if math.Trunc(src.Float) != src.Float {
			return 0, invalidErr("scan column %q: cannot scan non-integral float %v into *%s", col, src.Float, name)
		}
		limit := math.Ldexp(1, bits-1)
		if src.Float < -limit || src.Float >= limit {
			return 0, invalidErr("scan column %q: %v overflows *%s", col, src.Float, name)
		}
		return int64(src.Float), nil
	case sqlite.Null:
		return 0, invalidErr("scan column %q: cannot scan NULL into *%s (use *any or sql.Null*)", col, name)
	default:
		return 0, invalidErr("scan column %q: cannot scan %s into *%s", col, valueKind(src.Type), name)
	}
}

func scanUint(col string, src sqlite.Value, bits int) (uint64, error) {
	name := "uint"
	if bits != strconv.IntSize {
		name = "uint" + strconv.Itoa(bits)
	}
	switch src.Type {
	case sqlite.Integer:
		if src.Int < 0 {
			return 0, invalidErr("scan column %q: cannot scan %d into *%s", col, src.Int, name)
		}
		if bits < 64 && uint64(src.Int) >= uint64(1)<<bits {
			return 0, invalidErr("scan column %q: %d overflows *%s", col, src.Int, name)
		}
		return uint64(src.Int), nil
	case sqlite.Float:
		if math.Trunc(src.Float) != src.Float || src.Float < 0 {
			return 0, invalidErr("scan column %q: cannot scan float %v into *%s", col, src.Float, name)
		}
		if src.Float >= math.Ldexp(1, bits) {
			return 0, invalidErr("scan column %q: %v overflows *%s", col, src.Float, name)
		}
		return uint64(src.Float), nil
	case sqlite.Null:
		return 0, invalidErr("scan column %q: cannot scan NULL into *%s (use *any or sql.Null*)", col, name)
	default:
		return 0, invalidErr("scan column %q: cannot scan %s into *%s", col, valueKind(src.Type), name)
	}
}

func scanFloat(col string, src sqlite.Value) (float64, error) {
	switch src.Type {
	case sqlite.Float:
		return src.Float, nil
	case sqlite.Integer:
		return float64(src.Int), nil
	case sqlite.Null:
		return 0, invalidErr("scan column %q: cannot scan NULL into *float64 (use *any or sql.Null*)", col)
	default:
		return 0, invalidErr("scan column %q: cannot scan %s into *float64", col, valueKind(src.Type))
	}
}

func scanTime(col string, src sqlite.Value) (time.Time, error) {
	switch src.Type {
	case sqlite.Text:
		for _, layout := range scanTimeLayouts {
			if v, err := time.Parse(layout, src.Str); err == nil {
				return v, nil
			}
		}
		return time.Time{}, invalidErr("scan column %q: cannot parse %q as a timestamp", col, src.Str)
	case sqlite.Integer:
		return time.Unix(src.Int, 0).UTC(), nil
	case sqlite.Float:
		sec, frac := math.Modf(src.Float)
		return time.Unix(int64(sec), int64(frac*1e9)).UTC(), nil
	case sqlite.Null:
		return time.Time{}, invalidErr("scan column %q: cannot scan NULL into *time.Time (use *any or sql.Null*)", col)
	default:
		return time.Time{}, invalidErr("scan column %q: cannot scan %s into *time.Time", col, valueKind(src.Type))
	}
}
