package murmur

import (
	"bytes"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/internal/sqlite"
)

// scanTestDDL covers every SQLite storage class plus NULLs.
const scanTestDDL = `CREATE TABLE vals(
	id INTEGER PRIMARY KEY,
	i INTEGER,
	f REAL,
	t TEXT,
	b BLOB,
	n INTEGER
)`

func openScanDB(t *testing.T) *Database {
	t.Helper()
	dir := t.TempDir()
	db := openTestDB(t, dir+"/db")
	if err := db.Migrate(NewMigration(scanTestDDL)); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	execMust(t, db, "INSERT INTO vals(id, i, f, t, b, n) VALUES (1, -42, 1.5, 'hello', x'0001ff', NULL)")
	execMust(t, db, "INSERT INTO vals(id, i, f, t, b) VALUES (2, 7, 2.0, '', x'')")
	return db
}

// naturalOf converts a sqlite.Value to the natural Go value Scan assigns
// into *any, mirroring the documented contract.
func naturalOf(v sqlite.Value) any {
	switch v.Type {
	case sqlite.Null:
		return nil
	case sqlite.Integer:
		return v.Int
	case sqlite.Float:
		return v.Float
	case sqlite.Text:
		return v.Str
	case sqlite.Blob:
		return v.Blob
	}
	return nil
}

// TestRowScanAssignsNaturalTypes pins Scan into *any against the buffered
// Query baseline: identical values, plus column names.
func TestRowScanAssignsNaturalTypes(t *testing.T) {
	db := openScanDB(t)
	defer db.Close()
	v, err := db.Read(func(s *Sql) (any, error) {
		return s.Row("SELECT i, f, t, b, n FROM vals WHERE id = 1")
	})
	if err != nil {
		t.Fatalf("row: %v", err)
	}
	row := v.(*Row)
	if got := row.Columns(); len(got) != 5 || got[0] != "i" || got[4] != "n" {
		t.Fatalf("columns = %v, want [i f t b n]", got)
	}
	var i, f, tx, bl, n any
	if err := row.Scan(&i, &f, &tx, &bl, &n); err != nil {
		t.Fatalf("scan: %v", err)
	}
	base, err := db.Read(func(s *Sql) (any, error) {
		return s.QueryRow("SELECT i, f, t, b, n FROM vals WHERE id = 1")
	})
	if err != nil {
		t.Fatalf("baseline: %v", err)
	}
	want := base.([]sqlite.Value)
	got := []any{i, f, tx, bl, n}
	for c := range want {
		w := naturalOf(want[c])
		if wb, ok := w.([]byte); ok {
			if !bytes.Equal(got[c].([]byte), wb) {
				t.Fatalf("col %d = %v, want %v", c, got[c], w)
			}
			continue
		}
		if got[c] != w {
			t.Fatalf("col %d = %#v, want %#v", c, got[c], w)
		}
	}
	if n != nil {
		t.Fatalf("NULL scanned into *any = %#v, want nil", n)
	}
	if tx.(string) != "hello" || i.(int64) != -42 || f.(float64) != 1.5 {
		t.Fatalf("scanned = %v %v %v, want -42 1.5 hello", i, f, tx)
	}
}

// TestRowScanConvertsScalars pins the scalar conversion matrix.
func TestRowScanConvertsScalars(t *testing.T) {
	db := openScanDB(t)
	defer db.Close()
	v, err := db.Read(func(s *Sql) (any, error) {
		return s.Row("SELECT i, f, t, b FROM vals WHERE id = 1")
	})
	if err != nil {
		t.Fatalf("row: %v", err)
	}
	row := v.(*Row)
	var i int
	var i8 int8
	var i64 int64
	var u uint
	var f32 float32
	var f64 float64
	var s string
	var sb []byte
	if err := row.Scan(&i, &f64, &s, &sb); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if i != -42 || f64 != 1.5 || s != "hello" || !bytes.Equal(sb, []byte{0, 1, 0xff}) {
		t.Fatalf("scan = %d %v %q %x", i, f64, s, sb)
	}
	v, err = db.Read(func(s *Sql) (any, error) {
		return s.Row("SELECT 200, 3.0, 1, 0, 100, 200")
	})
	if err != nil {
		t.Fatalf("row: %v", err)
	}
	row = v.(*Row)
	var t1, t0 bool
	if err := row.Scan(&i64, &f32, &t1, &t0, &i8, &u); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if i64 != 200 || f32 != 3 || !t1 || t0 || i8 != 100 || u != 200 {
		t.Fatalf("scan = %d %v %v %v %d %d", i64, f32, t1, t0, i8, u)
	}
}

// TestRowScanRejectsBadConversions pins Scan errors: NULL into scalars,
// overflow, truncation, bad bools, count and shape mismatches.
func TestRowScanRejectsBadConversions(t *testing.T) {
	db := openScanDB(t)
	defer db.Close()
	scan := func(query string, dest ...any) error {
		v, err := db.Read(func(s *Sql) (any, error) {
			return s.Row(query)
		})
		if err != nil {
			t.Fatalf("row %q: %v", query, err)
		}
		return v.(*Row).Scan(dest...)
	}
	var s string
	var i int64
	var i8 int8
	var u8 uint8
	var b bool
	var f float64
	if err := scan("SELECT NULL", &s); err == nil {
		t.Fatalf("NULL into *string succeeded, want error")
	}
	if err := scan("SELECT NULL", &i); err == nil {
		t.Fatalf("NULL into *int64 succeeded, want error")
	}
	if err := scan("SELECT 1000", &i8); err == nil {
		t.Fatalf("1000 into *int8 succeeded, want overflow error")
	}
	if err := scan("SELECT -1", &u8); err == nil {
		t.Fatalf("-1 into *uint8 succeeded, want error")
	}
	if err := scan("SELECT 1.5", &i); err == nil {
		t.Fatalf("1.5 into *int64 succeeded, want truncation error")
	}
	if err := scan("SELECT 2", &b); err == nil {
		t.Fatalf("2 into *bool succeeded, want error")
	}
	if err := scan("SELECT 'abc'", &i); err == nil {
		t.Fatalf("'abc' into *int64 succeeded, want error")
	}
	if err := scan("SELECT 1, 2", &i); err == nil {
		t.Fatalf("1 dest for 2 columns succeeded, want error")
	}
	if err := scan("SELECT 1", &i, &f); err == nil {
		t.Fatalf("2 dests for 1 column succeeded, want error")
	}
	if err := scan("SELECT 1", nil); err == nil {
		t.Fatalf("nil dest succeeded, want error")
	}
	if err := scan("SELECT 1", 42); err == nil {
		t.Fatalf("non-pointer dest succeeded, want error")
	}
	var p *int64
	if err := scan("SELECT 1", &p); err == nil {
		t.Fatalf("**int64 dest succeeded, want error")
	}
}

// TestTxnVerbsRejected pins that bare transaction statements fail fast on
// every Sql entry point instead of disturbing the engine's framing, and
// that the connection stays healthy afterwards.
func TestTxnVerbsRejected(t *testing.T) {
	db := openScanDB(t)
	defer db.Close()
	verbs := []string{"BEGIN", "COMMIT", "ROLLBACK", "END", "START TRANSACTION",
		"-- comment\nBEGIN", "BEGIN;"}
	_, err := db.Transaction(func(s *Sql) (any, error) {
		for _, q := range verbs {
			if _, err := s.Execute(q); err == nil {
				t.Fatalf("Execute(%q) succeeded, want error", q)
			}
			if _, err := s.ExecuteMany(q, []any{}); err == nil {
				t.Fatalf("ExecuteMany(%q) succeeded, want error", q)
			}
			if err := s.ExecuteBatch(q); err == nil {
				t.Fatalf("ExecuteBatch(%q) succeeded, want error", q)
			}
			if _, err := s.Prepare(q); err == nil {
				t.Fatalf("Prepare(%q) succeeded, want error", q)
			}
			if _, err := s.Query(q); err == nil {
				t.Fatalf("Query(%q) succeeded, want error", q)
			}
			if _, err := s.QueryRow(q); err == nil {
				t.Fatalf("QueryRow(%q) succeeded, want error", q)
			}
			if _, err := s.Row(q); err == nil {
				t.Fatalf("Row(%q) succeeded, want error", q)
			}
			if _, err := s.Rows(q); err == nil {
				t.Fatalf("Rows(%q) succeeded, want error", q)
			}
		}
		// Quoted lookalikes and real statements still run.
		if _, err := s.Execute("INSERT INTO vals(id, i) VALUES(100, 1)"); err != nil {
			return nil, err
		}
		return s.Query("SELECT 'BEGIN'")
	})
	if err != nil {
		t.Fatalf("transaction: %v", err)
	}
	v, err := db.Read(func(s *Sql) (any, error) {
		return s.QueryRow("SELECT COUNT(*) FROM vals")
	})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got := v.([]sqlite.Value)[0].Int; got != 3 {
		t.Fatalf("count = %d, want 3", got)
	}
}

// TestRowScanTime pins timestamp scanning from the server's text format,
// dates, RFC3339, and unix-epoch integers and reals.
func TestRowScanTime(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, dir+"/db")
	defer db.Close()
	if err := db.Migrate(NewMigration(
		`CREATE TABLE ts(id INTEGER PRIMARY KEY, t TEXT, u INTEGER, r REAL)`)); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	execMust(t, db, `INSERT INTO ts(id, t, u, r) VALUES
		(1, '2024-05-06 07:08:09.000000', 0, 1.5),
		(2, '2024-05-06', 86400, 0),
		(3, '2024-05-06T07:08:09Z', -1, 0),
		(4, 'not a time', 0, 0)`)
	scanTime := func(id int) (time.Time, error) {
		v, err := db.Read(func(s *Sql) (any, error) {
			return s.Row("SELECT t FROM ts WHERE id = ?", id)
		})
		if err != nil {
			t.Fatalf("row: %v", err)
		}
		var ts time.Time
		if err := v.(*Row).Scan(&ts); err != nil {
			return time.Time{}, err
		}
		return ts, nil
	}
	got, err := scanTime(1)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if want := time.Date(2024, 5, 6, 7, 8, 9, 0, time.UTC); !got.Equal(want) {
		t.Fatalf("t = %v, want %v", got, want)
	}
	got, err = scanTime(2)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if want := time.Date(2024, 5, 6, 0, 0, 0, 0, time.UTC); !got.Equal(want) {
		t.Fatalf("t = %v, want %v", got, want)
	}
	got, err = scanTime(3)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if want := time.Date(2024, 5, 6, 7, 8, 9, 0, time.UTC); !got.Equal(want) {
		t.Fatalf("t = %v, want %v", got, want)
	}
	if _, err := scanTime(4); err == nil {
		t.Fatalf("bad timestamp scanned, want error")
	}
	v, err := db.Read(func(s *Sql) (any, error) {
		return s.Row("SELECT u, r FROM ts WHERE id = 1")
	})
	if err != nil {
		t.Fatalf("row: %v", err)
	}
	var epoch, frac time.Time
	if err := v.(*Row).Scan(&epoch, &frac); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if want := time.Unix(0, 0).UTC(); !epoch.Equal(want) {
		t.Fatalf("epoch = %v, want %v", epoch, want)
	}
	if want := time.Unix(1, 500*1e6).UTC(); !frac.Equal(want) {
		t.Fatalf("frac = %v, want %v", frac, want)
	}
}

type scanRecorder struct{ got any }

func (r *scanRecorder) Scan(v any) error {
	r.got = v
	return nil
}

// TestRowScanHonorsScanner pins sql.Scanner destinations, including the
// standard nullable types.
func TestRowScanHonorsScanner(t *testing.T) {
	db := openScanDB(t)
	defer db.Close()
	v, err := db.Read(func(s *Sql) (any, error) {
		return s.Row("SELECT t, n, i FROM vals WHERE id = 1")
	})
	if err != nil {
		t.Fatalf("row: %v", err)
	}
	var text, null sql.NullString
	var rec scanRecorder
	if err := v.(*Row).Scan(&text, &null, &rec); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if !text.Valid || text.String != "hello" {
		t.Fatalf("text = %+v, want hello", text)
	}
	if null.Valid {
		t.Fatalf("null = %+v, want invalid", null)
	}
	if rec.got != int64(-42) {
		t.Fatalf("recorder got %#v, want int64(-42)", rec.got)
	}
}

// TestRowNoRows pins that a single-row lookup with no match reports
// ErrNotFound for errors.Is matching.
func TestRowNoRows(t *testing.T) {
	db := openScanDB(t)
	defer db.Close()
	_, err := db.Read(func(s *Sql) (any, error) {
		return s.Row("SELECT i FROM vals WHERE id = 999")
	})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want errors.Is ErrNotFound", err)
	}
}

// collectRows drains Rows into natural values, mirroring database/sql's
// iteration contract.
func collectRows(t *testing.T, s *Sql, query string, args ...any) ([]string, [][]any) {
	t.Helper()
	rows, err := s.Rows(query, args...)
	if err != nil {
		t.Fatalf("rows: %v", err)
	}
	defer rows.Close()
	cols := rows.Columns()
	var out [][]any
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			t.Fatalf("values: %v", err)
		}
		out = append(out, vals)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("err: %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return cols, out
}

// TestRowsIteratesAll pins streaming iteration against the buffered Query
// baseline: same rows, same order, plus column names.
func TestRowsIteratesAll(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, dir+"/db")
	defer db.Close()
	if err := db.Migrate(NewMigration(
		`CREATE TABLE items(id INTEGER PRIMARY KEY, name TEXT, score REAL)`)); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if _, err := db.Transaction(func(s *Sql) (any, error) {
		for i := 0; i < 50; i++ {
			if _, err := s.Execute("INSERT INTO items(id, name, score) VALUES(?, ?, ?)",
				i, string(rune('a'+i%26)), float64(i)*0.5); err != nil {
				return nil, err
			}
		}
		return nil, nil
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	base, err := db.Read(func(s *Sql) (any, error) {
		return s.Query("SELECT id, name, score FROM items ORDER BY id")
	})
	if err != nil {
		t.Fatalf("baseline: %v", err)
	}
	want := base.([][]sqlite.Value)
	v, err := db.Read(func(s *Sql) (any, error) {
		cols, out := collectRows(t, s, "SELECT id, name, score FROM items ORDER BY id")
		return []any{cols, out}, nil
	})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	parts := v.([]any)
	cols := parts[0].([]string)
	got := parts[1].([][]any)
	if len(cols) != 3 || cols[0] != "id" || cols[1] != "name" || cols[2] != "score" {
		t.Fatalf("columns = %v", cols)
	}
	if len(got) != len(want) {
		t.Fatalf("streamed %d rows, want %d", len(got), len(want))
	}
	for r := range want {
		for c := range want[r] {
			if got[r][c] != naturalOf(want[r][c]) {
				t.Fatalf("row %d col %d = %#v, want %#v", r, c, got[r][c], naturalOf(want[r][c]))
			}
		}
	}
}

// TestRowsScanCurrent pins Scan on the streaming cursor: one row at a time,
// invalid before the first Next.
func TestRowsScanCurrent(t *testing.T) {
	db := openScanDB(t)
	defer db.Close()
	_, err := db.Read(func(s *Sql) (any, error) {
		rows, err := s.Rows("SELECT id, t FROM vals ORDER BY id")
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var id int64
		var name string
		if err := rows.Scan(&id, &name); err == nil {
			t.Fatalf("Scan before Next succeeded, want error")
		}
		var ids []int64
		var names []string
		for rows.Next() {
			if err := rows.Scan(&id, &name); err != nil {
				return nil, err
			}
			ids = append(ids, id)
			names = append(names, name)
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		if len(ids) != 2 || ids[0] != 1 || ids[1] != 2 || names[0] != "hello" || names[1] != "" {
			t.Fatalf("streamed = %v %q", ids, names)
		}
		if rows.Next() {
			t.Fatalf("Next after exhaustion returned true")
		}
		return nil, nil
	})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
}

// TestRowsEmpty pins the empty-result contract: no rows, no error, names
// still available.
func TestRowsEmpty(t *testing.T) {
	db := openScanDB(t)
	defer db.Close()
	v, err := db.Read(func(s *Sql) (any, error) {
		rows, err := s.Rows("SELECT i, t FROM vals WHERE id = 999")
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		if rows.Next() {
			t.Fatalf("Next on empty result returned true")
		}
		return rows.Columns(), rows.Err()
	})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if cols := v.([]string); len(cols) != 2 || cols[0] != "i" || cols[1] != "t" {
		t.Fatalf("columns = %v", cols)
	}
}

// TestRowsDiesAfterCallback pins that a Rows handle escaping its callback
// fails closed instead of touching the writer connection off-thread.
func TestRowsDiesAfterCallback(t *testing.T) {
	db := openScanDB(t)
	defer db.Close()
	v, err := db.Read(func(s *Sql) (any, error) {
		return s.Rows("SELECT i FROM vals")
	})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	rows := v.(*Rows)
	if rows.Next() {
		t.Fatalf("Next after callback returned true, want fail-closed")
	}
	if rows.Err() == nil {
		t.Fatalf("Err after callback is nil, want error")
	}
	var i int64
	if err := rows.Scan(&i); err == nil {
		t.Fatalf("Scan after callback succeeded, want error")
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// TestExecuteManyInsertsBatch pins single-prepare bulk execution: the total
// change count and contents match looped Execute.
func TestExecuteManyInsertsBatch(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, dir+"/db")
	defer db.Close()
	if err := db.Migrate(NewMigration(
		`CREATE TABLE kv(k TEXT NOT NULL PRIMARY KEY, v TEXT NOT NULL)`)); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	v, err := db.Transaction(func(s *Sql) (any, error) {
		return s.ExecuteMany("INSERT INTO kv(k, v) VALUES(?, ?)",
			[]any{"a", "1"},
			[]any{"b", "2"})
	})
	if err != nil {
		t.Fatalf("bulk: %v", err)
	}
	if v.(int64) != 2 {
		t.Fatalf("changes = %v, want 2", v)
	}
	// NULL violates the NOT NULL constraint: the batch fails and the
	// transaction rolls back, leaving the earlier rows untouched.
	_, err = db.Transaction(func(s *Sql) (any, error) {
		return s.ExecuteMany("INSERT INTO kv(k, v) VALUES(?, ?)",
			[]any{"x", "1"},
			[]any{"y", nil})
	})
	if err == nil {
		t.Fatalf("bulk with NULL succeeded, want constraint error")
	}
	count, err := db.Read(func(s *Sql) (any, error) {
		return s.Query("SELECT k, v FROM kv ORDER BY k")
	})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	rows := count.([][]sqlite.Value)
	if len(rows) != 2 || rows[0][0].Str != "a" || rows[1][1].Str != "2" {
		t.Fatalf("rows = %v", rows)
	}
	v, err = db.Transaction(func(s *Sql) (any, error) {
		return s.ExecuteMany("INSERT INTO kv(k, v) VALUES(?, ?)",
			[]any{"x", "1"},
			[]any{"y", "2"})
	})
	if err != nil {
		t.Fatalf("bulk: %v", err)
	}
	if v.(int64) != 2 {
		t.Fatalf("changes = %v, want 2", v)
	}
	v, err = db.Transaction(func(s *Sql) (any, error) {
		return s.ExecuteMany("UPDATE kv SET v = ? WHERE k = ?", []any{"9", "x"}, []any{"8", "zzz"})
	})
	if err != nil {
		t.Fatalf("bulk update: %v", err)
	}
	if v.(int64) != 1 {
		t.Fatalf("update changes = %v, want 1", v)
	}
	v, err = db.Transaction(func(s *Sql) (any, error) {
		return s.ExecuteMany("INSERT INTO kv(k, v) VALUES(?, ?)")
	})
	if err != nil {
		t.Fatalf("empty bulk: %v", err)
	}
	if v.(int64) != 0 {
		t.Fatalf("empty changes = %v, want 0", v)
	}
}

// TestStmtCacheReusesStatements pins transparent prepare reuse: repeated
// identical statements share one cache entry, and the cache is finalized
// when the callback returns.
func TestStmtCacheReusesStatements(t *testing.T) {
	db := openScanDB(t)
	defer db.Close()
	var seen *Sql
	_, err := db.Transaction(func(s *Sql) (any, error) {
		seen = s
		for i := 0; i < 3; i++ {
			if _, err := s.Execute("UPDATE vals SET i = i + 1 WHERE id = 1"); err != nil {
				return nil, err
			}
		}
		if _, err := s.Query("SELECT i FROM vals WHERE id = 1"); err != nil {
			return nil, err
		}
		if _, err := s.Query("SELECT i FROM vals WHERE id = 1"); err != nil {
			return nil, err
		}
		if n := len(seen.stmts.entries); n != 2 {
			t.Fatalf("cache entries = %d, want 2", n)
		}
		return nil, nil
	})
	if err != nil {
		t.Fatalf("transaction: %v", err)
	}
	if !seen.dead {
		t.Fatalf("sql handle not marked dead after callback")
	}
	if n := len(seen.stmts.entries); n != 0 {
		t.Fatalf("cache entries after callback = %d, want 0", n)
	}
	v, err := db.Read(func(s *Sql) (any, error) {
		return s.QueryRow("SELECT i FROM vals WHERE id = 1")
	})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got := v.([]sqlite.Value)[0].Int; got != -39 {
		t.Fatalf("i = %d, want -39", got)
	}
}

// TestStmtCacheEvictsLRU pins the bound: distinct statements beyond the cap
// evict the least recently used entry.
func TestStmtCacheEvictsLRU(t *testing.T) {
	dir := t.TempDir()
	options := DefaultOptions()
	options.Mode = OpenCreateNew
	options.StmtCacheSize = 2
	db, err := Open(dir+"/db", testKey(), options)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	if err := db.Migrate(NewMigration(`CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)`)); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	_, err = db.Transaction(func(s *Sql) (any, error) {
		stmts := []string{
			"INSERT INTO t(id, v) VALUES(1, 'a')",
			"INSERT INTO t(id, v) VALUES(2, 'b')",
			"INSERT INTO t(id, v) VALUES(3, 'c')",
		}
		for _, q := range stmts {
			if _, err := s.Execute(q); err != nil {
				return nil, err
			}
		}
		if n := len(s.stmts.entries); n != 2 {
			t.Fatalf("cache entries = %d, want 2", n)
		}
		if _, ok := s.stmts.entries[stmts[0]]; ok {
			t.Fatalf("LRU entry still cached, want evicted")
		}
		return nil, nil
	})
	if err != nil {
		t.Fatalf("transaction: %v", err)
	}
}

// TestStmtCacheSizeOption pins the option: zero disables the cache and a
// negative size fails Open.
func TestStmtCacheSizeOption(t *testing.T) {
	dir := t.TempDir()
	options := DefaultOptions()
	options.Mode = OpenCreateNew
	options.StmtCacheSize = 0
	db, err := Open(dir+"/db", testKey(), options)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	if err := db.Migrate(NewMigration(`CREATE TABLE t(id INTEGER PRIMARY KEY)`)); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	_, err = db.Transaction(func(s *Sql) (any, error) {
		if s.stmts != nil {
			t.Fatalf("cache present with size 0, want disabled")
		}
		_, err := s.Execute("INSERT INTO t(id) VALUES(1)")
		return nil, err
	})
	if err != nil {
		t.Fatalf("transaction: %v", err)
	}
	bad := DefaultOptions()
	bad.Mode = OpenCreateNew
	bad.StmtCacheSize = -1
	if _, err := Open(dir+"/db2", testKey(), bad); err == nil {
		t.Fatalf("open with negative cache size succeeded, want error")
	}
	if got := DefaultOptions().StmtCacheSize; got <= 0 {
		t.Fatalf("default cache size = %d, want positive", got)
	}
}

// TestStmtCacheNestedSameSQL pins that a cached statement already checked
// out by an open Rows cursor is not shared: the nested query prepares
// fresh and both read correctly.
func TestStmtCacheNestedSameSQL(t *testing.T) {
	db := openScanDB(t)
	defer db.Close()
	_, err := db.Read(func(s *Sql) (any, error) {
		rows, err := s.Rows("SELECT id FROM vals ORDER BY id")
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		if !rows.Next() {
			t.Fatalf("first Next returned false")
		}
		base, err := s.Query("SELECT id FROM vals ORDER BY id")
		if err != nil {
			return nil, err
		}
		var ids []int64
		for {
			vals, err := rows.Values()
			if err != nil {
				return nil, err
			}
			ids = append(ids, vals[0].(int64))
			if !rows.Next() {
				break
			}
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		if len(ids) != len(base) || len(ids) != 2 {
			t.Fatalf("streamed = %v, baseline len %d", ids, len(base))
		}
		for i := range base {
			if ids[i] != base[i][0].Int {
				t.Fatalf("streamed[%d] = %d, baseline %d", i, ids[i], base[i][0].Int)
			}
		}
		return nil, nil
	})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
}
