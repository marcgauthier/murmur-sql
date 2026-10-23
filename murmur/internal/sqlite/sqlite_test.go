package sqlite

import (
	"strings"
	"testing"
)

func openTest(t *testing.T) *Conn {
	t.Helper()
	c, err := OpenMemory()
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestExecQuery(t *testing.T) {
	c := openTest(t)
	if err := c.ExecBatch(`
		CREATE TABLE items(id INTEGER PRIMARY KEY, v TEXT NOT NULL, r REAL, b BLOB);
		INSERT INTO items VALUES(1, 'one', 1.5, x'0102');
	`); err != nil {
		t.Fatalf("batch: %v", err)
	}
	if err := c.Exec(`INSERT INTO items VALUES(?, ?, ?, ?)`, 2, "two", nil, []byte{9}); err != nil {
		t.Fatalf("exec: %v", err)
	}
	rows, err := c.Query(`SELECT id, v, r, b FROM items ORDER BY id`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(rows))
	}
	if rows[0][0].Int != 1 || rows[0][1].Str != "one" || rows[0][2].Float != 1.5 {
		t.Fatalf("row0 = %v", rows[0])
	}
	if len(rows[0][3].Blob) != 2 || rows[0][3].Blob[0] != 1 {
		t.Fatalf("row0 blob = %v", rows[0][3].Blob)
	}
	if rows[1][2].Type != Null {
		t.Fatalf("row1 r type = %v, want Null", rows[1][2].Type)
	}
	row, err := c.QueryRow(`SELECT v FROM items WHERE id = ?`, 2)
	if err != nil || row[0].Str != "two" {
		t.Fatalf("queryrow = %v, %v", row, err)
	}
	if _, err := c.QueryRow(`SELECT v FROM items WHERE id = 99`); err != ErrNoRows {
		t.Fatalf("missing row err = %v, want ErrNoRows", err)
	}
	if got := c.TotalChanges(); got != 2 {
		t.Fatalf("total changes = %d, want 2", got)
	}
	if got := c.LastInsertRowid(); got != 2 {
		t.Fatalf("last rowid = %d, want 2", got)
	}
	s, err := c.Prepare(`SELECT id AS ident, v FROM items WHERE id = ?`)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	defer s.Close()
	if s.ColumnCount() != 2 || s.ColumnName(0) != "ident" {
		t.Fatalf("columns = %d/%q", s.ColumnCount(), s.ColumnName(0))
	}
	if err := s.Bind(1); err != nil {
		t.Fatalf("bind: %v", err)
	}
	has, err := s.Step()
	if err != nil || !has {
		t.Fatalf("step = %v, %v", has, err)
	}
	if got := s.Columns(); got[0].Int != 1 {
		t.Fatalf("cols = %v", got)
	}
	if s.ColumnInt64(0) != 1 || s.ColumnInt(0) != 1 {
		t.Fatalf("ColumnInt = %d, want 1", s.ColumnInt(0))
	}
	if s.ColumnText(1) != "one" {
		t.Fatalf("ColumnText = %q, want 'one'", s.ColumnText(1))
	}
	if s.ColumnIsNull(0) || s.ColumnType(0) != Integer {
		t.Fatalf("ColumnType(0) = %v, want Integer", s.ColumnType(0))
	}
	if s.ColumnType(1) != Text {
		t.Fatalf("ColumnType(1) = %v, want Text", s.ColumnType(1))
	}
	if s.ColumnValue(0).Int != 1 || s.ColumnValue(1).Str != "one" {
		t.Fatalf("ColumnValue mismatch")
	}
	if has, _ := s.Step(); has {
		t.Fatalf("second step still has row")
	}
	if err := c.Exec(`SELECT 1`); err == nil {
		t.Fatalf("Exec with rows succeeded, want error")
	}
	if err := c.Exec(`BAD SQL ((`); err == nil {
		t.Fatalf("bad SQL succeeded, want error")
	}
}

func TestAuthorizer(t *testing.T) {
	c := openTest(t)
	if err := c.ExecBatch(`CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'a');`); err != nil {
		t.Fatalf("setup: %v", err)
	}
	if err := c.SetAuthorizer(func(action AuthAction, a1, a2, a3, a4 string) AuthResult {
		if action == AuthInsert {
			return AuthDeny
		}
		return AuthOK
	}); err != nil {
		t.Fatalf("set authorizer: %v", err)
	}
	if _, err := c.Query(`SELECT v FROM t`); err != nil {
		t.Fatalf("allowed select failed: %v", err)
	}
	err := c.Exec(`INSERT INTO t VALUES(2,'b')`)
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "authorizer") && !strings.Contains(strings.ToLower(err.Error()), "not authorized") {
		t.Fatalf("denied insert err = %v", err)
	}
	if err := c.SetAuthorizer(nil); err != nil {
		t.Fatalf("remove authorizer: %v", err)
	}
	if err := c.Exec(`INSERT INTO t VALUES(2,'b')`); err != nil {
		t.Fatalf("insert after removal: %v", err)
	}
}

func TestScalarFunction(t *testing.T) {
	c := openTest(t)
	err := c.CreateScalarFunction("_echo", -1, true, func(ctx *FuncContext, args []Value) {
		if len(args) == 0 {
			ctx.ResultNull()
			return
		}
		if args[0].Type == Text && args[0].Str == "boom" {
			ctx.ResultError("boom requested")
			return
		}
		ctx.ResultValue(args[0])
	})
	if err != nil {
		t.Fatalf("create function: %v", err)
	}
	row, err := c.QueryRow(`SELECT _echo('hi'), _echo(42), _echo(NULL)`)
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if row[0].Str != "hi" || row[1].Int != 42 || row[2].Type != Null {
		t.Fatalf("results = %v", row)
	}
	if _, err := c.QueryRow(`SELECT _echo('boom')`); err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("error call err = %v", err)
	}
	// Deterministic: usable in generated contexts repeatedly.
	row, err = c.QueryRow(`SELECT _echo('a') = _echo('a')`)
	if err != nil || row[0].Int != 1 {
		t.Fatalf("deterministic = %v, %v", row, err)
	}
}

func collectChanges(t *testing.T, buf []byte) []Change {
	t.Helper()
	var out []Change
	if err := IterateChangeset(buf, func(ch Change) error {
		out = append(out, ch)
		return nil
	}); err != nil {
		t.Fatalf("iterate: %v", err)
	}
	return out
}

func newSession(t *testing.T, c *Conn, table string) *Session {
	t.Helper()
	s, err := c.CreateSession()
	if err != nil {
		t.Fatalf("session: %v", err)
	}
	t.Cleanup(s.Close)
	if err := s.Attach(table); err != nil {
		t.Fatalf("attach: %v", err)
	}
	return s
}

func TestSessionChangeset(t *testing.T) {
	c := openTest(t)
	if err := c.ExecBatch(`CREATE TABLE items(id INTEGER PRIMARY KEY, v TEXT);`); err != nil {
		t.Fatalf("setup: %v", err)
	}
	s := newSession(t, c, "items")
	if !s.IsEmpty() {
		t.Fatalf("fresh session not empty")
	}
	if empty, err := s.Changeset(); err != nil || len(empty) != 0 {
		t.Fatalf("fresh changeset = %d bytes, %v", len(empty), err)
	}
	if err := c.Exec(`INSERT INTO items VALUES(1, 'a')`); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if s.IsEmpty() || s.MemoryUsed() <= 0 || s.ChangesetSize() <= 0 {
		t.Fatalf("empty=%v memory=%d size=%d", s.IsEmpty(), s.MemoryUsed(), s.ChangesetSize())
	}
	buf, err := s.Changeset()
	if err != nil || len(buf) == 0 {
		t.Fatalf("changeset = %d bytes, %v", len(buf), err)
	}
	// No drain: a second read without further writes repeats the bytes.
	again, err := s.Changeset()
	if err != nil || string(again) != string(buf) {
		t.Fatalf("second changeset differs: %d vs %d bytes, %v", len(again), len(buf), err)
	}
	changes := collectChanges(t, buf)
	if len(changes) != 1 {
		t.Fatalf("ops = %d, want 1", len(changes))
	}
	ins := changes[0]
	if ins.Op != OpInsert || ins.Table != "items" || ins.Columns != 2 || ins.Indirect {
		t.Fatalf("insert = %+v", ins)
	}
	if len(ins.PK) != 2 || !ins.PK[0] || ins.PK[1] {
		t.Fatalf("insert pk = %v", ins.PK)
	}
	if ins.Old[0].Type != Null || ins.New[0].Int != 1 || ins.New[1].Str != "a" {
		t.Fatalf("insert old/new = %v / %v", ins.Old, ins.New)
	}

	// UPDATE shape in a fresh generation.
	s2 := newSession(t, c, "items")
	if err := c.Exec(`UPDATE items SET v = 'B' WHERE id = 1`); err != nil {
		t.Fatalf("update: %v", err)
	}
	buf2, err := s2.Changeset()
	if err != nil {
		t.Fatalf("changeset2: %v", err)
	}
	changes2 := collectChanges(t, buf2)
	if len(changes2) != 1 || changes2[0].Op != OpUpdate {
		t.Fatalf("update ops = %+v", changes2)
	}
	upd := changes2[0]
	if upd.Old[0].Int != 1 || upd.Old[1].Str != "a" || upd.New[1].Str != "B" {
		t.Fatalf("update old/new = %v / %v", upd.Old, upd.New)
	}

	// DELETE shape in a fresh generation.
	s3 := newSession(t, c, "items")
	if err := c.Exec(`DELETE FROM items WHERE id = 1`); err != nil {
		t.Fatalf("delete: %v", err)
	}
	buf3, err := s3.Changeset()
	if err != nil {
		t.Fatalf("changeset3: %v", err)
	}
	changes3 := collectChanges(t, buf3)
	if len(changes3) != 1 || changes3[0].Op != OpDelete {
		t.Fatalf("delete ops = %+v", changes3)
	}
	del := changes3[0]
	if del.Old[0].Int != 1 || del.Old[1].Str != "B" || del.New[0].Type != Null {
		t.Fatalf("delete old/new = %v / %v", del.Old, del.New)
	}

	// Net effect within one generation: insert+update+delete coalesce.
	s4 := newSession(t, c, "items")
	if err := c.ExecBatch(`
		INSERT INTO items VALUES(10, 'x');
		UPDATE items SET v = 'y' WHERE id = 10;
		DELETE FROM items WHERE id = 10;
		INSERT INTO items VALUES(11, 'z');
	`); err != nil {
		t.Fatalf("writes: %v", err)
	}
	net := collectChanges(t, mustChangeset(t, s4))
	if len(net) != 1 || net[0].Op != OpInsert || net[0].New[0].Int != 11 {
		t.Fatalf("net ops = %+v", net)
	}

	var none int
	if err := IterateChangeset(nil, func(Change) error { none++; return nil }); err != nil || none != 0 {
		t.Fatalf("empty iterate = %d ops, %v", none, err)
	}
	if err := IterateChangeset([]byte{1, 2, 3}, func(Change) error { return nil }); err == nil {
		t.Fatalf("garbage changeset decoded, want error")
	}
}

func mustChangeset(t *testing.T, s *Session) []byte {
	t.Helper()
	buf, err := s.Changeset()
	if err != nil {
		t.Fatalf("changeset: %v", err)
	}
	return buf
}

// The Rust backend concatenates per-generation changesets (pending_delta)
// and reads them with a single iterator; the Go wrapper must do the same.
func TestConcatenatedChangesets(t *testing.T) {
	c := openTest(t)
	if err := c.ExecBatch(`CREATE TABLE items(id INTEGER PRIMARY KEY, v TEXT);`); err != nil {
		t.Fatalf("setup: %v", err)
	}
	if err := c.Exec(`INSERT INTO items VALUES(1, 'a')`); err != nil {
		t.Fatalf("insert: %v", err)
	}
	first := mustChangeset(t, newSessionBefore(t, c, "items", `INSERT INTO items VALUES(1, 'a')`))
	second := mustChangeset(t, newSessionBefore(t, c, "items", `INSERT INTO items VALUES(2, 'b')`))
	changes := collectChanges(t, append(append([]byte{}, first...), second...))
	if len(changes) != 2 || changes[0].New[0].Int != 1 || changes[1].New[0].Int != 2 {
		t.Fatalf("concatenated ops = %+v", changes)
	}
}

// newSessionBefore wipes the table, starts a generation, and runs one write.
func newSessionBefore(t *testing.T, c *Conn, table, write string) *Session {
	t.Helper()
	if table == "items" {
		if err := c.Exec(`DELETE FROM items`); err != nil {
			t.Fatalf("wipe: %v", err)
		}
	}
	s := newSession(t, c, table)
	if err := c.Exec(write); err != nil {
		t.Fatalf("write: %v", err)
	}
	return s
}

func TestFTS5(t *testing.T) {
	c := openTest(t)
	if err := c.ExecBatch(`
		CREATE VIRTUAL TABLE docs USING fts5(title, body);
		INSERT INTO docs VALUES('hello world', 'the quick brown fox');
		INSERT INTO docs VALUES('other', 'nothing relevant here');
	`); err != nil {
		t.Fatalf("fts5 setup: %v", err)
	}
	row, err := c.QueryRow(`SELECT title FROM docs WHERE docs MATCH 'quick'`)
	if err != nil || row[0].Str != "hello world" {
		t.Fatalf("match = %v, %v", row, err)
	}
}

func TestMemoryStats(t *testing.T) {
	c := openTest(t)
	if err := c.ExecBatch(`CREATE TABLE t(x TEXT); INSERT INTO t VALUES('pad me');`); err != nil {
		t.Fatalf("setup: %v", err)
	}
	if MemoryUsed() <= 0 || MemoryHighwater() <= 0 {
		t.Fatalf("memory used=%d highwater=%d", MemoryUsed(), MemoryHighwater())
	}
}

func TestEmptyBindStaysEmpty(t *testing.T) {
	c := openTest(t)
	if err := c.ExecBatch(`CREATE TABLE e(t TEXT NOT NULL, b BLOB NOT NULL);`); err != nil {
		t.Fatalf("batch: %v", err)
	}
	// Empty string/blob must bind as empty values, not NULL: SQLite
	// treats a NULL pointer with length 0 as NULL.
	if err := c.Exec(`INSERT INTO e VALUES(?, ?)`, "", []byte{}); err != nil {
		t.Fatalf("exec empty: %v", err)
	}
	row, err := c.QueryRow(`SELECT t, b, typeof(t), typeof(b) FROM e`)
	if err != nil {
		t.Fatalf("queryrow: %v", err)
	}
	if row[0].Type != Text || row[0].Str != "" {
		t.Fatalf("text = %+v, want empty Text", row[0])
	}
	if row[1].Type != Blob || len(row[1].Blob) != 0 {
		t.Fatalf("blob = %+v, want empty Blob", row[1])
	}
	if row[2].Str != "text" || row[3].Str != "blob" {
		t.Fatalf("typeofs = %q %q, want text blob", row[2].Str, row[3].Str)
	}
}
