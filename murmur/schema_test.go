package murmur

import (
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"

	"github.com/marcgauthier/murmur-sql-go/murmur/internal/sqlite"
)

func TestIndexTableVectors(t *testing.T) {
	cases := []struct {
		sql  string
		want string
		ok   bool
	}{
		{"CREATE INDEX items_n ON items(n)", "ITEMS", true},
		{`create index "my idx" on "my table" (a)`, "my table", true},
		{"CREATE INDEX IF NOT EXISTS i ON main.items(a)", "ITEMS", true},
		{"CREATE UNIQUE INDEX [i] ON [items](a)", "items", true},
		{"-- leading comment\nCREATE INDEX i ON items(a) /* tail */", "ITEMS", true},
		{`CREATE INDEX "on" ON "where"(a)`, "where", true},
		{"CREATE TABLE t(a)", "", false},
		{"DROP INDEX i", "", false},
		{"CREATE INDEX", "", false},
		{"CREATE INDEX i", "", false},
		{"not sql at all", "", false},
		{"", "", false},
	}
	for _, tc := range cases {
		got, ok := IndexTable(tc.sql)
		if got != tc.want || ok != tc.ok {
			t.Errorf("IndexTable(%q) = (%q, %v), want (%q, %v)", tc.sql, got, ok, tc.want, tc.ok)
		}
	}
}

func opShapes(sql string) []string {
	var out []string
	for _, op := range DestructiveOps(sql) {
		switch op.Kind {
		case OpDropTable:
			out = append(out, "drop-table:"+op.Table)
		case OpRenameTable:
			out = append(out, "rename-table:"+op.Table)
		case OpDropColumn:
			out = append(out, "drop-column:"+op.Table+"."+op.Column)
		case OpRenameColumn:
			out = append(out, "rename-column:"+op.Table+"."+op.Column)
		}
	}
	return out
}

func TestDestructiveOpsVectors(t *testing.T) {
	cases := []struct {
		sql  string
		want []string
	}{
		{"DROP TABLE items", []string{"drop-table:ITEMS"}},
		{"drop table if exists items", []string{"drop-table:ITEMS"}},
		{"ALTER TABLE items RENAME TO things", []string{"rename-table:ITEMS"}},
		{"ALTER TABLE items RENAME COLUMN a TO b; ALTER TABLE items DROP COLUMN c",
			[]string{"rename-column:ITEMS.A", "drop-column:ITEMS.C"}},
		{"ALTER TABLE items RENAME a TO b", []string{"rename-column:ITEMS.A"}},
		{"ALTER TABLE items DROP a", []string{"drop-column:ITEMS.A"}},
		{"DROP TABLE main.items", []string{"drop-table:ITEMS"}},
		{`DROP TABLE "my table"`, []string{"drop-table:my table"}},
		{"DROP TABLE [items]", []string{"drop-table:items"}},
		{"ALTER TABLE `items` RENAME TO `things`", []string{"rename-table:items"}},
		{"-- DROP TABLE items\nDROP TABLE real", []string{"drop-table:REAL"}},
		{"DROP TABLE temp.cache", nil},
		{"ALTER TABLE TEMP.t RENAME TO u", nil},
		{"CREATE TRIGGER tr AFTER INSERT ON t BEGIN DROP TABLE x; END", nil},
		{"INSERT INTO t VALUES('DROP TABLE x; ALTER TABLE y RENAME TO z')", nil},
		{"CREATE INDEX i ON t(a)", nil},
		{"ALTER TABLE t ADD COLUMN c TEXT", nil},
		{"DROP INDEX i", nil},
	}
	for _, tc := range cases {
		got := opShapes(tc.sql)
		if len(got) != len(tc.want) {
			t.Errorf("DestructiveOps(%q) = %q, want %q", tc.sql, got, tc.want)
			continue
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Errorf("DestructiveOps(%q) = %q, want %q", tc.sql, got, tc.want)
				break
			}
		}
	}
}

func TestKeywordsSkipsQuoted(t *testing.T) {
	got := Keywords(`a TEXT DEFAULT 'virtual check' /* generated */ CHECK (a <> "references")`)
	for _, w := range got {
		if w == "VIRTUAL" || w == "GENERATED" || w == "REFERENCES" {
			t.Fatalf("quoted word leaked into keywords: %q", got)
		}
	}
	found := false
	for _, w := range got {
		if w == "CHECK" {
			found = true
		}
	}
	if !found {
		t.Fatalf("bare CHECK missing from keywords: %q", got)
	}
}

func TestFormatMappings(t *testing.T) {
	if got := FormatMappings(nil); got != "[]" {
		t.Fatalf("empty mappings = %q", got)
	}
	got := FormatMappings([]TableMapping{{
		OldName: "a", NewName: "b",
		Columns: []TableColumnMapping{{Old: "x", New: "y"}, {Old: "q\"q", New: "n"}},
	}})
	want := `[TableMapping { old_name: "a", new_name: "b", columns: [("x", "y"), ("q\"q", "n")] }]`
	if got != want {
		t.Fatalf("mappings = %q, want %q", got, want)
	}
	// Rust-verified escapes (rustc Debug probe): backslash, NUL, C1
	// control, and raw non-ASCII pass through exactly.
	got = FormatMappings([]TableMapping{{
		OldName: "nl\nтъb\there", NewName: "bs\\nul\x00end",
		Columns: []TableColumnMapping{{Old: "c1", New: "uni-héllo‐ﬁ"}},
	}})
	want = `[TableMapping { old_name: "nl\nтъb\there", new_name: "bs\\nul\0end", columns: [("c1\u{85}", "uni-héllo‐ﬁ")] }]`
	if got != want {
		t.Fatalf("escapes = %q, want %q", got, want)
	}
}

func openItems(t *testing.T) *sqlite.Conn {
	t.Helper()
	c, err := sqlite.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	if err := c.Exec("CREATE TABLE items(id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatal(err)
	}
	return c
}

func TestInspectBootstrap(t *testing.T) {
	c := openItems(t)
	s, err := Inspect(c, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(s.Tables) != 1 || s.Tables[0].Name != "items" {
		t.Fatalf("tables = %+v", s.Tables)
	}
	tbl := s.Tables[0]
	sum := sha256.Sum256([]byte("sqlite-table/v4/items"))
	wantID := fmt.Sprintf("%x", sum)
	if tbl.ID != wantID {
		t.Fatalf("table id = %q, want %q", tbl.ID, wantID)
	}
	if len(tbl.Columns) != 2 || tbl.Columns[0].Name != "id" || tbl.Columns[0].PrimaryKey != 1 {
		t.Fatalf("columns = %+v", tbl.Columns)
	}
	var zero [32]byte
	buf := append(append(append([]byte{}, zero[:]...), ""...), "[]"...)
	wantHistory := sha256.Sum256(buf)
	if s.History != wantHistory {
		t.Fatalf("history = %x, want %x", s.History, wantHistory)
	}
	if s.Indexes == nil || len(s.Indexes) != 0 {
		t.Fatalf("indexes = %#v, want empty non-nil", s.Indexes)
	}
}

func TestInspectMigrationKeepsIDs(t *testing.T) {
	c := openItems(t)
	prev, err := Inspect(c, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	mig := NewMigration("ALTER TABLE items ADD COLUMN price REAL")
	if err := c.Exec(mig.SQL); err != nil {
		t.Fatal(err)
	}
	next, err := Inspect(c, prev, &mig)
	if err != nil {
		t.Fatal(err)
	}
	if next.Tables[0].ID != prev.Tables[0].ID {
		t.Fatalf("table id changed: %q -> %q", prev.Tables[0].ID, next.Tables[0].ID)
	}
	if len(next.Tables[0].Columns) != 3 {
		t.Fatalf("columns = %+v", next.Tables[0].Columns)
	}
	for _, col := range next.Tables[0].Columns[:2] {
		old := prev.Tables[0].Columns[0]
		if col.Name == "name" {
			old = prev.Tables[0].Columns[1]
		}
		if col.ID != old.ID {
			t.Fatalf("column %s id changed", col.Name)
		}
	}
	if next.History == prev.History {
		t.Fatal("history did not advance")
	}
}

func TestInspectRejects(t *testing.T) {
	c, err := sqlite.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	if err := c.Exec("CREATE TABLE bad(a TEXT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := Inspect(c, nil, nil); err == nil || !strings.Contains(err.Error(), "non-null primary key") {
		t.Fatalf("missing pk err = %v", err)
	}
}

func TestInspectRejectsDestructiveOnReplicated(t *testing.T) {
	c := openItems(t)
	prev, err := Inspect(c, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	prev.TableReplication = map[string]bool{prev.Tables[0].ID: true}
	mig := NewMigration("DROP TABLE items")
	if err := c.Exec(mig.SQL); err != nil {
		t.Fatal(err)
	}
	_, err = Inspect(c, prev, &mig)
	if err == nil || !strings.Contains(err.Error(), "cannot drop replicated table") {
		t.Fatalf("drop err = %v", err)
	}
}

func TestRowRoundTrip(t *testing.T) {
	c := openItems(t)
	s, err := Inspect(c, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	tbl := &s.Tables[0]
	if err := c.Exec("INSERT INTO items(id, name) VALUES (2, 'b'), (1, 'a')"); err != nil {
		t.Fatal(err)
	}
	rows, err := ReadTableRows(c, tbl)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].Key[0].I != 1 || rows[1].Key[0].I != 2 {
		t.Fatalf("rows not key-ordered: %+v", rows)
	}
	all, err := ReadRows(c, s)
	if err != nil || len(all) != 2 {
		t.Fatalf("read rows = %d, %v", len(all), err)
	}
	// Delete one row by key, re-add via PutUserRows, delete via DeleteUserRows.
	if err := DeleteUserRows(c, s, rows[:1]); err != nil {
		t.Fatal(err)
	}
	rest, err := ReadTableRows(c, tbl)
	if err != nil || len(rest) != 1 || rest[0].Key[0].I != 2 {
		t.Fatalf("after delete = %+v, %v", rest, err)
	}
	if err := PutUserRows(c, s, rows[:1]); err != nil {
		t.Fatal(err)
	}
	if err := PutUserRow(c, tbl, &rows[1]); err != nil {
		t.Fatal(err)
	}
	back, err := ReadTableRows(c, tbl)
	if err != nil || len(back) != 2 {
		t.Fatalf("after put = %+v, %v", back, err)
	}
	if err := DeleteUserRows(c, s, back); err != nil {
		t.Fatal(err)
	}
	empty, err := ReadTableRows(c, tbl)
	if err != nil || len(empty) != 0 {
		t.Fatalf("after full delete = %+v, %v", empty, err)
	}
}

func TestChunkMigrationRows(t *testing.T) {
	mk := func(table string, key int64) RowState {
		return RowState{Table: table, Key: []Value{{Tag: ValueInteger, I: key}}}
	}
	before := []RowState{mk("t", 1), mk("t", 3)}
	after := []RowState{mk("t", 2), mk("t", 3)}
	chunks := ChunkMigrationRows(before, after, 100, 1<<20)
	if len(chunks) != 1 {
		t.Fatalf("chunks = %d", len(chunks))
	}
	if len(chunks[0].Before) != 2 || len(chunks[0].After) != 2 {
		t.Fatalf("chunk = %+v", chunks[0])
	}
	if len(ChunkMigrationRows(nil, nil, 10, 4096)) != 0 {
		t.Fatal("empty input must yield no chunks")
	}
	split := ChunkMigrationRows(before, after, 1, 1<<20)
	if len(split) != 3 {
		t.Fatalf("row-bounded chunks = %d", len(split))
	}
}
