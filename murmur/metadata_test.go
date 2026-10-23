package murmur

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/marcgauthier/murmur-sql-go/murmur/internal/postcard"
	"github.com/marcgauthier/murmur-sql-go/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql-go/murmur/spool"
)

func TestRowMetaVectors(t *testing.T) {
	var node [16]byte
	for i := range node {
		node[i] = 3
	}
	ver := Version{Millis: 1700000000000, Logical: 7, Node: node}
	meta := RowMeta{
		Key:        []byte{1, 2, 3},
		Generation: ver,
		Cells: []Cell{
			{Column: "c1", Value: IntValue(42)},
			{Column: "c2", Value: TextValue("hi"), Version: ver},
		},
	}
	got := hex.EncodeToString(EncodeRowMeta(meta))
	want := "0301020380d095ffbc3107030303030303030303030303030303030202633101540000000000000000000000000000000000000263320302686980d095ffbc310703030303030303030303030303030303"
	if got != want {
		t.Fatalf("rowmeta = %s", got)
	}
	back, err := DecodeRowMeta(EncodeRowMeta(meta))
	if err != nil || !bytes.Equal(back.Key, meta.Key) || back.Generation != ver || len(back.Cells) != 2 {
		t.Fatalf("roundtrip = %+v, %v", back, err)
	}
	// Tombstone identity tuple: (key bytes, generation).
	buf := encodeOne(func(e *postcard.Encoder) {
		e.ByteString([]byte{4, 5})
		encodeVersion(e, Version{Millis: 9})
	})
	if got := hex.EncodeToString(buf); got != "020405090000000000000000000000000000000000" {
		t.Fatalf("tombkey = %s", got)
	}
	// Blob descriptors.
	ib := encodeOne(func(e *postcard.Encoder) {
		encodeDescriptor(e, blobDescriptor{inline: true, value: []byte{9, 9}})
	})
	if got := hex.EncodeToString(ib); got != "00020909" {
		t.Fatalf("inline = %s", got)
	}
	var digest [32]byte
	for i := range digest {
		digest[i] = 5
	}
	pb := encodeOne(func(e *postcard.Encoder) {
		encodeDescriptor(e, blobDescriptor{bytes: 3, digest: digest, count: 1})
	})
	if got := hex.EncodeToString(pb); got != "0103050505050505050505050505050505050505050505050505050505050505050501" {
		t.Fatalf("pieces = %s", got)
	}
}

func metaFixture(t *testing.T) (*sqlite.Conn, *Schema, *LogicalStore) {
	t.Helper()
	c, err := sqlite.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	if err := c.Exec("CREATE TABLE items(id INTEGER PRIMARY KEY, v TEXT)"); err != nil {
		t.Fatal(err)
	}
	s, err := Inspect(c, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	opts := spool.DefaultOptions(t.TempDir())
	opts.MasterKey = bytesRepeat(9, 32)
	opts.ReclaimInterval = 0
	raw, err := spool.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { raw.Close() })
	return c, s, NewLogicalStore(raw)
}

func capture(t *testing.T, c *sqlite.Conn, table string, writes ...string) []byte {
	t.Helper()
	s, err := c.CreateSession()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Attach(table); err != nil {
		t.Fatal(err)
	}
	for _, w := range writes {
		if err := c.Exec(w); err != nil {
			t.Fatal(err)
		}
	}
	buf, err := s.Changeset()
	if err != nil {
		t.Fatal(err)
	}
	return buf
}

func commitAll(t *testing.T, store *LogicalStore, muts []spool.Mutation) {
	t.Helper()
	if err := store.Commit(muts, spool.DurabilitySync); err != nil {
		t.Fatal(err)
	}
}

func TestDeltaCoalesce(t *testing.T) {
	c, s, store := metaFixture(t)
	v1 := Version{Millis: 10}
	v2 := Version{Millis: 20}
	v3 := Version{Millis: 30}
	// INSERT then UPDATE in one batch: single net INSERT at v1.
	cs := capture(t, c, "items",
		"INSERT INTO items VALUES(1, 'a')",
		"UPDATE items SET v='b' WHERE id=1",
	)
	muts, err := Delta(store, s, cs, v1)
	if err != nil {
		t.Fatal(err)
	}
	if len(muts) != 1 || muts[0].Deleted {
		t.Fatalf("insert batch = %+v", muts)
	}
	commitAll(t, store, muts)
	key := EncodeValues([]Value{IntValue(1)})
	meta, err := ReadRowMeta(store, s.Tables[0].ID, key)
	if err != nil || meta == nil {
		t.Fatal(err)
	}
	if meta.Generation != v1 || meta.Cells[1].Value.S != "b" || meta.Cells[1].Version != v1 {
		t.Fatalf("meta = %+v", meta)
	}
	// Net UPDATE: generation kept, untouched cell version kept.
	cs = capture(t, c, "items", "UPDATE items SET v='c' WHERE id=1")
	muts, err = Delta(store, s, cs, v2)
	if err != nil {
		t.Fatal(err)
	}
	commitAll(t, store, muts)
	meta, _ = ReadRowMeta(store, s.Tables[0].ID, key)
	if meta.Generation != v1 || meta.Cells[0].Version != v1 || meta.Cells[1].Version != v2 {
		t.Fatalf("updated meta = %+v", meta)
	}
	// INSERT then DELETE in one batch: no net effect.
	cs = capture(t, c, "items",
		"INSERT INTO items VALUES(2, 'x')",
		"DELETE FROM items WHERE id=2",
	)
	muts, err = Delta(store, s, cs, v3)
	if err != nil || len(muts) != 0 {
		t.Fatalf("insert+delete = %+v, %v", muts, err)
	}
	// DELETE of a live row: tombstone plus row delete.
	cs = capture(t, c, "items", "DELETE FROM items WHERE id=1")
	muts, err = Delta(store, s, cs, v3)
	if err != nil || len(muts) != 2 {
		t.Fatalf("delete = %+v, %v", muts, err)
	}
	commitAll(t, store, muts)
	if meta, _ := ReadRowMeta(store, s.Tables[0].ID, key); meta != nil {
		t.Fatalf("row survived delete: %+v", meta)
	}
	if n := len(store.KeysWithPrefix([]byte("05/meta/tomb/"))); n != 1 {
		t.Fatalf("tombs = %d", n)
	}
}

func TestDeltaRejects(t *testing.T) {
	c, s, store := metaFixture(t)
	// UPDATE with no stored row: corrupt.
	if err := c.Exec("INSERT INTO items VALUES(1, 'a')"); err != nil {
		t.Fatal(err)
	}
	cs := capture(t, c, "items", "UPDATE items SET v='b' WHERE id=1")
	if _, err := Delta(store, s, cs, Version{Millis: 1}); err == nil {
		t.Fatal("rowless update accepted")
	}
	// Unknown table.
	bad := capture(t, c, "items", "INSERT INTO items VALUES(9, 'z')")
	s2 := *s
	s2.Tables = append([]Table(nil), s.Tables...)
	s2.Tables[0].Name = "other"
	if _, err := Delta(store, &s2, bad, Version{Millis: 1}); err == nil {
		t.Fatal("unknown table accepted")
	}
}

func TestTablesAndOverlap(t *testing.T) {
	c, s, _ := metaFixture(t)
	cs := capture(t, c, "items", "INSERT INTO items VALUES(1, 'a')")
	if got := TablesInChangeset(cs); len(got) != 1 || got[0] != "items" {
		t.Fatalf("tables = %q", got)
	}
	if got := TablesInChangeset(nil); len(got) != 0 {
		t.Fatalf("empty tables = %q", got)
	}
	if got := TablesInChangeset([]byte("junk-bytes!!")); len(got) != 0 {
		t.Fatalf("junk tables = %q", got)
	}
	key := EncodeValues([]Value{IntValue(1)})
	hit, err := ChangesetOverlaps(s, cs, map[RowID]bool{{s.Tables[0].ID, string(key)}: true})
	if err != nil || !hit {
		t.Fatalf("overlap = %v, %v", hit, err)
	}
	miss, err := ChangesetOverlaps(s, cs, map[RowID]bool{{s.Tables[0].ID, "nope"}: true})
	if err != nil || miss {
		t.Fatalf("overlap = %v, %v", miss, err)
	}
}

func TestMigrationExportStates(t *testing.T) {
	c, s, store := metaFixture(t)
	v1 := Version{Millis: 10}
	v2 := Version{Millis: 20}
	if err := c.Exec("INSERT INTO items VALUES(1, 'a'), (2, 'b')"); err != nil {
		t.Fatal(err)
	}
	before, err := ReadTableRows(c, &s.Tables[0])
	if err != nil {
		t.Fatal(err)
	}
	commitAll(t, store, mustMigration(t, store, nil, before, v1))
	if err := c.Exec("UPDATE items SET v='c' WHERE id=1"); err != nil {
		t.Fatal(err)
	}
	if err := c.Exec("DELETE FROM items WHERE id=2"); err != nil {
		t.Fatal(err)
	}
	after, err := ReadTableRows(c, &s.Tables[0])
	if err != nil {
		t.Fatal(err)
	}
	muts, err := MigrationMutations(store, before, after, v2)
	if err != nil {
		t.Fatal(err)
	}
	// One tomb + one row delete for id=2, one row put for id=1.
	if len(muts) != 3 {
		t.Fatalf("migration = %d mutations", len(muts))
	}
	commitAll(t, store, muts)
	rows, err := ReadTableRows(c, &s.Tables[0])
	if err != nil {
		t.Fatal(err)
	}
	var db [16]byte
	st, err := ExportState(store, *s, db, rows, v2)
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Rows) != 1 || len(st.Tombstones) != 1 {
		t.Fatalf("export = %d rows %d tombs", len(st.Rows), len(st.Tombstones))
	}
	if st.Rows[0].Generation != v1 {
		t.Fatalf("generation = %+v", st.Rows[0].Generation)
	}
	full, err := ReadState(store, *s, db, v2)
	if err != nil || len(full.Rows) != 1 || len(full.Tombstones) != 1 {
		t.Fatalf("state = %+v, %v", full, err)
	}
	// ChangedState reads one committed capture back.
	one := capture(t, c, "items", "UPDATE items SET v='d' WHERE id=1")
	dm, err := Delta(store, s, one, Version{Millis: 30})
	if err != nil {
		t.Fatal(err)
	}
	commitAll(t, store, dm)
	ch, err := ChangedState(dm, *s, db, Version{Millis: 30})
	if err != nil || len(ch.Rows) != 1 {
		t.Fatalf("changed = %+v, %v", ch, err)
	}
	if ch.Rows[0].Cells[1].Value.S != "d" {
		t.Fatalf("changed value = %+v", ch.Rows[0])
	}
}

func mustMigration(t *testing.T, store *LogicalStore, before, after []RowState, v Version) []spool.Mutation {
	t.Helper()
	muts, err := MigrationMutations(store, before, after, v)
	if err != nil {
		t.Fatal(err)
	}
	return muts
}

func TestPatchSummarizeAffected(t *testing.T) {
	_, s, store := metaFixture(t)
	v1 := Version{Millis: 10}
	key := EncodeValues([]Value{IntValue(1)})
	row := RowState{
		Table:      s.Tables[0].ID,
		Key:        []Value{IntValue(1)},
		Generation: v1,
		Cells: []Cell{
			{Column: s.Tables[0].Columns[0].ID, Value: IntValue(1), Version: v1},
			{Column: s.Tables[0].Columns[1].ID, Value: TextValue("a"), Version: v1},
		},
	}
	muts, err := PatchMutations(store, []RowState{row}, nil, nil)
	if err != nil || len(muts) != 1 {
		t.Fatal(err)
	}
	commitAll(t, store, muts)
	tomb := Tombstone{Table: s.Tables[0].ID, Key: key, Generation: v1, Version: v1}
	muts, err = PatchMutations(store, nil, []RowState{row}, []Tombstone{tomb})
	if err != nil || len(muts) != 2 {
		t.Fatal(err)
	}
	summed, err := SummarizeTombs(store, muts)
	if err != nil {
		t.Fatal(err)
	}
	if len(summed) != 3 {
		t.Fatalf("summarized = %d", len(summed))
	}
	commitAll(t, store, summed)
	var db [16]byte
	aff, err := AffectedState(store, *s, db, v1, map[RowID]bool{{s.Tables[0].ID, string(key)}: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(aff.Rows) != 0 || len(aff.Tombstones) != 1 {
		t.Fatalf("affected = %+v", aff)
	}
}

func TestTransitionTable(t *testing.T) {
	_, s, store := metaFixture(t)
	v1 := Version{Millis: 10}
	v2 := Version{Millis: 20}
	table := s.Tables[0].ID
	row := RowState{
		Table:      table,
		Key:        []Value{IntValue(1)},
		Generation: v1,
		Cells: []Cell{
			{Column: s.Tables[0].Columns[0].ID, Value: IntValue(1), Version: v1},
			{Column: s.Tables[0].Columns[1].ID, Value: TextValue("a"), Version: v1},
		},
	}
	commitAll(t, store, mustMigration(t, store, nil, []RowState{row}, v1))
	// Disable: rows fenced into the replicated archive.
	muts, err := TransitionTable(store, table, false, []RowState{row}, nil, v2)
	if err != nil {
		t.Fatal(err)
	}
	commitAll(t, store, muts)
	if n := len(store.KeysWithPrefix([]byte("04/meta/row/"))); n != 0 {
		t.Fatalf("rows remain: %d", n)
	}
	if n := len(store.KeysWithPrefix([]byte("12/key-transition/" + table + "/replicated/"))); n == 0 {
		t.Fatal("no archive written")
	}
	// Re-enable: tombstones restored, fresh row incarnation.
	muts, err = TransitionTable(store, table, true, nil, []RowState{row}, v2)
	if err != nil {
		t.Fatal(err)
	}
	commitAll(t, store, muts)
	if n := len(store.KeysWithPrefix([]byte("05/meta/tomb/"))); n != 1 {
		t.Fatalf("tombs = %d", n)
	}
	meta, err := ReadRowMeta(store, table, EncodeValues(row.Key))
	if err != nil || meta == nil || meta.Generation != v2 {
		t.Fatalf("meta = %+v, %v", meta, err)
	}
}
