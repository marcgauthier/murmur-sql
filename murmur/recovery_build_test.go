package murmur

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/recovery"
)

func recoveryTestOptions() Options {
	options := DefaultOptions()
	ro := DefaultRecoveryOptions()
	ro.Enabled = true
	ro.AutoBuild = true
	options.Recovery = &ro
	return options
}

// waitRecoveryBuild waits for the running build to finish and returns
// its status, failing on timeout or build error.
func waitRecoveryBuild(t *testing.T, db *Database, timeout time.Duration) RecoveryBuildStatus {
	t.Helper()
	if db.inner.recovery == nil {
		t.Fatal("recovery scheduler not started")
	}
	deadline := time.Now().Add(timeout)
	for {
		st := db.inner.recovery.Status()
		if st.Err != "" {
			t.Fatalf("build failed: %s", st.Err)
		}
		if !st.Running && !st.Started.IsZero() {
			return st
		}
		if time.Now().After(deadline) {
			t.Fatalf("build timed out: %+v", st)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func snapshotSegments(t *testing.T, dbPath string) map[string][]byte {
	t.Helper()
	out := map[string][]byte{}
	segDir := filepath.Join(dbPath, "segments")
	entries, err := os.ReadDir(segDir)
	if err != nil {
		t.Fatalf("read segments: %v", err)
	}
	for _, e := range entries {
		raw, err := os.ReadFile(filepath.Join(segDir, e.Name()))
		if err != nil {
			t.Fatalf("read segment: %v", err)
		}
		out[e.Name()] = raw
	}
	return out
}

// openTestGeneration discovers the CURRENT generation using the live
// store's retained data keys, exactly as PR4 restore will.
func openTestGeneration(t *testing.T, db *Database, recoveryDir string) *recovery.Generation {
	t.Helper()
	opener := &recovery.Opener{
		// Readers must verify with the builder's comparator.
		Less: recoveryRowLess,
	}
	opener.CipherFor = func(ref string, encrypted bool) (*recovery.Cipher, error) {
		if !encrypted {
			return recovery.NewCipher(nil)
		}
		id, err := parseRecoveryKeyRef(ref)
		if err != nil {
			return nil, err
		}
		key, err := db.inner.store.RecoveryKey(id)
		if err != nil {
			return nil, err
		}
		defer func() {
			for i := range key {
				key[i] = 0
			}
		}()
		return recovery.NewCipher(key[:])
	}
	g, err := recovery.Discover(recoveryDir, db.inner.identity.Database, opener)
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	return g
}

func formatMurmurValue(v Value) string {
	switch v.Tag {
	case ValueNull:
		return "NULL"
	case ValueInteger:
		return fmt.Sprintf("i:%d", v.I)
	case ValueReal:
		return fmt.Sprintf("f:%v", v.F)
	case ValueText:
		return "t:" + v.S
	case ValueBlob:
		return fmt.Sprintf("b:%x", v.B)
	}
	return "?"
}

// dumpCachedTable formats every cached row of one table, in stream
// order, using the same cell projection as RecoverCurrent.
func dumpCachedTable(t *testing.T, schema *Schema, tableID string, rows []*recovery.Row) []string {
	t.Helper()
	var table *Table
	for i := range schema.Tables {
		if schema.Tables[i].ID == tableID {
			table = &schema.Tables[i]
		}
	}
	if table == nil {
		t.Fatalf("unknown table %q", tableID)
	}
	var out []string
	for _, r := range rows {
		if r.Table != tableID {
			continue
		}
		meta, err := DecodeRowMeta(r.Data)
		if err != nil {
			t.Fatalf("decode cached row: %v", err)
		}
		if err := meta.ExtendNullable(table); err != nil {
			t.Fatalf("extend cached row: %v", err)
		}
		values, err := CellValues(table, meta.Cells)
		if err != nil {
			t.Fatalf("cells: %v", err)
		}
		parts := make([]string, len(values))
		for i, v := range values {
			parts[i] = formatMurmurValue(v)
		}
		out = append(out, strings.Join(parts, "\x1f"))
	}
	return out
}

func toSet(lines []string) map[string]int {
	out := map[string]int{}
	for _, l := range lines {
		out[l]++
	}
	return out
}

func TestRecoveryBuildEndToEnd(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")
	db := openRecoveryDB(t, path, OpenCreateNew)
	spec := recoveryFixtureSpec{Docs: 200, Events: 200, Pairs: 50, Mutate: true}
	buildRecoveryFixture(t, db, spec)
	before := snapshotRecoveryState(t, db)
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	segBefore := snapshotSegments(t, path)

	options := recoveryTestOptions()
	// Small run/block/memory budgets exercise splitting and sorter
	// spills at small scale.
	options.Recovery.TargetRunBytes = 32 << 10
	options.Recovery.TargetBlockBytes = 1024
	options.Recovery.MaxBuildMemory = 64 << 10
	db2, err := Open(path, testKey(), options)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()
	st := waitRecoveryBuild(t, db2, 60*time.Second)
	var wantRows uint64
	for _, dump := range before {
		wantRows += uint64(strings.Count(dump, "\n"))
	}
	if st.Rows != wantRows {
		t.Fatalf("built rows = %d, want %d", st.Rows, wantRows)
	}

	// The cache round-trips every row byte-identically.
	recoveryDir := filepath.Join(path, "recovery")
	g := openTestGeneration(t, db2, recoveryDir)
	if g.Manifest.RowCount != wantRows {
		t.Fatalf("manifest rows = %d, want %d", g.Manifest.RowCount, wantRows)
	}
	rs, err := g.OpenRuns()
	if err != nil {
		t.Fatalf("open runs: %v", err)
	}
	var cached []*recovery.Row
	for {
		r, err := rs.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("stream: %v", err)
		}
		cached = append(cached, r)
	}
	if err := rs.Close(); err != nil {
		t.Fatalf("close stream: %v", err)
	}
	if uint64(len(cached)) != wantRows {
		t.Fatalf("streamed = %d, want %d", len(cached), wantRows)
	}
	schema, err := currentTestSchema(db2)
	if err != nil {
		t.Fatalf("schema: %v", err)
	}
	tableIDs := map[string]string{} // table ID -> snapshot name
	for _, tbl := range schema.Tables {
		tableIDs[tbl.ID] = tbl.Name
	}
	for id, name := range tableIDs {
		want := toSet(strings.Split(strings.TrimSuffix(before[name], "\n"), "\n"))
		// Empty tables dump as one empty line; normalize.
		if len(before[name]) == 0 {
			want = map[string]int{}
		}
		got := toSet(dumpCachedTable(t, schema, id, cached))
		if len(got) != len(want) {
			t.Fatalf("table %s: %d cached rows, want %d", name, len(got), len(want))
		}
		for line, n := range want {
			if got[line] != n {
				t.Fatalf("table %s: row mismatch", name)
			}
		}
	}
	// Index covers every row key exactly once.
	is, err := g.OpenIndex()
	if err != nil {
		t.Fatalf("open index: %v", err)
	}
	var entries int
	var prev []byte
	for {
		e, err := is.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("index stream: %v", err)
		}
		if prev != nil && bytes.Compare(e.Key, prev) <= 0 {
			t.Fatal("index not strictly sorted")
		}
		prev = append([]byte(nil), e.Key...)
		entries++
	}
	if err := is.Close(); err != nil {
		t.Fatalf("close index: %v", err)
	}
	if uint64(entries) != wantRows {
		t.Fatalf("index entries = %d, want %d", entries, wantRows)
	}

	// Authoritative segments are append-only across the build: the
	// checkpoint rotation may seal/extend the active segment and add a
	// fresh empty one, but must never rewrite existing bytes.
	for name, before := range segBefore {
		after, err := os.ReadFile(filepath.Join(path, "segments", name))
		if err != nil {
			t.Fatalf("segment %s removed by build", name)
		}
		if !bytes.HasPrefix(after, before) {
			t.Fatalf("segment %s rewritten by build", name)
		}
	}
	entries2, err := os.ReadDir(recoveryDir)
	if err != nil {
		t.Fatalf("read recovery dir: %v", err)
	}
	for _, e := range entries2 {
		name := e.Name()
		if name == "CURRENT" {
			continue
		}
		if _, ok := recovery.ParseGenerationDir(name); !ok {
			t.Fatalf("debris left in recovery/: %s", name)
		}
	}

	// Authoritative state still reopens identically via legacy path.
	if err := db2.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	db3 := reopenTestDB(t, path)
	defer db3.Close()
	after := snapshotRecoveryState(t, db3)
	for table, want := range before {
		if after[table] != want {
			t.Fatalf("table %s changed by build", table)
		}
	}
}

func currentTestSchema(db *Database) (*Schema, error) {
	v, err := db.Call(func(c *Core) (any, error) {
		cp := CloneSchema(c.schema)
		return &cp, nil
	})
	if err != nil {
		return nil, err
	}
	return v.(*Schema), nil
}

func TestRecoveryBuildDisabledByDefault(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")
	db := openRecoveryDB(t, path, OpenCreateNew)
	buildRecoveryFixture(t, db, recoveryFixtureSpec{Docs: 10, Events: 10, Pairs: 5})
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	db2 := reopenTestDB(t, path)
	if db2.inner.recovery != nil {
		t.Fatal("scheduler started while disabled")
	}
	if err := db2.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, err := os.Stat(filepath.Join(path, "recovery")); !os.IsNotExist(err) {
		t.Fatalf("recovery dir status = %v", err)
	}
}

func TestRecoveryBuildEmptyDB(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")
	db := openRecoveryDB(t, path, OpenCreateNew)
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	db2, err := Open(path, testKey(), recoveryTestOptions())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()
	st := waitRecoveryBuild(t, db2, 60*time.Second)
	if st.Rows != 0 {
		t.Fatalf("rows = %d", st.Rows)
	}
	g := openTestGeneration(t, db2, filepath.Join(path, "recovery"))
	rs, err := g.OpenRuns()
	if err != nil {
		t.Fatalf("open runs: %v", err)
	}
	defer rs.Close()
	if _, err := rs.Next(); err != io.EOF {
		t.Fatalf("rows err = %v, want EOF", err)
	}
}

func TestRecoveryTriggerSingleFlight(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")
	db := openRecoveryDB(t, path, OpenCreateNew)
	buildRecoveryFixture(t, db, recoveryFixtureSpec{Docs: 50, Events: 50, Pairs: 10})
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	options := recoveryTestOptions()
	options.Recovery.AutoBuild = false
	db2, err := Open(path, testKey(), options)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()
	sched := db2.inner.recovery
	if sched == nil {
		t.Fatal("scheduler not started")
	}
	sched.Trigger()
	sched.Trigger()
	sched.Trigger()
	waitRecoveryBuild(t, db2, 60*time.Second)
	entries, err := os.ReadDir(filepath.Join(path, "recovery"))
	if err != nil {
		t.Fatalf("read recovery: %v", err)
	}
	generations := 0
	for _, e := range entries {
		if _, ok := recovery.ParseGenerationDir(e.Name()); ok {
			generations++
		}
	}
	if generations != 1 {
		t.Fatalf("generations = %d, want 1", generations)
	}
}

func TestRecoveryBuildCancel(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")
	db := openRecoveryDB(t, path, OpenCreateNew)
	buildRecoveryFixture(t, db, recoveryFixtureSpec{Docs: 2000, Events: 2000, Pairs: 500})
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	db2, err := Open(path, testKey(), recoveryTestOptions())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	// Stop immediately: the build may or may not have published, but
	// the tree must be consistent either way.
	db2.inner.recovery.Stop()
	if st := db2.inner.recovery.Status(); st.Running {
		t.Fatal("still running after Stop")
	}
	// Trigger after stop is a no-op.
	db2.inner.recovery.Trigger()
	if st := db2.inner.recovery.Status(); st.Running {
		t.Fatal("triggered after Stop")
	}
	recoveryDir := filepath.Join(path, "recovery")
	if _, err := os.Stat(filepath.Join(recoveryDir, "CURRENT")); os.IsNotExist(err) {
		if err := db2.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
		return
	}
	// A published generation must stream clean even after cancel.
	g := openTestGeneration(t, db2, recoveryDir)
	rs, err := g.OpenRuns()
	if err != nil {
		t.Fatalf("open runs: %v", err)
	}
	for {
		_, err := rs.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("stream: %v", err)
		}
	}
	if err := rs.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := db2.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestRecoveryRowLess(t *testing.T) {
	row := func(pk []Value, key string) recovery.Row {
		return recovery.Row{Table: "t", Sort: EncodeValues(pk), Key: []byte(key), Data: []byte("d")}
	}
	less := func(a, b recovery.Row) bool { return recoveryRowLess(&a, &b) }
	intRow := func(n int64) recovery.Row { return row([]Value{IntValue(n)}, fmt.Sprintf("k%d", n)) }
	// Numeric order beats byte order: 255 < 256 numerically, but the
	// varint first bytes compare the other way.
	if !less(intRow(255), intRow(256)) || less(intRow(256), intRow(255)) {
		t.Fatal("integer order wrong")
	}
	if !less(row([]Value{RealValue(1.5)}, "a"), row([]Value{IntValue(2)}, "b")) {
		t.Fatal("int/real cross order wrong")
	}
	if !less(row([]Value{TextValue("a")}, "a"), row([]Value{TextValue("b")}, "b")) {
		t.Fatal("text order wrong")
	}
	if !less(row([]Value{TextValue("x")}, "a"), row([]Value{BlobValue([]byte("a"))}, "b")) {
		t.Fatal("text/blob order wrong")
	}
	// Composite keys compare component-wise.
	a := row([]Value{IntValue(1), TextValue("a")}, "a")
	b := row([]Value{IntValue(1), TextValue("x")}, "b")
	c := row([]Value{IntValue(2), TextValue("a")}, "c")
	if !less(a, b) || !less(b, c) || less(c, a) {
		t.Fatal("composite order wrong")
	}
	// Tables sort before keys; spool keys break PK ties.
	t1 := row([]Value{IntValue(9)}, "k")
	t1.Table = "a"
	t2 := row([]Value{IntValue(1)}, "k")
	t2.Table = "b"
	if !less(t1, t2) {
		t.Fatal("table order wrong")
	}
	d1 := row([]Value{IntValue(1)}, "k1")
	d2 := row([]Value{IntValue(1)}, "k2")
	if !less(d1, d2) || less(d2, d1) || less(d1, d1) {
		t.Fatal("tie-break wrong")
	}
	// Total order is deterministic across shuffles.
	rows := []recovery.Row{c, a, b, d2, d1}
	for trial := 0; trial < 10; trial++ {
		cp := append([]recovery.Row(nil), rows...)
		sort.Slice(cp, func(i, j int) bool { return less(cp[i], cp[j]) })
		for i := 1; i < len(cp); i++ {
			if less(cp[i], cp[i-1]) {
				t.Fatalf("trial %d not sorted", trial)
			}
		}
		// Rotate input deterministically.
		rows = append(rows[1:], rows[0])
	}
}

func TestRecoveryKeyRef(t *testing.T) {
	if got := formatRecoveryKeyRef(42); got != "spool-key:42" {
		t.Fatalf("ref = %q", got)
	}
	for _, tc := range []struct {
		ref string
		id  uint32
		ok  bool
	}{
		{"spool-key:0", 0, true},
		{"spool-key:42", 42, true},
		{"spool-key:4294967295", 4294967295, true},
		{"spool-key:", 0, false},
		{"spool-key:x", 0, false},
		{"spool-key:-1", 0, false},
		{"other:1", 0, false},
		{"", 0, false},
	} {
		id, err := parseRecoveryKeyRef(tc.ref)
		if tc.ok && (err != nil || id != tc.id) {
			t.Fatalf("parse %q = (%d, %v)", tc.ref, id, err)
		}
		if !tc.ok && err == nil {
			t.Fatalf("parse %q accepted", tc.ref)
		}
	}
}

func TestRecoveryCipherForPlain(t *testing.T) {
	c, ref, err := recoveryCipherFor(nil, false)
	if err != nil || c.Encrypted() || ref != "" {
		t.Fatalf("plain cipher = %v %q %v", c.Encrypted(), ref, err)
	}
}

func TestRecoveryRowFromRecord(t *testing.T) {
	tables := map[string]*Table{"T1": {ID: "T1", Name: "t1"}}
	pk := EncodeValues([]Value{IntValue(7)})
	goodKey := RowKey("T1", pk)
	goodValue := EncodeRowMeta(RowMeta{Key: pk, Cells: []Cell{{Column: "id", Value: IntValue(7)}}})
	row, err := recoveryRowFromRecord(goodKey, goodValue, 99, tables)
	if err != nil {
		t.Fatalf("valid record: %v", err)
	}
	if row.Table != "T1" || row.Seq != 99 || !bytes.Equal(row.Data, goodValue) {
		t.Fatalf("row = %+v", row)
	}
	// Wrong table hash: same payload under another key fails closed.
	if _, err := recoveryRowFromRecord(RowKey("T1", EncodeValues([]Value{IntValue(8)})), goodValue, 99, tables); err == nil {
		t.Fatal("key collision accepted")
	}
	if _, err := recoveryRowFromRecord([]byte("04/meta/row/"), goodValue, 99, tables); err == nil {
		t.Fatal("short key accepted")
	}
	if _, err := recoveryRowFromRecord([]byte("04/meta/row/NOPE/"+strings.Repeat("a", 64)), goodValue, 99, tables); err == nil {
		t.Fatal("unknown table accepted")
	}
	if _, err := recoveryRowFromRecord(goodKey, []byte("garbage"), 99, tables); err == nil {
		t.Fatal("bad payload accepted")
	}
}
