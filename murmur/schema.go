package murmur

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/marcgauthier/murmur-sql/murmur/internal/sqlite"
)

// Schema management: inspect SQLite into Schema, detect destructive
// migrations, read rows for merge and checkpoint chunking.

// CaptureSchema attaches change capture to every managed table.
func CaptureSchema(c *sqlite.Conn, s *Schema) (*Capture, error) {
	tables := make([]string, 0, len(s.Tables))
	for i := range s.Tables {
		tables = append(tables, s.Tables[i].Name)
	}
	return NewCapture(c, tables)
}

// Inspect reads the live database into a new schema, carrying stable
// identities from previous across migration m.
func Inspect(c *sqlite.Conn, previous *Schema, m *Migration) (*Schema, error) {
	var prev Schema
	if previous != nil {
		prev = *previous
	}
	var mig Migration
	if m != nil {
		mig = *m
	}
	entries, err := c.Query("SELECT name,sql FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' AND lower(name) NOT GLOB '_crdt_*' ORDER BY name")
	if err != nil {
		return nil, sqlErr(err)
	}
	tables := make([]Table, 0, len(entries))
	for _, e := range entries {
		if e[0].Type == sqlite.Null || e[1].Type == sqlite.Null {
			return nil, sqlErr(errors.New("sqlite_schema entry with null name or sql"))
		}
		name, sqlText := e[0].Str, e[1].Str
		upper := Keywords(sqlText)
		for _, w := range []string{"VIRTUAL", "CHECK", "REFERENCES", "GENERATED", "AUTOINCREMENT"} {
			for _, part := range upper {
				if part == w {
					return nil, invalidErr("unsupported schema for %s", name)
				}
			}
		}
		var mapping *TableMapping
		for i := range mig.Mappings {
			if mig.Mappings[i].NewName == name {
				mapping = &mig.Mappings[i]
				break
			}
		}
		wantOld := name
		if mapping != nil {
			wantOld = mapping.OldName
		}
		var old *Table
		for i := range prev.Tables {
			if prev.Tables[i].Name == wantOld {
				old = &prev.Tables[i]
				break
			}
		}
		tableID := ""
		if old != nil {
			tableID = old.ID
		} else {
			tableID = id(append([]byte("sqlite-table/v4/"), name...))
		}
		prows, err := c.Query(fmt.Sprintf("PRAGMA table_xinfo(%s)", quote(name)))
		if err != nil {
			return nil, sqlErr(err)
		}
		columns := make([]Column, 0, len(prows))
		for _, r := range prows {
			col, declared := r[1].Str, r[2].Str
			notNull := r[3].Int != 0
			pkOrd := int(r[5].Int)
			hidden := r[6].Int
			if hidden != 0 {
				return nil, invalidErr("generated/hidden columns unsupported")
			}
			source := col
			if mapping != nil {
				for _, mc := range mapping.Columns {
					if mc.New == col {
						source = mc.Old
						break
					}
				}
			}
			var oldcol *Column
			if old != nil {
				for i := range old.Columns {
					if old.Columns[i].Name == source {
						oldcol = &old.Columns[i]
						break
					}
				}
			}
			var vc *VectorColumnConfig
			var fts bool
			policy := PolicyLww
			columnID := ""
			if oldcol != nil {
				columnID = oldcol.ID
				vc = oldcol.Vector
				fts = oldcol.Fts
				policy = oldcol.Policy
			} else {
				columnID = id(append([]byte(tableID), col...))
			}
			columns = append(columns, Column{
				Vector: vc, Fts: fts, Policy: policy, ID: columnID,
				Name: col, PrimaryKey: pkOrd, DeclaredType: declared, NotNull: notNull,
			})
		}
		var appKey *ApplicationKey
		if old != nil && old.ApplicationKey != nil {
			cp := *old.ApplicationKey
			appKey = &cp
		}
		if appKey == nil {
			for _, col := range columns {
				if strings.EqualFold(col.Name, ManagedKeyColumn) {
					return nil, invalidErr("%s is reserved for replication", ManagedKeyColumn)
				}
			}
		}
		pkCount := 0
		for _, col := range columns {
			if col.PrimaryKey > 0 {
				pkCount++
			}
		}
		badPK := pkCount == 0
		for _, col := range columns {
			if col.PrimaryKey > 0 && !col.NotNull &&
				!(pkCount == 1 && strings.EqualFold(col.DeclaredType, "INTEGER")) {
				badPK = true
				break
			}
		}
		if badPK {
			return nil, invalidErr("%s requires a non-null primary key", name)
		}
		if old != nil {
			type pkID struct {
				ord int
				id  string
				typ string
			}
			var a, b []pkID
			for _, col := range old.Columns {
				if col.PrimaryKey > 0 {
					a = append(a, pkID{col.PrimaryKey, col.ID, strings.ToUpper(col.DeclaredType)})
				}
			}
			for _, col := range columns {
				if col.PrimaryKey > 0 {
					b = append(b, pkID{col.PrimaryKey, col.ID, strings.ToUpper(col.DeclaredType)})
				}
			}
			less := func(x []pkID) func(i, j int) bool {
				return func(i, j int) bool {
					if x[i].ord != x[j].ord {
						return x[i].ord < x[j].ord
					}
					if x[i].id != x[j].id {
						return x[i].id < x[j].id
					}
					return x[i].typ < x[j].typ
				}
			}
			sort.Slice(a, less(a))
			sort.Slice(b, less(b))
			same := len(a) == len(b)
			for i := range a {
				if same && a[i] != b[i] {
					same = false
				}
			}
			if !same {
				return nil, invalidErr("migration changes primary-key identity")
			}
		}
		applicationIndexes := 0
		ilrows, err := c.Query(fmt.Sprintf("PRAGMA index_list(%s)", quote(name)))
		if err != nil {
			return nil, sqlErr(err)
		}
		for _, ir := range ilrows {
			indexName := ir[1].Str
			unique := ir[2].Int != 0
			origin := ir[3].Str
			if !unique || origin == "pk" {
				continue
			}
			matched := false
			if appKey != nil {
				xirows, err := c.Query(fmt.Sprintf("PRAGMA index_xinfo(%s)", quote(indexName)))
				if err != nil {
					return nil, sqlErr(err)
				}
				type part struct {
					name *string
					desc bool
					coll string
				}
				var parts []part
				for _, xr := range xirows {
					if xr[5].Int == 0 {
						continue
					}
					var nm *string
					if xr[2].Type != sqlite.Null {
						s := xr[2].Str
						nm = &s
					}
					parts = append(parts, part{nm, xr[3].Int != 0, xr[4].Str})
				}
				if origin == "u" && len(parts) == len(appKey.Columns) {
					ok := true
					for i, p := range parts {
						if p.name == nil || *p.name != appKey.Columns[i] ||
							p.desc != appKey.Descending[i] ||
							!strings.EqualFold(p.coll, appKey.Collations[i]) {
							ok = false
							break
						}
					}
					if ok {
						applicationIndexes++
						matched = true
					}
				}
			}
			if !matched {
				return nil, invalidErr("non-primary-key unique constraints unsupported")
			}
		}
		if appKey != nil && applicationIndexes != 1 {
			return nil, invalidErr("replicated table requires its application-key unique index")
		}
		tables = append(tables, Table{
			ApplicationKey: appKey, ID: tableID, Name: name, SQL: sqlText, Columns: columns,
		})
	}
	for i := range tables {
		if err := ValidateTable(&tables[i]); err != nil {
			return nil, err
		}
	}
	forbidden, err := c.Query("SELECT count(*) FROM sqlite_schema WHERE type IN ('trigger','view') AND lower(name) NOT GLOB '_crdt_*'")
	if err != nil {
		return nil, sqlErr(err)
	}
	if len(forbidden) == 0 || forbidden[0][0].Int > 0 {
		if len(forbidden) == 0 {
			return nil, sqlErr(errors.New("count query returned no rows"))
		}
		return nil, invalidErr("application triggers and views unsupported")
	}
	// Destructive statements against replicated tables are rejected even
	// when the after-schema hides them: a rebuild that drops a table and
	// reuses its name keeps the stable id, so the id diff below cannot see
	// it. Bare names arrived uppercased from the scanner; SQLite identifiers
	// are case-insensitive, so resolution ignores case.
	for _, op := range DestructiveOps(mig.SQL) {
		var oldT *Table
		for i := range prev.Tables {
			if strings.EqualFold(prev.Tables[i].Name, op.Table) {
				oldT = &prev.Tables[i]
				break
			}
		}
		if oldT == nil || !ReplicationEnabled(&prev, oldT.ID) {
			continue
		}
		switch op.Kind {
		case OpDropColumn:
			return nil, invalidErr("cannot drop column %s from replicated table %s; disable replication first", op.Column, oldT.Name)
		case OpRenameColumn:
			return nil, invalidErr("cannot rename column %s in replicated table %s; disable replication first", op.Column, oldT.Name)
		case OpDropTable:
			return nil, invalidErr("cannot drop replicated table %s; disable replication first", oldT.Name)
		case OpRenameTable:
			return nil, invalidErr("cannot rename replicated table %s; disable replication first", oldT.Name)
		}
	}
	// Replicated tables keep stable identities: drops and renames would
	// diverge peers that only adopt additive steps. Local-only tables may
	// drop and rename freely. Unmapped renames surface as drop+create and
	// are rejected the same way.
	for i := range prev.Tables {
		oldT := &prev.Tables[i]
		var newT *Table
		for j := range tables {
			if tables[j].ID == oldT.ID {
				newT = &tables[j]
				break
			}
		}
		if newT == nil {
			if ReplicationEnabled(&prev, oldT.ID) {
				return nil, invalidErr("cannot drop replicated table %s; disable replication first", oldT.Name)
			}
			continue
		}
		if newT.Name != oldT.Name && ReplicationEnabled(&prev, oldT.ID) {
			return nil, invalidErr("cannot rename replicated table %s; disable replication first", oldT.Name)
		}
		for _, oc := range oldT.Columns {
			var nc *Column
			for j := range newT.Columns {
				if newT.Columns[j].ID == oc.ID {
					nc = &newT.Columns[j]
					break
				}
			}
			if nc == nil {
				if ReplicationEnabled(&prev, oldT.ID) {
					return nil, invalidErr("cannot drop column %s.%s from replicated table %s; disable replication first", oldT.Name, oc.Name, oldT.Name)
				}
			} else if nc.Name != oc.Name && ReplicationEnabled(&prev, oldT.ID) {
				return nil, invalidErr("cannot rename column %s.%s in replicated table %s; disable replication first", oldT.Name, oc.Name, oldT.Name)
			}
		}
	}
	ixrows, err := c.Query("SELECT sql FROM sqlite_schema WHERE type='index' AND sql IS NOT NULL ORDER BY name")
	if err != nil {
		return nil, sqlErr(err)
	}
	indexes := make([]string, 0, len(ixrows))
	for _, r := range ixrows {
		indexes = append(indexes, r[0].Str)
	}
	seen := make(map[string]bool, len(tables))
	dup := false
	for _, t := range tables {
		if seen[t.ID] {
			dup = true
			break
		}
		seen[t.ID] = true
	}
	if !dup {
		for _, t := range tables {
			cseen := make(map[string]bool, len(t.Columns))
			for _, col := range t.Columns {
				if cseen[col.ID] {
					dup = true
					break
				}
				cseen[col.ID] = true
			}
			if dup {
				break
			}
		}
	}
	if dup {
		return nil, invalidErr("ambiguous migration identity mappings")
	}
	for _, mp := range mig.Mappings {
		var oldT, newT *Table
		for i := range prev.Tables {
			if prev.Tables[i].Name == mp.OldName {
				oldT = &prev.Tables[i]
				break
			}
		}
		for i := range tables {
			if tables[i].Name == mp.NewName {
				newT = &tables[i]
				break
			}
		}
		if oldT == nil || newT == nil {
			return nil, invalidErr("migration mapping references a missing table")
		}
		for _, mc := range mp.Columns {
			foundOld, foundNew := false, false
			for _, col := range oldT.Columns {
				if col.Name == mc.Old {
					foundOld = true
					break
				}
			}
			for _, col := range newT.Columns {
				if col.Name == mc.New {
					foundNew = true
					break
				}
			}
			if !foundOld || !foundNew {
				return nil, invalidErr("migration mapping references a missing column")
			}
		}
	}
	// SQL and explicit mappings are part of schema history; all replicas must
	// deploy the same migration, rather than infer identity from local clocks.
	buf := make([]byte, 0, 32+len(mig.SQL)+64)
	buf = append(buf, prev.History[:]...)
	buf = append(buf, mig.SQL...)
	buf = append(buf, FormatMappings(mig.Mappings)...)
	history := hash(buf)
	repl := make(map[string]bool)
	for id, enabled := range prev.TableReplication {
		for _, t := range tables {
			if t.ID == id {
				repl[id] = enabled
				break
			}
		}
	}
	return &Schema{TableReplication: repl, Tables: tables, Indexes: indexes, History: history}, nil
}

// PkColumns returns the table's primary-key columns in ordinal order.
func PkColumns(t *Table) []*Column {
	var out []*Column
	for i := range t.Columns {
		if t.Columns[i].PrimaryKey > 0 {
			out = append(out, &t.Columns[i])
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PrimaryKey < out[j].PrimaryKey })
	return out
}

// ReadTableRows reads every row of table as row states, ordered by key.
func ReadTableRows(c *sqlite.Conn, t *Table) ([]RowState, error) {
	cols := make([]string, len(t.Columns))
	for i := range t.Columns {
		cols[i] = quote(t.Columns[i].Name)
	}
	pk := PkColumns(t)
	ords := make([]string, len(pk))
	for i, col := range pk {
		ords[i] = quote(col.Name)
	}
	rows, err := c.Query(fmt.Sprintf("SELECT %s FROM %s ORDER BY %s",
		strings.Join(cols, ","), quote(t.Name), strings.Join(ords, ",")))
	if err != nil {
		return nil, sqlErr(err)
	}
	out := make([]RowState, 0, len(rows))
	for _, r := range rows {
		values := make([]Value, 0, len(r))
		for _, v := range r {
			mv, err := valueFromSQLite(v)
			if err != nil {
				return nil, err
			}
			values = append(values, mv)
		}
		key := make([]Value, 0, len(pk))
		for _, col := range pk {
			for i := range t.Columns {
				if t.Columns[i].ID == col.ID {
					key = append(key, values[i])
					break
				}
			}
		}
		for _, v := range key {
			if v.Tag == ValueNull {
				return nil, invalidErr("null primary key")
			}
		}
		cells := make([]Cell, 0, len(values))
		for i, v := range values {
			cells = append(cells, Cell{Column: t.Columns[i].ID, Value: v})
		}
		out = append(out, RowState{Table: t.ID, Key: key, Cells: cells})
	}
	return out, nil
}

// ReadRows reads every row of every table. Used only for explicit
// export and schema migration, never normal commits.
func ReadRows(c *sqlite.Conn, s *Schema) ([]RowState, error) {
	var rows []RowState
	for i := range s.Tables {
		more, err := ReadTableRows(c, &s.Tables[i])
		if err != nil {
			return nil, err
		}
		rows = append(rows, more...)
	}
	return rows, nil
}

// RowApproxSize estimates a row state in bytes.
func RowApproxSize(r *RowState) int {
	size := 64 + len(r.Key)*16
	for _, c := range r.Cells {
		size += len(c.Column)
		switch c.Value.Tag {
		case ValueText:
			size += len(c.Value.S)
		case ValueBlob:
			size += len(c.Value.B)
		default:
			size += 16
		}
	}
	return size
}

// MigrationChunk is one payload-bounded before/after row batch.
type MigrationChunk struct {
	Before []RowState
	After  []RowState
}

// ChunkMigrationRows splits before/after rows into payload-bounded
// batches, merge-joining on keys within each table.
func ChunkMigrationRows(before, after []RowState, chunkRows, maxChunkBytes int) []MigrationChunk {
	if len(before) == 0 && len(after) == 0 {
		return nil
	}
	if chunkRows < 1 {
		chunkRows = 1
	}
	if maxChunkBytes < 1024 {
		maxChunkBytes = 1024
	}
	var tableOrder []string
	seen := make(map[string]bool)
	for _, r := range before {
		if !seen[r.Table] {
			seen[r.Table] = true
			tableOrder = append(tableOrder, r.Table)
		}
	}
	for _, r := range after {
		if !seen[r.Table] {
			seen[r.Table] = true
			tableOrder = append(tableOrder, r.Table)
		}
	}
	beforeByTable := make(map[string][]RowState)
	for _, r := range before {
		beforeByTable[r.Table] = append(beforeByTable[r.Table], r)
	}
	afterByTable := make(map[string][]RowState)
	for _, r := range after {
		afterByTable[r.Table] = append(afterByTable[r.Table], r)
	}
	var chunks []MigrationChunk
	for _, tableID := range tableOrder {
		bRows := beforeByTable[tableID]
		aRows := afterByTable[tableID]
		if len(bRows) == 0 && len(aRows) == 0 {
			continue
		}
		bi, ai := 0, 0
		for bi < len(bRows) || ai < len(aRows) {
			var chunkB, chunkA []RowState
			count, chunkBytes := 0, 0
			for (count < chunkRows && chunkBytes < maxChunkBytes) &&
				(bi < len(bRows) || ai < len(aRows)) {
				switch {
				case bi < len(bRows) && ai < len(aRows):
					switch CmpValues(bRows[bi].Key, aRows[ai].Key) {
					case -1:
						chunkBytes += RowApproxSize(&bRows[bi])
						chunkB = append(chunkB, bRows[bi])
						bi++
					case 1:
						chunkBytes += RowApproxSize(&aRows[ai])
						chunkA = append(chunkA, aRows[ai])
						ai++
					default:
						chunkBytes += RowApproxSize(&bRows[bi]) + RowApproxSize(&aRows[ai])
						chunkB = append(chunkB, bRows[bi])
						chunkA = append(chunkA, aRows[ai])
						bi++
						ai++
					}
				case bi < len(bRows):
					chunkBytes += RowApproxSize(&bRows[bi])
					chunkB = append(chunkB, bRows[bi])
					bi++
				case ai < len(aRows):
					chunkBytes += RowApproxSize(&aRows[ai])
					chunkA = append(chunkA, aRows[ai])
					ai++
				}
				count++
			}
			if len(chunkB) > 0 || len(chunkA) > 0 {
				chunks = append(chunks, MigrationChunk{Before: chunkB, After: chunkA})
			}
		}
	}
	return chunks
}

// CmpValues compares key component lists.
func CmpValues(a, b []Value) int {
	for i := 0; i < len(a) && i < len(b); i++ {
		if c := CmpValue(a[i], b[i]); c != 0 {
			return c
		}
	}
	switch {
	case len(a) < len(b):
		return -1
	case len(a) > len(b):
		return 1
	}
	return 0
}

// CmpValue compares values: NULL < number < text < blob; numbers
// compare in float64 total order across the int/real boundary.
func CmpValue(a, b Value) int {
	isNumber := func(v Value) bool { return v.Tag == ValueInteger || v.Tag == ValueReal }
	switch {
	case a.Tag == ValueNull && b.Tag == ValueNull:
		return 0
	case a.Tag == ValueNull:
		return -1
	case b.Tag == ValueNull:
		return 1
	case a.Tag == ValueInteger && b.Tag == ValueInteger:
		switch {
		case a.I < b.I:
			return -1
		case a.I > b.I:
			return 1
		}
		return 0
	case a.Tag == ValueReal && b.Tag == ValueReal:
		return cmpFloat(a.F, b.F)
	case a.Tag == ValueInteger && b.Tag == ValueReal:
		return cmpFloat(float64(a.I), b.F)
	case a.Tag == ValueReal && b.Tag == ValueInteger:
		return cmpFloat(a.F, float64(b.I))
	case isNumber(a):
		return -1
	case isNumber(b):
		return 1
	case a.Tag == ValueText && b.Tag == ValueText:
		return strings.Compare(a.S, b.S)
	case a.Tag == ValueText:
		return -1
	case b.Tag == ValueText:
		return 1
	default:
		return bytes.Compare(a.B, b.B)
	}
}

// floatOrd maps a float to total order.
func floatOrd(f float64) uint64 {
	u := math.Float64bits(f)
	if u>>63 == 1 {
		return ^u
	}
	return u ^ (1 << 63)
}

func cmpFloat(a, b float64) int {
	oa, ob := floatOrd(a), floatOrd(b)
	switch {
	case oa < ob:
		return -1
	case oa > ob:
		return 1
	}
	return 0
}

// rowValues returns the row's cell values in table column order.
func rowValues(table *Table, row *RowState) ([]Value, error) {
	return CellValues(table, row.Cells)
}

// CellValues returns cell values in table column order.
func CellValues(table *Table, cells []Cell) ([]Value, error) {
	out := make([]Value, 0, len(table.Columns))
	for i := range table.Columns {
		found := false
		for ci := range cells {
			if cells[ci].Column == table.Columns[i].ID {
				out = append(out, cells[ci].Value)
				found = true
				break
			}
		}
		if !found {
			return nil, corruptErr("missing cell")
		}
	}
	return out, nil
}

// RowInsertSQL renders the row upsert statement.
func RowInsertSQL(table *Table) string {
	names := make([]string, len(table.Columns))
	place := make([]string, len(table.Columns))
	for i := range table.Columns {
		names[i] = quote(table.Columns[i].Name)
		place[i] = "?"
	}
	return fmt.Sprintf("INSERT OR REPLACE INTO %s(%s) VALUES(%s)",
		quote(table.Name), strings.Join(names, ","), strings.Join(place, ","))
}

// PutUserRow validates and upserts one row.
func PutUserRow(c *sqlite.Conn, table *Table, row *RowState) error {
	if err := ValidateRow(table, row); err != nil {
		return err
	}
	values, err := rowValues(table, row)
	if err != nil {
		return err
	}
	bind := make([]any, len(values))
	for i, v := range values {
		bind[i] = sqliteBind(v)
	}
	if err := c.Exec(RowInsertSQL(table), bind...); err != nil {
		return sqlErr(err)
	}
	return nil
}

// PutUserRows validates then upserts rows, preserving row order while
// reusing a statement for adjacent rows in a table.
func PutUserRows(c *sqlite.Conn, schema *Schema, rows []RowState) error {
	byID := make(map[string]*Table, len(schema.Tables))
	for i := range schema.Tables {
		byID[schema.Tables[i].ID] = &schema.Tables[i]
	}
	for i := range rows {
		t, ok := byID[rows[i].Table]
		if !ok {
			return ErrSchemaMismatch
		}
		if err := ValidateRow(t, &rows[i]); err != nil {
			return err
		}
	}
	for len(rows) > 0 {
		n := 1
		for n < len(rows) && rows[n].Table == rows[0].Table {
			n++
		}
		group := rows[:n]
		rows = rows[n:]
		t := byID[group[0].Table]
		stmt, err := c.Prepare(RowInsertSQL(t))
		if err != nil {
			return sqlErr(err)
		}
		var runErr error
		for i := range group {
			values, err := rowValues(t, &group[i])
			if err != nil {
				runErr = err
				break
			}
			bind := make([]any, len(values))
			for j, v := range values {
				bind[j] = sqliteBind(v)
			}
			if err := stmt.Bind(bind...); err != nil {
				runErr = sqlErr(err)
				break
			}
			if _, err := stmt.Step(); err != nil {
				runErr = sqlErr(err)
				break
			}
			if err := stmt.Reset(); err != nil {
				runErr = sqlErr(err)
				break
			}
		}
		if err := stmt.Close(); err != nil && runErr == nil {
			runErr = sqlErr(err)
		}
		if runErr != nil {
			return runErr
		}
	}
	return nil
}

// DeleteUserRows validates then deletes rows by key, preserving row
// order while reusing a statement for adjacent rows in a table.
func DeleteUserRows(c *sqlite.Conn, schema *Schema, rows []RowState) error {
	byID := make(map[string]*Table, len(schema.Tables))
	for i := range schema.Tables {
		byID[schema.Tables[i].ID] = &schema.Tables[i]
	}
	for i := range rows {
		t, ok := byID[rows[i].Table]
		if !ok {
			return ErrSchemaMismatch
		}
		if err := ValidateRow(t, &rows[i]); err != nil {
			return err
		}
	}
	for len(rows) > 0 {
		n := 1
		for n < len(rows) && rows[n].Table == rows[0].Table {
			n++
		}
		group := rows[:n]
		rows = rows[n:]
		t := byID[group[0].Table]
		pk := PkColumns(t)
		preds := make([]string, len(pk))
		for i, col := range pk {
			preds[i] = fmt.Sprintf("%s IS ?", quote(col.Name))
		}
		stmt, err := c.Prepare(fmt.Sprintf("DELETE FROM %s WHERE %s", quote(t.Name), strings.Join(preds, " AND ")))
		if err != nil {
			return sqlErr(err)
		}
		var runErr error
		for i := range group {
			bind := make([]any, len(group[i].Key))
			for j, v := range group[i].Key {
				bind[j] = sqliteBind(v)
			}
			if err := stmt.Bind(bind...); err != nil {
				runErr = sqlErr(err)
				break
			}
			if _, err := stmt.Step(); err != nil {
				runErr = sqlErr(err)
				break
			}
			if err := stmt.Reset(); err != nil {
				runErr = sqlErr(err)
				break
			}
		}
		if err := stmt.Close(); err != nil && runErr == nil {
			runErr = sqlErr(err)
		}
		if runErr != nil {
			return runErr
		}
	}
	return nil
}

type scanTokenKind int

const (
	scanBare scanTokenKind = iota
	scanName
	scanDot
	scanSemi
)

type scanToken struct {
	kind scanTokenKind
	text string
}

// IndexTable names the table of a plain `CREATE [UNIQUE] INDEX
// [IF NOT EXISTS] <name> ON [<schema>.]<table> (...)` statement.
// Quoted identifiers and comments are honored. It reports false when
// the shape is unrecognized; callers keep such indexes and let SQLite
// validate them on adopt.
func IndexTable(sql string) (string, bool) {
	b := []byte(sql)
	var toks []scanToken
	i := 0
	for i < len(b) {
		c := b[i]
		switch {
		case c == '\'':
			i++
			for i < len(b) {
				if b[i] == '\'' {
					i++
					if i < len(b) && b[i] == '\'' {
						i++
					} else {
						break
					}
				} else {
					i++
				}
			}
		case c == '"' || c == '`':
			i++
			var raw []byte
			for i < len(b) {
				if b[i] == c {
					if i+1 < len(b) && b[i+1] == c {
						raw = append(raw, c)
						i += 2
					} else {
						i++
						break
					}
				} else {
					raw = append(raw, b[i])
					i++
				}
			}
			toks = append(toks, scanToken{scanName, string(raw)})
		case c == '[':
			i++
			start := i
			for i < len(b) && b[i] != ']' {
				i++
			}
			toks = append(toks, scanToken{scanName, string(b[start:i])})
			i++
			if i > len(b) {
				i = len(b)
			}
		case c == '-' && strings.HasPrefix(sql[i:], "--"):
			for i < len(b) && b[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < len(b) && b[i+1] == '*':
			i += 2
			for i+1 < len(b) && !(b[i] == '*' && b[i+1] == '/') {
				i++
			}
			i += 2
			if i > len(b) {
				i = len(b)
			}
		case c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c == '_':
			start := i
			for i < len(b) && (b[i] >= 'a' && b[i] <= 'z' || b[i] >= 'A' && b[i] <= 'Z' || b[i] >= '0' && b[i] <= '9' || b[i] == '_' || b[i] == '$') {
				i++
			}
			toks = append(toks, scanToken{scanBare, strings.ToUpper(sql[start:i])})
		case c == '.':
			toks = append(toks, scanToken{scanDot, ""})
			i++
		default:
			i++
		}
	}
	k := 0
	eat := func(word string) bool {
		if k < len(toks) && toks[k].kind == scanBare && toks[k].text == word {
			k++
			return true
		}
		return false
	}
	if !eat("CREATE") {
		return "", false
	}
	eat("UNIQUE")
	if !eat("INDEX") {
		return "", false
	}
	if eat("IF") {
		if !eat("NOT") || !eat("EXISTS") {
			return "", false
		}
	}
	if k >= len(toks) || toks[k].kind != scanBare && toks[k].kind != scanName {
		return "", false
	}
	k++
	if !eat("ON") {
		return "", false
	}
	if k >= len(toks) || toks[k].kind != scanBare && toks[k].kind != scanName {
		return "", false
	}
	first := toks[k].text
	k++
	if k < len(toks) && toks[k].kind == scanDot {
		k++
		if k >= len(toks) || toks[k].kind != scanBare && toks[k].kind != scanName {
			return "", false
		}
		return toks[k].text, true
	}
	// Bare tokens were uppercased for keyword matching; table names compare
	// case-insensitively at use sites.
	return first, true
}

// DestructiveOpKind names the operation.
type DestructiveOpKind int

const (
	// OpDropTable drops a table.
	OpDropTable DestructiveOpKind = iota
	// OpRenameTable renames a table.
	OpRenameTable
	// OpDropColumn drops a column.
	OpDropColumn
	// OpRenameColumn renames a column.
	OpRenameColumn
)

// DestructiveOp is destructive DDL targeting a main-schema table.
// Callers resolve names against the previous schema and reject
// operations on replicated tables.
type DestructiveOp struct {
	Table  string
	Column string // empty unless DropColumn/RenameColumn
	Kind   DestructiveOpKind
}

// DestructiveOps lists statements that drop or rename main-schema
// tables or columns. Only statements starting with DROP or ALTER are
// examined, so trigger bodies and string literals elsewhere never
// match. `temp.` targets are skipped: managed TEMP objects are
// invisible to schema inspection.
func DestructiveOps(sql string) []DestructiveOp {
	tokenize := func(stmt string) []scanToken {
		b := []byte(stmt)
		var toks []scanToken
		i := 0
		for i < len(b) {
			c := b[i]
			switch {
			case c == '\'' || c == '"' || c == '`':
				i++
				var raw []byte
				for i < len(b) {
					if b[i] == c {
						if i+1 < len(b) && b[i+1] == c {
							raw = append(raw, c)
							i += 2
						} else {
							i++
							break
						}
					} else {
						raw = append(raw, b[i])
						i++
					}
				}
				toks = append(toks, scanToken{scanName, string(raw)})
			case c == '[':
				i++
				var raw []byte
				for i < len(b) {
					if b[i] == ']' {
						if i+1 < len(b) && b[i+1] == ']' {
							raw = append(raw, ']')
							i += 2
						} else {
							i++
							break
						}
					} else {
						raw = append(raw, b[i])
						i++
					}
				}
				toks = append(toks, scanToken{scanName, string(raw)})
			case c == '-' && strings.HasPrefix(stmt[i:], "--"):
				for i < len(b) && b[i] != '\n' {
					i++
				}
			case c == '/' && i+1 < len(b) && b[i+1] == '*':
				i += 2
				for i+1 < len(b) && !(b[i] == '*' && b[i+1] == '/') {
					i++
				}
				i += 2
				if i > len(b) {
					i = len(b)
				}
			case c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c == '_':
				start := i
				for i < len(b) && (b[i] >= 'a' && b[i] <= 'z' || b[i] >= 'A' && b[i] <= 'Z' || b[i] >= '0' && b[i] <= '9' || b[i] == '_' || b[i] == '$') {
					i++
				}
				toks = append(toks, scanToken{scanBare, strings.ToUpper(stmt[start:i])})
			case c == '.':
				toks = append(toks, scanToken{scanDot, ""})
				i++
			case c == ';':
				toks = append(toks, scanToken{scanSemi, ""})
				i++
			default:
				i++
			}
		}
		return toks
	}
	// Split on top-level semicolons; quotes and comments are honored so
	// a ';' inside them never splits statements.
	b := []byte(sql)
	var stmts []string
	start, i := 0, 0
	for i < len(b) {
		c := b[i]
		switch {
		case c == '\'' || c == '"' || c == '`':
			i++
			for i < len(b) {
				if b[i] == c {
					i++
					if i < len(b) && b[i] == c {
						i++
					} else {
						break
					}
				} else {
					i++
				}
			}
		case c == '[':
			i++
			for i < len(b) && b[i] != ']' {
				i++
			}
			i++
			if i > len(b) {
				i = len(b)
			}
		case c == '-' && strings.HasPrefix(sql[i:], "--"):
			for i < len(b) && b[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < len(b) && b[i+1] == '*':
			i += 2
			for i+1 < len(b) && !(b[i] == '*' && b[i+1] == '/') {
				i++
			}
			i += 2
			if i > len(b) {
				i = len(b)
			}
		case c == ';':
			stmts = append(stmts, sql[start:i])
			i++
			start = i
		default:
			i++
		}
	}
	stmts = append(stmts, sql[start:])
	var ops []DestructiveOp
	for _, stmt := range stmts {
		raw := tokenize(stmt)
		toks := raw[:0]
		for _, t := range raw {
			if t.kind != scanSemi {
				toks = append(toks, t)
			}
		}
		if len(toks) == 0 {
			continue
		}
		k := 0
		eat := func(word string) bool {
			if k < len(toks) && toks[k].kind == scanBare && toks[k].text == word {
				k++
				return true
			}
			return false
		}
		ident := func() (string, bool) {
			if k < len(toks) && (toks[k].kind == scanBare || toks[k].kind == scanName) {
				s := toks[k].text
				k++
				return s, true
			}
			return "", false
		}
		// Optional `schema.` prefix; reports false for `temp.`.
		tableName := func() (string, bool) {
			first, ok := ident()
			if !ok {
				return "", false
			}
			if k < len(toks) && toks[k].kind == scanDot {
				k++
				name, ok := ident()
				if !ok {
					return "", false
				}
				if strings.EqualFold(first, "temp") {
					return "", false
				}
				return name, true
			}
			return first, true
		}
		if eat("DROP") {
			if !eat("TABLE") {
				continue
			}
			if eat("IF") {
				if !eat("EXISTS") {
					continue
				}
			}
			if name, ok := tableName(); ok {
				ops = append(ops, DestructiveOp{Table: name, Kind: OpDropTable})
			}
		} else if eat("ALTER") {
			if !eat("TABLE") {
				continue
			}
			table, ok := tableName()
			if !ok {
				continue
			}
			if eat("RENAME") {
				if eat("TO") {
					ops = append(ops, DestructiveOp{Table: table, Kind: OpRenameTable})
				} else {
					eat("COLUMN")
					if column, ok := ident(); ok && eat("TO") {
						ops = append(ops, DestructiveOp{Table: table, Column: column, Kind: OpRenameColumn})
					}
				}
			} else if eat("DROP") {
				eat("COLUMN")
				if column, ok := ident(); ok {
					ops = append(ops, DestructiveOp{Table: table, Column: column, Kind: OpDropColumn})
				}
			}
		}
	}
	return ops
}

// Keywords uppercases the unquoted words of a column definition,
// ignoring quoted values/identifiers and comments. SQLite remains the
// SQL parser and validates syntax.
func Keywords(sql string) []string {
	b := []byte(sql)
	i := 0
	var out []string
	for i < len(b) {
		switch {
		case b[i] == '\'' || b[i] == '"' || b[i] == '`' || b[i] == '[':
			end := b[i]
			if end == '[' {
				end = ']'
			}
			i++
			for i < len(b) {
				if b[i] == end {
					i++
					if i < len(b) && b[i] == end {
						i++
					} else {
						break
					}
				} else {
					i++
				}
			}
		case strings.HasPrefix(sql[i:], "--"):
			for i < len(b) && b[i] != '\n' {
				i++
			}
		case strings.HasPrefix(sql[i:], "/*"):
			i += 2
			for i+1 < len(b) && !strings.HasPrefix(sql[i:], "*/") {
				i++
			}
			i += 2
			if i > len(b) {
				i = len(b)
			}
		case b[i] >= 'a' && b[i] <= 'z' || b[i] >= 'A' && b[i] <= 'Z':
			start := i
			for i < len(b) && (b[i] >= 'a' && b[i] <= 'z' || b[i] >= 'A' && b[i] <= 'Z' || b[i] >= '0' && b[i] <= '9' || b[i] == '_') {
				i++
			}
			out = append(out, strings.ToUpper(sql[start:i]))
		default:
			i++
		}
	}
	return out
}

// debugEscape renders a string like Rust's Debug formatter. Control
// characters escape as `\0`, `\t`, `\r`, `\n`, or `\u{..}`; exotic
// non-printables outside the C0/C1 ranges pass through.
func debugEscape(s string) string {
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			b.WriteString("\\\"")
		case '\\':
			b.WriteString("\\\\")
		case '\n':
			b.WriteString("\\n")
		case '\r':
			b.WriteString("\\r")
		case '\t':
			b.WriteString("\\t")
		case 0:
			b.WriteString("\\0")
		default:
			if r < 0x20 || r == 0x7f || r >= 0x80 && r <= 0x9f {
				fmt.Fprintf(&b, "\\u{%x}", r)
			} else {
				b.WriteRune(r)
			}
		}
	}
	b.WriteByte('"')
	return b.String()
}

// FormatMappings renders table mappings like Rust's Debug formatter
// for schema history hashing.
func FormatMappings(mappings []TableMapping) string {
	if len(mappings) == 0 {
		return "[]"
	}
	parts := make([]string, len(mappings))
	for i, mp := range mappings {
		cols := make([]string, len(mp.Columns))
		for j, c := range mp.Columns {
			cols[j] = "(" + debugEscape(c.Old) + ", " + debugEscape(c.New) + ")"
		}
		parts[i] = "TableMapping { old_name: " + debugEscape(mp.OldName) +
			", new_name: " + debugEscape(mp.NewName) +
			", columns: [" + strings.Join(cols, ", ") + "] }"
	}
	return "[" + strings.Join(parts, ", ") + "]"
}
