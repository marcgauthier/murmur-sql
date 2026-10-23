package murmur

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/marcgauthier/murmur-sql-go/murmur/internal/postcard"
	"github.com/marcgauthier/murmur-sql-go/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql-go/murmur/spool"
)

// Conflict versions live in Spool. These types are transient record
// payloads, not an independently maintained in-memory metadata database.

// ExtendNullable adds implicit-NULL cells for adopted nullable columns.
// Schema adoption can add nullable columns without rewriting every Spool
// row. An absent cell reads as SQLite's implicit NULL with the oldest
// version so a real incoming value wins. Mutations persist the expanded row.
func (m *RowMeta) ExtendNullable(table *Table) error {
	for _, column := range table.Columns {
		found := false
		for _, cell := range m.Cells {
			if cell.Column == column.ID {
				found = true
				break
			}
		}
		if found {
			continue
		}
		if column.PrimaryKey > 0 || column.NotNull {
			return ErrSchemaMismatch
		}
		m.Cells = append(m.Cells, Cell{Column: column.ID, Value: NullValue()})
	}
	return nil
}

// RowKey renders a row metadata key.
func RowKey(table string, key []byte) []byte {
	return []byte("04/meta/row/" + table + "/" + hashHex(key))
}

func tombKey(t *Tombstone) []byte {
	buf := encodeOne(func(e *postcard.Encoder) {
		e.ByteString(t.Key)
		encodeVersion(e, t.Generation)
	})
	return []byte("05/meta/tomb/" + t.Table + "/" + hashHex(buf))
}

// ReadRowMeta reads one row's metadata, reporting nil when absent.
func ReadRowMeta(store *LogicalStore, table string, key []byte) (*RowMeta, error) {
	raw, err := store.Get(RowKey(table, key))
	if err != nil {
		return nil, err
	}
	if raw == nil {
		return nil, nil
	}
	meta, err := DecodeRowMeta(raw)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(meta.Key, key) {
		return nil, corruptErr("metadata key collision")
	}
	return &meta, nil
}

// PutRowMeta renders a row metadata put.
func PutRowMeta(table string, meta *RowMeta) (spool.Mutation, error) {
	return spool.Mutation{Key: RowKey(table, meta.Key), Value: EncodeRowMeta(*meta)}, nil
}

func putTomb(table string, key []byte, generation, version Version) (spool.Mutation, error) {
	t := Tombstone{Table: table, Key: key, Generation: generation, Version: version}
	return spool.Mutation{Key: tombKey(&t), Value: EncodeTombstone(t)}, nil
}

// TablesInChangeset lists tables touched by concatenated changesets,
// ignoring malformed input.
func TablesInChangeset(changes []byte) []string {
	if len(changes) == 0 {
		return nil
	}
	seen := make(map[string]bool)
	var out []string
	_ = sqlite.IterateChangeset(changes, func(ch sqlite.Change) error {
		if !seen[ch.Table] {
			seen[ch.Table] = true
			out = append(out, ch.Table)
		}
		return nil
	})
	return out
}

// ChangesetOverlaps inspects only primary keys in a pending session
// capture. Used by background publication to prove that uncaptured local
// SQL cannot invalidate a point merge.
func ChangesetOverlaps(schema *Schema, changes []byte, keys map[RowID]bool) (bool, error) {
	if len(changes) == 0 {
		return false, nil
	}
	hit := false
	err := sqlite.IterateChangeset(changes, func(ch sqlite.Change) error {
		var table *Table
		for i := range schema.Tables {
			if schema.Tables[i].Name == ch.Table {
				table = &schema.Tables[i]
				break
			}
		}
		if table == nil {
			return ErrSchemaMismatch
		}
		if ch.Columns != len(table.Columns) {
			return ErrSchemaMismatch
		}
		key, err := changesetKey(table, ch)
		if err != nil {
			return err
		}
		if keys[RowID{table.ID, string(key)}] {
			hit = true
		}
		return nil
	})
	if err != nil {
		return false, err
	}
	return hit, nil
}

// changesetKey extracts the encoded primary key from one op.
func changesetKey(table *Table, ch sqlite.Change) ([]byte, error) {
	pk := PkColumns(table)
	key := make([]Value, 0, len(pk))
	for _, col := range pk {
		idx := -1
		for i := range table.Columns {
			if table.Columns[i].ID == col.ID {
				idx = i
				break
			}
		}
		var v Value
		var err error
		if ch.Op == sqlite.OpInsert {
			if !ch.NewPresent[idx] {
				return nil, sqlErr(fmt.Errorf("changeset key image missing"))
			}
			v, err = valueFromSQLite(ch.New[idx])
		} else {
			if !ch.OldPresent[idx] {
				return nil, sqlErr(fmt.Errorf("changeset key image missing"))
			}
			v, err = valueFromSQLite(ch.Old[idx])
		}
		if err != nil {
			return nil, err
		}
		key = append(key, v)
	}
	return EncodeValues(key), nil
}

type coalesced struct {
	table      int
	key        []byte
	firstIns   bool
	lastDelete bool
	lastNew    []Value
	lastSet    []bool
}

// Delta converts concatenated session changesets into metadata
// mutations stamped at version, coalescing to one net-effect op per row.
func Delta(store *LogicalStore, schema *Schema, changes []byte, version Version) ([]spool.Mutation, error) {
	if len(changes) == 0 {
		return nil, nil
	}
	// Checkpoint bytes concatenate one changeset per committed transaction,
	// so a row may appear in several ops (e.g. INSERT then UPDATE then
	// DELETE). A long-lived SQLite session coalesces those to a single
	// net-effect op per row; replicate that here so downstream merge sees
	// exactly one op per row per batch. First pass: validate every op and
	// accumulate each row's first/last op plus final column values.
	var order []coalesced
	index := make(map[RowID]int)
	err := sqlite.IterateChangeset(changes, func(ch sqlite.Change) error {
		tableIdx := -1
		for i := range schema.Tables {
			if schema.Tables[i].Name == ch.Table {
				tableIdx = i
				break
			}
		}
		if tableIdx < 0 {
			return corruptErr("unknown changeset table")
		}
		table := &schema.Tables[tableIdx]
		pkBad := false
		if ch.Columns != len(table.Columns) || len(ch.PK) != len(table.Columns) {
			pkBad = true
		} else {
			for i, c := range table.Columns {
				if ch.PK[i] != (c.PrimaryKey > 0) {
					pkBad = true
					break
				}
			}
		}
		if pkBad {
			return ErrSchemaMismatch
		}
		key, err := changesetKey(table, ch)
		if err != nil {
			return err
		}
		vals, err := DecodeValues(key)
		if err != nil {
			return err
		}
		for _, v := range vals {
			if v.Tag == ValueNull {
				return corruptErr("null changeset primary key")
			}
		}
		id := RowID{table.ID, string(key)}
		slot, ok := index[id]
		if !ok {
			slot = len(order)
			index[id] = slot
			order = append(order, coalesced{
				table:    tableIdx,
				key:      key,
				firstIns: ch.Op == sqlite.OpInsert,
				lastNew:  make([]Value, len(table.Columns)),
				lastSet:  make([]bool, len(table.Columns)),
			})
		}
		entry := &order[slot]
		switch ch.Op {
		case sqlite.OpInsert:
			for i := range entry.lastNew {
				if !ch.NewPresent[i] {
					continue
				}
				v, err := valueFromSQLite(ch.New[i])
				if err != nil {
					return err
				}
				entry.lastNew[i] = v
				entry.lastSet[i] = true
			}
			entry.lastDelete = false
		case sqlite.OpUpdate:
			for i := range entry.lastNew {
				if !ch.NewPresent[i] {
					continue
				}
				v, err := valueFromSQLite(ch.New[i])
				if err != nil {
					return err
				}
				entry.lastNew[i] = v
				entry.lastSet[i] = true
			}
			entry.lastDelete = false
		case sqlite.OpDelete:
			entry.lastDelete = true
		default:
			return corruptErr("unsupported changeset action")
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	// Second pass: emit one net-effect op per row, in first-seen order.
	var out []spool.Mutation
	for i := range order {
		entry := &order[i]
		table := &schema.Tables[entry.table]
		old, err := ReadRowMeta(store, table.ID, entry.key)
		if err != nil {
			return nil, err
		}
		if old != nil {
			if err := old.ExtendNullable(table); err != nil {
				return nil, err
			}
		}
		if entry.lastDelete {
			// Deleted by batch end: a row that never existed at batch
			// start (INSERT then DELETE) has no net effect.
			if old != nil {
				t, err := putTomb(table.ID, entry.key, old.Generation, version)
				if err != nil {
					return nil, err
				}
				out = append(out, t, spool.Mutation{Key: RowKey(table.ID, entry.key), Deleted: true})
			}
			continue
		}
		if old != nil {
			// Net UPDATE (the row existed at batch start): keep its
			// generation, stamp touched cells, like a coalesced session.
			for ci, c := range table.Columns {
				if !entry.lastSet[ci] {
					continue
				}
				cell := -1
				for j := range old.Cells {
					if old.Cells[j].Column == c.ID {
						cell = j
						break
					}
				}
				if cell < 0 {
					return nil, ErrSchemaMismatch
				}
				old.Cells[cell].Version = version
				old.Cells[cell].Value = entry.lastNew[ci]
			}
			m, err := PutRowMeta(table.ID, old)
			if err != nil {
				return nil, err
			}
			out = append(out, m)
			continue
		}
		// The row is new to this batch: its first op must have been an
		// INSERT carrying every column, as in a coalesced session.
		if !entry.firstIns {
			return nil, corruptErr("update has no row metadata")
		}
		meta := RowMeta{Key: entry.key, Generation: version}
		for ci, c := range table.Columns {
			if !entry.lastSet[ci] {
				return nil, corruptErr("insert misses column values")
			}
			meta.Cells = append(meta.Cells, Cell{Column: c.ID, Value: entry.lastNew[ci], Version: version})
		}
		m, err := PutRowMeta(table.ID, &meta)
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, nil
}

// MigrationMutations diffs before/after migration rows into metadata
// mutations stamped at version.
func MigrationMutations(store *LogicalStore, before, after []RowState, version Version) ([]spool.Mutation, error) {
	if len(before) == 0 && len(after) == 0 {
		return nil, nil
	}
	var out []spool.Mutation
	beforeMap := make(map[RowID]*RowState, len(before))
	for i := range before {
		r := &before[i]
		beforeMap[RowID{r.Table, string(EncodeValues(r.Key))}] = r
	}
	afterMap := make(map[RowID]*RowState, len(after))
	for i := range after {
		r := &after[i]
		afterMap[RowID{r.Table, string(EncodeValues(r.Key))}] = r
	}
	for id := range beforeMap {
		if _, ok := afterMap[id]; !ok {
			old, err := ReadRowMeta(store, id.Table, []byte(id.Key))
			if err != nil {
				return nil, err
			}
			if old == nil {
				return nil, corruptErr("migration row metadata missing")
			}
			t, err := putTomb(id.Table, []byte(id.Key), old.Generation, version)
			if err != nil {
				return nil, err
			}
			out = append(out, t, spool.Mutation{Key: RowKey(id.Table, []byte(id.Key)), Deleted: true})
		}
	}
	for id := range afterMap {
		row := afterMap[id]
		old, err := ReadRowMeta(store, id.Table, []byte(id.Key))
		if err != nil {
			return nil, err
		}
		prior := beforeMap[id]
		cells := make([]Cell, 0, len(row.Cells))
		for _, cell := range row.Cells {
			unchanged := false
			if prior != nil {
				for _, c := range prior.Cells {
					if c.Column == cell.Column && ValuesEqual(c.Value, cell.Value) {
						unchanged = true
						break
					}
				}
			}
			v := version
			if unchanged {
				if old != nil {
					for _, c := range old.Cells {
						if c.Column == cell.Column {
							v = c.Version
							break
						}
					}
				}
			}
			cells = append(cells, Cell{Column: cell.Column, Value: cell.Value, Version: v})
		}
		gen := version
		if old != nil {
			gen = old.Generation
		}
		m, err := PutRowMeta(id.Table, &RowMeta{Key: []byte(id.Key), Generation: gen, Cells: cells})
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, nil
}

// ReplicaMutations replaces all row metadata with state: the validated
// join, including all previously known tombstones.
func ReplicaMutations(store *LogicalStore, state *ReplicaState) ([]spool.Mutation, error) {
	var out []spool.Mutation
	for _, key := range store.KeysWithPrefix([]byte("04/meta/row/")) {
		out = append(out, spool.Mutation{Key: key, Deleted: true})
	}
	for i := range state.Rows {
		r := &state.Rows[i]
		m, err := PutRowMeta(r.Table, &RowMeta{Key: EncodeValues(r.Key), Generation: r.Generation, Cells: r.Cells})
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	for i := range state.Tombstones {
		t := &state.Tombstones[i]
		out = append(out, spool.Mutation{Key: tombKey(t), Value: EncodeTombstone(*t)})
	}
	return out, nil
}

// ExportState stamps rows with stored generations and cell versions,
// returning the full replica state at clock.
func ExportState(store *LogicalStore, schema Schema, databaseID [16]byte, rows []RowState, clock Version) (*ReplicaState, error) {
	for i := range rows {
		row := &rows[i]
		meta, err := ReadRowMeta(store, row.Table, EncodeValues(row.Key))
		if err != nil {
			return nil, err
		}
		if meta == nil {
			return nil, corruptErr("row metadata missing")
		}
		var table *Table
		for ti := range schema.Tables {
			if schema.Tables[ti].ID == row.Table {
				table = &schema.Tables[ti]
				break
			}
		}
		if table == nil {
			return nil, ErrSchemaMismatch
		}
		if err := meta.ExtendNullable(table); err != nil {
			return nil, err
		}
		row.Generation = meta.Generation
		for ci := range row.Cells {
			found := false
			for _, c := range meta.Cells {
				if c.Column == row.Cells[ci].Column {
					row.Cells[ci].Version = c.Version
					found = true
					break
				}
			}
			if !found {
				return nil, ErrSchemaMismatch
			}
		}
	}
	var tombstones []Tombstone
	for _, key := range store.KeysWithPrefix([]byte("05/meta/tomb/")) {
		raw, err := store.Get(key)
		if err != nil {
			return nil, err
		}
		if raw == nil {
			return nil, corruptErr("tombstone missing")
		}
		t, err := DecodeTombstone(raw)
		if err != nil {
			return nil, err
		}
		tombstones = append(tombstones, t)
	}
	sort.Slice(tombstones, func(i, j int) bool {
		if tombstones[i].Table != tombstones[j].Table {
			return tombstones[i].Table < tombstones[j].Table
		}
		if c := bytes.Compare(tombstones[i].Key, tombstones[j].Key); c != 0 {
			return c < 0
		}
		return tombstones[i].Generation.Compare(tombstones[j].Generation) < 0
	})
	return &ReplicaState{DatabaseID: databaseID, Schema: schema, Rows: rows, Tombstones: tombstones, Clock: clock}, nil
}

// ReadState reads a transient logical state from the authoritative
// encrypted Spool. The checkpoint worker serializes this read with all
// metadata commits.
func ReadState(store *LogicalStore, schema Schema, databaseID [16]byte, clock Version) (*ReplicaState, error) {
	var rows []RowState
	for ti := range schema.Tables {
		table := &schema.Tables[ti]
		for _, key := range store.KeysWithPrefix([]byte("04/meta/row/" + table.ID + "/")) {
			raw, err := store.Get(key)
			if err != nil {
				return nil, err
			}
			if raw == nil {
				return nil, corruptErr("missing row record")
			}
			meta, err := DecodeRowMeta(raw)
			if err != nil {
				return nil, err
			}
			if err := meta.ExtendNullable(table); err != nil {
				return nil, err
			}
			rv, err := DecodeValues(meta.Key)
			if err != nil {
				return nil, err
			}
			rows = append(rows, RowState{Table: table.ID, Key: rv, Generation: meta.Generation, Cells: meta.Cells})
		}
	}
	return ExportState(store, schema, databaseID, rows, clock)
}

// ChangedState keeps only the rows/tombstones changed by one committed
// local capture.
func ChangedState(records []spool.Mutation, schema Schema, databaseID [16]byte, clock Version) (*ReplicaState, error) {
	var rows []RowState
	var tombstones []Tombstone
	for i := range records {
		r := &records[i]
		if r.Deleted {
			continue
		}
		switch {
		case bytes.HasPrefix(r.Key, []byte("04/meta/row/")):
			meta, err := DecodeRowMeta(r.Value)
			if err != nil {
				return nil, err
			}
			if !utf8.Valid(r.Key) {
				return nil, corruptErr("row key")
			}
			parts := strings.Split(string(r.Key), "/")
			if len(parts) < 4 {
				return nil, ErrSchemaMismatch
			}
			rv, err := DecodeValues(meta.Key)
			if err != nil {
				return nil, err
			}
			rows = append(rows, RowState{Table: parts[3], Key: rv, Generation: meta.Generation, Cells: meta.Cells})
		case bytes.HasPrefix(r.Key, []byte("05/meta/tomb/")):
			t, err := DecodeTombstone(r.Value)
			if err != nil {
				return nil, err
			}
			tombstones = append(tombstones, t)
		}
	}
	return &ReplicaState{DatabaseID: databaseID, Schema: schema, Rows: rows, Tombstones: tombstones, Clock: clock}, nil
}

// PatchMutations converts a merge patch into metadata mutations,
// maxing tombstone versions against stored ones.
func PatchMutations(store *LogicalStore, rows, deleted []RowState, tombstones []Tombstone) ([]spool.Mutation, error) {
	var out []spool.Mutation
	for i := range deleted {
		r := &deleted[i]
		out = append(out, spool.Mutation{Key: RowKey(r.Table, EncodeValues(r.Key)), Deleted: true})
	}
	for i := range rows {
		r := &rows[i]
		m, err := PutRowMeta(r.Table, &RowMeta{Key: EncodeValues(r.Key), Generation: r.Generation, Cells: r.Cells})
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	for i := range tombstones {
		t := tombstones[i]
		key := tombKey(&t)
		if raw, err := store.Get(key); err != nil {
			return nil, err
		} else if raw != nil {
			old, err := DecodeTombstone(raw)
			if err != nil {
				return nil, err
			}
			t.Version = t.Version.Max(old.Version)
		}
		out = append(out, spool.Mutation{Key: key, Value: EncodeTombstone(t)})
	}
	return out, nil
}

// deletedKey renders a deletion-floor summary key.
func deletedKey(table string, key []byte) []byte {
	return []byte("05/meta/deleted/" + table + "/" + hashHex(key))
}

// SummarizeTombs folds tomb records into maximum-deleted-generation
// summaries, appending the summary puts to records. Historical
// tombstones remain intact.
func SummarizeTombs(store *LogicalStore, records []spool.Mutation) ([]spool.Mutation, error) {
	summaries := make(map[string]Tombstone)
	for i := range records {
		r := &records[i]
		if r.Deleted || !bytes.HasPrefix(r.Key, []byte("05/meta/tomb/")) {
			continue
		}
		tomb, err := DecodeTombstone(r.Value)
		if err != nil {
			return nil, err
		}
		key := string(deletedKey(tomb.Table, tomb.Key))
		if _, ok := summaries[key]; !ok {
			if raw, err := store.Get([]byte(key)); err != nil {
				return nil, err
			} else if raw != nil {
				seed, err := DecodeTombstone(raw)
				if err != nil {
					return nil, err
				}
				summaries[key] = seed
			}
		}
		cur, ok := summaries[key]
		if !ok {
			summaries[key] = tomb
			continue
		}
		if !bytes.Equal(cur.Key, tomb.Key) || cur.Table != tomb.Table {
			return nil, corruptErr("deleted key collision")
		}
		if tomb.Generation.Compare(cur.Generation) > 0 ||
			tomb.Generation == cur.Generation && tomb.Version.Compare(cur.Version) > 0 {
			summaries[key] = tomb
		}
	}
	keys := make([]string, 0, len(summaries))
	for k := range summaries {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		out := summaries[k]
		records = append(records, spool.Mutation{Key: []byte(k), Value: EncodeTombstone(out)})
	}
	return records, nil
}

// AffectedState reads only rows and deletion floors that can be
// affected by a batch.
func AffectedState(store *LogicalStore, schema Schema, databaseID [16]byte, clock Version, keys map[RowID]bool) (*ReplicaState, error) {
	var rows []RowState
	var tombstones []Tombstone
	ids := make([]RowID, 0, len(keys))
	for id := range keys {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		if ids[i].Table != ids[j].Table {
			return ids[i].Table < ids[j].Table
		}
		return ids[i].Key < ids[j].Key
	})
	for _, id := range ids {
		if meta, err := ReadRowMeta(store, id.Table, []byte(id.Key)); err != nil {
			return nil, err
		} else if meta != nil {
			var def *Table
			for ti := range schema.Tables {
				if schema.Tables[ti].ID == id.Table {
					def = &schema.Tables[ti]
					break
				}
			}
			if def == nil {
				return nil, ErrSchemaMismatch
			}
			if err := meta.ExtendNullable(def); err != nil {
				return nil, err
			}
			rv, err := DecodeValues(meta.Key)
			if err != nil {
				return nil, err
			}
			rows = append(rows, RowState{Table: id.Table, Key: rv, Generation: meta.Generation, Cells: meta.Cells})
		}
		if raw, err := store.Get(deletedKey(id.Table, []byte(id.Key))); err != nil {
			return nil, err
		} else if raw != nil {
			tomb, err := DecodeTombstone(raw)
			if err != nil {
				return nil, err
			}
			if tomb.Table != id.Table || !bytes.Equal(tomb.Key, []byte(id.Key)) {
				return nil, corruptErr("deleted key collision")
			}
			tombstones = append(tombstones, tomb)
		}
	}
	return &ReplicaState{DatabaseID: databaseID, Schema: schema, Clock: clock, Rows: rows, Tombstones: tombstones}, nil
}

// TransitionTable switches physical key domains without mixing local
// application-key tombstones with UUID tombstones. Archived records are
// authoritative Spool records, never a second SQLite database or an
// in-memory metadata mirror.
func TransitionTable(store *LogicalStore, table string, enabled bool, before, after []RowState, version Version) ([]spool.Mutation, error) {
	var out []spool.Mutation
	domain := "replicated"
	if enabled {
		domain = "local"
	}
	archive := "12/key-transition/" + table + "/" + domain + "/"
	for _, prefix := range []string{
		"04/meta/row/" + table + "/",
		"05/meta/tomb/" + table + "/",
		"05/meta/deleted/" + table + "/",
		"10/policy/state/" + table + "/",
		"09/bridge/owner/" + table + "/",
	} {
		for _, key := range store.KeysWithPrefix([]byte(prefix)) {
			if !bytes.HasPrefix(key, []byte("04/meta/row/")) && !bytes.HasPrefix(key, []byte("05/meta/deleted/")) {
				raw, err := store.Get(key)
				if err != nil {
					return nil, err
				}
				if raw == nil {
					return nil, corruptErr("transition record missing")
				}
				// A reversible path encoding preserves every historical tombstone.
				out = append(out, spool.Mutation{Key: []byte(archive + hexEncode(key)), Value: raw})
			}
			out = append(out, spool.Mutation{Key: key, Deleted: true})
		}
	}
	if !enabled {
		// Fence all published incarnations. These fences stay out of outgoing
		// batches until re-enabling publishes a new snapshot of the table.
		for i := range before {
			key := EncodeValues(before[i].Key)
			old, err := ReadRowMeta(store, table, key)
			if err != nil {
				return nil, err
			}
			if old == nil {
				return nil, corruptErr("transition row missing")
			}
			record, err := putTomb(table, key, old.Generation, version)
			if err != nil {
				return nil, err
			}
			out = append(out, spool.Mutation{Key: []byte(archive + hexEncode(record.Key)), Value: record.Value})
		}
	} else {
		for _, key := range store.KeysWithPrefix([]byte("12/key-transition/" + table + "/replicated/")) {
			raw, err := store.Get(key)
			if err != nil {
				return nil, err
			}
			if raw == nil {
				return nil, corruptErr("transition archive missing")
			}
			// Only deletion records are restored. Causal policy state starts a
			// fresh incarnation from the current application values.
			leaf := string(key)
			if i := strings.LastIndexByte(leaf, '/'); i >= 0 {
				leaf = leaf[i+1:]
			}
			if strings.HasPrefix(leaf, "30352f6d6574612f746f6d622f") {
				var deletion Tombstone
				deletion, err = DecodeTombstone(raw)
				if err != nil {
					return nil, err
				}
				deletion.Version = version
				out = append(out, spool.Mutation{Key: tombKey(&deletion), Value: EncodeTombstone(deletion)})
			}
		}
	}
	for i := range after {
		row := &after[i]
		cells := make([]Cell, 0, len(row.Cells))
		for _, c := range row.Cells {
			cells = append(cells, Cell{Column: c.Column, Value: c.Value, Version: version})
		}
		m, err := PutRowMeta(table, &RowMeta{Key: EncodeValues(row.Key), Generation: version, Cells: cells})
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, nil
}
