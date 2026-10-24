package murmur

import (
	"bytes"
	"sort"
)

// Pure state joins. No SQLite handles or local clock allocation.

// RowID identifies a row by table id and encoded key.
type RowID struct {
	Table string
	Key   string
}

func mergeRow(schema *Schema, rows map[RowID]*RowState, row *RowState, floors map[RowID]Version) (RowID, error) {
	var table *Table
	for i := range schema.Tables {
		if schema.Tables[i].ID == row.Table {
			table = &schema.Tables[i]
			break
		}
	}
	if table == nil {
		return RowID{}, corruptErr("remote table")
	}
	pk := PkColumns(table)
	nullKey := false
	for _, k := range row.Key {
		if k.Tag == ValueNull {
			nullKey = true
			break
		}
	}
	if len(row.Key) != len(pk) || nullKey || len(row.Cells) != len(table.Columns) {
		return RowID{}, corruptErr("remote row shape")
	}
	for _, col := range table.Columns {
		n := 0
		for _, c := range row.Cells {
			if c.Column == col.ID {
				n++
			}
		}
		if n != 1 {
			return RowID{}, corruptErr("remote columns")
		}
	}
	for i, col := range pk {
		for _, c := range row.Cells {
			if c.Column == col.ID && !ValuesEqual(c.Value, row.Key[i]) {
				return RowID{}, corruptErr("remote key mismatch")
			}
		}
	}
	if err := ValidateRow(table, row); err != nil {
		return RowID{}, err
	}
	key := RowID{row.Table, string(EncodeValues(row.Key))}
	old, ok := rows[key]
	if !ok {
		rows[key] = cloneRow(row)
		return key, nil
	}
	switch cmp := row.Generation.Compare(old.Generation); {
	case cmp > 0:
		// A delete at or past the old incarnation fences its extrema
		// (delete-then-reinsert starts fresh); otherwise the unfenced
		// Min/Max cells join forward as values, not LWW.
		if floor, fenced := floors[key]; fenced && floor.Compare(old.Generation) >= 0 {
			*old = *cloneRow(row)
		} else {
			merged := cloneRow(row)
			if err := joinExtrema(table, merged, old); err != nil {
				return RowID{}, err
			}
			*old = *merged
		}
	case cmp == 0:
		for _, cell := range row.Cells {
			var target *Cell
			for i := range old.Cells {
				if old.Cells[i].Column == cell.Column {
					target = &old.Cells[i]
					break
				}
			}
			var policy MergePolicy
			found := false
			for _, c := range table.Columns {
				if c.ID == cell.Column {
					policy = c.Policy
					found = true
					break
				}
			}
			if !found {
				return RowID{}, ErrSchemaMismatch
			}
			if policy == PolicyMin || policy == PolicyMax {
				order, err := PolicyCompare(cell.Value, target.Value)
				if err != nil {
					return RowID{}, err
				}
				if policy == PolicyMin && order < 0 || policy == PolicyMax && order > 0 {
					*target = cell
				}
				continue
			}
			switch vc := cell.Version.Compare(target.Version); {
			case vc > 0:
				*target = cell
			case vc == 0 && !ValuesEqual(cell.Value, target.Value) &&
				policy != PolicyCounter && policy != PolicySet && policy != PolicyJson && policy != PolicyList:
				return RowID{}, corruptErr("different values for identical version")
			}
		}
	}
	if row.Generation.Compare(old.Generation) < 0 {
		// Older incarnation: ordinary cells stay ignored, but unfenced
		// Min/Max extrema still join forward (value semilattice).
		if floor, fenced := floors[key]; !fenced || floor.Compare(row.Generation) < 0 {
			if err := joinExtrema(table, old, row); err != nil {
				return RowID{}, err
			}
		}
	}
	return key, nil
}

// joinExtrema value-joins Min/Max cells from source into target (same key).
func joinExtrema(table *Table, target *RowState, source *RowState) error {
	for i := range target.Cells {
		cell := &target.Cells[i]
		var policy MergePolicy
		found := false
		for _, c := range table.Columns {
			if c.ID == cell.Column {
				policy = c.Policy
				found = true
				break
			}
		}
		if !found {
			return ErrSchemaMismatch
		}
		if policy != PolicyMin && policy != PolicyMax {
			continue
		}
		var other *Cell
		for j := range source.Cells {
			if source.Cells[j].Column == cell.Column {
				other = &source.Cells[j]
				break
			}
		}
		if other == nil {
			continue
		}
		order, err := PolicyCompare(other.Value, cell.Value)
		if err != nil {
			return err
		}
		if policy == PolicyMin && order < 0 || policy == PolicyMax && order > 0 {
			*cell = *other
		}
	}
	return nil
}

type tombID struct {
	Table string
	Key   string
	Gen   Version
}

// Join merges two replica states.
func Join(a, b *ReplicaState) (*ReplicaState, error) {
	if a.DatabaseID != b.DatabaseID || !SchemasEqual(&a.Schema, &b.Schema) {
		return nil, ErrSchemaMismatch
	}
	tombs := make(map[tombID]*Tombstone)
	for _, list := range [][]Tombstone{a.Tombstones, b.Tombstones} {
		for i := range list {
			t := &list[i]
			k := tombID{t.Table, string(t.Key), t.Generation}
			if e, ok := tombs[k]; ok {
				e.Version = e.Version.Max(t.Version)
			} else {
				cp := *t
				tombs[k] = &cp
			}
		}
	}
	floors := make(map[RowID]Version)
	for k := range tombs {
		rk := RowID{k.Table, k.Key}
		if g, ok := floors[rk]; ok {
			floors[rk] = g.Max(k.Gen)
		} else {
			floors[rk] = k.Gen
		}
	}
	rows := make(map[RowID]*RowState)
	for _, list := range [][]RowState{a.Rows, b.Rows} {
		for i := range list {
			if _, err := mergeRow(&a.Schema, rows, &list[i], floors); err != nil {
				return nil, err
			}
		}
	}
	for k, row := range rows {
		if g, ok := floors[k]; ok && g.Compare(row.Generation) >= 0 {
			delete(rows, k)
		}
	}
	clock := a.Clock.Max(b.Clock)
	for _, row := range rows {
		clock = rowClock(clock, row)
	}
	for _, t := range tombs {
		clock = clock.Max(t.Version).Max(t.Generation)
	}
	out := &ReplicaState{DatabaseID: a.DatabaseID, Schema: a.Schema, Clock: clock}
	for _, k := range sortedRowKeys(rows) {
		out.Rows = append(out.Rows, *rows[k])
	}
	for _, k := range sortedTombKeys(tombs) {
		out.Tombstones = append(out.Tombstones, *tombs[k])
	}
	return out, nil
}

// JoinBatch preserves sequential join semantics while keeping one
// transient keyed batch. Unchanged rows/tombstones are visited once,
// rather than once per transaction.
func JoinBatch(baseline *ReplicaState, incoming []ReplicaState) (*ReplicaState, error) {
	if len(incoming) == 0 {
		cp := *baseline
		return &cp, nil
	}
	tombs := make(map[tombID]*Tombstone)
	floors := make(map[RowID]Version)
	for i := range baseline.Tombstones {
		t := &baseline.Tombstones[i]
		k := RowID{t.Table, string(t.Key)}
		if g, ok := floors[k]; ok {
			floors[k] = g.Max(t.Generation)
		} else {
			floors[k] = t.Generation
		}
		tk := tombID{t.Table, string(t.Key), t.Generation}
		if e, ok := tombs[tk]; ok {
			e.Version = e.Version.Max(t.Version)
		} else {
			cp := *t
			tombs[tk] = &cp
		}
	}
	rows := make(map[RowID]*RowState)
	for i := range baseline.Rows {
		if _, err := mergeRow(&baseline.Schema, rows, &baseline.Rows[i], floors); err != nil {
			return nil, err
		}
	}
	clock := baseline.Clock
	for index := range incoming {
		state := &incoming[index]
		if state.DatabaseID != baseline.DatabaseID || !SchemasEqual(&state.Schema, &baseline.Schema) {
			return nil, ErrSchemaMismatch
		}
		clock = clock.Max(state.Clock)
		var touched []RowID
		// Tombstones land before rows so each row merges against exactly the
		// floors its sequential join would see (accumulated plus current).
		for i := range state.Tombstones {
			t := &state.Tombstones[i]
			k := RowID{t.Table, string(t.Key)}
			touched = append(touched, k)
			if g, ok := floors[k]; ok {
				floors[k] = g.Max(t.Generation)
			} else {
				floors[k] = t.Generation
			}
			tk := tombID{t.Table, string(t.Key), t.Generation}
			if e, ok := tombs[tk]; ok {
				e.Version = e.Version.Max(t.Version)
			} else {
				cp := *t
				tombs[tk] = &cp
			}
			clock = clock.Max(t.Generation).Max(t.Version)
		}
		for i := range state.Rows {
			k, err := mergeRow(&baseline.Schema, rows, &state.Rows[i], floors)
			if err != nil {
				return nil, err
			}
			touched = append(touched, k)
		}
		if index == 0 {
			// The first sequential join also validates/prunes the baseline.
			for k, row := range rows {
				if g, ok := floors[k]; ok && g.Compare(row.Generation) >= 0 {
					delete(rows, k)
				}
			}
			for _, row := range rows {
				clock = rowClock(clock, row)
			}
			for _, t := range tombs {
				clock = clock.Max(t.Generation).Max(t.Version)
			}
		} else {
			for _, k := range touched {
				if row, ok := rows[k]; ok {
					if g, fenced := floors[k]; fenced && g.Compare(row.Generation) >= 0 {
						delete(rows, k)
					}
				}
				if row, ok := rows[k]; ok {
					clock = rowClock(clock, row)
				}
			}
		}
	}
	out := &ReplicaState{DatabaseID: baseline.DatabaseID, Schema: baseline.Schema, Clock: clock}
	for _, k := range sortedRowKeys(rows) {
		out.Rows = append(out.Rows, *rows[k])
	}
	for _, k := range sortedTombKeys(tombs) {
		out.Tombstones = append(out.Tombstones, *tombs[k])
	}
	return out, nil
}

// cloneRow copies a row's slices so merges never mutate caller state.
func cloneRow(r *RowState) *RowState {
	cp := CloneRow(*r)
	return &cp
}

func rowClock(clock Version, row *RowState) Version {
	clock = clock.Max(row.Generation)
	for _, cell := range row.Cells {
		clock = clock.Max(cell.Version)
	}
	return clock
}

func sortedRowKeys(rows map[RowID]*RowState) []RowID {
	keys := make([]RowID, 0, len(rows))
	for k := range rows {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Table != keys[j].Table {
			return keys[i].Table < keys[j].Table
		}
		return keys[i].Key < keys[j].Key
	})
	return keys
}

func sortedTombKeys(tombs map[tombID]*Tombstone) []tombID {
	keys := make([]tombID, 0, len(tombs))
	for k := range tombs {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Table != keys[j].Table {
			return keys[i].Table < keys[j].Table
		}
		if keys[i].Key != keys[j].Key {
			return keys[i].Key < keys[j].Key
		}
		return keys[i].Gen.Compare(keys[j].Gen) < 0
	})
	return keys
}

// Patch is the delta between a state and its join.
type Patch struct {
	Rows       []RowState
	Deleted    []RowState
	Tombstones []Tombstone
	Cursor     uint64
	Clock      Version
	State      *ReplicaState
}

// Prepare joins b into a and diffs the result against a.
func Prepare(a, b *ReplicaState, cursor uint64) (*Patch, error) {
	joined, err := Join(a, b)
	if err != nil {
		return nil, err
	}
	return PrepareJoined(a, joined, cursor)
}

// CloneValue deep-copies a value's blob bytes.
func CloneValue(v Value) Value {
	if v.B != nil {
		v.B = append([]byte(nil), v.B...)
	}
	return v
}

// CloneRow deep-copies a row's key and cells.
func CloneRow(r RowState) RowState {
	out := r
	if r.Key != nil {
		out.Key = make([]Value, len(r.Key))
		for i, v := range r.Key {
			out.Key[i] = CloneValue(v)
		}
	}
	if r.Cells != nil {
		out.Cells = make([]Cell, len(r.Cells))
		for i, c := range r.Cells {
			out.Cells[i] = c
			out.Cells[i].Value = CloneValue(c.Value)
		}
	}
	return out
}

// CloneTombstone deep-copies a tombstone's key bytes.
func CloneTombstone(t Tombstone) Tombstone {
	if t.Key != nil {
		t.Key = append([]byte(nil), t.Key...)
	}
	return t
}

// CloneReplicaState deep-copies rows and tombstones so join projection
// can mutate the clone without corrupting the baseline it diffs against.
// Schema is shared read-only (mirrors Rust's clone-then-read usage).
func CloneReplicaState(s *ReplicaState) *ReplicaState {
	out := *s
	if s.Rows != nil {
		out.Rows = make([]RowState, len(s.Rows))
		for i, r := range s.Rows {
			out.Rows[i] = CloneRow(r)
		}
	}
	if s.Tombstones != nil {
		out.Tombstones = make([]Tombstone, len(s.Tombstones))
		for i, t := range s.Tombstones {
			out.Tombstones[i] = CloneTombstone(t)
		}
	}
	return &out
}

// PrepareJoined diffs an already-joined state against a.
func PrepareJoined(a *ReplicaState, joined *ReplicaState, cursor uint64) (*Patch, error) {
	oldRows := make(map[RowID]*RowState)
	for i := range a.Rows {
		r := &a.Rows[i]
		oldRows[RowID{r.Table, string(EncodeValues(r.Key))}] = r
	}
	newRows := make(map[RowID]*RowState)
	for i := range joined.Rows {
		r := &joined.Rows[i]
		newRows[RowID{r.Table, string(EncodeValues(r.Key))}] = r
	}
	oldTombs := make(map[tombID]*Tombstone)
	for i := range a.Tombstones {
		t := &a.Tombstones[i]
		oldTombs[tombID{t.Table, string(t.Key), t.Generation}] = t
	}
	var patch Patch
	for _, k := range sortedRowKeys(newRows) {
		row := newRows[k]
		if old, ok := oldRows[k]; !ok || !RowsEqual(old, row) {
			patch.Rows = append(patch.Rows, *row)
		}
	}
	for _, k := range sortedRowKeys(oldRows) {
		if _, ok := newRows[k]; !ok {
			patch.Deleted = append(patch.Deleted, *oldRows[k])
		}
	}
	for _, t := range joined.Tombstones {
		k := tombID{t.Table, string(t.Key), t.Generation}
		if old, ok := oldTombs[k]; !ok || !TombsEqual(old, &t) {
			patch.Tombstones = append(patch.Tombstones, t)
		}
	}
	patch.Cursor = cursor
	patch.Clock = joined.Clock
	patch.State = joined
	return &patch, nil
}

// PrepareDelta diffs like PrepareJoined but drops the joined state.
func PrepareDelta(a *ReplicaState, joined *ReplicaState, cursor uint64) (*Patch, error) {
	patch, err := PrepareJoined(a, joined, cursor)
	if err != nil {
		return nil, err
	}
	patch.State = nil
	return patch, nil
}

// ValuesEqual reports deep value equality.
func ValuesEqual(a, b Value) bool {
	if a.Tag != b.Tag {
		return false
	}
	switch a.Tag {
	case ValueNull:
		return true
	case ValueInteger:
		return a.I == b.I
	case ValueReal:
		return a.F == b.F || isNaN(a.F) && isNaN(b.F)
	case ValueText:
		return a.S == b.S
	case ValueBlob:
		return bytes.Equal(a.B, b.B)
	}
	return false
}

func isNaN(f float64) bool { return f != f }

// RowsEqual reports deep row equality.
func RowsEqual(a, b *RowState) bool {
	if a.Table != b.Table || a.Generation != b.Generation ||
		len(a.Key) != len(b.Key) || len(a.Cells) != len(b.Cells) {
		return false
	}
	for i := range a.Key {
		if !ValuesEqual(a.Key[i], b.Key[i]) {
			return false
		}
	}
	for i := range a.Cells {
		if a.Cells[i].Column != b.Cells[i].Column ||
			!ValuesEqual(a.Cells[i].Value, b.Cells[i].Value) ||
			a.Cells[i].Version != b.Cells[i].Version {
			return false
		}
	}
	return true
}

// TombsEqual reports deep tombstone equality.
func TombsEqual(a, b *Tombstone) bool {
	return a.Table == b.Table && bytes.Equal(a.Key, b.Key) &&
		a.Generation == b.Generation && a.Version == b.Version
}

func stringsEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// SchemasEqual reports deep schema equality.
func SchemasEqual(a, b *Schema) bool {
	if len(a.Tables) != len(b.Tables) || len(a.Indexes) != len(b.Indexes) ||
		a.History != b.History || len(a.TableReplication) != len(b.TableReplication) {
		return false
	}
	for k, v := range a.TableReplication {
		if bv, ok := b.TableReplication[k]; !ok || bv != v {
			return false
		}
	}
	for i := range a.Indexes {
		if a.Indexes[i] != b.Indexes[i] {
			return false
		}
	}
	for i := range a.Tables {
		at, bt := &a.Tables[i], &b.Tables[i]
		if at.ID != bt.ID || at.Name != bt.Name || at.SQL != bt.SQL ||
			len(at.Columns) != len(bt.Columns) {
			return false
		}
		if (at.ApplicationKey == nil) != (bt.ApplicationKey == nil) {
			return false
		}
		if at.ApplicationKey != nil {
			ak, bk := at.ApplicationKey, bt.ApplicationKey
			if !stringsEqual(ak.Columns, bk.Columns) ||
				!stringsEqual(ak.Collations, bk.Collations) ||
				len(ak.Descending) != len(bk.Descending) ||
				ak.WithoutRow != bk.WithoutRow || ak.Strict != bk.Strict {
				return false
			}
			for j := range ak.Descending {
				if ak.Descending[j] != bk.Descending[j] {
					return false
				}
			}
			if (ak.Conflict == nil) != (bk.Conflict == nil) {
				return false
			}
			if ak.Conflict != nil && *ak.Conflict != *bk.Conflict {
				return false
			}
		}
		for j := range at.Columns {
			ac, bc := &at.Columns[j], &bt.Columns[j]
			if ac.ID != bc.ID || ac.Name != bc.Name ||
				ac.PrimaryKey != bc.PrimaryKey || ac.DeclaredType != bc.DeclaredType ||
				ac.NotNull != bc.NotNull || ac.Policy != bc.Policy || ac.Fts != bc.Fts {
				return false
			}
			if (ac.Vector == nil) != (bc.Vector == nil) {
				return false
			}
			if ac.Vector != nil && *ac.Vector != *bc.Vector {
				return false
			}
		}
	}
	return true
}
