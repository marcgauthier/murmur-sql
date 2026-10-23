package murmur

import (
	"bytes"
	"math/bits"

	"github.com/marcgauthier/murmur-sql/murmur/internal/postcard"
	"github.com/marcgauthier/murmur-sql/murmur/replication"
	"github.com/marcgauthier/murmur-sql/murmur/spool"
)

// PolicyState is durable causal state for one counter/set cell.
type PolicyState struct {
	Version    Version
	Domain     [16]byte
	Branch     Version
	Scope      Version
	Baseline   Value
	Basis      Version
	Table      string
	Column     string
	Key        []Value
	Generation Version
	Causal     *replication.Causal
}

// EncodePolicyState renders state in Rust postcard field order.
func EncodePolicyState(s *PolicyState) []byte {
	return encodeOne(func(e *postcard.Encoder) {
		encodeVersion(e, s.Version)
		e.Raw(s.Domain[:])
		encodeVersion(e, s.Branch)
		encodeVersion(e, s.Scope)
		encodeValue(e, s.Baseline)
		encodeVersion(e, s.Basis)
		e.String(s.Table)
		e.String(s.Column)
		e.Len(len(s.Key))
		for _, v := range s.Key {
			encodeValue(e, v)
		}
		encodeVersion(e, s.Generation)
		e.Raw(replication.EncodeCausal(s.Causal))
	})
}

// DecodePolicyState parses state rendered by EncodePolicyState.
func DecodePolicyState(b []byte) (*PolicyState, error) {
	d := postcard.NewDecoder(b)
	s := &PolicyState{}
	s.Version = decodeVersion(d)
	copy(s.Domain[:], d.Raw(16))
	s.Branch = decodeVersion(d)
	s.Scope = decodeVersion(d)
	s.Baseline = decodeValue(d)
	s.Basis = decodeVersion(d)
	s.Table = d.String()
	s.Column = d.String()
	n := d.Len()
	for i := 0; i < n; i++ {
		s.Key = append(s.Key, decodeValue(d))
	}
	s.Generation = decodeVersion(d)
	if err := d.Err(); err != nil {
		return nil, corruptErr("%v", err)
	}
	causal, err := replication.DecodeCausal(b[d.Offset():])
	if err != nil {
		return nil, corruptErr("%v", err)
	}
	s.Causal = causal
	return s, nil
}

// PolicyStateKey renders the storage key for state.
func PolicyStateKey(s *PolicyState) ([]byte, error) {
	key, err := func() ([]byte, error) { return EncodeValues(s.Key), nil }()
	if err != nil {
		return nil, err
	}
	scope := encodeOne(func(e *postcard.Encoder) {
		encodeVersion(e, s.Generation)
		e.Raw(s.Domain[:])
		encodeVersion(e, s.Branch)
		encodeVersion(e, s.Scope)
	})
	return []byte("10/policy/state/" + s.Table + "/" + s.Column + "/" + id(key) + "/" + id(scope)), nil
}

// ConfigurePolicy applies configured merge policies to schema columns.
func ConfigurePolicy(schema *Schema, config *replication.Config) error {
	if config == nil {
		return nil
	}
	for col, policy := range config.MergePolicies {
		t := findTableByName(schema, col.Table)
		if t == nil {
			return invalidErr("unknown policy table %s", col.Table)
		}
		appKeyCol := false
		if t.ApplicationKey != nil {
			for _, c := range t.ApplicationKey.Columns {
				if c == col.Column {
					appKeyCol = true
				}
			}
		}
		c := findColumnByName(t, col.Column)
		if c == nil {
			return invalidErr("unknown policy column %s", col.Column)
		}
		if c.PrimaryKey > 0 || appKeyCol {
			return invalidErr("primary key merge policy")
		}
		switch policy {
		case replication.PolicyLww:
			c.Policy = MergePolicyLww
		case replication.PolicyCounter:
			c.Policy = MergePolicyCounter
		case replication.PolicySet:
			c.Policy = MergePolicySet
		case replication.PolicyMin:
			c.Policy = MergePolicyMin
		case replication.PolicyMax:
			c.Policy = MergePolicyMax
		}
	}
	return nil
}

func findTableByName(schema *Schema, name string) *Table {
	for i := range schema.Tables {
		if schema.Tables[i].Name == name {
			return &schema.Tables[i]
		}
	}
	return nil
}

func findColumnByName(t *Table, name string) *Column {
	for i := range t.Columns {
		if t.Columns[i].Name == name {
			return &t.Columns[i]
		}
	}
	return nil
}

// MaterializePolicyState rewrites counter/set projections for one batch,
// persisting causal state alongside the row metadata.
func MaterializePolicyState(o *Opened, changes *[]spool.Mutation, intents []PolicyIntent, version Version) error {
	tables := make(map[string]*Table)
	for i := range o.Schema.Tables {
		t := &o.Schema.Tables[i]
		for _, c := range t.Columns {
			if c.Policy == MergePolicyCounter || c.Policy == MergePolicySet {
				tables[t.ID] = t
				break
			}
		}
	}
	if len(tables) == 0 {
		return nil
	}
	positions := make(map[string]int)
	deleted := make(map[string]bool)
	for i, record := range *changes {
		if _, ok := positions[string(record.Key)]; !ok {
			positions[string(record.Key)] = i
		}
		if record.Deleted {
			deleted[string(record.Key)] = true
		}
	}
	var control []replication.Record
	for _, record := range *changes {
		if record.Deleted {
			continue
		}
		if bytes.HasPrefix(record.Key, []byte("09/bridge/owner/")) || bytes.HasPrefix(record.Key, []byte("09/bridge/shadow/")) {
			control = append(control, replication.PutRecord(record.Key, record.Value))
		}
	}
	type rowRef struct {
		table string
		key   string
	}
	rows := make(map[rowRef]*RowMeta)
	var rowOrder []rowRef
	for _, record := range *changes {
		if record.Deleted || !bytes.HasPrefix(record.Key, []byte("04/meta/row/")) {
			continue
		}
		parts := bytes.Split(record.Key, []byte("/"))
		if len(parts) < 4 {
			return corruptErr("policy row key")
		}
		table := string(parts[3])
		if _, ok := tables[table]; !ok {
			continue
		}
		meta, err := DecodeRowMeta(record.Value)
		if err != nil {
			return err
		}
		ref := rowRef{table: table, key: string(meta.Key)}
		if _, ok := rows[ref]; !ok {
			rowOrder = append(rowOrder, ref)
		}
		metaCopy := meta
		rows[ref] = &metaCopy
	}
	for _, intent := range intents {
		t := findTableByName(&o.Schema, intent.Table)
		if t == nil {
			return ErrSchemaMismatch
		}
		if _, ok := tables[t.ID]; !ok {
			continue
		}
		key := EncodeValues(intent.Key)
		if deleted[string(RowKey(t.ID, key))] {
			continue
		}
		ref := rowRef{table: t.ID, key: string(key)}
		if _, ok := rows[ref]; !ok {
			if meta, err := ReadRowMeta(o.Store, t.ID, key); err != nil {
				return err
			} else if meta != nil {
				rows[ref] = meta
				rowOrder = append(rowOrder, ref)
			}
		}
	}
	for _, ref := range rowOrder {
		meta := rows[ref]
		table, ok := tables[ref.table]
		if !ok {
			return ErrSchemaMismatch
		}
		key := []byte(ref.key)
		for _, column := range table.Columns {
			if column.Policy != MergePolicyCounter && column.Policy != MergePolicySet {
				continue
			}
			cell := findCellByColumn(meta, column.ID)
			if cell == nil {
				return ErrSchemaMismatch
			}
			var operations []*PolicyIntent
			for i := range intents {
				intent := &intents[i]
				if intent.Table == table.Name && intent.Column == column.Name && bytes.Equal(EncodeValues(intent.Key), key) {
					operations = append(operations, intent)
				}
			}
			initial := cell.Value
			if len(operations) > 0 {
				initial = operations[0].Before
			}
			rowKey, err := DecodeValues(key)
			if err != nil {
				return err
			}
			visible := &RowState{Table: ref.table, Key: rowKey, Generation: meta.Generation}
			domain, branch, scope, basis, err := HighLowPolicyScope(o, visible, column.ID, control)
			if err != nil {
				return err
			}
			var causal *replication.Causal
			switch column.Policy {
			case MergePolicyCounter:
				if initial.Tag != ValueInteger {
					return invalidErr("counter requires an integer")
				}
				causal = &replication.Causal{Counter: &replication.Counter{}}
			case MergePolicySet:
				set := &replication.Set{}
				var values []Value
				switch initial.Tag {
				case ValueBlob:
					values, err = DecodeValues(initial.B)
					if err != nil {
						return err
					}
				case ValueNull:
				default:
					return invalidErr("set requires a BLOB")
				}
				seed := encodeOne(func(e *postcard.Encoder) {
					e.String(ref.table)
					e.Raw(key)
					encodeVersion(e, meta.Generation)
					e.Raw(domain[:])
					encodeVersion(e, branch)
					encodeVersion(e, scope)
					encodeVersion(e, basis)
				})
				sum := hash(seed)
				var transaction [16]byte
				copy(transaction[:], sum[:16])
				for index, value := range values {
					set.Add(EncodeValue(value), replication.Tag{Database: domain, Transaction: transaction, Operation: uint32(index)})
				}
				causal = &replication.Causal{Set: set}
			}
			state := &PolicyState{Version: version, Domain: domain, Branch: branch, Scope: scope,
				Baseline: initial, Basis: basis, Table: ref.table, Column: column.ID,
				Key: append([]Value(nil), rowKey...), Generation: meta.Generation, Causal: causal}
			storageKey, err := PolicyStateKey(state)
			if err != nil {
				return err
			}
			if raw, err := o.Store.Get(storageKey); err != nil {
				return err
			} else if raw != nil {
				loaded, err := DecodePolicyState(raw)
				if err != nil {
					return err
				}
				state = loaded
			}
			for _, intent := range operations {
				switch {
				case state.Causal.Counter != nil && intent.Operation.Tag == PolicyOpCounter:
					if err := state.Causal.Counter.Add(o.Identity.Database, o.Identity.Node, intent.Operation.Delta); err != nil {
						return corruptErr("%v", err)
					}
				case state.Causal.Set != nil && intent.Operation.Tag == PolicyOpAdd:
					state.Causal.Set.Add(EncodeValue(intent.Operation.V), replication.Tag{Database: o.Identity.Database, Node: o.Identity.Node, Transaction: intent.ID, Operation: 0})
				case state.Causal.Set != nil && intent.Operation.Tag == PolicyOpRemove:
					state.Causal.Set.Remove(EncodeValue(intent.Operation.V))
				default:
					return corruptErr("causal operation mismatch")
				}
			}
			state.Version = state.Version.Max(version)
			projected, err := PolicyProjection(state)
			if err != nil {
				return err
			}
			cell.Value = projected
			cell.Version = cell.Version.Max(state.Version)
			*changes = append(*changes, spool.Mutation{Key: storageKey, Value: EncodePolicyState(state)})
		}
		record, err := PutRowMeta(ref.table, meta)
		if err != nil {
			return err
		}
		if index, ok := positions[string(record.Key)]; ok {
			(*changes)[index] = record
		} else {
			positions[string(record.Key)] = len(*changes)
			*changes = append(*changes, record)
		}
	}
	return nil
}

func findCellByColumn(meta *RowMeta, column string) *Cell {
	for i := range meta.Cells {
		if meta.Cells[i].Column == column {
			return &meta.Cells[i]
		}
	}
	return nil
}

// PolicyProjection renders the visible value of causal state.
func PolicyProjection(state *PolicyState) (Value, error) {
	switch {
	case state.Causal.Counter != nil:
		if state.Baseline.Tag != ValueInteger {
			return Value{}, corruptErr("counter baseline")
		}
		c := state.Causal.Counter
		// Signed 128-bit accumulation: base + positives - negatives.
		var lo uint64 = uint64(state.Baseline.I)
		var hi uint64
		if state.Baseline.I < 0 {
			hi = ^uint64(0)
		}
		for _, n := range c.Positive {
			var carry uint64
			lo, carry = bits.Add64(lo, n, 0)
			hi += carry
		}
		for _, n := range c.Negative {
			var borrow uint64
			lo, borrow = bits.Sub64(lo, n, 0)
			hi -= borrow
		}
		if (hi == 0 && lo <= uint64(1)<<63-1) || (hi == ^uint64(0) && lo >= uint64(1)<<63) {
			return IntValue(int64(lo)), nil
		}
		return Value{}, limitErr("counter projection overflow")
	case state.Causal.Set != nil:
		blobs := state.Causal.Set.Values()
		values := make([]Value, 0, len(blobs))
		for _, b := range blobs {
			v, err := DecodeValue(b)
			if err != nil {
				return Value{}, err
			}
			values = append(values, v)
		}
		return BlobValue(EncodeValues(values)), nil
	}
	return Value{}, corruptErr("causal projection")
}

// JoinPolicyStates merges incoming policy records into state,
// returning the ordinary (non-policy) records plus joined states.
func JoinPolicyStates(o *Opened, records []replication.Record, state *ReplicaState) ([]replication.Record, error) {
	joined := make(map[string]*PolicyState)
	var order []string
	var ordinary []replication.Record
	for _, record := range records {
		if !bytes.HasPrefix(record.Key, []byte("10/policy/state/")) {
			ordinary = append(ordinary, record)
			continue
		}
		if record.Value == nil {
			return nil, corruptErr("policy record deletion")
		}
		incoming, err := DecodePolicyState(record.Value)
		if err != nil {
			return nil, err
		}
		key, err := PolicyStateKey(incoming)
		if err != nil {
			return nil, err
		}
		if !bytes.Equal(key, record.Key) {
			return nil, corruptErr("policy record identity")
		}
		resolved, ok := joined[string(record.Key)]
		if !ok {
			if raw, err := o.Store.Get(record.Key); err != nil {
				return nil, err
			} else if raw != nil {
				loaded, err := DecodePolicyState(raw)
				if err != nil {
					return nil, err
				}
				resolved = loaded
			} else {
				clone := *incoming
				resolved = &clone
			}
			joined[string(record.Key)] = resolved
			order = append(order, string(record.Key))
		}
		if err := joinPolicyState(resolved, incoming); err != nil {
			return nil, err
		}
	}
	for _, key := range order {
		policy := joined[key]
		delete(joined, key)
		if index := findPolicyRow(state, policy.Table, policy.Key, policy.Generation, true); index >= 0 {
			row := &state.Rows[index]
			domain, branch, scope, _, err := HighLowPolicyScope(o, row, policy.Column, ordinary)
			if err != nil {
				return nil, err
			}
			if policy.Domain != domain || policy.Branch != branch || policy.Scope != scope {
				ordinary = append(ordinary, replication.PutRecord([]byte(key), EncodePolicyState(policy)))
				continue
			}
			column := findSchemaColumn(&state.Schema, row.Table, policy.Column)
			if column == nil {
				return nil, ErrSchemaMismatch
			}
			counterOK := policy.Causal.Counter != nil && column.Policy == MergePolicyCounter
			setOK := policy.Causal.Set != nil && column.Policy == MergePolicySet
			if !counterOK && !setOK {
				return nil, ErrSchemaMismatch
			}
			cell := findStateCell(row, policy.Column)
			if cell == nil {
				return nil, ErrSchemaMismatch
			}
			projected, err := PolicyProjection(policy)
			if err != nil {
				return nil, err
			}
			cell.Value = projected
		} else if rindex := findPolicyRow(state, policy.Table, policy.Key, policy.Generation, false); rindex >= 0 {
			rowKey := EncodeValues(policy.Key)
			fenced := false
			for _, t := range state.Tombstones {
				if t.Table == policy.Table && bytes.Equal(t.Key, rowKey) && t.Generation.Compare(policy.Generation) >= 0 {
					fenced = true
					break
				}
			}
			if !fenced {
				row := state.Rows[rindex]
				domain, branch, scope, basis, err := HighLowPolicyScope(o, &row, policy.Column, ordinary)
				if err != nil {
					return nil, err
				}
				probe := &PolicyState{Domain: domain, Branch: branch, Scope: scope, Basis: basis,
					Table: row.Table, Column: policy.Column, Key: row.Key, Generation: row.Generation,
					Causal: &replication.Causal{Counter: &replication.Counter{}}}
				wkey, err := PolicyStateKey(probe)
				if err != nil {
					return nil, err
				}
				if winner, ok := joined[string(wkey)]; ok {
					if rebased, err := rebaseOrphan(winner, policy); err != nil {
						return nil, err
					} else if rebased {
						ordinary = append(ordinary, replication.PutRecord(wkey, EncodePolicyState(winner)))
					}
				} else if raw, err := o.Store.Get(wkey); err != nil {
					return nil, err
				} else if raw != nil {
					winner, err := DecodePolicyState(raw)
					if err != nil {
						return nil, err
					}
					if rebased, err := rebaseOrphan(winner, policy); err != nil {
						return nil, err
					} else if rebased {
						ordinary = append(ordinary, replication.PutRecord(append([]byte(nil), wkey...), EncodePolicyState(winner)))
						joined[string(wkey)] = winner
					}
				}
			}
		}
		ordinary = append(ordinary, replication.PutRecord([]byte(key), EncodePolicyState(policy)))
	}
	for index := range state.Rows {
		row := state.Rows[index]
		table := findSchemaTable(&state.Schema, row.Table)
		if table == nil {
			return nil, ErrSchemaMismatch
		}
		for _, column := range table.Columns {
			if column.Policy != MergePolicyCounter && column.Policy != MergePolicySet {
				continue
			}
			domain, branch, scope, basis, err := HighLowPolicyScope(o, &row, column.ID, ordinary)
			if err != nil {
				return nil, err
			}
			probe := &PolicyState{Domain: domain, Branch: branch, Scope: scope, Basis: basis,
				Table: row.Table, Column: column.ID, Key: row.Key, Generation: row.Generation,
				Causal: &replication.Causal{Counter: &replication.Counter{}}}
			key, err := PolicyStateKey(probe)
			if err != nil {
				return nil, err
			}
			var raw []byte
			for i := len(ordinary) - 1; i >= 0; i-- {
				if bytes.Equal(ordinary[i].Key, key) {
					raw = ordinary[i].Value
					break
				}
			}
			if raw == nil {
				if stored, err := o.Store.Get(key); err != nil {
					return nil, err
				} else {
					raw = stored
				}
			}
			if raw == nil {
				continue
			}
			causal, err := DecodePolicyState(raw)
			if err != nil {
				return nil, err
			}
			rowKey := EncodeValues(row.Key)
			prefix := []byte("10/policy/state/" + row.Table + "/" + column.ID + "/" + id(rowKey) + "/")
			modified := false
			for _, okey := range o.Store.KeysWithPrefix(prefix) {
				if bytes.Equal(okey, key) {
					continue
				}
				var ob []byte
				for i := len(ordinary) - 1; i >= 0; i-- {
					if bytes.Equal(ordinary[i].Key, okey) {
						ob = ordinary[i].Value
						break
					}
				}
				if ob == nil {
					if stored, err := o.Store.Get(okey); err != nil {
						return nil, err
					} else {
						ob = stored
					}
				}
				if ob == nil {
					continue
				}
				orphan, err := DecodePolicyState(ob)
				if err != nil {
					return nil, err
				}
				if orphan.Generation.Compare(causal.Generation) >= 0 {
					continue
				}
				fenced := false
				for _, t := range state.Tombstones {
					if t.Table == row.Table && bytes.Equal(t.Key, rowKey) && t.Generation.Compare(orphan.Generation) >= 0 {
						fenced = true
						break
					}
				}
				if fenced {
					continue
				}
				if rebased, err := rebaseOrphan(causal, orphan); err != nil {
					return nil, err
				} else if rebased {
					modified = true
				}
			}
			cell := findStateCell(&state.Rows[index], column.ID)
			if cell == nil {
				return nil, ErrSchemaMismatch
			}
			projected, err := PolicyProjection(causal)
			if err != nil {
				return nil, err
			}
			cell.Value = projected
			cell.Version = causal.Version
			if modified {
				ordinary = append(ordinary, replication.PutRecord(key, EncodePolicyState(causal)))
			}
		}
	}
	return ordinary, nil
}

// findPolicyRow locates a row by table/key, requiring equal (exact) or
// strictly greater (rebase) generation.
func findPolicyRow(state *ReplicaState, table string, key []Value, generation Version, exact bool) int {
	for i := range state.Rows {
		row := &state.Rows[i]
		if row.Table != table || !valuesEqual(row.Key, key) {
			continue
		}
		cmp := row.Generation.Compare(generation)
		if exact && cmp == 0 {
			return i
		}
		if !exact && cmp > 0 {
			return i
		}
	}
	return -1
}

func valuesEqual(a, b []Value) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !ValuesEqual(a[i], b[i]) {
			return false
		}
	}
	return true
}

func findSchemaTable(schema *Schema, id string) *Table {
	for i := range schema.Tables {
		if schema.Tables[i].ID == id {
			return &schema.Tables[i]
		}
	}
	return nil
}

func findStateCell(row *RowState, column string) *Cell {
	for i := range row.Cells {
		if row.Cells[i].Column == column {
			return &row.Cells[i]
		}
	}
	return nil
}

func stripSeedTags(set *replication.Set) {
	for value, tags := range set.Additions {
		for tag := range tags {
			if tag.Node == [16]byte{} {
				delete(tags, tag)
			}
		}
		if len(tags) == 0 {
			delete(set.Additions, value)
		}
	}
}

func rebaseOrphan(winner, orphan *PolicyState) (bool, error) {
	if orphan.Domain != winner.Domain || orphan.Branch != winner.Branch || orphan.Basis.Compare(winner.Basis) > 0 {
		return false, nil
	}
	source := *orphan
	if !ValuesEqual(source.Baseline, winner.Baseline) {
		if source.Causal.Set != nil {
			stripSeedTags(source.Causal.Set)
		}
		source.Baseline = winner.Baseline
	}
	if err := joinPolicyState(winner, &source); err != nil {
		return false, err
	}
	return true, nil
}

func joinPolicyState(target, source *PolicyState) error {
	target.Version = target.Version.Max(source.Version)
	if source.Basis.Compare(target.Basis) > 0 {
		if target.Causal.Set != nil {
			stripSeedTags(target.Causal.Set)
		}
		target.Baseline = source.Baseline
		target.Basis = source.Basis
	} else if source.Basis.Compare(target.Basis) < 0 {
		if source.Causal.Set != nil {
			stripSeedTags(source.Causal.Set)
		}
	} else if !ValuesEqual(source.Baseline, target.Baseline) {
		return corruptErr("different causal baselines for one identity")
	}
	if err := target.Causal.Join(source.Causal); err != nil {
		return corruptErr("%v", err)
	}
	return nil
}
