package murmur

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"github.com/marcgauthier/murmur-sql/murmur/internal/postcard"
	"github.com/marcgauthier/murmur-sql/murmur/replication"
	"github.com/marcgauthier/murmur-sql/murmur/spool"
)

// HighLowOwner is one cell or row ownership record.
type HighLowOwner struct {
	Table        string
	Key          []Value
	Column       string
	Epoch        Version
	ReleaseEpoch Version
	Owned        bool
	Cell         *Cell
}

func encodeHighLowOwner(e *postcard.Encoder, o *HighLowOwner) {
	e.String(o.Table)
	e.Len(len(o.Key))
	for _, v := range o.Key {
		encodeValue(e, v)
	}
	e.String(o.Column)
	encodeVersion(e, o.Epoch)
	encodeVersion(e, o.ReleaseEpoch)
	e.Bool(o.Owned)
	if o.Cell == nil {
		e.None()
	} else {
		e.Some()
		encodeCell(e, *o.Cell)
	}
}

func decodeHighLowOwner(d *postcard.Decoder) HighLowOwner {
	var o HighLowOwner
	o.Table = d.String()
	n := d.Len()
	for i := 0; i < n; i++ {
		o.Key = append(o.Key, decodeValue(d))
	}
	o.Column = d.String()
	o.Epoch = decodeVersion(d)
	o.ReleaseEpoch = decodeVersion(d)
	o.Owned = d.Bool()
	if d.Present() {
		cell := decodeCell(d)
		o.Cell = &cell
	}
	return o
}

// EncodeHighLowOwner renders an ownership record.
func EncodeHighLowOwner(o *HighLowOwner) []byte {
	return encodeOne(func(e *postcard.Encoder) { encodeHighLowOwner(e, o) })
}

// DecodeHighLowOwner parses an ownership record.
func DecodeHighLowOwner(b []byte) (HighLowOwner, error) {
	return decodeOne(b, decodeHighLowOwner)
}

type highLowShadow struct {
	Database   [16]byte
	Row        *RowState
	Tombstones []Tombstone
	Table      string
	Key        []Value
}

func encodeHighLowShadow(e *postcard.Encoder, s *highLowShadow) {
	e.Raw(s.Database[:])
	if s.Row == nil {
		e.None()
	} else {
		e.Some()
		encodeRowState(e, *s.Row)
	}
	e.Len(len(s.Tombstones))
	for _, t := range s.Tombstones {
		encodeTombstone(e, t)
	}
	e.String(s.Table)
	e.Len(len(s.Key))
	for _, v := range s.Key {
		encodeValue(e, v)
	}
}

func decodeHighLowShadow(d *postcard.Decoder) highLowShadow {
	var s highLowShadow
	copy(s.Database[:], d.Raw(16))
	if d.Present() {
		row := decodeRowState(d)
		s.Row = &row
	}
	n := d.Len()
	for i := 0; i < n; i++ {
		s.Tombstones = append(s.Tombstones, decodeTombstone(d))
	}
	s.Table = d.String()
	n = d.Len()
	for i := 0; i < n; i++ {
		s.Key = append(s.Key, decodeValue(d))
	}
	return s
}

// HighLowOwnerKey renders the ownership record key.
func HighLowOwnerKey(table string, key []Value, column string) []byte {
	return []byte("09/bridge/owner/" + table + "/" + id(EncodeValues(key)) + "/" + column)
}

func highLowShadowKey(shadow *highLowShadow) []byte {
	return []byte("09/bridge/shadow/" + shadow.Table + "/" + id(EncodeValues(shadow.Key)) + "/" + id(shadow.Database[:]))
}

// HighLowCurrentOwner reads the current ownership record, if any.
func HighLowCurrentOwner(o *Opened, table string, key []Value, column string) (*HighLowOwner, error) {
	raw, err := o.Store.Get(HighLowOwnerKey(table, key, column))
	if err != nil {
		return nil, err
	}
	if raw == nil {
		return nil, nil
	}
	owner, err := DecodeHighLowOwner(raw)
	if err != nil {
		return nil, err
	}
	return &owner, nil
}

// HighLowPromote claims High ownership of freshly changed cells on a
// High receiver.
func HighLowPromote(o *Opened, records *[]spool.Mutation, version Version) error {
	if o.ReplicationConfig == nil || o.ReplicationConfig.Bridge == nil ||
		o.ReplicationConfig.Bridge.Role != replication.BridgeHighReceiver {
		return nil
	}
	rows, err := ChangedState(*records, o.Schema, o.Identity.Database, version)
	if err != nil {
		return err
	}
	for _, row := range rows.Rows {
		table := findSchemaTable(&o.Schema, row.Table)
		if table == nil {
			return ErrSchemaMismatch
		}
		for _, cell := range row.Cells {
			if cell.Version != version {
				continue
			}
			pk := false
			for _, c := range table.Columns {
				if c.ID == cell.Column && c.PrimaryKey > 0 {
					pk = true
				}
			}
			if pk {
				continue
			}
			previous, err := HighLowCurrentOwner(o, row.Table, row.Key, cell.Column)
			if err != nil {
				return err
			}
			epoch := version
			if previous != nil && previous.Owned {
				epoch = previous.Epoch
			}
			var release Version
			if previous != nil {
				release = previous.ReleaseEpoch
			}
			cellCopy := cell
			owner := &HighLowOwner{Table: row.Table, Key: row.Key, Column: cell.Column,
				Epoch: epoch, ReleaseEpoch: release, Owned: true, Cell: &cellCopy}
			*records = append(*records, spool.Mutation{Key: HighLowOwnerKey(owner.Table, owner.Key, owner.Column), Value: EncodeHighLowOwner(owner)})
		}
	}
	for _, tomb := range rows.Tombstones {
		key, err := DecodeValues(tomb.Key)
		if err != nil {
			return err
		}
		owner := &HighLowOwner{Table: tomb.Table, Key: key, Column: "__row__", Epoch: version, Owned: true}
		*records = append(*records, spool.Mutation{Key: HighLowOwnerKey(owner.Table, owner.Key, owner.Column), Value: EncodeHighLowOwner(owner)})
	}
	return nil
}

func joinHighLowShadow(a, b *highLowShadow, schema *Schema) (*highLowShadow, error) {
	if a.Database != b.Database || a.Table != b.Table || !valuesEqual(a.Key, b.Key) {
		return nil, corruptErr("shadow identity")
	}
	stateOf := func(s *highLowShadow) *ReplicaState {
		st := &ReplicaState{DatabaseID: s.Database, Schema: *schema, Tombstones: s.Tombstones}
		if s.Row != nil {
			st.Rows = []RowState{*s.Row}
		}
		return st
	}
	joined, err := Join(stateOf(a), stateOf(b))
	if err != nil {
		return nil, err
	}
	out := &highLowShadow{Database: a.Database, Tombstones: joined.Tombstones, Table: a.Table, Key: a.Key}
	if len(joined.Rows) > 0 {
		row := joined.Rows[0]
		out.Row = &row
	}
	return out, nil
}

// HighLowJoin merges bridge control epochs before projecting cells.
func HighLowJoin(o *Opened, records []replication.Record, state *ReplicaState) ([]replication.Record, error) {
	changes := make(map[string][]byte)
	var order []string
	var other []replication.Record
	for _, record := range records {
		if !bytes.HasPrefix(record.Key, []byte("09/bridge/owner/")) && !bytes.HasPrefix(record.Key, []byte("09/bridge/shadow/")) {
			other = append(other, record)
			continue
		}
		if record.Value == nil {
			return nil, corruptErr("bridge control deletion")
		}
		old, ok := changes[string(record.Key)]
		if !ok {
			stored, err := o.Store.Get(record.Key)
			if err != nil {
				return nil, err
			}
			old = stored
		}
		var resolved []byte
		if bytes.HasPrefix(record.Key, []byte("09/bridge/owner/")) {
			incoming, err := DecodeHighLowOwner(record.Value)
			if err != nil {
				return nil, err
			}
			if !bytes.Equal(HighLowOwnerKey(incoming.Table, incoming.Key, incoming.Column), record.Key) {
				return nil, corruptErr("ownership identity")
			}
			if old != nil {
				previous, err := DecodeHighLowOwner(old)
				if err != nil {
					return nil, err
				}
				if ownerPrecedes(&previous, &incoming) {
					resolved = old
				} else {
					resolved = record.Value
				}
			} else {
				resolved = record.Value
			}
		} else {
			incoming, err := decodeOne(record.Value, decodeHighLowShadow)
			if err != nil {
				return nil, err
			}
			if !bytes.Equal(highLowShadowKey(&incoming), record.Key) {
				return nil, corruptErr("shadow record identity")
			}
			if old != nil {
				previous, err := decodeOne(old, decodeHighLowShadow)
				if err != nil {
					return nil, err
				}
				merged, err := joinHighLowShadow(&previous, &incoming, &state.Schema)
				if err != nil {
					return nil, err
				}
				resolved = encodeOne(func(e *postcard.Encoder) { encodeHighLowShadow(e, merged) })
			} else {
				resolved = record.Value
			}
		}
		if _, ok := changes[string(record.Key)]; !ok {
			order = append(order, string(record.Key))
		}
		changes[string(record.Key)] = resolved
	}
	type affectedRef struct {
		table string
		key   string
	}
	affected := make(map[affectedRef]bool)
	var affectedOrder []affectedRef
	addAffected := func(table string, key []byte) {
		ref := affectedRef{table: table, key: string(key)}
		if !affected[ref] {
			affected[ref] = true
			affectedOrder = append(affectedOrder, ref)
		}
	}
	for _, row := range state.Rows {
		addAffected(row.Table, EncodeValues(row.Key))
	}
	var changeRecords []replication.Record
	keys := make([]string, 0, len(changes))
	for k := range changes {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		changeRecords = append(changeRecords, replication.PutRecord([]byte(k), changes[k]))
	}
	extra, err := HighLowAffectedRecords(changeRecords)
	if err != nil {
		return nil, err
	}
	for _, ref := range extra {
		addAffected(ref.Table, ref.Key)
	}
	owners := make(map[string]*HighLowOwner)
	shadows := make(map[string]*highLowShadow)
	for _, ref := range affectedOrder {
		values, err := DecodeValues([]byte(ref.key))
		if err != nil {
			return nil, err
		}
		table := findSchemaTable(&state.Schema, ref.table)
		if table == nil {
			return nil, ErrSchemaMismatch
		}
		columns := []string{"__row__"}
		for _, c := range table.Columns {
			columns = append(columns, c.ID)
		}
		for _, column := range columns {
			key := HighLowOwnerKey(ref.table, values, column)
			raw, ok := changes[string(key)]
			if !ok {
				stored, err := o.Store.Get(key)
				if err != nil {
					return nil, err
				}
				raw = stored
			}
			if raw != nil {
				owner, err := DecodeHighLowOwner(raw)
				if err != nil {
					return nil, err
				}
				ownerCopy := owner
				owners[string(key)] = &ownerCopy
			}
		}
		for _, key := range o.Store.KeysWithPrefix([]byte(highLowShadowPrefix(ref.table, []byte(ref.key)))) {
			raw, err := o.Store.Get(key)
			if err != nil {
				return nil, err
			}
			if raw != nil {
				shadow, err := decodeOne(raw, decodeHighLowShadow)
				if err != nil {
					return nil, err
				}
				shadowCopy := shadow
				shadows[string(key)] = &shadowCopy
			}
		}
	}
	for _, key := range keys {
		if strings.HasPrefix(key, "09/bridge/shadow/") {
			shadow, err := decodeOne(changes[key], decodeHighLowShadow)
			if err != nil {
				return nil, err
			}
			shadowCopy := shadow
			shadows[key] = &shadowCopy
		}
	}
	shadowKeys := make([]string, 0, len(shadows))
	for k := range shadows {
		shadowKeys = append(shadowKeys, k)
	}
	sort.Strings(shadowKeys)
	for _, sk := range shadowKeys {
		shadow := shadows[sk]
		rowOwner := owners[string(HighLowOwnerKey(shadow.Table, shadow.Key, "__row__"))]
		if rowOwner != nil && rowOwner.Owned {
			kept := state.Rows[:0]
			for _, r := range state.Rows {
				if r.Table != shadow.Table || !valuesEqual(r.Key, shadow.Key) {
					kept = append(kept, r)
				}
			}
			state.Rows = kept
			continue
		}
		if low := shadow.Row; low != nil {
			index := -1
			for i := range state.Rows {
				if state.Rows[i].Table == low.Table && valuesEqual(state.Rows[i].Key, low.Key) {
					index = i
					break
				}
			}
			if index < 0 {
				row := *low
				if rowOwner != nil && !rowOwner.Owned {
					row.Generation = row.Generation.Max(rowOwner.Epoch)
				}
				state.Rows = append(state.Rows, row)
				index = len(state.Rows) - 1
			}
			row := &state.Rows[index]
			owned := false
			for _, owner := range owners {
				if owner.Table == row.Table && valuesEqual(owner.Key, row.Key) && owner.Owned {
					owned = true
					break
				}
			}
			if !owned {
				var epoch Version
				if rowOwner != nil {
					epoch = rowOwner.Epoch
				}
				row.Generation = low.Generation.Max(epoch)
			}
			for _, cell := range low.Cells {
				owner := owners[string(HighLowOwnerKey(row.Table, row.Key, cell.Column))]
				switch {
				case owner != nil && owner.Owned && owner.Cell != nil:
					for i := range row.Cells {
						if row.Cells[i].Column == cell.Column {
							row.Cells[i] = *owner.Cell
						}
					}
				case owner == nil || !owner.Owned:
					for i := range row.Cells {
						if row.Cells[i].Column == cell.Column {
							row.Cells[i] = cell
						}
					}
				}
			}
		} else {
			owned := false
			for _, owner := range owners {
				if owner.Table == shadow.Table && valuesEqual(owner.Key, shadow.Key) && owner.Owned {
					owned = true
					break
				}
			}
			if !owned {
				kept := state.Rows[:0]
				for _, r := range state.Rows {
					if r.Table != shadow.Table || !valuesEqual(r.Key, shadow.Key) {
						kept = append(kept, r)
					}
				}
				state.Rows = kept
			}
		}
	}
	for _, key := range keys {
		other = append(other, replication.PutRecord([]byte(key), changes[key]))
	}
	return other, nil
}

// ownerPrecedes reports whether previous wins over incoming by
// (epoch, cell version) ordering.
func ownerPrecedes(previous, incoming *HighLowOwner) bool {
	if previous.Epoch != incoming.Epoch {
		return previous.Epoch.Compare(incoming.Epoch) > 0
	}
	var pv, iv Version
	var pok, iok bool
	if previous.Cell != nil {
		pv, pok = previous.Cell.Version, true
	}
	if incoming.Cell != nil {
		iv, iok = incoming.Cell.Version, true
	}
	switch {
	case pok && iok:
		return pv.Compare(iv) > 0
	case pok:
		return true
	default:
		return false
	}
}

// HighLowAffectedRef names one affected row.
type HighLowAffectedRef struct {
	Table string
	Key   []byte
}

// HighLowAffectedRecords collects affected rows from control records.
func HighLowAffectedRecords(records []replication.Record) ([]HighLowAffectedRef, error) {
	var out []HighLowAffectedRef
	seen := make(map[string]bool)
	for _, record := range records {
		if record.Value == nil {
			continue
		}
		var table string
		var key []byte
		switch {
		case bytes.HasPrefix(record.Key, []byte("09/bridge/owner/")):
			owner, err := DecodeHighLowOwner(record.Value)
			if err != nil {
				return nil, err
			}
			table, key = owner.Table, EncodeValues(owner.Key)
		case bytes.HasPrefix(record.Key, []byte("09/bridge/shadow/")):
			shadow, err := decodeOne(record.Value, decodeHighLowShadow)
			if err != nil {
				return nil, err
			}
			table, key = shadow.Table, EncodeValues(shadow.Key)
		case bytes.HasPrefix(record.Key, []byte("10/policy/state/")):
			policy, err := DecodePolicyState(record.Value)
			if err != nil {
				return nil, err
			}
			table, key = policy.Table, EncodeValues(policy.Key)
		default:
			continue
		}
		k := table + "\x00" + string(key)
		if !seen[k] {
			seen[k] = true
			out = append(out, HighLowAffectedRef{Table: table, Key: key})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Table != out[j].Table {
			return out[i].Table < out[j].Table
		}
		return bytes.Compare(out[i].Key, out[j].Key) < 0
	})
	return out, nil
}

// HighLowPolicyScope resolves the causal branch for one cell.
func HighLowPolicyScope(o *Opened, row *RowState, column string, records []replication.Record) ([16]byte, Version, Version, Version, error) {
	var zero [16]byte
	key := HighLowOwnerKey(row.Table, row.Key, column)
	var raw []byte
	for i := len(records) - 1; i >= 0; i-- {
		if bytes.Equal(records[i].Key, key) {
			raw = records[i].Value
			break
		}
	}
	var owner *HighLowOwner
	if raw != nil {
		decoded, err := DecodeHighLowOwner(raw)
		if err != nil {
			return zero, Version{}, Version{}, Version{}, err
		}
		owner = &decoded
	} else {
		current, err := HighLowCurrentOwner(o, row.Table, row.Key, column)
		if err != nil {
			return zero, Version{}, Version{}, Version{}, err
		}
		owner = current
	}
	if owner != nil && owner.Owned {
		return o.Identity.Database, owner.ReleaseEpoch, row.Generation, owner.Epoch, nil
	}
	prefix := []byte(highLowShadowPrefix(row.Table, EncodeValues(row.Key)))
	shadows := make(map[string][]byte)
	for _, k := range o.Store.KeysWithPrefix(prefix) {
		if raw, err := o.Store.Get(k); err != nil {
			return zero, Version{}, Version{}, Version{}, err
		} else if raw != nil {
			shadows[string(k)] = raw
		}
	}
	for _, r := range records {
		if bytes.HasPrefix(r.Key, prefix) && r.Value != nil {
			shadows[string(r.Key)] = r.Value
		}
	}
	var latest *versionCandidate
	for _, raw := range shadows {
		shadow, err := decodeOne(raw, decodeHighLowShadow)
		if err != nil {
			return zero, Version{}, Version{}, Version{}, err
		}
		if shadow.Table != row.Table || !valuesEqual(shadow.Key, row.Key) {
			continue
		}
		if shadow.Row == nil {
			continue
		}
		for _, cell := range shadow.Row.Cells {
			if cell.Column != column {
				continue
			}
			c := &versionCandidate{version: cell.Version, database: shadow.Database, generation: shadow.Row.Generation}
			if latest == nil || compareCandidate(c, latest) > 0 {
				latest = c
			}
		}
	}
	if latest != nil {
		return latest.database, Version{}, latest.generation, latest.version, nil
	}
	var release Version
	if owner != nil {
		release = owner.ReleaseEpoch
	}
	return o.Identity.Database, release, row.Generation, row.Generation, nil
}

type versionCandidate struct {
	version    Version
	database   [16]byte
	generation Version
}

func compareCandidate(a, b *versionCandidate) int {
	if c := a.version.Compare(b.version); c != 0 {
		return c
	}
	if c := bytes.Compare(a.database[:], b.database[:]); c != 0 {
		return c
	}
	return a.generation.Compare(b.generation)
}

func highLowShadowPrefix(table string, key []byte) string {
	return "09/bridge/shadow/" + table + "/" + id(key) + "/"
}

// HighLowImport ingests one sealed bridge bundle through the adapter.
// checkBridgeIdentityCollision fails closed when an imported row identity
// collides: the same (table, key) was already imported from a different
// source domain, or a live High row exists with no import provenance at
// all. This mirrors the original bridge importer's prepareBundle gate for
// row records; the stream-mismatch and High-owner refinements are not
// ported (shadows carry no stream, and High-owned cells keep projecting
// through ownership). Nothing is committed before this check, so a
// rejection leaves first-writer state intact.
func checkBridgeIdentityCollision(o *Opened, baseline *ReplicaState, records []replication.Record, source [16]byte) error {
	for _, r := range records {
		if !bytes.HasPrefix(r.Key, []byte("09/bridge/shadow/")) || r.Value == nil {
			continue
		}
		shadow, err := decodeOne(r.Value, decodeHighLowShadow)
		if err != nil {
			return err
		}
		prefix := []byte(highLowShadowPrefix(shadow.Table, EncodeValues(shadow.Key)))
		provenance := false
		for _, key := range o.Store.KeysWithPrefix(prefix) {
			raw, err := o.Store.Get(key)
			if err != nil {
				return err
			}
			if raw == nil {
				continue
			}
			stored, err := decodeOne(raw, decodeHighLowShadow)
			if err != nil {
				return err
			}
			if stored.Table != shadow.Table || !valuesEqual(stored.Key, shadow.Key) {
				continue
			}
			provenance = true
			if stored.Database != source {
				return &replication.Error{Kind: replication.KindCollision,
					Msg: fmt.Sprintf("imported row identity collides: table %s already imported from another domain", shadow.Table)}
			}
		}
		if !provenance {
			for _, row := range baseline.Rows {
				if row.Table == shadow.Table && valuesEqual(row.Key, shadow.Key) {
					return &replication.Error{Kind: replication.KindCollision,
						Msg: fmt.Sprintf("imported row identity collides: table %s row exists with no matching Low provenance", shadow.Table)}
				}
			}
		}
	}
	return nil
}

func HighLowImport(adapter *Adapter, header replication.Header, payload []byte) (bool, error) {
	content, err := DecodeBridgeContent(payload)
	if err != nil {
		return false, storageNetErr(err)
	}
	var low *ReplicaState
	if content.SQLite != nil {
		st, err := DecodeReplicaState(content.SQLite)
		if err != nil {
			return false, storageNetErr(err)
		}
		low = &st
	}
	if low != nil {
		if err := adapter.Observe(low.Clock.Millis, low.Clock.Logical); err != nil {
			return false, err
		}
	}
	database, err := adapter.Database()
	if err != nil {
		return false, err
	}
	cursor, err := database.Call(func(c *Core) (any, error) {
		c.status.Mu.Lock()
		defer c.status.Mu.Unlock()
		st := c.status.Status
		if st.Committed == c.capturedCursor && st.Durable == c.capturedCursor && st.StorageError == nil {
			v := st.Committed
			return &v, nil
		}
		return nil, nil
	})
	if err != nil {
		return false, storageNetErr(err)
	}
	cursorPtr, _ := cursor.(*uint64)
	if cursorPtr == nil {
		return false, nil
	}
	prepared, err := adapter.Spool(func(o *Opened) (any, error) {
		if o.ReplicationConfig == nil || o.ReplicationConfig.Bridge == nil {
			return nil, invalidErr("untrusted bridge domain")
		}
		bridge := o.ReplicationConfig.Bridge
		if bridge.Role != replication.BridgeHighReceiver || !bridge.TrustedDomains[header.SourceDatabase] {
			return nil, invalidErr("untrusted bridge domain")
		}
		origin := id(header.Bundle[:])
		if header.Origin != nil {
			origin = fmt.Sprintf("%s-%020d", id(header.Origin.Node[:]), header.Origin.Sequence)
		}
		receipt := []byte("09/bridge/receipt/" + id(header.SourceDatabase[:]) + "/" + origin)
		if previous, err := o.Store.Get(receipt); err != nil {
			return nil, err
		} else if previous != nil {
			if !bytes.Equal(previous, header.Digest[:]) {
				return nil, corruptErr("conflicting bridge receipt")
			}
			return nil, nil
		}
		if o.Cursor != *cursorPtr {
			return nil, ErrBusy
		}
		state := &ReplicaState{DatabaseID: o.Identity.Database, Schema: o.Schema, Clock: o.Clock}
		records := content.Records
		if low != nil {
			if low.DatabaseID != header.SourceDatabase {
				return nil, ErrSchemaMismatch
			}
			allowed := make(map[string]bool)
			for _, source := range low.Schema.Tables {
				target := findTableByName(&state.Schema, source.Name)
				if target == nil || ReplicationEnabled(&state.Schema, target.ID) {
					allowed[source.ID] = true
				}
			}
			kept := low.Rows[:0]
			for _, row := range low.Rows {
				if allowed[row.Table] {
					kept = append(kept, row)
				}
			}
			low.Rows = kept
			keptTombs := low.Tombstones[:0]
			for _, tomb := range low.Tombstones {
				if allowed[tomb.Table] {
					keptTombs = append(keptTombs, tomb)
				}
			}
			low.Tombstones = keptTombs
			for i := range low.Rows {
				row := &low.Rows[i]
				lowTable := findSchemaTable(&low.Schema, row.Table)
				if lowTable == nil {
					return nil, ErrSchemaMismatch
				}
				highTable := findTableByName(&state.Schema, lowTable.Name)
				if highTable == nil {
					return nil, ErrSchemaMismatch
				}
				if !samePkShape(lowTable, highTable) {
					return nil, ErrSchemaMismatch
				}
				for j := range row.Cells {
					cell := &row.Cells[j]
					lowColumn := findColumnByID(lowTable, cell.Column)
					if lowColumn == nil {
						return nil, ErrSchemaMismatch
					}
					high := findHighColumn(highTable, lowColumn)
					if high == nil {
						return nil, ErrSchemaMismatch
					}
					cell.Column = high.ID
				}
				row.Table = highTable.ID
				for _, column := range highTable.Columns {
					found := false
					for _, c := range row.Cells {
						if c.Column == column.ID {
							found = true
							break
						}
					}
					if !found {
						if column.NotNull || column.PrimaryKey > 0 {
							return nil, ErrSchemaMismatch
						}
						row.Cells = append(row.Cells, Cell{Column: column.ID, Value: NullValue()})
					}
				}
				shadow := &highLowShadow{Database: header.SourceDatabase, Row: &low.Rows[i], Table: row.Table, Key: row.Key}
				records = append(records, replication.PutRecord(highLowShadowKey(shadow),
					encodeOne(func(e *postcard.Encoder) { encodeHighLowShadow(e, shadow) })))
			}
			for _, tomb := range low.Tombstones {
				lowTable := findSchemaTable(&low.Schema, tomb.Table)
				if lowTable == nil {
					return nil, ErrSchemaMismatch
				}
				high := findTableByName(&state.Schema, lowTable.Name)
				if high == nil {
					return nil, ErrSchemaMismatch
				}
				t := tomb
				t.Table = high.ID
				key, err := DecodeValues(t.Key)
				if err != nil {
					return nil, err
				}
				shadow := &highLowShadow{Database: header.SourceDatabase, Tombstones: []Tombstone{t}, Table: high.ID, Key: key}
				records = append(records, replication.PutRecord(highLowShadowKey(shadow),
					encodeOne(func(e *postcard.Encoder) { encodeHighLowShadow(e, shadow) })))
			}
			state.Clock = state.Clock.Max(low.Clock)
		}
		records, err = FilterRecords(records, &o.Schema)
		if err != nil {
			return nil, err
		}
		affected, err := HighLowAffectedRecords(records)
		if err != nil {
			return nil, err
		}
		keys := make(map[RowID]bool)
		for _, ref := range affected {
			keys[RowID{Table: ref.Table, Key: string(ref.Key)}] = true
		}
		baseline, err := AffectedState(o.Store, o.Schema, o.Identity.Database, o.Clock, keys)
		if err != nil {
			return nil, err
		}
		if err := checkBridgeIdentityCollision(o, baseline, records, header.SourceDatabase); err != nil {
			return nil, err
		}
		cloned := CloneReplicaState(baseline)
		state.Rows = cloned.Rows
		state.Tombstones = cloned.Tombstones
		records, err = HighLowJoin(o, records, state)
		if err != nil {
			return nil, err
		}
		for i := range records {
			if bytes.HasPrefix(records[i].Key, []byte("10/policy/state/")) {
				if records[i].Value == nil {
					return nil, ErrSchemaMismatch
				}
				policy, err := DecodePolicyState(records[i].Value)
				if err != nil {
					return nil, err
				}
				for _, row := range state.Rows {
					if row.Table == policy.Table && valuesEqual(row.Key, policy.Key) {
						policy.Generation = row.Generation
						key, err := PolicyStateKey(policy)
						if err != nil {
							return nil, err
						}
						records[i].Key = key
						records[i].Value = EncodePolicyState(policy)
						break
					}
				}
			}
		}
		records, err = JoinPolicyStates(o, records, state)
		if err != nil {
			return nil, err
		}
		records = append(records, replication.PutRecord(receipt, append([]byte(nil), header.Digest[:]...)))
		records = append(records, replication.PutRecord(
			[]byte("09/bridge/provenance/bundle/"+id(header.Bundle[:])), replication.EncodeHeader(&header)))
		records = append(records, replication.PutRecord(
			[]byte("09/bridge/applied/"+replication.Hex(header.SourceDatabase[:])+"/"+header.Stream),
			encodeOne(func(e *postcard.Encoder) { e.U64(header.Sequence) })))
		PreserveLocal(state, baseline)
		patch, err := PrepareDelta(baseline, state, *cursorPtr)
		if err != nil {
			return nil, err
		}
		return &highLowPrepared{patch: patch, records: records}, nil
	})
	if err != nil {
		return false, err
	}
	if prepared == nil {
		return true, nil
	}
	hp := prepared.(*highLowPrepared)
	return highLowPublish(adapter, hp.patch, hp.records)
}

type highLowPrepared struct {
	patch   *Patch
	records []replication.Record
}

func highLowPublish(adapter *Adapter, patch *Patch, records []replication.Record) (bool, error) {
	database, err := adapter.Database()
	if err != nil {
		return false, err
	}
	published, err := database.Call(func(c *Core) (any, error) {
		return c.PublishNetwork(patch, records, true)
	})
	if err != nil {
		return false, storageNetErr(err)
	}
	if !published.(bool) {
		return false, nil
	}
	if _, err := adapter.Spool(func(o *Opened) (any, error) {
		return nil, o.Store.Flush()
	}); err != nil {
		return false, err
	}
	if status := database.Status(); status.StorageError != nil {
		return false, storageNetErr2(*status.StorageError)
	}
	return true, nil
}

// HighLowRelease drops High ownership of one field or row.
func HighLowRelease(adapter *Adapter, table string, key []byte, column *string) (bool, error) {
	values, err := DecodeValues(key)
	if err != nil {
		return false, storageNetErr(err)
	}
	database, err := adapter.Database()
	if err != nil {
		return false, err
	}
	name := table
	resolved, err := database.Call(func(c *Core) (any, error) {
		t := findTableByNameFold(&c.schema, name)
		if t == nil {
			return nil, ErrSchemaMismatch
		}
		resolved, err := ResolveKey(c.connection, t, values)
		if err != nil {
			return nil, err
		}
		c.status.Mu.Lock()
		defer c.status.Mu.Unlock()
		st := c.status.Status
		var cursor *uint64
		if st.Committed == c.capturedCursor && st.Durable == c.capturedCursor {
			v := st.Committed
			cursor = &v
		}
		return &highLowResolved{cursor: cursor, values: resolved}, nil
	})
	if err != nil {
		return false, storageNetErr(err)
	}
	rr := resolved.(*highLowResolved)
	if rr.cursor == nil {
		return false, nil
	}
	outcome, err := adapter.Spool(func(o *Opened) (any, error) {
		t := findTableByNameFold(&o.Schema, table)
		if t == nil {
			return nil, ErrSchemaMismatch
		}
		columnID := "__row__"
		if column != nil {
			c := findColumnByNameFold(t, *column)
			if c == nil {
				return nil, ErrSchemaMismatch
			}
			columnID = c.ID
		}
		version, err := o.Clock.Next(o.Identity.Node)
		if err != nil {
			return nil, err
		}
		owner, err := HighLowCurrentOwner(o, t.ID, rr.values, columnID)
		if err != nil {
			return nil, err
		}
		if owner == nil || !owner.Owned {
			return nil, nil
		}
		owner.Owned = false
		owner.Epoch = version
		owner.ReleaseEpoch = version
		record := replication.PutRecord(HighLowOwnerKey(owner.Table, owner.Key, owner.Column), EncodeHighLowOwner(owner))
		affected, err := HighLowAffectedRecords([]replication.Record{record})
		if err != nil {
			return nil, err
		}
		keys := make(map[RowID]bool)
		for _, ref := range affected {
			keys[RowID{Table: ref.Table, Key: string(ref.Key)}] = true
		}
		baseline, err := AffectedState(o.Store, o.Schema, o.Identity.Database, o.Clock, keys)
		if err != nil {
			return nil, err
		}
		state := CloneReplicaState(baseline)
		records, err := HighLowJoin(o, []replication.Record{record}, state)
		if err != nil {
			return nil, err
		}
		records, err = JoinPolicyStates(o, records, state)
		if err != nil {
			return nil, err
		}
		state.Clock = version
		patch, err := PrepareDelta(baseline, state, *rr.cursor)
		if err != nil {
			return nil, err
		}
		return &highLowPrepared{patch: patch, records: records}, nil
	})
	if err != nil {
		return false, err
	}
	if outcome == nil {
		return false, &replication.Error{Kind: replication.KindInvalid, Msg: "field is not High-owned"}
	}
	hp := outcome.(*highLowPrepared)
	return highLowPublish(adapter, hp.patch, hp.records)
}

type highLowResolved struct {
	cursor *uint64
	values []Value
}

func samePkShape(a, b *Table) bool {
	pa, pb := PkColumns(a), PkColumns(b)
	if len(pa) != len(pb) {
		return false
	}
	for i := range pa {
		if pa[i].Name != pb[i].Name || pa[i].DeclaredType != pb[i].DeclaredType || pa[i].PrimaryKey != pb[i].PrimaryKey {
			return false
		}
	}
	return true
}

func findColumnByID(t *Table, id string) *Column {
	for i := range t.Columns {
		if t.Columns[i].ID == id {
			return &t.Columns[i]
		}
	}
	return nil
}

func findHighColumn(high *Table, low *Column) *Column {
	for i := range high.Columns {
		c := &high.Columns[i]
		if c.Name == low.Name && c.Policy == low.Policy && asciiEqualFold(c.DeclaredType, low.DeclaredType) {
			return c
		}
	}
	return nil
}

func findTableByNameFold(schema *Schema, name string) *Table {
	for i := range schema.Tables {
		if asciiEqualFold(schema.Tables[i].Name, name) {
			return &schema.Tables[i]
		}
	}
	return nil
}

func findColumnByNameFold(t *Table, name string) *Column {
	for i := range t.Columns {
		if asciiEqualFold(t.Columns[i].Name, name) {
			return &t.Columns[i]
		}
	}
	return nil
}
