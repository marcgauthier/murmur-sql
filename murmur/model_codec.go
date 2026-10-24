package murmur

import (
	"bytes"
	"encoding/json"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/internal/postcard"
)

// Postcard codecs for the model types, byte-compatible with the Rust
// serde derives (field order = declaration order, enums as variant
// indexes). Schema uses the StoredSchema layout with FTS/vector
// extension strings, exactly like schema_codec.rs.

// --- durations ---

// Duration encodes like serde: seconds and nanoseconds. Negative
// values clamp to zero (durations here are always elapsed times).
func encodeDuration(e *postcard.Encoder, d time.Duration) {
	if d < 0 {
		d = 0
	}
	e.U64(uint64(d / time.Second))
	e.U32(uint32(d % time.Second))
}

func decodeDuration(d *postcard.Decoder) time.Duration {
	return time.Duration(d.U64())*time.Second + time.Duration(d.U32())
}

// --- version ---

func encodeVersion(e *postcard.Encoder, v Version) {
	e.U64(v.Millis)
	e.U32(v.Logical)
	e.Raw(v.Node[:])
}

func decodeVersion(d *postcard.Decoder) Version {
	millis := d.U64()
	logical := d.U32()
	var node [16]byte
	copy(node[:], d.Raw(16))
	return Version{Millis: millis, Logical: logical, Node: node}
}

// --- value ---

func encodeValue(e *postcard.Encoder, v Value) {
	e.Discriminant(uint32(v.Tag))
	switch v.Tag {
	case ValueInteger:
		e.I64(v.I)
	case ValueReal:
		e.F64(v.F)
	case ValueText:
		e.String(v.S)
	case ValueBlob:
		e.ByteString(v.B)
	}
}

func decodeValue(d *postcard.Decoder) Value {
	switch d.Discriminant() {
	case 0:
		return NullValue()
	case 1:
		return IntValue(d.I64())
	case 2:
		return RealValue(d.F64())
	case 3:
		return TextValue(d.String())
	case 4:
		return BlobValue(append([]byte(nil), d.Bytes()...))
	default:
		d.Fail(corruptErr("bad value discriminant"))
		return Value{}
	}
}

// --- merge policy ---

func encodeMergePolicy(e *postcard.Encoder, p MergePolicy) {
	e.Discriminant(uint32(p))
}

func decodeMergePolicy(d *postcard.Decoder) MergePolicy {
	v := d.Discriminant()
	if v > 6 {
		d.Fail(corruptErr("bad merge policy"))
	}
	return MergePolicy(v)
}

// --- policy operation / intent ---

func encodePolicyOperation(e *postcard.Encoder, op PolicyOperation) {
	e.Discriminant(uint32(op.Tag))
	switch op.Tag {
	case PolicyOpCounter:
		e.I64(op.Delta)
	case PolicyOpJsonSet:
		e.String(op.Path)
		encodeValue(e, op.V)
	case PolicyOpJsonRemove:
		e.String(op.Path)
	default:
		encodeValue(e, op.V)
	}
}

func decodePolicyOperation(d *postcard.Decoder) PolicyOperation {
	switch d.Discriminant() {
	case 0:
		return PolicyOperation{Tag: PolicyOpCounter, Delta: d.I64()}
	case 1:
		return PolicyOperation{Tag: PolicyOpAdd, V: decodeValue(d)}
	case 2:
		return PolicyOperation{Tag: PolicyOpRemove, V: decodeValue(d)}
	case 3:
		return PolicyOperation{Tag: PolicyOpMin, V: decodeValue(d)}
	case 4:
		return PolicyOperation{Tag: PolicyOpMax, V: decodeValue(d)}
	case 5:
		return PolicyOperation{Tag: PolicyOpJsonSet, Path: d.String(), V: decodeValue(d)}
	case 6:
		return PolicyOperation{Tag: PolicyOpJsonRemove, Path: d.String()}
	case 7:
		return PolicyOperation{Tag: PolicyOpListAppend, V: decodeValue(d)}
	case 8:
		return PolicyOperation{Tag: PolicyOpListRemove, V: decodeValue(d)}
	default:
		d.Fail(corruptErr("bad policy operation"))
		return PolicyOperation{}
	}
}

func encodePolicyIntent(e *postcard.Encoder, p PolicyIntent) {
	e.String(p.Table)
	e.String(p.Column)
	e.Len(len(p.Key))
	for _, v := range p.Key {
		encodeValue(e, v)
	}
	encodeValue(e, p.Before)
	encodePolicyOperation(e, p.Operation)
	e.Raw(p.ID[:])
}

func decodePolicyIntent(d *postcard.Decoder) PolicyIntent {
	var p PolicyIntent
	p.Table = d.String()
	p.Column = d.String()
	for n := d.Len(); n > 0; n-- {
		p.Key = append(p.Key, decodeValue(d))
	}
	p.Before = decodeValue(d)
	p.Operation = decodePolicyOperation(d)
	copy(p.ID[:], d.Raw(16))
	return p
}

// --- distance / vector config ---

func encodeDistance(e *postcard.Encoder, m DistanceMetric) {
	e.Discriminant(uint32(m))
}

func decodeDistance(d *postcard.Decoder) DistanceMetric {
	v := d.Discriminant()
	if v > 2 {
		d.Fail(corruptErr("bad distance metric"))
	}
	return DistanceMetric(v)
}

func encodeVectorConfig(e *postcard.Encoder, c VectorColumnConfig) {
	e.U64(uint64(c.Dimensions))
	encodeDistance(e, c.Distance)
}

func decodeVectorConfig(d *postcard.Decoder) VectorColumnConfig {
	return VectorColumnConfig{Dimensions: int(d.U64()), Distance: decodeDistance(d)}
}

// --- column / table (full derived shapes) ---

func encodeColumn(e *postcard.Encoder, c Column) {
	if c.Vector == nil {
		e.None()
	} else {
		e.Some()
		encodeVectorConfig(e, *c.Vector)
	}
	e.Bool(c.Fts)
	encodeMergePolicy(e, c.Policy)
	e.String(c.ID)
	e.String(c.Name)
	e.U64(uint64(c.PrimaryKey))
	e.String(c.DeclaredType)
	e.Bool(c.NotNull)
}

func decodeColumn(d *postcard.Decoder) Column {
	var c Column
	if d.Present() {
		vc := decodeVectorConfig(d)
		c.Vector = &vc
	}
	c.Fts = d.Bool()
	c.Policy = decodeMergePolicy(d)
	c.ID = d.String()
	c.Name = d.String()
	c.PrimaryKey = int(d.U64())
	c.DeclaredType = d.String()
	c.NotNull = d.Bool()
	return c
}

// encodeStoredColumn writes the format-6 positional layout: policy,
// id, name, primary key, declared type, nullability. Vector and FTS
// declarations travel as extension index strings.
func encodeStoredColumn(e *postcard.Encoder, c Column) {
	encodeMergePolicy(e, c.Policy)
	e.String(c.ID)
	e.String(c.Name)
	e.U64(uint64(c.PrimaryKey))
	e.String(c.DeclaredType)
	e.Bool(c.NotNull)
}

func decodeStoredColumn(d *postcard.Decoder) Column {
	return Column{
		Policy:       decodeMergePolicy(d),
		ID:           d.String(),
		Name:         d.String(),
		PrimaryKey:   int(d.U64()),
		DeclaredType: d.String(),
		NotNull:      d.Bool(),
	}
}

func encodeApplicationKey(e *postcard.Encoder, k *ApplicationKey) {
	if k == nil {
		e.None()
		return
	}
	e.Some()
	e.Len(len(k.Columns))
	for _, c := range k.Columns {
		e.String(c)
	}
	e.Len(len(k.Collations))
	for _, c := range k.Collations {
		e.String(c)
	}
	e.Len(len(k.Descending))
	for _, desc := range k.Descending {
		e.Bool(desc)
	}
	e.Bool(k.WithoutRow)
	e.Bool(k.Strict)
	if k.Conflict == nil {
		e.None()
	} else {
		e.Some()
		e.String(*k.Conflict)
	}
}

func decodeApplicationKey(d *postcard.Decoder) *ApplicationKey {
	if !d.Present() {
		return nil
	}
	k := &ApplicationKey{}
	for n := d.Len(); n > 0; n-- {
		k.Columns = append(k.Columns, d.String())
	}
	for n := d.Len(); n > 0; n-- {
		k.Collations = append(k.Collations, d.String())
	}
	for n := d.Len(); n > 0; n-- {
		k.Descending = append(k.Descending, d.Bool())
	}
	k.WithoutRow = d.Bool()
	k.Strict = d.Bool()
	if d.Present() {
		s := d.String()
		k.Conflict = &s
	}
	return k
}

func encodeTable(e *postcard.Encoder, t Table) {
	encodeApplicationKey(e, t.ApplicationKey)
	e.String(t.ID)
	e.String(t.Name)
	e.String(t.SQL)
	e.Len(len(t.Columns))
	for _, c := range t.Columns {
		encodeColumn(e, c)
	}
}

func decodeTable(d *postcard.Decoder) Table {
	t := Table{ApplicationKey: decodeApplicationKey(d)}
	t.ID = d.String()
	t.Name = d.String()
	t.SQL = d.String()
	for n := d.Len(); n > 0; n-- {
		t.Columns = append(t.Columns, decodeColumn(d))
	}
	return t
}

func encodeStoredTable(e *postcard.Encoder, t Table) {
	encodeApplicationKey(e, t.ApplicationKey)
	e.String(t.ID)
	e.String(t.Name)
	e.String(t.SQL)
	e.Len(len(t.Columns))
	for _, c := range t.Columns {
		encodeStoredColumn(e, c)
	}
}

func decodeStoredTable(d *postcard.Decoder) Table {
	t := Table{ApplicationKey: decodeApplicationKey(d)}
	t.ID = d.String()
	t.Name = d.String()
	t.SQL = d.String()
	for n := d.Len(); n > 0; n-- {
		t.Columns = append(t.Columns, decodeStoredColumn(d))
	}
	return t
}

// --- schema ---

const (
	extFTS    = "\x00murmur/column-fts/v1/"
	extVector = "\x00murmur/column-vector/v1/"
)

func jsonString(s string) string {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(s); err != nil {
		return `""`
	}
	out := buf.Bytes()
	if len(out) > 0 && out[len(out)-1] == '\n' {
		out = out[:len(out)-1]
	}
	return string(out)
}

func encodeSchema(e *postcard.Encoder, s Schema) {
	indexes := append([]string(nil), s.Indexes...)
	for _, t := range s.Tables {
		for _, c := range t.Columns {
			if c.Vector != nil {
				indexes = append(indexes, extVector+"["+jsonString(t.ID)+","+jsonString(c.ID)+","+
					`{"dimensions":`+itoa(c.Vector.Dimensions)+`,"distance":`+jsonString(c.Vector.Distance.String())+"}]")
			}
			if c.Fts {
				indexes = append(indexes, extFTS+"["+jsonString(t.ID)+","+jsonString(c.ID)+"]")
			}
		}
	}
	e.Len(len(s.Tables))
	for _, t := range s.Tables {
		encodeStoredTable(e, t)
	}
	e.Len(len(indexes))
	for _, ix := range indexes {
		e.String(ix)
	}
	e.Raw(s.History[:])
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

func decodeSchema(d *postcard.Decoder) Schema {
	var s Schema
	for n := d.Len(); n > 0; n-- {
		s.Tables = append(s.Tables, decodeStoredTable(d))
	}
	var indexes []string
	for n := d.Len(); n > 0; n-- {
		indexes = append(indexes, d.String())
	}
	copy(s.History[:], d.Raw(32))
	for _, ix := range indexes {
		switch {
		case len(ix) > len(extFTS) && ix[:len(extFTS)] == extFTS:
			var target [2]string
			if err := json.Unmarshal([]byte(ix[len(extFTS):]), &target); err != nil {
				d.Fail(corruptErr("bad FTS declaration: %v", err))
				return s
			}
			col := findSchemaColumn(&s, target[0], target[1])
			if col == nil {
				d.Fail(corruptErr("FTS declaration references a missing column"))
				return s
			}
			if col.Fts {
				d.Fail(corruptErr("duplicate FTS declaration"))
				return s
			}
			col.Fts = true
		case len(ix) > len(extVector) && ix[:len(extVector)] == extVector:
			var target [3]json.RawMessage
			if err := json.Unmarshal([]byte(ix[len(extVector):]), &target); err != nil {
				d.Fail(corruptErr("bad vector declaration: %v", err))
				return s
			}
			var tableID, columnID string
			var config VectorColumnConfig
			if err := json.Unmarshal(target[0], &tableID); err != nil {
				d.Fail(corruptErr("bad vector declaration: %v", err))
				return s
			}
			if err := json.Unmarshal(target[1], &columnID); err != nil {
				d.Fail(corruptErr("bad vector declaration: %v", err))
				return s
			}
			if err := config.unmarshalJSON(target[2]); err != nil {
				d.Fail(corruptErr("bad vector declaration: %v", err))
				return s
			}
			col := findSchemaColumn(&s, tableID, columnID)
			if col == nil {
				d.Fail(corruptErr("vector declaration references a missing column"))
				return s
			}
			if col.Vector != nil {
				d.Fail(corruptErr("duplicate vector declaration"))
				return s
			}
			col.Vector = &config
		case len(ix) > 0 && ix[0] == 0:
			d.Fail(corruptErr("unknown schema extension"))
			return s
		default:
			s.Indexes = append(s.Indexes, ix)
		}
	}
	return s
}

func findSchemaColumn(s *Schema, tableID, columnID string) *Column {
	for ti := range s.Tables {
		if s.Tables[ti].ID != tableID {
			continue
		}
		for ci := range s.Tables[ti].Columns {
			if s.Tables[ti].Columns[ci].ID == columnID {
				return &s.Tables[ti].Columns[ci]
			}
		}
	}
	return nil
}

// --- cells / rows / tombstones / replica ---

func encodeCell(e *postcard.Encoder, c Cell) {
	e.String(c.Column)
	encodeValue(e, c.Value)
	encodeVersion(e, c.Version)
}

func decodeCell(d *postcard.Decoder) Cell {
	return Cell{Column: d.String(), Value: decodeValue(d), Version: decodeVersion(d)}
}

func encodeRowState(e *postcard.Encoder, r RowState) {
	e.String(r.Table)
	e.Len(len(r.Key))
	for _, v := range r.Key {
		encodeValue(e, v)
	}
	encodeVersion(e, r.Generation)
	e.Len(len(r.Cells))
	for _, c := range r.Cells {
		encodeCell(e, c)
	}
}

func decodeRowState(d *postcard.Decoder) RowState {
	r := RowState{Table: d.String()}
	for n := d.Len(); n > 0; n-- {
		r.Key = append(r.Key, decodeValue(d))
	}
	r.Generation = decodeVersion(d)
	for n := d.Len(); n > 0; n-- {
		r.Cells = append(r.Cells, decodeCell(d))
	}
	return r
}

func encodeRowMeta(e *postcard.Encoder, m RowMeta) {
	e.ByteString(m.Key)
	encodeVersion(e, m.Generation)
	e.Len(len(m.Cells))
	for _, c := range m.Cells {
		encodeCell(e, c)
	}
}

func decodeRowMeta(d *postcard.Decoder) RowMeta {
	m := RowMeta{Key: append([]byte(nil), d.Bytes()...)}
	m.Generation = decodeVersion(d)
	for n := d.Len(); n > 0; n-- {
		m.Cells = append(m.Cells, decodeCell(d))
	}
	return m
}

func encodeTombstone(e *postcard.Encoder, t Tombstone) {
	e.String(t.Table)
	e.ByteString(t.Key)
	encodeVersion(e, t.Generation)
	encodeVersion(e, t.Version)
}

func decodeTombstone(d *postcard.Decoder) Tombstone {
	t := Tombstone{Table: d.String()}
	t.Key = append([]byte(nil), d.Bytes()...)
	t.Generation = decodeVersion(d)
	t.Version = decodeVersion(d)
	return t
}

func encodeReplicaState(e *postcard.Encoder, r ReplicaState) {
	e.Raw(r.DatabaseID[:])
	encodeSchema(e, r.Schema)
	e.Len(len(r.Rows))
	for _, row := range r.Rows {
		encodeRowState(e, row)
	}
	e.Len(len(r.Tombstones))
	for _, t := range r.Tombstones {
		encodeTombstone(e, t)
	}
	encodeVersion(e, r.Clock)
}

func decodeReplicaState(d *postcard.Decoder) ReplicaState {
	var r ReplicaState
	copy(r.DatabaseID[:], d.Raw(16))
	r.Schema = decodeSchema(d)
	for n := d.Len(); n > 0; n-- {
		r.Rows = append(r.Rows, decodeRowState(d))
	}
	for n := d.Len(); n > 0; n-- {
		r.Tombstones = append(r.Tombstones, decodeTombstone(d))
	}
	r.Clock = decodeVersion(d)
	return r
}

// --- open phase / progress / metrics ---

func encodeOpenPhase(e *postcard.Encoder, p OpenPhase) {
	e.Discriminant(uint32(p))
}

func decodeOpenPhase(d *postcard.Decoder) OpenPhase {
	v := d.Discriminant()
	if v > 6 {
		d.Fail(corruptErr("bad open phase"))
	}
	return OpenPhase(v)
}

func encodeOpenProgress(e *postcard.Encoder, p OpenProgress) {
	encodeOpenPhase(e, p.Phase)
	encodeDuration(e, p.Elapsed)
	encodeDuration(e, p.PhaseElapsed)
	e.U64(p.ProcessedItems)
	if p.CurrentTable == nil {
		e.None()
	} else {
		e.Some()
		e.String(*p.CurrentTable)
	}
	e.U64(p.RowsInserted)
	e.U64(p.RowsSkipped)
	if p.Error == nil {
		e.None()
	} else {
		e.Some()
		e.String(*p.Error)
	}
}

func decodeOpenProgress(d *postcard.Decoder) OpenProgress {
	p := OpenProgress{Phase: decodeOpenPhase(d)}
	p.Elapsed = decodeDuration(d)
	p.PhaseElapsed = decodeDuration(d)
	p.ProcessedItems = d.U64()
	if d.Present() {
		s := d.String()
		p.CurrentTable = &s
	}
	p.RowsInserted = d.U64()
	p.RowsSkipped = d.U64()
	if d.Present() {
		s := d.String()
		p.Error = &s
	}
	return p
}

func encodeSqliteMetrics(e *postcard.Encoder, m SqliteMetrics) {
	e.I64(m.MemoryUsedBytes)
	e.I64(m.MemoryHighwaterBytes)
	e.I64(m.PageSize)
	e.I64(m.PageCount)
	e.I64(m.DatabaseSizeBytes)
	e.I64(m.FreelistPages)
	e.I64(m.SchemaVersion)
	e.U64(uint64(m.TableCount))
	e.U64(uint64(m.IndexCount))
	e.U64(m.TotalRows)
	e.U64(uint64(m.SessionMemoryBytes))
	e.U64(uint64(m.SessionChangesetBytes))
}

func decodeSqliteMetrics(d *postcard.Decoder) SqliteMetrics {
	return SqliteMetrics{
		MemoryUsedBytes:       d.I64(),
		MemoryHighwaterBytes:  d.I64(),
		PageSize:              d.I64(),
		PageCount:             d.I64(),
		DatabaseSizeBytes:     d.I64(),
		FreelistPages:         d.I64(),
		SchemaVersion:         d.I64(),
		TableCount:            int(d.U64()),
		IndexCount:            int(d.U64()),
		TotalRows:             d.U64(),
		SessionMemoryBytes:    int(d.U64()),
		SessionChangesetBytes: int(d.U64()),
	}
}

func encodeSearchIndexStatus(e *postcard.Encoder, s SearchIndexStatus) {
	e.String(s.Name)
	e.Bool(s.Ready)
	e.Bool(s.Rebuilding)
	e.U64(uint64(s.PendingKeys))
	e.U64(uint64(s.QueuedBytes))
	if s.Error == nil {
		e.None()
	} else {
		e.Some()
		e.String(*s.Error)
	}
}

func decodeSearchIndexStatus(d *postcard.Decoder) SearchIndexStatus {
	s := SearchIndexStatus{Name: d.String(), Ready: d.Bool(), Rebuilding: d.Bool()}
	s.PendingKeys = int(d.U64())
	s.QueuedBytes = int(d.U64())
	if d.Present() {
		msg := d.String()
		s.Error = &msg
	}
	return s
}

func encodeIdentity(e *postcard.Encoder, v Identity) {
	e.Raw(v.Database[:])
	e.Raw(v.Node[:])
	e.U32(v.Format)
}

func decodeIdentity(d *postcard.Decoder) Identity {
	var v Identity
	copy(v.Database[:], d.Raw(16))
	copy(v.Node[:], d.Raw(16))
	v.Format = d.U32()
	return v
}
