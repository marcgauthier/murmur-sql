package murmur

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"time"
)

// JSON codecs mirroring serde_json: field order follows declaration,
// enums render as variant names, newtype variants as {"Variant":v},
// byte arrays as number lists, Durations as {"secs","nanos"}.
// Output never HTML-escapes, matching serde_json.

// MarshalJSON renders v without HTML escaping.
func marshalNoEscape(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	out := buf.Bytes()
	if len(out) > 0 && out[len(out)-1] == '\n' {
		out = out[:len(out)-1]
	}
	return out, nil
}

// formatFloat renders f like serde_json: shortest round-trip with a
// ".0" suffix for integral values.
func formatFloat(f float64) string {
	if math.IsInf(f, 1) || math.IsInf(f, -1) || math.IsNaN(f) {
		return "null"
	}
	abs := math.Abs(f)
	s := strconv.FormatFloat(f, 'g', -1, 64)
	if abs < 1e21 && abs >= 1e-6 || f == 0 {
		hasDot := false
		for i := 0; i < len(s); i++ {
			if s[i] == '.' || s[i] == 'e' || s[i] == 'E' {
				hasDot = true
				break
			}
		}
		if !hasDot {
			s += ".0"
		}
	}
	return s
}

// --- version ---

// byteList renders bytes as a JSON number list, matching serde.
type byteList []byte

// MarshalJSON renders the number list.
func (b byteList) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte('[')
	for i, c := range b {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.WriteString(itoa(int(c)))
	}
	buf.WriteByte(']')
	return buf.Bytes(), nil
}

// MarshalJSON renders the version.
func (v Version) MarshalJSON() ([]byte, error) {
	return marshalNoEscape(struct {
		Millis  uint64   `json:"millis"`
		Logical uint32   `json:"logical"`
		Node    byteList `json:"node"`
	}{v.Millis, v.Logical, v.Node[:]})
}

// UnmarshalJSON parses the version.
func (v *Version) UnmarshalJSON(data []byte) error {
	var raw struct {
		Millis  uint64  `json:"millis"`
		Logical uint32  `json:"logical"`
		Node    []uint8 `json:"node"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if len(raw.Node) != 16 {
		return fmt.Errorf("murmur: bad node id length %d", len(raw.Node))
	}
	v.Millis, v.Logical = raw.Millis, raw.Logical
	copy(v.Node[:], raw.Node)
	return nil
}

// --- value ---

// MarshalJSON renders the value in externally-tagged form.
func (v Value) MarshalJSON() ([]byte, error) {
	switch v.Tag {
	case ValueNull:
		return []byte(`"Null"`), nil
	case ValueInteger:
		return marshalNoEscape(struct {
			I int64 `json:"Integer"`
		}{v.I})
	case ValueReal:
		return []byte(`{"Real":` + formatFloat(v.F) + `}`), nil
	case ValueText:
		return marshalNoEscape(struct {
			S string `json:"Text"`
		}{v.S})
	case ValueBlob:
		return marshalNoEscape(struct {
			B byteList `json:"Blob"`
		}{v.B})
	}
	return nil, fmt.Errorf("murmur: bad value tag %d", v.Tag)
}

// UnmarshalJSON parses the value.
func (v *Value) UnmarshalJSON(data []byte) error {
	data = bytes.TrimSpace(data)
	if bytes.Equal(data, []byte(`"Null"`)) {
		*v = NullValue()
		return nil
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if len(raw) != 1 {
		return fmt.Errorf("murmur: bad value %s", data)
	}
	for key, val := range raw {
		switch key {
		case "Integer":
			var n int64
			if err := json.Unmarshal(val, &n); err != nil {
				return err
			}
			*v = IntValue(n)
		case "Real":
			var f float64
			if err := json.Unmarshal(val, &f); err != nil {
				return err
			}
			*v = RealValue(f)
		case "Text":
			var s string
			if err := json.Unmarshal(val, &s); err != nil {
				return err
			}
			*v = TextValue(s)
		case "Blob":
			var b []byte
			if err := json.Unmarshal(val, &b); err != nil {
				return err
			}
			*v = BlobValue(b)
		default:
			return fmt.Errorf("murmur: bad value variant %q", key)
		}
	}
	return nil
}

// --- merge policy ---

// MarshalJSON renders the policy name.
func (p MergePolicy) MarshalJSON() ([]byte, error) {
	if p > PolicyMax {
		return nil, fmt.Errorf("murmur: bad merge policy %d", p)
	}
	return marshalNoEscape(p.String())
}

// UnmarshalJSON parses the policy name.
func (p *MergePolicy) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	switch s {
	case "Lww":
		*p = PolicyLww
	case "Counter":
		*p = PolicyCounter
	case "Set":
		*p = PolicySet
	case "Min":
		*p = PolicyMin
	case "Max":
		*p = PolicyMax
	default:
		return fmt.Errorf("murmur: bad merge policy %q", s)
	}
	return nil
}

// --- distance ---

// MarshalJSON renders the lowercase metric name.
func (d DistanceMetric) MarshalJSON() ([]byte, error) {
	if d > DistL1 {
		return nil, fmt.Errorf("murmur: bad distance metric %d", d)
	}
	return marshalNoEscape(d.String())
}

// UnmarshalJSON parses the metric name.
func (d *DistanceMetric) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	switch s {
	case "l2":
		*d = DistL2
	case "cosine":
		*d = DistCosine
	case "l1":
		*d = DistL1
	default:
		return fmt.Errorf("murmur: bad distance metric %q", s)
	}
	return nil
}

// --- vector config ---

// MarshalJSON renders the config.
func (c VectorColumnConfig) MarshalJSON() ([]byte, error) {
	return marshalNoEscape(struct {
		Dimensions int            `json:"dimensions"`
		Distance   DistanceMetric `json:"distance"`
	}{c.Dimensions, c.Distance})
}

// UnmarshalJSON parses the config.
func (c *VectorColumnConfig) UnmarshalJSON(data []byte) error {
	return c.unmarshalJSON(data)
}

func (c *VectorColumnConfig) unmarshalJSON(data []byte) error {
	var raw struct {
		Dimensions int            `json:"dimensions"`
		Distance   DistanceMetric `json:"distance"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	c.Dimensions, c.Distance = raw.Dimensions, raw.Distance
	return nil
}

// --- column / application key / table ---

// MarshalJSON renders the column.
func (c Column) MarshalJSON() ([]byte, error) {
	return marshalNoEscape(struct {
		Vector       *VectorColumnConfig `json:"vector"`
		Fts          bool                `json:"fts"`
		Policy       MergePolicy         `json:"policy"`
		ID           string              `json:"id"`
		Name         string              `json:"name"`
		PrimaryKey   int                 `json:"primary_key"`
		DeclaredType string              `json:"declared_type"`
		NotNull      bool                `json:"not_null"`
	}{c.Vector, c.Fts, c.Policy, c.ID, c.Name, c.PrimaryKey, c.DeclaredType, c.NotNull})
}

// UnmarshalJSON parses the column.
func (c *Column) UnmarshalJSON(data []byte) error {
	var raw struct {
		Vector       *VectorColumnConfig `json:"vector"`
		Fts          bool                `json:"fts"`
		Policy       MergePolicy         `json:"policy"`
		ID           string              `json:"id"`
		Name         string              `json:"name"`
		PrimaryKey   int                 `json:"primary_key"`
		DeclaredType string              `json:"declared_type"`
		NotNull      bool                `json:"not_null"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	*c = Column{Vector: raw.Vector, Fts: raw.Fts, Policy: raw.Policy, ID: raw.ID,
		Name: raw.Name, PrimaryKey: raw.PrimaryKey, DeclaredType: raw.DeclaredType, NotNull: raw.NotNull}
	return nil
}

// MarshalJSON renders the application key.
func (k ApplicationKey) MarshalJSON() ([]byte, error) {
	return marshalNoEscape(struct {
		Columns    []string `json:"columns"`
		Collations []string `json:"collations"`
		Descending []bool   `json:"descending"`
		WithoutRow bool     `json:"without_rowid"`
		Strict     bool     `json:"strict"`
		Conflict   *string  `json:"conflict"`
	}{k.Columns, k.Collations, k.Descending, k.WithoutRow, k.Strict, k.Conflict})
}

// UnmarshalJSON parses the application key.
func (k *ApplicationKey) UnmarshalJSON(data []byte) error {
	var raw struct {
		Columns    []string `json:"columns"`
		Collations []string `json:"collations"`
		Descending []bool   `json:"descending"`
		WithoutRow bool     `json:"without_rowid"`
		Strict     bool     `json:"strict"`
		Conflict   *string  `json:"conflict"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	*k = ApplicationKey{Columns: raw.Columns, Collations: raw.Collations, Descending: raw.Descending,
		WithoutRow: raw.WithoutRow, Strict: raw.Strict, Conflict: raw.Conflict}
	return nil
}

// MarshalJSON renders the table.
func (t Table) MarshalJSON() ([]byte, error) {
	return marshalNoEscape(struct {
		ApplicationKey *ApplicationKey `json:"application_key"`
		ID             string          `json:"id"`
		Name           string          `json:"name"`
		SQL            string          `json:"sql"`
		Columns        []Column        `json:"columns"`
	}{t.ApplicationKey, t.ID, t.Name, t.SQL, t.Columns})
}

// UnmarshalJSON parses the table.
func (t *Table) UnmarshalJSON(data []byte) error {
	var raw struct {
		ApplicationKey *ApplicationKey `json:"application_key"`
		ID             string          `json:"id"`
		Name           string          `json:"name"`
		SQL            string          `json:"sql"`
		Columns        []Column        `json:"columns"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	*t = Table{ApplicationKey: raw.ApplicationKey, ID: raw.ID, Name: raw.Name, SQL: raw.SQL, Columns: raw.Columns}
	return nil
}

// --- schema ---

// MarshalJSON renders the human-readable schema: tables, indexes,
// history. Table replication travels separately, like the Rust codec.
func (s Schema) MarshalJSON() ([]byte, error) {
	return marshalNoEscape(struct {
		Tables  []Table  `json:"tables"`
		Indexes []string `json:"indexes"`
		History byteList `json:"history"`
	}{s.Tables, s.Indexes, s.History[:]})
}

// UnmarshalJSON parses the human-readable schema.
func (s *Schema) UnmarshalJSON(data []byte) error {
	var raw struct {
		Tables  []Table  `json:"tables"`
		Indexes []string `json:"indexes"`
		History []uint8  `json:"history"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if len(raw.History) != 32 {
		return fmt.Errorf("murmur: bad schema history length %d", len(raw.History))
	}
	*s = Schema{Tables: raw.Tables, Indexes: raw.Indexes}
	copy(s.History[:], raw.History)
	return nil
}

// --- cell / row / tombstone / replica ---

// MarshalJSON renders the cell.
func (c Cell) MarshalJSON() ([]byte, error) {
	return marshalNoEscape(struct {
		Column  string  `json:"column"`
		Value   Value   `json:"value"`
		Version Version `json:"version"`
	}{c.Column, c.Value, c.Version})
}

// UnmarshalJSON parses the cell.
func (c *Cell) UnmarshalJSON(data []byte) error {
	var raw struct {
		Column  string  `json:"column"`
		Value   Value   `json:"value"`
		Version Version `json:"version"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	*c = Cell{Column: raw.Column, Value: raw.Value, Version: raw.Version}
	return nil
}

// MarshalJSON renders the row state.
func (r RowState) MarshalJSON() ([]byte, error) {
	return marshalNoEscape(struct {
		Table      string  `json:"table"`
		Key        []Value `json:"key"`
		Generation Version `json:"generation"`
		Cells      []Cell  `json:"cells"`
	}{r.Table, r.Key, r.Generation, r.Cells})
}

// UnmarshalJSON parses the row state.
func (r *RowState) UnmarshalJSON(data []byte) error {
	var raw struct {
		Table      string  `json:"table"`
		Key        []Value `json:"key"`
		Generation Version `json:"generation"`
		Cells      []Cell  `json:"cells"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	*r = RowState{Table: raw.Table, Key: raw.Key, Generation: raw.Generation, Cells: raw.Cells}
	return nil
}

// MarshalJSON renders the tombstone.
func (t Tombstone) MarshalJSON() ([]byte, error) {
	return marshalNoEscape(struct {
		Table      string   `json:"table"`
		Key        byteList `json:"key"`
		Generation Version  `json:"generation"`
		Version    Version  `json:"version"`
	}{t.Table, t.Key, t.Generation, t.Version})
}

// UnmarshalJSON parses the tombstone.
func (t *Tombstone) UnmarshalJSON(data []byte) error {
	var raw struct {
		Table      string  `json:"table"`
		Key        []uint8 `json:"key"`
		Generation Version `json:"generation"`
		Version    Version `json:"version"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	*t = Tombstone{Table: raw.Table, Key: raw.Key, Generation: raw.Generation, Version: raw.Version}
	return nil
}

// MarshalJSON renders the replica state.
func (r ReplicaState) MarshalJSON() ([]byte, error) {
	return marshalNoEscape(struct {
		DatabaseID byteList    `json:"database_id"`
		Schema     Schema      `json:"schema"`
		Rows       []RowState  `json:"rows"`
		Tombstones []Tombstone `json:"tombstones"`
		Clock      Version     `json:"clock"`
	}{r.DatabaseID[:], r.Schema, r.Rows, r.Tombstones, r.Clock})
}

// UnmarshalJSON parses the replica state.
func (r *ReplicaState) UnmarshalJSON(data []byte) error {
	var raw struct {
		DatabaseID [16]uint8   `json:"database_id"`
		Schema     Schema      `json:"schema"`
		Rows       []RowState  `json:"rows"`
		Tombstones []Tombstone `json:"tombstones"`
		Clock      Version     `json:"clock"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	*r = ReplicaState{DatabaseID: raw.DatabaseID, Schema: raw.Schema, Rows: raw.Rows, Tombstones: raw.Tombstones, Clock: raw.Clock}
	return nil
}

// --- open phase / progress / metrics ---

// MarshalJSON renders the phase name.
func (p OpenPhase) MarshalJSON() ([]byte, error) {
	if p > PhaseCancelled {
		return nil, fmt.Errorf("murmur: bad open phase %d", p)
	}
	return marshalNoEscape(p.String())
}

// UnmarshalJSON parses the phase name.
func (p *OpenPhase) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	switch s {
	case "Opening":
		*p = PhaseOpening
	case "Rebuilding":
		*p = PhaseRebuilding
	case "Indexing":
		*p = PhaseIndexing
	case "Finalizing":
		*p = PhaseFinalizing
	case "Ready":
		*p = PhaseReady
	case "Failed":
		*p = PhaseFailed
	case "Cancelled":
		*p = PhaseCancelled
	default:
		return fmt.Errorf("murmur: bad open phase %q", s)
	}
	return nil
}

type durationJSON struct {
	Secs  uint64 `json:"secs"`
	Nanos uint32 `json:"nanos"`
}

func splitDuration(d time.Duration) durationJSON {
	if d < 0 {
		d = 0
	}
	return durationJSON{Secs: uint64(d / time.Second), Nanos: uint32(d % time.Second)}
}

func joinDuration(j durationJSON) time.Duration {
	return time.Duration(j.Secs)*time.Second + time.Duration(j.Nanos)
}

// MarshalJSON renders the progress snapshot.
func (p OpenProgress) MarshalJSON() ([]byte, error) {
	return marshalNoEscape(struct {
		Phase          OpenPhase    `json:"phase"`
		Elapsed        durationJSON `json:"elapsed"`
		PhaseElapsed   durationJSON `json:"phase_elapsed"`
		ProcessedItems uint64       `json:"processed_items"`
		CurrentTable   *string      `json:"current_table"`
		RowsInserted   uint64       `json:"rows_inserted"`
		RowsSkipped    uint64       `json:"rows_skipped"`
		Error          *string      `json:"error"`
	}{p.Phase, splitDuration(p.Elapsed), splitDuration(p.PhaseElapsed), p.ProcessedItems,
		p.CurrentTable, p.RowsInserted, p.RowsSkipped, p.Error})
}

// UnmarshalJSON parses the progress snapshot.
func (p *OpenProgress) UnmarshalJSON(data []byte) error {
	var raw struct {
		Phase          OpenPhase    `json:"phase"`
		Elapsed        durationJSON `json:"elapsed"`
		PhaseElapsed   durationJSON `json:"phase_elapsed"`
		ProcessedItems uint64       `json:"processed_items"`
		CurrentTable   *string      `json:"current_table"`
		RowsInserted   uint64       `json:"rows_inserted"`
		RowsSkipped    uint64       `json:"rows_skipped"`
		Error          *string      `json:"error"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	*p = OpenProgress{Phase: raw.Phase, Elapsed: joinDuration(raw.Elapsed),
		PhaseElapsed: joinDuration(raw.PhaseElapsed), ProcessedItems: raw.ProcessedItems,
		CurrentTable: raw.CurrentTable, RowsInserted: raw.RowsInserted,
		RowsSkipped: raw.RowsSkipped, Error: raw.Error}
	return nil
}

// MarshalJSON renders SQLite metrics.
func (m SqliteMetrics) MarshalJSON() ([]byte, error) {
	return marshalNoEscape(struct {
		MemoryUsedBytes       int64  `json:"memory_used_bytes"`
		MemoryHighwaterBytes  int64  `json:"memory_highwater_bytes"`
		PageSize              int64  `json:"page_size"`
		PageCount             int64  `json:"page_count"`
		DatabaseSizeBytes     int64  `json:"database_size_bytes"`
		FreelistPages         int64  `json:"freelist_pages"`
		SchemaVersion         int64  `json:"schema_version"`
		TableCount            int    `json:"table_count"`
		IndexCount            int    `json:"index_count"`
		TotalRows             uint64 `json:"total_rows"`
		SessionMemoryBytes    int    `json:"session_memory_bytes"`
		SessionChangesetBytes int    `json:"session_changeset_bytes"`
	}{m.MemoryUsedBytes, m.MemoryHighwaterBytes, m.PageSize, m.PageCount,
		m.DatabaseSizeBytes, m.FreelistPages, m.SchemaVersion, m.TableCount,
		m.IndexCount, m.TotalRows, m.SessionMemoryBytes, m.SessionChangesetBytes})
}

// UnmarshalJSON parses SQLite metrics.
func (m *SqliteMetrics) UnmarshalJSON(data []byte) error {
	var raw struct {
		MemoryUsedBytes       int64  `json:"memory_used_bytes"`
		MemoryHighwaterBytes  int64  `json:"memory_highwater_bytes"`
		PageSize              int64  `json:"page_size"`
		PageCount             int64  `json:"page_count"`
		DatabaseSizeBytes     int64  `json:"database_size_bytes"`
		FreelistPages         int64  `json:"freelist_pages"`
		SchemaVersion         int64  `json:"schema_version"`
		TableCount            int    `json:"table_count"`
		IndexCount            int    `json:"index_count"`
		TotalRows             uint64 `json:"total_rows"`
		SessionMemoryBytes    int    `json:"session_memory_bytes"`
		SessionChangesetBytes int    `json:"session_changeset_bytes"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	*m = SqliteMetrics{MemoryUsedBytes: raw.MemoryUsedBytes, MemoryHighwaterBytes: raw.MemoryHighwaterBytes,
		PageSize: raw.PageSize, PageCount: raw.PageCount, DatabaseSizeBytes: raw.DatabaseSizeBytes,
		FreelistPages: raw.FreelistPages, SchemaVersion: raw.SchemaVersion, TableCount: raw.TableCount,
		IndexCount: raw.IndexCount, TotalRows: raw.TotalRows, SessionMemoryBytes: raw.SessionMemoryBytes,
		SessionChangesetBytes: raw.SessionChangesetBytes}
	return nil
}

// MarshalJSON renders search index status.
func (s SearchIndexStatus) MarshalJSON() ([]byte, error) {
	return marshalNoEscape(struct {
		Name        string  `json:"name"`
		Ready       bool    `json:"ready"`
		Rebuilding  bool    `json:"rebuilding"`
		PendingKeys int     `json:"pending_keys"`
		QueuedBytes int     `json:"queued_bytes"`
		Error       *string `json:"error"`
	}{s.Name, s.Ready, s.Rebuilding, s.PendingKeys, s.QueuedBytes, s.Error})
}

// UnmarshalJSON parses search index status.
func (s *SearchIndexStatus) UnmarshalJSON(data []byte) error {
	var raw struct {
		Name        string  `json:"name"`
		Ready       bool    `json:"ready"`
		Rebuilding  bool    `json:"rebuilding"`
		PendingKeys int     `json:"pending_keys"`
		QueuedBytes int     `json:"queued_bytes"`
		Error       *string `json:"error"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	*s = SearchIndexStatus{Name: raw.Name, Ready: raw.Ready, Rebuilding: raw.Rebuilding,
		PendingKeys: raw.PendingKeys, QueuedBytes: raw.QueuedBytes, Error: raw.Error}
	return nil
}
