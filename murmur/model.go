package murmur

import (
	"bytes"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/spool"
)

// Version is a hybrid logical clock. Total order: physical
// milliseconds, logical counter, node identity.
type Version struct {
	Millis  uint64
	Logical uint32
	Node    [16]byte
}

// Compare orders versions: millis, then logical, then node.
func (v Version) Compare(o Version) int {
	if v.Millis != o.Millis {
		if v.Millis < o.Millis {
			return -1
		}
		return 1
	}
	if v.Logical != o.Logical {
		if v.Logical < o.Logical {
			return -1
		}
		return 1
	}
	return bytes.Compare(v.Node[:], o.Node[:])
}

// IsZero reports the default clock.
func (v Version) IsZero() bool {
	return v.Millis == 0 && v.Logical == 0 && v.Node == [16]byte{}
}

// Max returns the greater version.
func (v Version) Max(o Version) Version {
	if v.Compare(o) >= 0 {
		return v
	}
	return o
}

// Next advances the clock for node: wall time when ahead, else the
// logical counter, else the next millisecond.
func (v Version) Next(node [16]byte) (Version, error) {
	var now uint64
	if millis := time.Now().UnixMilli(); millis > 0 {
		now = uint64(millis)
	}
	if now > v.Millis {
		return Version{Millis: now, Node: node}, nil
	}
	if v.Logical < ^uint32(0) {
		return Version{Millis: v.Millis, Logical: v.Logical + 1, Node: node}, nil
	}
	if v.Millis == ^uint64(0) {
		return Version{}, invalidErr("clock exhausted")
	}
	return Version{Millis: v.Millis + 1, Node: node}, nil
}

// ValueTag classifies a Value.
type ValueTag int

const (
	ValueNull ValueTag = iota
	ValueInteger
	ValueReal
	ValueText
	ValueBlob
)

// Value is one SQLite cell value.
type Value struct {
	Tag ValueTag
	I   int64
	F   float64
	S   string
	B   []byte
}

// Value constructors.
func NullValue() Value          { return Value{Tag: ValueNull} }
func IntValue(v int64) Value    { return Value{Tag: ValueInteger, I: v} }
func RealValue(v float64) Value { return Value{Tag: ValueReal, F: v} }
func TextValue(v string) Value  { return Value{Tag: ValueText, S: v} }
func BlobValue(v []byte) Value  { return Value{Tag: ValueBlob, B: v} }

// IsNull reports a NULL value.
func (v Value) IsNull() bool { return v.Tag == ValueNull }

// MergePolicy selects a column's conflict resolution.
type MergePolicy uint8

const (
	PolicyLww MergePolicy = iota
	PolicyCounter
	PolicySet
	PolicyMin
	PolicyMax
	PolicyJson
	PolicyList
)

// MergePolicy* aliases for the replication-facing policy names.
const (
	MergePolicyLww     = PolicyLww
	MergePolicyCounter = PolicyCounter
	MergePolicySet     = PolicySet
	MergePolicyMin     = PolicyMin
	MergePolicyMax     = PolicyMax
	MergePolicyJson    = PolicyJson
	MergePolicyList    = PolicyList
)

// String renders the policy name.
func (p MergePolicy) String() string {
	switch p {
	case PolicyLww:
		return "Lww"
	case PolicyCounter:
		return "Counter"
	case PolicySet:
		return "Set"
	case PolicyMin:
		return "Min"
	case PolicyMax:
		return "Max"
	case PolicyJson:
		return "Json"
	case PolicyList:
		return "List"
	}
	return "Unknown"
}

// PolicyOpTag classifies a PolicyOperation.
type PolicyOpTag int

const (
	PolicyOpCounter PolicyOpTag = iota
	PolicyOpAdd
	PolicyOpRemove
	PolicyOpMin
	PolicyOpMax
	PolicyOpJsonSet
	PolicyOpJsonRemove
	PolicyOpListAppend
	PolicyOpListRemove
)

// PolicyOperation is one policy merge step: a counter delta, a
// set/min/max/list operand value, or a JSON path write (Path plus V,
// or Path alone for removal).
type PolicyOperation struct {
	Tag   PolicyOpTag
	Delta int64
	V     Value
	Path  string
}

// PolicyIntent is a pending policy merge step.
type PolicyIntent struct {
	Table     string
	Column    string
	Key       []Value
	Before    Value
	Operation PolicyOperation
	ID        [16]byte
}

// Column is one managed table column.
type Column struct {
	// Persistent vector-search declaration; index contents are disposable.
	Vector *VectorColumnConfig
	// Include this TEXT column in the table's managed FTS5 index.
	Fts          bool
	Policy       MergePolicy
	ID           string
	Name         string
	PrimaryKey   int
	DeclaredType string
	NotNull      bool
}

// ApplicationKey is the original application key retained while
// SQLite uses the managed UUID key.
type ApplicationKey struct {
	Columns    []string
	Collations []string
	Descending []bool
	WithoutRow bool
	Strict     bool
	Conflict   *string
}

// Table is one managed table.
type Table struct {
	ApplicationKey *ApplicationKey
	ID             string
	Name           string
	SQL            string
	Columns        []Column
}

// Schema is the managed database schema.
type Schema struct {
	// Local table replication settings, keyed by stable table ID.
	// Missing means local-only. Persisted alongside the schema in
	// Spool, excluded from wire data.
	TableReplication map[string]bool
	Tables           []Table
	Indexes          []string
	History          [32]byte
}

// CloneSchema deep-copies a schema. Schemas cross the writer/storage-worker
// boundary inside events and through the Schema accessors; every handoff
// must transfer private backing so neither side observes the other's
// mutations.
func CloneSchema(s Schema) Schema {
	out := s
	out.Tables = make([]Table, len(s.Tables))
	for i := range s.Tables {
		out.Tables[i] = CloneTable(&s.Tables[i])
	}
	out.Indexes = append([]string(nil), s.Indexes...)
	if s.TableReplication != nil {
		out.TableReplication = make(map[string]bool, len(s.TableReplication))
		for k, v := range s.TableReplication {
			out.TableReplication[k] = v
		}
	}
	return out
}

// CloneTable deep-copies one table.
func CloneTable(t *Table) Table {
	out := *t
	out.Columns = make([]Column, len(t.Columns))
	for i := range t.Columns {
		out.Columns[i] = CloneColumn(&t.Columns[i])
	}
	out.ApplicationKey = CloneApplicationKey(t.ApplicationKey)
	return out
}

// CloneColumn deep-copies one column.
func CloneColumn(c *Column) Column {
	out := *c
	if c.Vector != nil {
		vc := *c.Vector
		out.Vector = &vc
	}
	return out
}

// CloneApplicationKey deep-copies an application key, preserving nil.
func CloneApplicationKey(k *ApplicationKey) *ApplicationKey {
	if k == nil {
		return nil
	}
	out := *k
	out.Columns = append([]string(nil), k.Columns...)
	out.Collations = append([]string(nil), k.Collations...)
	out.Descending = append([]bool(nil), k.Descending...)
	if k.Conflict != nil {
		conflict := *k.Conflict
		out.Conflict = &conflict
	}
	return &out
}

// Cell is one versioned column value.
type Cell struct {
	Column  string
	Value   Value
	Version Version
}

// RowState is one row's portable logical state.
type RowState struct {
	Table      string
	Key        []Value
	Generation Version
	Cells      []Cell
}

// RowMeta is the stored per-row conflict metadata: encoded key,
// generation, and versioned cells.
type RowMeta struct {
	Key        []byte
	Generation Version
	Cells      []Cell
}

// Tombstone is one row deletion marker.
type Tombstone struct {
	Table      string
	Key        []byte
	Generation Version
	Version    Version
}

// ReplicaState is portable logical state. Local node identity and
// persistence cursors are not merged.
type ReplicaState struct {
	DatabaseID [16]byte
	Schema     Schema
	Rows       []RowState
	Tombstones []Tombstone
	Clock      Version
}

// TableMapping renames a table and its columns across a migration.
type TableMapping struct {
	OldName string
	NewName string
	Columns []TableColumnMapping
}

// TableColumnMapping renames one column.
type TableColumnMapping struct {
	Old string
	New string
}

// Migration is a schema migration: SQL plus table mappings.
type Migration struct {
	SQL      string
	Mappings []TableMapping
}

// NewMigration builds a migration from SQL.
func NewMigration(sql string) Migration {
	return Migration{SQL: sql}
}

// OpenMode selects open behavior.
type OpenMode int

const (
	// OpenExisting opens a previously created store.
	OpenExisting OpenMode = iota
	// OpenCreateNew creates a fresh store.
	OpenCreateNew
)

// RestoreOptions configures Database restore.
type RestoreOptions struct {
	// Replace the database identity on restore. Nil preserves the
	// backup's database id (clone/reseed as a new replica of the
	// same database).
	NewDatabaseID *[16]byte
	// Replace the node identity on restore. Nil preserves the
	// backup's node id. Clones that will exchange state must use a
	// distinct node id.
	NewNodeID *[16]byte
}

// OpenPhase names startup or reload work phases.
type OpenPhase int

const (
	PhaseOpening OpenPhase = iota
	PhaseRebuilding
	PhaseIndexing
	PhaseFinalizing
	PhaseReady
	PhaseFailed
	PhaseCancelled
)

// String renders the phase name.
func (p OpenPhase) String() string {
	switch p {
	case PhaseOpening:
		return "Opening"
	case PhaseRebuilding:
		return "Rebuilding"
	case PhaseIndexing:
		return "Indexing"
	case PhaseFinalizing:
		return "Finalizing"
	case PhaseReady:
		return "Ready"
	case PhaseFailed:
		return "Failed"
	case PhaseCancelled:
		return "Cancelled"
	}
	return "Unknown"
}

// OpenProgress is a point-in-time snapshot of reload or startup progress.
type OpenProgress struct {
	Phase          OpenPhase
	Elapsed        time.Duration
	PhaseElapsed   time.Duration
	ProcessedItems uint64
	CurrentTable   *string
	RowsInserted   uint64
	RowsSkipped    uint64
	Error          *string
}

// SqliteMetrics carries in-memory SQLite resource and size diagnostics.
type SqliteMetrics struct {
	MemoryUsedBytes       int64
	MemoryHighwaterBytes  int64
	PageSize              int64
	PageCount             int64
	DatabaseSizeBytes     int64
	FreelistPages         int64
	SchemaVersion         int64
	TableCount            int
	IndexCount            int
	TotalRows             uint64
	SessionMemoryBytes    int
	SessionChangesetBytes int
}

// SpoolMetrics carries Spool persistence storage engine metrics.
type SpoolMetrics struct {
	Keys               uint64
	Segments           uint64
	DiskBytes          uint64
	LiveBytes          uint64
	LiveRecords        uint64
	DeadRecords        uint64
	PendingRecords     uint64
	PendingBytes       uint64
	BlocksWritten      uint64
	BytesWritten       uint64
	CompressionRatio   float64
	Compactions        uint64
	CompactionBytes    uint64
	Flushes            uint64
	FlushErrors        uint64
	Reclaims           uint64
	ReclaimErrors      uint64
	Commits            uint64
	Syncs              uint64
	CommitNanos        uint64
	SyncNanos          uint64
	StorageFailures    uint64
	IndexBytesEstimate uint64
}

// SpoolMetricsFromStats converts spool stats.
func SpoolMetricsFromStats(s spool.Stats) SpoolMetrics {
	return SpoolMetrics{
		Keys:               s.Keys,
		Segments:           s.Segments,
		DiskBytes:          s.DiskBytes,
		LiveBytes:          s.LiveBytes,
		LiveRecords:        s.LiveRecords,
		DeadRecords:        s.DeadRecords,
		PendingRecords:     s.PendingRecords,
		PendingBytes:       s.PendingBytes,
		BlocksWritten:      s.BlocksWritten,
		BytesWritten:       s.BytesWritten,
		CompressionRatio:   s.CompressionRatio,
		Compactions:        s.Compactions,
		CompactionBytes:    s.CompactionBytes,
		Flushes:            s.Flushes,
		FlushErrors:        s.FlushErrors,
		Reclaims:           s.Reclaims,
		ReclaimErrors:      s.ReclaimErrors,
		Commits:            s.Commits,
		Syncs:              s.Syncs,
		CommitNanos:        s.CommitNanos,
		SyncNanos:          s.SyncNanos,
		StorageFailures:    s.StorageFailures,
		IndexBytesEstimate: s.IndexBytesEstimate,
	}
}

// Identity carries database, node, and format identities.
type Identity struct {
	Database [16]byte
	Node     [16]byte
	Format   uint32
}
