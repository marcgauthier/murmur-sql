package murmur

// Search index declarations. The indexing engine lives in search.go;
// this file holds the configuration types shared with the schema codec.

// DistanceMetric selects vector comparison.
type DistanceMetric int

const (
	// DistL2 is Euclidean distance (default).
	DistL2 DistanceMetric = iota
	// DistCosine is cosine distance.
	DistCosine
	// DistL1 is Manhattan distance.
	DistL1
)

// String renders the lowercase metric name.
func (d DistanceMetric) String() string {
	switch d {
	case DistL2:
		return "l2"
	case DistCosine:
		return "cosine"
	case DistL1:
		return "l1"
	}
	return "unknown"
}

// VectorColumnConfig is the persistent configuration for one
// application's BLOB vector column.
type VectorColumnConfig struct {
	Dimensions int
	Distance   DistanceMetric
}

// Fts5Index declares a node-local FTS5 index over a source table.
type Fts5Index struct {
	Name        string
	SourceTable string
	Columns     []string
	// A built-in tokenizer: unicode61 (default), ascii, porter, or trigram.
	Tokenizer string
}

// NewFts5Index builds an index with the default tokenizer.
func NewFts5Index(name, sourceTable string, columns []string) Fts5Index {
	return Fts5Index{Name: name, SourceTable: sourceTable, Columns: columns, Tokenizer: "unicode61"}
}

// VectorIndex declares a node-local vector index over a source table.
type VectorIndex struct {
	Name        string
	SourceTable string
	// Source BLOB containing little-endian float32 values; also the index column name.
	Column     string
	Dimensions int
	Distance   DistanceMetric
}

// NewVectorIndex builds an index with L2 distance.
func NewVectorIndex(name, sourceTable, column string, dimensions int) VectorIndex {
	return VectorIndex{Name: name, SourceTable: sourceTable, Column: column, Dimensions: dimensions, Distance: DistL2}
}

// SearchIndexTag classifies a SearchIndex.
type SearchIndexTag int

const (
	// SearchFts5 is a full-text index.
	SearchFts5 SearchIndexTag = iota
	// SearchVector is a vector index.
	SearchVector
)

// SearchIndex is one node-local, rebuildable search index declaration.
type SearchIndex struct {
	Tag    SearchIndexTag
	Fts5   Fts5Index
	Vector VectorIndex
}

// Fts5SearchIndex wraps an FTS5 declaration.
func Fts5SearchIndex(idx Fts5Index) SearchIndex {
	return SearchIndex{Tag: SearchFts5, Fts5: idx}
}

// VectorSearchIndex wraps a vector declaration.
func VectorSearchIndex(idx VectorIndex) SearchIndex {
	return SearchIndex{Tag: SearchVector, Vector: idx}
}

// SearchIndexStatus reports one index's build state.
type SearchIndexStatus struct {
	Name        string
	Ready       bool
	Rebuilding  bool
	PendingKeys int
	QueuedBytes int
	Error       *string
}
