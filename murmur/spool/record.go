package spool

// Record flags stored per record in a block body.
const (
	// flagTombstone marks a deletion record. Tombstones carry an empty
	// value.
	flagTombstone = 1 << 0
)

// recordOverhead bounds the framing bytes around key+value inside a
// block body: sequence(8) + flags(1) + keyLen(4) + valueLen(4), plus
// slack for block-level framing amortized per record.
const recordOverhead = 8 + 1 + 4 + 4 + 64

// Record is one decoded key/value mutation delivered to LoadFunc.
// Each key arrives at most once per load, with its current version.
// Key and Value reference the decoded block buffer and are only
// valid for the duration of the callback; copy them to retain.
type Record struct {
	// Key is the record key. Never empty... (empty keys are allowed;
	// the slice is non-nil but may have length zero).
	Key []byte
	// Value is nil for tombstones.
	Value []byte
	// Sequence orders versions of a key; higher wins.
	Sequence uint64
	// Deleted reports a tombstone.
	Deleted bool
}

// KV is one key/value pair for PutBatch. The store copies both
// slices; the caller retains ownership.
type KV struct {
	Key   []byte
	Value []byte
}

// Mutation is one operation in a Commit group. Key and Value are
// copied on admission; the caller retains ownership. A deleted
// mutation must have no value.
type Mutation struct {
	Key     []byte
	Value   []byte
	Deleted bool
}

// pendingRecord is a buffered, not yet sealed mutation.
type pendingRecord struct {
	key   []byte
	value []byte // nil for tombstones
	seq   uint64
	tomb  bool
	// group is the commit group id, assigned at admission.
	group uint64
	// size precomputes key+value bytes for buffer accounting.
	size int
}
