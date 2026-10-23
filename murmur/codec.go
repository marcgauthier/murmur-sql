package murmur

import (
	"github.com/marcgauthier/murmur-sql-go/murmur/internal/postcard"
)

// Top-level postcard encode/decode helpers. Decode failures report
// KindCorrupt, mirroring model::decode.

func encodeOne(fn func(*postcard.Encoder)) []byte {
	e := postcard.NewEncoder(64)
	fn(e)
	return append([]byte(nil), e.Bytes()...)
}

func decodeOne[T any](b []byte, fn func(*postcard.Decoder) T) (T, error) {
	var zero T
	d := postcard.NewDecoder(b)
	v := fn(d)
	if err := d.End(); err != nil {
		return zero, &Error{Kind: KindCorrupt, Msg: err.Error(), Err: err}
	}
	return v, nil
}

// EncodeVersion encodes a version.
func EncodeVersion(v Version) []byte {
	return encodeOne(func(e *postcard.Encoder) { encodeVersion(e, v) })
}

// DecodeVersion decodes a version.
func DecodeVersion(b []byte) (Version, error) { return decodeOne(b, decodeVersion) }

// EncodeValue encodes a value.
func EncodeValue(v Value) []byte { return encodeOne(func(e *postcard.Encoder) { encodeValue(e, v) }) }

// DecodeValue decodes a value.
func DecodeValue(b []byte) (Value, error) { return decodeOne(b, decodeValue) }

// EncodeValues encodes a value list (row keys).
func EncodeValues(v []Value) []byte {
	return encodeOne(func(e *postcard.Encoder) {
		e.Len(len(v))
		for _, x := range v {
			encodeValue(e, x)
		}
	})
}

// DecodeValues decodes a value list (row keys).
func DecodeValues(b []byte) ([]Value, error) {
	return decodeOne(b, func(d *postcard.Decoder) []Value {
		var out []Value
		for n := d.Len(); n > 0; n-- {
			out = append(out, decodeValue(d))
		}
		return out
	})
}

// EncodeMergePolicy encodes a merge policy.
func EncodeMergePolicy(p MergePolicy) []byte {
	return encodeOne(func(e *postcard.Encoder) { encodeMergePolicy(e, p) })
}

// DecodeMergePolicy decodes a merge policy.
func DecodeMergePolicy(b []byte) (MergePolicy, error) { return decodeOne(b, decodeMergePolicy) }

// EncodePolicyIntent encodes a policy intent.
func EncodePolicyIntent(p PolicyIntent) []byte {
	return encodeOne(func(e *postcard.Encoder) { encodePolicyIntent(e, p) })
}

// DecodePolicyIntent decodes a policy intent.
func DecodePolicyIntent(b []byte) (PolicyIntent, error) { return decodeOne(b, decodePolicyIntent) }

// EncodeDistance encodes a distance metric.
func EncodeDistance(m DistanceMetric) []byte {
	return encodeOne(func(e *postcard.Encoder) { encodeDistance(e, m) })
}

// DecodeDistance decodes a distance metric.
func DecodeDistance(b []byte) (DistanceMetric, error) { return decodeOne(b, decodeDistance) }

// EncodeVectorConfig encodes a vector column config.
func EncodeVectorConfig(c VectorColumnConfig) []byte {
	return encodeOne(func(e *postcard.Encoder) { encodeVectorConfig(e, c) })
}

// DecodeVectorConfig decodes a vector column config.
func DecodeVectorConfig(b []byte) (VectorColumnConfig, error) {
	return decodeOne(b, decodeVectorConfig)
}

// EncodeColumn encodes a column (full derived shape).
func EncodeColumn(c Column) []byte {
	return encodeOne(func(e *postcard.Encoder) { encodeColumn(e, c) })
}

// DecodeColumn decodes a column.
func DecodeColumn(b []byte) (Column, error) { return decodeOne(b, decodeColumn) }

// EncodeApplicationKey encodes an application key option.
func EncodeApplicationKey(k *ApplicationKey) []byte {
	return encodeOne(func(e *postcard.Encoder) { encodeApplicationKey(e, k) })
}

// DecodeApplicationKey decodes an application key option.
func DecodeApplicationKey(b []byte) (*ApplicationKey, error) {
	return decodeOne(b, decodeApplicationKey)
}

// EncodeTable encodes a table (full derived shape).
func EncodeTable(t Table) []byte { return encodeOne(func(e *postcard.Encoder) { encodeTable(e, t) }) }

// DecodeTable decodes a table.
func DecodeTable(b []byte) (Table, error) { return decodeOne(b, decodeTable) }

// EncodeSchema encodes a schema (stored layout with extensions).
func EncodeSchema(s Schema) []byte {
	return encodeOne(func(e *postcard.Encoder) { encodeSchema(e, s) })
}

// DecodeSchema decodes a schema.
func DecodeSchema(b []byte) (Schema, error) { return decodeOne(b, decodeSchema) }

// EncodeCell encodes a cell.
func EncodeCell(c Cell) []byte { return encodeOne(func(e *postcard.Encoder) { encodeCell(e, c) }) }

// DecodeCell decodes a cell.
func DecodeCell(b []byte) (Cell, error) { return decodeOne(b, decodeCell) }

// EncodeRowMeta encodes row metadata.
func EncodeRowMeta(m RowMeta) []byte {
	return encodeOne(func(e *postcard.Encoder) { encodeRowMeta(e, m) })
}

// DecodeRowMeta decodes row metadata.
func DecodeRowMeta(b []byte) (RowMeta, error) { return decodeOne(b, decodeRowMeta) }

// EncodeRowState encodes a row state.
func EncodeRowState(r RowState) []byte {
	return encodeOne(func(e *postcard.Encoder) { encodeRowState(e, r) })
}

// DecodeRowState decodes a row state.
func DecodeRowState(b []byte) (RowState, error) { return decodeOne(b, decodeRowState) }

// EncodeTombstone encodes a tombstone.
func EncodeTombstone(t Tombstone) []byte {
	return encodeOne(func(e *postcard.Encoder) { encodeTombstone(e, t) })
}

// DecodeTombstone decodes a tombstone.
func DecodeTombstone(b []byte) (Tombstone, error) { return decodeOne(b, decodeTombstone) }

// EncodeReplicaState encodes a replica state.
func EncodeReplicaState(r ReplicaState) []byte {
	return encodeOne(func(e *postcard.Encoder) { encodeReplicaState(e, r) })
}

// DecodeReplicaState decodes a replica state.
func DecodeReplicaState(b []byte) (ReplicaState, error) { return decodeOne(b, decodeReplicaState) }

// EncodeOpenPhase encodes an open phase.
func EncodeOpenPhase(p OpenPhase) []byte {
	return encodeOne(func(e *postcard.Encoder) { encodeOpenPhase(e, p) })
}

// DecodeOpenPhase decodes an open phase.
func DecodeOpenPhase(b []byte) (OpenPhase, error) { return decodeOne(b, decodeOpenPhase) }

// EncodeOpenProgress encodes an open progress snapshot.
func EncodeOpenProgress(p OpenProgress) []byte {
	return encodeOne(func(e *postcard.Encoder) { encodeOpenProgress(e, p) })
}

// DecodeOpenProgress decodes an open progress snapshot.
func DecodeOpenProgress(b []byte) (OpenProgress, error) { return decodeOne(b, decodeOpenProgress) }

// EncodeSqliteMetrics encodes SQLite metrics.
func EncodeSqliteMetrics(m SqliteMetrics) []byte {
	return encodeOne(func(e *postcard.Encoder) { encodeSqliteMetrics(e, m) })
}

// DecodeSqliteMetrics decodes SQLite metrics.
func DecodeSqliteMetrics(b []byte) (SqliteMetrics, error) { return decodeOne(b, decodeSqliteMetrics) }

// EncodeIdentity encodes an identity.
func EncodeIdentity(v Identity) []byte {
	return encodeOne(func(e *postcard.Encoder) { encodeIdentity(e, v) })
}

// DecodeIdentity decodes an identity.
func DecodeIdentity(b []byte) (Identity, error) { return decodeOne(b, decodeIdentity) }

// EncodeRowStates encodes row states.
func EncodeRowStates(rows []RowState) []byte {
	return encodeOne(func(e *postcard.Encoder) {
		e.Len(len(rows))
		for i := range rows {
			encodeRowState(e, rows[i])
		}
	})
}

// EncodePolicyIntents encodes policy intents.
func EncodePolicyIntents(intents []PolicyIntent) []byte {
	return encodeOne(func(e *postcard.Encoder) {
		e.Len(len(intents))
		for i := range intents {
			encodePolicyIntent(e, intents[i])
		}
	})
}
