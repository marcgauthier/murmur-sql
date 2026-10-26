// Package recovery implements the disposable, read-optimized recovery
// cache: sorted, compressed, integrity-protected row runs rebuilt from
// authoritative Spool data. It handles bytes, file integrity, ordering,
// and generation lifecycle only; row payload semantics (RowMeta, schema)
// belong to the parent murmur package, which maps rows into the generic
// Row type defined here. This package must never import murmur.
package recovery

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

// On-disk format version. Readers reject anything else.
const FormatVersion uint16 = 1

// File magic prefixes.
const (
	MagicManifest = "MRRC"
	MagicRun      = "MRRR"
	MagicIndex    = "MRIX"
	MagicSpill    = "MRSP"
)

// File types in a generation manifest.
const (
	FileRun   uint8 = 1
	FileIndex uint8 = 2
)

// File flag bits in run/index/spill headers.
const (
	// FlagEncrypted marks GCM-sealed blocks. Without it, blocks carry a
	// CRC32-Castagnoli integrity tag instead (unencrypted database).
	FlagEncrypted uint8 = 1 << 0
)

// Fixed file names inside a generation or the recovery directory.
const (
	ManifestName = "MANIFEST"
	CurrentName  = "CURRENT"
	IndexName    = "row-index.ridx"
)

// Generation directory layout: generation-%08d; staging areas use
// .building-<hex>. Published generations are immutable.
const (
	GenerationPrefix = "generation-"
	BuildingPrefix   = ".building-"
)

// Decoding bounds. Every length, count, and allocation on the read path
// is capped so corrupt input fails instead of exhausting memory.
const (
	MaxKeyLen           = 1 << 20
	MaxTableIDLen       = 128
	MaxPayloadLen       = 64 << 20
	MaxBlockPlain       = 64 << 20
	MaxBlockRows        = 1 << 20
	MaxFileNameLen      = 255
	MaxFilesPerManifest = 1 << 20
	MaxManifestPlain    = 64 << 20
	MaxKeyRefLen        = 256
	MaxFileBytes        = int64(1) << 40
)

// Crypto framing sizes.
const (
	NonceLen  = 12
	GCMTagLen = 16
	HashLen   = 32
	CRCTagLen = 4
)

// Row is one cacheable table row. Sort carries the caller-encoded
// primary key used for ordering within a table; Key is the Spool row
// key; Seq is the Spool logical sequence; Data is the opaque row
// payload (murmur EncodeRowMeta bytes). The writer copies every slice.
type Row struct {
	Table string
	Sort  []byte
	Key   []byte
	Seq   uint64
	Data  []byte
}

// LessFunc orders rows. Writers take a caller-supplied comparator so
// primary-key collation stays in murmur; readers reuse it to verify
// run order. ByteOrder is the conservative binary fallback.
type LessFunc func(a, b *Row) bool

// ByteOrder orders rows by (Table, Sort, Key) byte-wise.
func ByteOrder(a, b *Row) bool {
	if a.Table != b.Table {
		return a.Table < b.Table
	}
	if c := bytes.Compare(a.Sort, b.Sort); c != 0 {
		return c < 0
	}
	return bytes.Compare(a.Key, b.Key) < 0
}

// IndexOrder orders index entries (represented as rows with Table == ""
// and Sort == Key) by raw Spool key bytes.
func IndexOrder(a, b *Row) bool {
	return bytes.Compare(a.Sort, b.Sort) < 0
}

// approxSize bounds the memory held by a buffered row.
func (r *Row) approxSize() int64 {
	return int64(len(r.Table)+len(r.Sort)+len(r.Key)+len(r.Data)) + 64
}

// cloneRow deep-copies a row.
func cloneRow(r Row) *Row {
	out := &Row{Table: r.Table, Seq: r.Seq}
	out.Sort = append([]byte(nil), r.Sort...)
	out.Key = append([]byte(nil), r.Key...)
	out.Data = append([]byte(nil), r.Data...)
	return out
}

// errCorrupt marks undecodable or integrity-failed input. Callers treat
// it as a cache miss (fall back to legacy recovery), never as an
// authoritative storage error.
type corruptError struct{ msg string }

func (e *corruptError) Error() string { return "recovery: corrupt cache: " + e.msg }

func corruptErr(format string, args ...any) error {
	return &corruptError{msg: fmt.Sprintf(format, args...)}
}

// IsCorrupt reports whether err is cache corruption (safe to fall back)
// as opposed to an environmental I/O failure.
func IsCorrupt(err error) bool {
	var c *corruptError
	return errors.As(err, &c)
}

// appendUvarint appends x in binary uvarint form.
func appendUvarint(dst []byte, x uint64) []byte {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], x)
	return append(dst, tmp[:n]...)
}

// appendBytes appends a length-prefixed byte slice.
func appendBytes(dst, b []byte) []byte {
	dst = appendUvarint(dst, uint64(len(b)))
	return append(dst, b...)
}

// byteReader is a bounds-checked cursor over untrusted input.
type byteReader struct {
	buf []byte
	err error
}

func (r *byteReader) uvarint() uint64 {
	if r.err != nil {
		return 0
	}
	x, n := binary.Uvarint(r.buf)
	if n <= 0 {
		r.err = corruptErr("malformed varint")
		return 0
	}
	r.buf = r.buf[n:]
	return x
}

func (r *byteReader) fixed(n int) []byte {
	if r.err != nil {
		return nil
	}
	if n < 0 || len(r.buf) < n {
		r.err = corruptErr("truncated field")
		return nil
	}
	out := r.buf[:n]
	r.buf = r.buf[n:]
	return out
}

func (r *byteReader) bytes(maxLen int, what string) []byte {
	if r.err != nil {
		return nil
	}
	n := r.uvarint()
	if r.err != nil {
		return nil
	}
	if n > uint64(maxLen) {
		r.err = corruptErr("%s length %d exceeds bound %d", what, n, maxLen)
		return nil
	}
	raw := r.fixed(int(n))
	if r.err != nil {
		return nil
	}
	return append([]byte(nil), raw...)
}
