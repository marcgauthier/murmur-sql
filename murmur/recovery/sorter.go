package recovery

import (
	"container/heap"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/marcgauthier/murmur-sql/murmur/compression"
)

// Sorter orders rows with bounded memory, spilling sorted runs to
// encrypted temporary files when the buffer exceeds maxBytes. Single
// use: Add rows, then Drain exactly once.
type Sorter struct {
	less         LessFunc
	maxBytes     int64
	dir          string
	cipher       *Cipher
	codec        compression.Codec
	dbID         [16]byte
	generation   uint64
	schemaDigest [32]byte
	buf          []*Row
	bufBytes     int64
	spills       []string
	seq          int
	drained      bool
}

// NewSorter builds a sorter spilling into dir (created if missing).
func NewSorter(less LessFunc, maxBytes int64, dir string, cipher *Cipher, codec compression.Codec, dbID [16]byte, generation uint64, schemaDigest [32]byte) (*Sorter, error) {
	if less == nil {
		return nil, fmt.Errorf("recovery: nil sort comparator")
	}
	if maxBytes <= 0 {
		return nil, fmt.Errorf("recovery: bad sorter memory bound %d", maxBytes)
	}
	if cipher == nil || codec == nil {
		return nil, fmt.Errorf("recovery: sorter needs cipher and codec")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("recovery: spill dir: %w", err)
	}
	return &Sorter{less: less, maxBytes: maxBytes, dir: dir, cipher: cipher, codec: codec,
		dbID: dbID, generation: generation, schemaDigest: schemaDigest}, nil
}

// Add buffers one row (copied). Rows breaching per-field bounds fail
// fast instead of poisoning a spill.
func (s *Sorter) Add(r Row) error {
	if s.drained {
		return fmt.Errorf("recovery: sorter already drained")
	}
	if len(r.Table) > MaxTableIDLen {
		return fmt.Errorf("recovery: table id too long")
	}
	if len(r.Sort) > MaxKeyLen || len(r.Key) > MaxKeyLen {
		return fmt.Errorf("recovery: row key too long")
	}
	if len(r.Data) > MaxPayloadLen {
		return fmt.Errorf("recovery: row payload too long")
	}
	cp := cloneRow(r)
	if s.bufBytes+cp.approxSize() > s.maxBytes && len(s.buf) > 0 {
		if err := s.flushSpill(); err != nil {
			return err
		}
	}
	s.buf = append(s.buf, cp)
	s.bufBytes += cp.approxSize()
	return nil
}

// Spills reports how many spill files exist (for tests).
func (s *Sorter) Spills() int { return len(s.spills) }

// Buffered reports in-memory buffered rows (for tests).
func (s *Sorter) Buffered() int { return len(s.buf) }

func (s *Sorter) flushSpill() error {
	sort.Slice(s.buf, func(i, j int) bool { return s.less(s.buf[i], s.buf[j]) })
	name := fmt.Sprintf(".spill-%05d.tmp", s.seq)
	s.seq++
	path := filepath.Join(s.dir, name)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("recovery: spill create: %w", err)
	}
	writeErr := func() error {
		hdr := spillHeader(s.cipher.Encrypted(), s.codec.ID(), s.schemaDigest, s.generation)
		if _, err := f.Write(hdr); err != nil {
			return err
		}
		aad := func(block uint32) []byte {
			return BlockAAD(s.dbID, s.generation, s.schemaDigest, name, block)
		}
		w := newBlockWriter(f, s.cipher, s.codec, name, aad, 256<<10)
		for _, r := range s.buf {
			if err := w.Add(r.Sort, encodeSpillRecord(r)); err != nil {
				return err
			}
		}
		return w.Flush()
	}()
	cerr := f.Close()
	if writeErr != nil {
		_ = os.Remove(path)
		return fmt.Errorf("recovery: spill write: %w", writeErr)
	}
	if cerr != nil {
		_ = os.Remove(path)
		return fmt.Errorf("recovery: spill close: %w", cerr)
	}
	s.spills = append(s.spills, path)
	s.buf = nil
	s.bufBytes = 0
	return nil
}

// Drain emits every row in order, then removes spill files. The callback
// must not retain the row.
func (s *Sorter) Drain(fn func(*Row) error) error {
	if s.drained {
		return fmt.Errorf("recovery: sorter already drained")
	}
	s.drained = true
	defer s.cleanup()
	sort.Slice(s.buf, func(i, j int) bool { return s.less(s.buf[i], s.buf[j]) })
	if len(s.spills) == 0 {
		for _, r := range s.buf {
			if err := fn(r); err != nil {
				return err
			}
		}
		s.buf = nil
		return nil
	}
	sources := make([]*spillSource, 0, len(s.spills)+1)
	for _, path := range s.spills {
		src, err := openSpillSource(path, s.cipher, s.codec, s.dbID, s.generation, s.schemaDigest)
		if err != nil {
			closeSources(sources)
			return err
		}
		sources = append(sources, src)
	}
	if len(s.buf) > 0 {
		sources = append(sources, &spillSource{rows: s.buf})
		s.buf = nil
	}
	m := &mergeHeap{less: s.less}
	for _, src := range sources {
		head, err := src.next()
		if err != nil {
			closeSources(sources)
			return err
		}
		if head != nil {
			heap.Push(m, &mergeItem{src: src, head: head})
		} else {
			src.close()
		}
	}
	heap.Init(m)
	for m.Len() > 0 {
		it := heap.Pop(m).(*mergeItem)
		if err := fn(it.head); err != nil {
			closeSources(sources)
			return err
		}
		head, err := it.src.next()
		if err != nil {
			closeSources(sources)
			return err
		}
		if head != nil {
			heap.Push(m, &mergeItem{src: it.src, head: head})
		} else {
			it.src.close()
		}
	}
	return nil
}

func (s *Sorter) cleanup() {
	for _, path := range s.spills {
		_ = os.Remove(path)
	}
	s.spills = nil
	s.buf = nil
	s.bufBytes = 0
}

func encodeSpillRecord(r *Row) []byte {
	var out []byte
	out = appendBytes(out, []byte(r.Table))
	out = appendBytes(out, r.Sort)
	out = appendBytes(out, r.Key)
	out = appendUvarint(out, r.Seq)
	return appendBytes(out, r.Data)
}

func decodeSpillRecord(raw *byteReader) (*Row, error) {
	r := &Row{}
	r.Table = string(raw.bytes(MaxTableIDLen, "table"))
	if raw.err != nil {
		return nil, raw.err
	}
	r.Sort = raw.bytes(MaxKeyLen, "sort key")
	if raw.err != nil {
		return nil, raw.err
	}
	r.Key = raw.bytes(MaxKeyLen, "row key")
	if raw.err != nil {
		return nil, raw.err
	}
	r.Seq = raw.uvarint()
	if raw.err != nil {
		return nil, raw.err
	}
	r.Data = raw.bytes(MaxPayloadLen, "payload")
	if raw.err != nil {
		return nil, raw.err
	}
	return r, nil
}

func spillHeader(encrypted bool, codecID uint8, digest [32]byte, generation uint64) []byte {
	var out []byte
	out = append(out, MagicSpill...)
	var tmp [8]byte
	binary.LittleEndian.PutUint16(tmp[:2], FormatVersion)
	out = append(out, tmp[:2]...)
	if encrypted {
		out = append(out, FlagEncrypted)
	} else {
		out = append(out, 0)
	}
	out = append(out, codecID)
	out = append(out, digest[:]...)
	binary.LittleEndian.PutUint64(tmp[:], generation)
	return append(out, tmp[:]...)
}

// spillSource streams rows from one spill file or memory buffer.
type spillSource struct {
	rows   []*Row
	pos    int
	file   *os.File
	reader *blockReader
	block  *byteReader
	left   uint32
	name   string
}

func openSpillSource(path string, cipher *Cipher, codec compression.Codec, dbID [16]byte, generation uint64, digest [32]byte) (*spillSource, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("recovery: spill open: %w", err)
	}
	base := filepath.Base(path)
	hdr := make([]byte, 4+2+1+1+32+8)
	if _, err := io.ReadFull(f, hdr); err != nil {
		f.Close()
		return nil, corruptErr("truncated spill header in %s", base)
	}
	if string(hdr[:4]) != MagicSpill {
		f.Close()
		return nil, corruptErr("bad spill magic in %s", base)
	}
	if v := binary.LittleEndian.Uint16(hdr[4:6]); v != FormatVersion {
		f.Close()
		return nil, corruptErr("bad spill format in %s", base)
	}
	wantFlag := uint8(0)
	if cipher.Encrypted() {
		wantFlag = FlagEncrypted
	}
	if hdr[6] != wantFlag {
		f.Close()
		return nil, corruptErr("spill encryption mismatch in %s", base)
	}
	if hdr[7] != codec.ID() {
		f.Close()
		return nil, corruptErr("spill codec mismatch in %s", base)
	}
	aad := func(block uint32) []byte {
		return BlockAAD(dbID, generation, digest, base, block)
	}
	return &spillSource{file: f, name: base,
		reader: newBlockReader(f, cipher, codec, base, aad, MaxBlockPlain)}, nil
}

func (s *spillSource) next() (*Row, error) {
	if s.reader == nil {
		if s.pos >= len(s.rows) {
			return nil, nil
		}
		r := s.rows[s.pos]
		s.pos++
		return r, nil
	}
	for {
		if s.block != nil && s.left > 0 {
			r, err := decodeSpillRecord(s.block)
			if err != nil {
				return nil, err
			}
			s.left--
			if s.left == 0 && len(s.block.buf) != 0 {
				return nil, corruptErr("spill block trailing bytes in %s", s.name)
			}
			return r, nil
		}
		_, _, plain, rows, err := s.reader.Next()
		if err == io.EOF {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		s.block = &byteReader{buf: plain}
		s.left = rows
	}
}

func (s *spillSource) close() {
	if s.file != nil {
		s.file.Close()
		s.file = nil
	}
}

func closeSources(sources []*spillSource) {
	for _, src := range sources {
		src.close()
	}
}

type mergeItem struct {
	src  *spillSource
	head *Row
}

type mergeHeap struct {
	less  LessFunc
	items []*mergeItem
}

func (h *mergeHeap) Len() int { return len(h.items) }
func (h *mergeHeap) Less(i, j int) bool {
	return h.less(h.items[i].head, h.items[j].head)
}
func (h *mergeHeap) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h *mergeHeap) Push(x any)    { h.items = append(h.items, x.(*mergeItem)) }
func (h *mergeHeap) Pop() any {
	old := h.items
	n := len(old)
	it := old[n-1]
	h.items = old[:n-1]
	return it
}
