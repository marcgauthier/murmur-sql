package recovery

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/marcgauthier/murmur-sql/murmur/compression"
)

// ErrNoCache reports that no usable cache generation exists. It is not
// corruption: callers fall back to legacy recovery and rebuild.
var ErrNoCache = errors.New("recovery: no usable cache generation")

// Opener configures cache discovery and reading.
type Opener struct {
	// Registry resolves manifest codec ids. Nil selects the default
	// registry (none, deflate, zstd).
	Registry *compression.Registry
	// CipherFor resolves the cipher for a manifest header. It must
	// return an error for unknown key references (the generation is
	// then skipped, never failed open).
	CipherFor func(keyRef string, encrypted bool) (*Cipher, error)
	// Less verifies run order. Nil selects ByteOrder; PR3 builders
	// sorting by collation-aware PK order must pass the same
	// comparator used at build time.
	Less LessFunc
}

func (o *Opener) registry() *compression.Registry {
	if o != nil && o.Registry != nil {
		return o.Registry
	}
	r, err := compression.NewRegistry(compression.MustZstd(3))
	if err != nil {
		// Unreachable: fixed ids cannot collide.
		panic(err)
	}
	return r
}

func (o *Opener) less() LessFunc {
	if o != nil && o.Less != nil {
		return o.Less
	}
	return ByteOrder
}

// Generation is one validated, readable cache generation.
type Generation struct {
	Dir      string
	Manifest *Manifest
	codec    compression.Codec
	cipher   *Cipher
	less     LessFunc
}

// CodecID reports the generation's compression codec id.
func (g *Generation) CodecID() uint8 { return g.Manifest.CodecID }

// Discover returns the best usable generation under dir for dbID: the
// CURRENT generation when valid, else the newest valid generation by
// id. Anything unusable yields ErrNoCache (fall back + rebuild).
func Discover(dir string, dbID [16]byte, o *Opener) (*Generation, error) {
	if o == nil || o.CipherFor == nil {
		return nil, fmt.Errorf("recovery: opener needs CipherFor")
	}
	if names, err := currentCandidate(dir); err == nil {
		if g, err := openGeneration(dir, names, dbID, o); err == nil {
			return g, nil
		}
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNoCache
		}
		return nil, fmt.Errorf("recovery: scan recovery dir: %w", err)
	}
	var ids []uint64
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		if id, ok := ParseGenerationDir(e.Name()); ok {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] > ids[j] })
	for _, id := range ids {
		if g, err := openGeneration(dir, GenerationDirName(id), dbID, o); err == nil {
			return g, nil
		}
	}
	return nil, ErrNoCache
}

func currentCandidate(dir string) (string, error) {
	raw, err := os.ReadFile(filepath.Join(dir, CurrentName))
	if err != nil {
		return "", err
	}
	name, wantHash, err := ParseCurrent(raw)
	if err != nil {
		return "", err
	}
	manifest, err := os.ReadFile(filepath.Join(dir, name, ManifestName))
	if err != nil {
		return "", err
	}
	if HashBytes(manifest) != wantHash {
		return "", corruptErr("CURRENT manifest hash mismatch")
	}
	return name, nil
}

func openGeneration(dir, name string, dbID [16]byte, o *Opener) (*Generation, error) {
	wantID, ok := ParseGenerationDir(name)
	if !ok {
		return nil, corruptErr("bad generation name %q", name)
	}
	genDir := filepath.Join(dir, name)
	raw, err := os.ReadFile(filepath.Join(genDir, ManifestName))
	if err != nil {
		return nil, err
	}
	encrypted, keyRef, err := peekManifestHeader(raw)
	if err != nil {
		return nil, err
	}
	cipher, err := o.CipherFor(keyRef, encrypted)
	if err != nil {
		return nil, fmt.Errorf("recovery: resolve cipher: %w", err)
	}
	m, err := openManifest(cipher, dbID, raw)
	if err != nil {
		return nil, err
	}
	if m.GenerationID != wantID {
		return nil, corruptErr("generation id mismatch")
	}
	codec, err := o.registry().Get(m.CodecID)
	if err != nil {
		return nil, corruptErr("unsupported codec %d", m.CodecID)
	}
	var haveIndex bool
	for i := range m.Files {
		f := &m.Files[i]
		if f.Type == FileIndex {
			haveIndex = true
		}
		st, err := os.Stat(filepath.Join(genDir, f.Name))
		if err != nil {
			return nil, corruptErr("missing file %s", f.Name)
		}
		if !st.Mode().IsRegular() || st.Size() != int64(f.Size) {
			return nil, corruptErr("bad file %s", f.Name)
		}
	}
	if !haveIndex {
		return nil, corruptErr("generation lacks index")
	}
	return &Generation{Dir: genDir, Manifest: m, codec: codec, cipher: cipher, less: o.less()}, nil
}

// RunStream streams every cached row in (table, sort-key) order.
//
// Rows are untrusted until the stream ends: GCM authenticates each
// block immediately, but the file header and manifest binding only
// verify at clean EOF. Consumers must build into a private result and
// discard it on any error (including a terminal hash failure),
// falling back to legacy recovery.
type RunStream struct {
	gen   *Generation
	files []FileDescriptor
	pos   int
	cur   *runFileReader
	prev  *Row
	rows  uint64
}

// OpenRuns opens the generation's run files in manifest order.
func (g *Generation) OpenRuns() (*RunStream, error) {
	var runs []FileDescriptor
	for _, f := range g.Manifest.Files {
		if f.Type == FileRun {
			runs = append(runs, f)
		}
	}
	return &RunStream{gen: g, files: runs}, nil
}

// Next returns the next row, or io.EOF after the final verified row.
// A terminal file-hash failure surfaces as corruption instead of EOF.
func (s *RunStream) Next() (*Row, error) {
	for {
		if s.cur == nil {
			if s.pos >= len(s.files) {
				return nil, io.EOF
			}
			cur, err := openRunFileReader(s.gen, s.files[s.pos])
			if err != nil {
				return nil, err
			}
			s.pos++
			s.cur = cur
		}
		row, err := s.cur.next()
		if err == io.EOF {
			s.cur = nil
			continue
		}
		if err != nil {
			return nil, err
		}
		if s.prev != nil && s.gen.less(row, s.prev) {
			return nil, corruptErr("run order violation")
		}
		s.prev = row
		s.rows++
		return row, nil
	}
}

// Close releases open files. All verification happens in Next; Close
// only reports release errors.
func (s *RunStream) Close() error {
	if s.cur != nil {
		err := s.cur.close()
		s.cur = nil
		return err
	}
	return nil
}

// runFileReader streams one verified run file.
type runFileReader struct {
	gen    *Generation
	desc   FileDescriptor
	file   *os.File
	hasher hash.Hash
	reader *blockReader
	block  *byteReader
	left   uint32
	rows   uint64
	table  string
}

func openRunFileReader(g *Generation, desc FileDescriptor) (*runFileReader, error) {
	return openDataFileReader(g, desc, MagicRun, true)
}

func openDataFileReader(g *Generation, desc FileDescriptor, magic string, wantTable bool) (*runFileReader, error) {
	path := filepath.Join(g.Dir, desc.Name)
	f, err := os.Open(path)
	if err != nil {
		return nil, corruptErr("open %s: %v", desc.Name, err)
	}
	h := sha256.New()
	tee := &hashReader{src: f, h: h}
	fixed := make([]byte, 4+2+1+1)
	if _, err := io.ReadFull(tee, fixed); err != nil {
		f.Close()
		return nil, corruptErr("truncated header in %s", desc.Name)
	}
	if string(fixed[:4]) != magic {
		f.Close()
		return nil, corruptErr("bad magic in %s", desc.Name)
	}
	if v := binary.LittleEndian.Uint16(fixed[4:6]); v != FormatVersion {
		f.Close()
		return nil, corruptErr("unsupported format %d in %s", v, desc.Name)
	}
	wantFlag := uint8(0)
	if g.cipher.Encrypted() {
		wantFlag = FlagEncrypted
	}
	if fixed[6] != wantFlag {
		f.Close()
		return nil, corruptErr("encryption mismatch in %s", desc.Name)
	}
	if fixed[7] != g.codec.ID() {
		f.Close()
		return nil, corruptErr("codec mismatch in %s", desc.Name)
	}
	br := &byteReader{buf: nil}
	_ = br
	// Length-prefixed table id + fixed schema digest, generation, key ref.
	table, err := readUvarintBytes(tee, MaxTableIDLen, desc.Name)
	if err != nil {
		f.Close()
		return nil, err
	}
	if wantTable && len(table) == 0 {
		f.Close()
		return nil, corruptErr("empty table in %s", desc.Name)
	}
	rest := make([]byte, 32+8)
	if _, err := io.ReadFull(tee, rest); err != nil {
		f.Close()
		return nil, corruptErr("truncated header in %s", desc.Name)
	}
	if _, err := readUvarintBytes(tee, MaxKeyRefLen, desc.Name); err != nil {
		f.Close()
		return nil, err
	}
	aad := func(block uint32) []byte {
		return BlockAAD(g.Manifest.DatabaseID, g.Manifest.GenerationID, g.Manifest.SchemaDigest, desc.Name, block)
	}
	return &runFileReader{gen: g, desc: desc, file: f, hasher: h,
		reader: newBlockReader(tee, g.cipher, g.codec, desc.Name, aad, MaxBlockPlain),
		table:  string(table)}, nil
}

func readUvarintBytes(r io.Reader, maxLen int, file string) ([]byte, error) {
	var x uint64
	for i := 0; i < binary.MaxVarintLen64; i++ {
		var b [1]byte
		if _, err := io.ReadFull(r, b[:]); err != nil {
			return nil, corruptErr("truncated length in %s", file)
		}
		if b[0] < 0x80 {
			if i == binary.MaxVarintLen64-1 && b[0] > 1 {
				return nil, corruptErr("varint overflow in %s", file)
			}
			x |= uint64(b[0]) << (7 * i)
			break
		}
		x |= uint64(b[0]&0x7f) << (7 * i)
		if i == binary.MaxVarintLen64-1 {
			return nil, corruptErr("malformed varint in %s", file)
		}
	}
	if x > uint64(maxLen) {
		return nil, corruptErr("length %d exceeds bound in %s", x, file)
	}
	out := make([]byte, x)
	if _, err := io.ReadFull(r, out); err != nil {
		return nil, corruptErr("truncated field in %s", file)
	}
	return out, nil
}

func (r *runFileReader) next() (*Row, error) {
	for {
		if r.block != nil && r.left > 0 {
			row, err := decodeRunRecord(r.block, r.table)
			if err != nil {
				return nil, err
			}
			r.left--
			r.rows++
			if r.left == 0 && len(r.block.buf) != 0 {
				return nil, corruptErr("block trailing bytes in %s", r.desc.Name)
			}
			return row, nil
		}
		_, _, plain, rows, err := r.reader.Next()
		if err == io.EOF {
			if err := r.verify(); err != nil {
				return nil, err
			}
			return nil, io.EOF
		}
		if err != nil {
			return nil, err
		}
		r.block = &byteReader{buf: plain}
		r.left = rows
	}
}

func (r *runFileReader) verify() error {
	if r.rows != r.desc.Rows {
		return corruptErr("row count mismatch in %s", r.desc.Name)
	}
	var sum [HashLen]byte
	copy(sum[:], r.hasher.Sum(nil))
	if sum != r.desc.Hash {
		return corruptErr("file hash mismatch in %s", r.desc.Name)
	}
	return nil
}

func (r *runFileReader) close() error {
	if r.file == nil {
		return nil
	}
	err := r.file.Close()
	r.file = nil
	return err
}

func decodeRunRecord(raw *byteReader, table string) (*Row, error) {
	r := &Row{Table: table}
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

// IndexEntry is one sorted (row key, Spool sequence) pair.
type IndexEntry struct {
	Key []byte
	Seq uint64
}

// IndexStream streams the generation's sorted key index.
type IndexStream struct {
	gen    *Generation
	reader *indexFileReader
	prev   []byte
}

// OpenIndex opens the generation's key index.
func (g *Generation) OpenIndex() (*IndexStream, error) {
	var desc *FileDescriptor
	for i := range g.Manifest.Files {
		if g.Manifest.Files[i].Type == FileIndex {
			desc = &g.Manifest.Files[i]
			break
		}
	}
	if desc == nil {
		return nil, corruptErr("generation lacks index")
	}
	r, err := openIndexFileReader(g, *desc)
	if err != nil {
		return nil, err
	}
	return &IndexStream{gen: g, reader: r}, nil
}

// Next returns the next index entry, or io.EOF after the final
// verified entry.
func (s *IndexStream) Next() (*IndexEntry, error) {
	e, err := s.reader.next()
	if err != nil {
		return nil, err
	}
	if s.prev != nil && bytes.Compare(e.Key, s.prev) < 0 {
		return nil, corruptErr("index order violation")
	}
	s.prev = e.Key
	return e, nil
}

// Close releases the index file.
func (s *IndexStream) Close() error {
	if s.reader != nil {
		return s.reader.close()
	}
	return nil
}

type indexFileReader struct {
	inner *runFileReader
}

func openIndexFileReader(g *Generation, desc FileDescriptor) (*indexFileReader, error) {
	inner, err := openDataFileReader(g, desc, MagicIndex, false)
	if err != nil {
		return nil, err
	}
	return &indexFileReader{inner: inner}, nil
}

func (r *indexFileReader) next() (*IndexEntry, error) {
	for {
		inner := r.inner
		if inner.block != nil && inner.left > 0 {
			raw := inner.block
			key := raw.bytes(MaxKeyLen, "row key")
			if raw.err != nil {
				return nil, raw.err
			}
			seq := raw.uvarint()
			if raw.err != nil {
				return nil, raw.err
			}
			inner.left--
			inner.rows++
			if inner.left == 0 && len(raw.buf) != 0 {
				return nil, corruptErr("block trailing bytes in %s", inner.desc.Name)
			}
			return &IndexEntry{Key: key, Seq: seq}, nil
		}
		_, _, plain, rows, err := inner.reader.Next()
		if err == io.EOF {
			if err := inner.verify(); err != nil {
				return nil, err
			}
			return nil, io.EOF
		}
		if err != nil {
			return nil, err
		}
		inner.block = &byteReader{buf: plain}
		inner.left = rows
	}
}

func (r *indexFileReader) close() error { return r.inner.close() }
