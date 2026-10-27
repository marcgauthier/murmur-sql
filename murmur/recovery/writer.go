package recovery

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/compression"
)

// PublishStage names Finish milestones. WriterConfig.Hook fires after
// each completed milestone so crash tests can abort at every
// publication point.
type PublishStage int

const (
	// StageFilesSynced fires after runs, index, and manifest are
	// written and synced inside staging.
	StageFilesSynced PublishStage = iota
	// StagePublished fires after staging is renamed to its generation
	// directory and the parent is synced.
	StagePublished
	// StageCurrentUpdated fires after CURRENT points at the new
	// generation and the parent is synced.
	StageCurrentUpdated
	// StagePruned fires after old-generation pruning completes.
	StagePruned
)

// WriterConfig configures one generation build.
type WriterConfig struct {
	Dir               string
	DatabaseID        [16]byte
	GenerationID      uint64
	CaptureCursor     uint64
	CaptureSequence   uint64
	SchemaDigest      [32]byte
	Codec             compression.Codec
	Cipher            *Cipher
	KeyRef            string
	TargetRunBytes    int64
	TargetBlockBytes  int
	MaxMemory         int64
	RetainGenerations int
	Less              LessFunc
	// Hook aborts Finish with its error after the named milestone
	// completes. Nil disables fault injection.
	Hook func(PublishStage) error
}

// Writer builds one cache generation in a staging directory and
// publishes it atomically. Single use: Add rows, then Finish or Abort.
type Writer struct {
	cfg     WriterConfig
	staging string
	rows    *Sorter
	done    bool
}

func checkWriterConfig(cfg *WriterConfig) error {
	if cfg.Dir == "" {
		return fmt.Errorf("recovery: empty recovery dir")
	}
	if cfg.Codec == nil || cfg.Cipher == nil {
		return fmt.Errorf("recovery: writer needs codec and cipher")
	}
	if cfg.Less == nil {
		return fmt.Errorf("recovery: writer needs row comparator")
	}
	if len(cfg.KeyRef) > MaxKeyRefLen {
		return fmt.Errorf("recovery: key ref too long")
	}
	if cfg.TargetRunBytes <= 0 || cfg.TargetRunBytes > MaxFileBytes {
		return fmt.Errorf("recovery: bad run size %d", cfg.TargetRunBytes)
	}
	if cfg.TargetBlockBytes <= 0 || cfg.TargetBlockBytes > MaxBlockPlain {
		return fmt.Errorf("recovery: bad block size %d", cfg.TargetBlockBytes)
	}
	if cfg.MaxMemory <= 0 {
		return fmt.Errorf("recovery: bad memory bound %d", cfg.MaxMemory)
	}
	if cfg.RetainGenerations < 1 {
		return fmt.Errorf("recovery: bad generation retention %d", cfg.RetainGenerations)
	}
	return nil
}

// NewWriter stages a new generation build under cfg.Dir.
func NewWriter(cfg WriterConfig) (*Writer, error) {
	if err := checkWriterConfig(&cfg); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(cfg.Dir, 0o700); err != nil {
		return nil, fmt.Errorf("recovery: create recovery dir: %w", err)
	}
	var staging string
	for tries := 0; tries < 16; tries++ {
		var rnd [8]byte
		if _, err := rand.Read(rnd[:]); err != nil {
			return nil, fmt.Errorf("recovery: random staging: %w", err)
		}
		staging = filepath.Join(cfg.Dir, fmt.Sprintf("%s%x", BuildingPrefix, rnd))
		if err := os.Mkdir(staging, 0o700); err == nil {
			break
		} else if !os.IsExist(err) {
			return nil, fmt.Errorf("recovery: create staging: %w", err)
		}
		staging = ""
	}
	if staging == "" {
		return nil, fmt.Errorf("recovery: cannot allocate staging dir")
	}
	rows, err := NewSorter("rows", cfg.Less, cfg.MaxMemory, staging, cfg.Cipher, cfg.Codec,
		cfg.DatabaseID, cfg.GenerationID, cfg.SchemaDigest)
	if err != nil {
		_ = os.RemoveAll(staging)
		return nil, err
	}
	return &Writer{cfg: cfg, staging: staging, rows: rows}, nil
}

// Staging returns the staging directory (for crash tests).
func (w *Writer) Staging() string { return w.staging }

// Add buffers one row (copied).
func (w *Writer) Add(r Row) error {
	if w.done {
		return fmt.Errorf("recovery: writer finished")
	}
	return w.rows.Add(r)
}

// Abort discards the staged build.
func (w *Writer) Abort() error {
	if w.done {
		return nil
	}
	w.done = true
	return os.RemoveAll(w.staging)
}

// dataHeader renders a run/index file header.
func dataHeader(magic string, encrypted bool, codecID uint8, table string, digest [32]byte, generation uint64, keyRef string) []byte {
	var out []byte
	out = append(out, magic...)
	var tmp [8]byte
	binary.LittleEndian.PutUint16(tmp[:2], FormatVersion)
	out = append(out, tmp[:2]...)
	if encrypted {
		out = append(out, FlagEncrypted)
	} else {
		out = append(out, 0)
	}
	out = append(out, codecID)
	out = appendBytes(out, []byte(table))
	out = append(out, digest[:]...)
	binary.LittleEndian.PutUint64(tmp[:], generation)
	out = append(out, tmp[:]...)
	return appendBytes(out, []byte(keyRef))
}

func encodeRunRecord(r *Row) []byte {
	var out []byte
	out = appendBytes(out, r.Sort)
	out = appendBytes(out, r.Key)
	out = appendUvarint(out, r.Seq)
	return appendBytes(out, r.Data)
}

func encodeIndexRecord(key []byte, seq uint64) []byte {
	var out []byte
	out = appendBytes(out, key)
	return appendUvarint(out, seq)
}

// runFile accumulates one sorted run file.
type runFile struct {
	path   string
	name   string
	table  string
	file   *os.File
	hasher hash.Hash
	bw     *blockWriter
	bytes  int64
	rows   uint64
	minKey []byte
	maxKey []byte
}

func (w *Writer) openRun(table string, seq int) (*runFile, error) {
	name := RunFileName(table, seq)
	path := filepath.Join(w.staging, name)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("recovery: create run: %w", err)
	}
	h := sha256.New()
	hdr := dataHeader(MagicRun, w.cfg.Cipher.Encrypted(), w.cfg.Codec.ID(),
		table, w.cfg.SchemaDigest, w.cfg.GenerationID, w.cfg.KeyRef)
	if _, err := f.Write(hdr); err != nil {
		f.Close()
		_ = os.Remove(path)
		return nil, fmt.Errorf("recovery: write run header: %w", err)
	}
	h.Write(hdr)
	aad := func(block uint32) []byte {
		return BlockAAD(w.cfg.DatabaseID, w.cfg.GenerationID, w.cfg.SchemaDigest, name, block)
	}
	return &runFile{path: path, name: name, table: table, file: f, hasher: h,
		bw:    newBlockWriter(io.MultiWriter(f, h), w.cfg.Cipher, w.cfg.Codec, name, aad, w.cfg.TargetBlockBytes),
		bytes: int64(len(hdr))}, nil
}

func (r *runFile) add(row *Row) error {
	if r.rows == 0 {
		r.minKey = append([]byte(nil), row.Sort...)
	}
	r.maxKey = append([]byte(nil), row.Sort...)
	before := r.bw.bytesOut
	if err := r.bw.Add(row.Sort, encodeRunRecord(row)); err != nil {
		return err
	}
	r.bytes += r.bw.bytesOut - before
	r.rows++
	return nil
}

func (r *runFile) close() (FileDescriptor, error) {
	var zero FileDescriptor
	before := r.bw.bytesOut
	if err := r.bw.Flush(); err != nil {
		r.file.Close()
		return zero, err
	}
	r.bytes += r.bw.bytesOut - before
	if err := r.file.Sync(); err != nil {
		r.file.Close()
		return zero, fmt.Errorf("recovery: sync run: %w", err)
	}
	if err := r.file.Close(); err != nil {
		return zero, fmt.Errorf("recovery: close run: %w", err)
	}
	var sum [HashLen]byte
	copy(sum[:], r.hasher.Sum(nil))
	return FileDescriptor{Name: r.name, Type: FileRun, Size: uint64(r.bytes),
		Hash: sum, Rows: r.rows, MinKey: r.minKey, MaxKey: r.maxKey}, nil
}

// Finish sorts, writes, syncs, and publishes the generation. On success
// it returns the published manifest.
func (w *Writer) Finish() (*Manifest, error) {
	if w.done {
		return nil, fmt.Errorf("recovery: writer finished")
	}
	w.done = true
	fail := func(err error) (*Manifest, error) {
		_ = os.RemoveAll(w.staging)
		return nil, err
	}
	m, err := w.build()
	if err != nil {
		return fail(err)
	}
	if err := w.hook(StageFilesSynced); err != nil {
		return nil, err // simulated crash: staging left behind
	}
	target := filepath.Join(w.cfg.Dir, GenerationDirName(w.cfg.GenerationID))
	if _, err := os.Lstat(target); err == nil {
		_ = os.RemoveAll(w.staging)
		return nil, fmt.Errorf("recovery: generation %d exists", w.cfg.GenerationID)
	}
	if err := os.Rename(w.staging, target); err != nil {
		return fail(fmt.Errorf("recovery: publish generation: %w", err))
	}
	if err := syncDir(w.cfg.Dir); err != nil {
		return nil, fmt.Errorf("recovery: sync recovery dir: %w", err)
	}
	if err := w.hook(StagePublished); err != nil {
		return nil, err
	}
	if err := w.writeCurrent(target, m); err != nil {
		return nil, err
	}
	if err := w.hook(StageCurrentUpdated); err != nil {
		return nil, err
	}
	w.prune(target)
	if err := syncDir(w.cfg.Dir); err != nil {
		return nil, fmt.Errorf("recovery: sync recovery dir: %w", err)
	}
	if err := w.hook(StagePruned); err != nil {
		return nil, err
	}
	return m, nil
}

func (w *Writer) hook(stage PublishStage) error {
	if w.cfg.Hook == nil {
		return nil
	}
	return w.cfg.Hook(stage)
}

// build drains the row sorter into runs plus the sorted index, writes
// the manifest, and syncs staging.
func (w *Writer) build() (*Manifest, error) {
	indexSorter, err := NewSorter("index", IndexOrder, w.cfg.MaxMemory, w.staging, w.cfg.Cipher,
		w.cfg.Codec, w.cfg.DatabaseID, w.cfg.GenerationID, w.cfg.SchemaDigest)
	if err != nil {
		return nil, err
	}
	m := &Manifest{
		DatabaseID:      w.cfg.DatabaseID,
		GenerationID:    w.cfg.GenerationID,
		CaptureCursor:   w.cfg.CaptureCursor,
		CaptureSequence: w.cfg.CaptureSequence,
		SchemaDigest:    w.cfg.SchemaDigest,
		CreatedUnixNano: uint64(time.Now().UnixNano()),
		CodecID:         w.cfg.Codec.ID(),
		Encrypted:       w.cfg.Cipher.Encrypted(),
		KeyRef:          w.cfg.KeyRef,
	}
	var cur *runFile
	var runSeq int
	var tables []string
	flushRun := func() error {
		if cur == nil {
			return nil
		}
		desc, err := cur.close()
		cur = nil
		if err != nil {
			return err
		}
		m.Files = append(m.Files, desc)
		return nil
	}
	drainErr := w.rows.Drain(func(r *Row) error {
		if cur == nil || cur.table != r.Table {
			if err := flushRun(); err != nil {
				return err
			}
			runSeq = 0
			tables = append(tables, r.Table)
		} else if cur.bytes >= w.cfg.TargetRunBytes {
			if err := flushRun(); err != nil {
				return err
			}
			runSeq++
		}
		if cur == nil {
			rf, err := w.openRun(r.Table, runSeq)
			if err != nil {
				return err
			}
			cur = rf
		}
		if err := cur.add(r); err != nil {
			return err
		}
		m.RowCount++
		return indexSorter.Add(Row{Sort: r.Key, Key: r.Key, Seq: r.Seq})
	})
	if drainErr != nil {
		if cur != nil {
			cur.file.Close()
		}
		return nil, drainErr
	}
	if err := flushRun(); err != nil {
		return nil, err
	}
	m.TableCount = uint32(len(tables))
	indexDesc, err := w.writeIndex(indexSorter)
	if err != nil {
		return nil, err
	}
	m.Files = append(m.Files, indexDesc)
	sealed, err := sealManifest(w.cfg.Cipher, m)
	if err != nil {
		return nil, err
	}
	manifestPath := filepath.Join(w.staging, ManifestName)
	if err := os.WriteFile(manifestPath, sealed, 0o600); err != nil {
		return nil, fmt.Errorf("recovery: write manifest: %w", err)
	}
	f, err := os.Open(manifestPath)
	if err != nil {
		return nil, fmt.Errorf("recovery: sync manifest: %w", err)
	}
	syncErr := f.Sync()
	cerr := f.Close()
	if syncErr != nil {
		return nil, fmt.Errorf("recovery: sync manifest: %w", syncErr)
	}
	if cerr != nil {
		return nil, fmt.Errorf("recovery: close manifest: %w", cerr)
	}
	if err := syncDir(w.staging); err != nil {
		return nil, fmt.Errorf("recovery: sync staging: %w", err)
	}
	return m, nil
}

// writeIndex drains the key sorter into row-index.ridx.
func (w *Writer) writeIndex(indexSorter *Sorter) (FileDescriptor, error) {
	var zero FileDescriptor
	path := filepath.Join(w.staging, IndexName)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return zero, fmt.Errorf("recovery: create index: %w", err)
	}
	h := sha256.New()
	hdr := dataHeader(MagicIndex, w.cfg.Cipher.Encrypted(), w.cfg.Codec.ID(),
		"", w.cfg.SchemaDigest, w.cfg.GenerationID, w.cfg.KeyRef)
	if _, err := f.Write(hdr); err != nil {
		f.Close()
		_ = os.Remove(path)
		return zero, fmt.Errorf("recovery: write index header: %w", err)
	}
	h.Write(hdr)
	aad := func(block uint32) []byte {
		return BlockAAD(w.cfg.DatabaseID, w.cfg.GenerationID, w.cfg.SchemaDigest, IndexName, block)
	}
	bw := newBlockWriter(io.MultiWriter(f, h), w.cfg.Cipher, w.cfg.Codec, IndexName, aad, w.cfg.TargetBlockBytes)
	var rows uint64
	drainErr := indexSorter.Drain(func(r *Row) error {
		rows++
		return bw.Add(r.Sort, encodeIndexRecord(r.Key, r.Seq))
	})
	if drainErr != nil {
		f.Close()
		return zero, drainErr
	}
	if err := bw.Flush(); err != nil {
		f.Close()
		return zero, err
	}
	size := int64(len(hdr)) + bw.bytesOut
	if err := f.Sync(); err != nil {
		f.Close()
		return zero, fmt.Errorf("recovery: sync index: %w", err)
	}
	if err := f.Close(); err != nil {
		return zero, fmt.Errorf("recovery: close index: %w", err)
	}
	var sum [HashLen]byte
	copy(sum[:], h.Sum(nil))
	return FileDescriptor{Name: IndexName, Type: FileIndex, Size: uint64(size),
		Hash: sum, Rows: rows}, nil
}

// writeCurrent points CURRENT at the published generation.
func (w *Writer) writeCurrent(target string, m *Manifest) error {
	raw, err := os.ReadFile(filepath.Join(target, ManifestName))
	if err != nil {
		return fmt.Errorf("recovery: read manifest: %w", err)
	}
	tmp, err := os.CreateTemp(w.cfg.Dir, ".CURRENT-*.tmp")
	if err != nil {
		return fmt.Errorf("recovery: write current: %w", err)
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(FormatCurrent(filepath.Base(target), HashBytes(raw))); err != nil {
		tmp.Close()
		_ = os.Remove(tmpName)
		return fmt.Errorf("recovery: write current: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		_ = os.Remove(tmpName)
		return fmt.Errorf("recovery: sync current: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("recovery: close current: %w", err)
	}
	if err := replaceFile(tmpName, filepath.Join(w.cfg.Dir, CurrentName)); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	if err := syncDir(w.cfg.Dir); err != nil {
		return fmt.Errorf("recovery: sync recovery dir: %w", err)
	}
	// Remove stray temp pointers from crashed updates (best effort).
	entries, err := os.ReadDir(w.cfg.Dir)
	if err != nil {
		return nil
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".CURRENT-") {
			_ = os.Remove(filepath.Join(w.cfg.Dir, e.Name()))
		}
	}
	return nil
}

// prune removes generations beyond retention, keeping target. Best
// effort: leftovers are harmless and retried by the next build.
func (w *Writer) prune(target string) {
	entries, err := os.ReadDir(w.cfg.Dir)
	if err != nil {
		return
	}
	keepName := filepath.Base(target)
	var ids []uint64
	for _, e := range entries {
		if !e.IsDir() || e.Name() == keepName {
			continue
		}
		if id, ok := ParseGenerationDir(e.Name()); ok {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] > ids[j] })
	keep := w.cfg.RetainGenerations - 1 // target is always retained
	if keep < 0 {
		keep = 0
	}
	for i, id := range ids {
		if i < keep {
			continue
		}
		_ = os.RemoveAll(filepath.Join(w.cfg.Dir, GenerationDirName(id)))
	}
}

// CleanupStaging removes abandoned staging and temp files under dir.
func CleanupStaging(dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), BuildingPrefix) ||
			strings.HasPrefix(e.Name(), ".CURRENT-") ||
			strings.HasPrefix(e.Name(), ".spill-") {
			_ = os.RemoveAll(filepath.Join(dir, e.Name()))
		}
	}
}

// replaceFile atomically installs tmp at dst. Windows cannot rename
// over an existing file, so remove first there.
func replaceFile(tmp, dst string) error {
	if err := os.Rename(tmp, dst); err == nil {
		return nil
	} else if runtime.GOOS != "windows" {
		return fmt.Errorf("recovery: install %s: %w", filepath.Base(dst), err)
	}
	if err := os.Remove(dst); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("recovery: replace %s: %w", filepath.Base(dst), err)
	}
	if err := os.Rename(tmp, dst); err != nil {
		return fmt.Errorf("recovery: install %s: %w", filepath.Base(dst), err)
	}
	return nil
}

// syncDir fsyncs a directory. Windows cannot fsync directories.
func syncDir(dir string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
