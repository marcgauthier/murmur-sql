package murmur

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/compression"
	"github.com/marcgauthier/murmur-sql/murmur/recovery"
	"github.com/marcgauthier/murmur-sql/murmur/spool"
)

// Row-record prefix scanned by the recovery builder.
const recoveryRowPrefix = "04/meta/row/"

// Blob-piece prefix reassembled by the recovery builder. Large logical
// values split into 11/blob/ pieces at commit time; the checkpoint
// scan reunites them before caching.
const recoveryBlobPrefix = "11/blob/"

// recoveryKeyRefPrefix tags recovery key references with their spool
// data-key id: "spool-key:<id>".
const recoveryKeyRefPrefix = "spool-key:"

func formatRecoveryKeyRef(id uint32) string {
	return recoveryKeyRefPrefix + strconv.FormatUint(uint64(id), 10)
}

func parseRecoveryKeyRef(ref string) (uint32, error) {
	id, err := strconv.ParseUint(strings.TrimPrefix(ref, recoveryKeyRefPrefix), 10, 32)
	if err != nil || !strings.HasPrefix(ref, recoveryKeyRefPrefix) {
		return 0, fmt.Errorf("murmur: bad recovery key ref %q", ref)
	}
	return uint32(id), nil
}

// RecoveryBuildResult carries a worker-captured checkpoint plus the
// atomic state snapshot the builder needs. Ownership transfers to the
// builder, which must call Cleanup exactly once.
type RecoveryBuildResult struct {
	Checkpoint     *spool.Checkpoint
	Schema         Schema
	DatabaseID     [16]byte
	Cursor         uint64
	Sequence       uint64
	Encrypted      bool
	LoadMasterKey  []byte
	LoadPassphrase string
	Err            error
}

// Cleanup releases the checkpoint, removes its directory, and wipes
// credential copies. Idempotent.
func (r *RecoveryBuildResult) Cleanup() {
	if r == nil {
		return
	}
	if r.Checkpoint != nil {
		dir := r.Checkpoint.Dir
		_ = r.Checkpoint.Release()
		if dir != "" {
			_ = os.RemoveAll(dir)
		}
		r.Checkpoint = nil
	}
	for i := range r.LoadMasterKey {
		r.LoadMasterKey[i] = 0
	}
	r.LoadMasterKey = nil
	r.LoadPassphrase = ""
}

// buildRecoveryCheckpoint flushes pending storage work, captures an
// immutable checkpoint under recoveryDir, and snapshots the atomic
// state describing it. It runs on the serialized storage worker, so
// the snapshot pairs exactly with the checkpoint contents.
func buildRecoveryCheckpoint(opened *Opened, recoveryDir string) RecoveryBuildResult {
	fail := func(err error) RecoveryBuildResult { return RecoveryBuildResult{Err: err} }
	if err := opened.Store.Flush(); err != nil {
		return fail(err)
	}
	if err := os.MkdirAll(recoveryDir, 0o700); err != nil {
		return fail(ioErr(err))
	}
	var dest string
	for tries := 0; tries < 16; tries++ {
		var rnd [8]byte
		if _, err := rand.Read(rnd[:]); err != nil {
			return fail(ioErr(err))
		}
		// ".building-" prefix keeps the checkpoint under the recovery
		// staging cleanup umbrella.
		dest = filepath.Join(recoveryDir, fmt.Sprintf(".building-ckpt-%x", rnd))
		cp, err := opened.Store.Checkpoint(dest)
		if err == nil {
			return RecoveryBuildResult{
				Checkpoint:     cp,
				Schema:         CloneSchema(opened.Schema),
				DatabaseID:     opened.Identity.Database,
				Cursor:         opened.Cursor,
				Sequence:       opened.Sequence,
				Encrypted:      len(opened.Opts.MasterKey) > 0 || opened.Opts.Passphrase != "",
				LoadMasterKey:  append([]byte(nil), opened.Opts.MasterKey...),
				LoadPassphrase: opened.Opts.Passphrase,
			}
		}
		if !os.IsExist(err) {
			return fail(err)
		}
	}
	return fail(ioErr(fmt.Errorf("cannot allocate checkpoint dir")))
}

// recoveryRowLess orders rows by table, then actual primary key via
// CmpValues over the decoded key, then Spool key bytes as a total
// deterministic tie-break. Decode failures (impossible after builder
// validation) fall back to byte order; the comparator never panics.
func recoveryRowLess(a, b *recovery.Row) bool {
	if a.Table != b.Table {
		return a.Table < b.Table
	}
	va, ea := DecodeValues(a.Sort)
	vb, eb := DecodeValues(b.Sort)
	if ea == nil && eb == nil {
		if c := CmpValues(va, vb); c != 0 {
			return c < 0
		}
	} else if c := bytes.Compare(a.Sort, b.Sort); c != 0 {
		return c < 0
	}
	return bytes.Compare(a.Key, b.Key) < 0
}

// recoveryBuildOutcome summarizes one finished build.
type recoveryBuildOutcome struct {
	Generation uint64
	Rows       uint64
	Bytes      uint64
	Seconds    float64
}

// runRecoveryBuild converts one checkpoint into a published recovery
// generation. It owns res and always cleans it up. All errors are
// build-local: the caller records them as status, never as database
// errors.
func runRecoveryBuild(ctx context.Context, res RecoveryBuildResult, store *spool.Store, recoveryDir string, opts RecoveryOptions, compressionLevel int) (recoveryBuildOutcome, error) {
	var zero recoveryBuildOutcome
	defer res.Cleanup()
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	start := time.Now()
	if res.Err != nil {
		return zero, res.Err
	}
	tables := make(map[string]*Table, len(res.Schema.Tables))
	for i := range res.Schema.Tables {
		tables[res.Schema.Tables[i].ID] = &res.Schema.Tables[i]
	}
	schemaSum := sha256.Sum256(EncodeSchema(res.Schema))
	cipher, keyRef, err := recoveryCipherFor(store, res.Encrypted)
	if err != nil {
		return zero, err
	}
	gen, err := recovery.NextGenerationID(recoveryDir)
	if err != nil {
		return zero, err
	}
	// Skip ids left behind by crashed or concurrent publishers.
	for tries := 0; tries < 4; tries++ {
		if _, serr := os.Lstat(filepath.Join(recoveryDir, recovery.GenerationDirName(gen))); os.IsNotExist(serr) {
			break
		} else if serr != nil {
			return zero, serr
		}
		gen++
	}
	w, err := recovery.NewWriter(recovery.WriterConfig{
		Dir:               recoveryDir,
		DatabaseID:        res.DatabaseID,
		GenerationID:      gen,
		CaptureCursor:     res.Cursor,
		CaptureSequence:   res.Sequence,
		SchemaDigest:      schemaSum,
		Codec:             compression.MustZstd(compressionLevel),
		Cipher:            cipher,
		KeyRef:            keyRef,
		TargetRunBytes:    opts.TargetRunBytes,
		TargetBlockBytes:  opts.TargetBlockBytes,
		MaxMemory:         opts.MaxBuildMemory,
		RetainGenerations: opts.RetainGenerations,
		Less:              recoveryRowLess,
	})
	if err != nil {
		return zero, err
	}
	rowPrefix := []byte(recoveryRowPrefix)
	blobPrefix := []byte(recoveryBlobPrefix)
	pending := map[string]*pendingBlob{}
	// orphans holds pieces that arrived before their row descriptor
	// (Load does not promise key order) or belong to non-row blobs.
	orphans := map[string][]byte{}
	finishRow := func(key, value []byte, seq uint64) error {
		row, err := recoveryRowFromRecord(key, value, seq, tables)
		if err != nil {
			return err
		}
		return w.Add(row)
	}
	collect := func(records []spool.Record) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		for i := range records {
			rec := &records[i]
			if rec.Deleted {
				continue
			}
			switch {
			case bytes.HasPrefix(rec.Key, blobPrefix):
				key, value, seq, done, err := stashBlobPiece(pending, orphans, rec.Key, rec.Value)
				if err != nil {
					return err
				}
				if done {
					if err := finishRow(key, value, seq); err != nil {
						return err
					}
				}
			case bytes.HasPrefix(rec.Key, rowPrefix):
				resolved, split, desc, err := resolveRowValue(rec.Value)
				if err != nil {
					return err
				}
				if !split {
					if err := finishRow(rec.Key, resolved, rec.Sequence); err != nil {
						return err
					}
					continue
				}
				if err := stashBlobDescriptor(pending, rec.Key, rec.Sequence, desc); err != nil {
					return err
				}
			}
		}
		return nil
	}
	loadOpts := spool.LoadOptions{
		Path:       res.Checkpoint.Dir,
		Passphrase: res.LoadPassphrase,
		Codecs:     []compression.Codec{compression.MustZstd(compressionLevel)},
	}
	if len(res.LoadMasterKey) > 0 {
		loadOpts.MasterKey = res.LoadMasterKey
	}
	if err := spool.LoadWithOptions(loadOpts, collect); err != nil {
		_ = w.Abort()
		return zero, err
	}
	// Adopt pieces that arrived before their descriptors.
	for key, piece := range orphans {
		rkey, rvalue, rseq, done, err := stashBlobPiece(pending, nil, []byte(key), piece)
		if err != nil {
			_ = w.Abort()
			return zero, err
		}
		if done {
			if err := finishRow(rkey, rvalue, rseq); err != nil {
				_ = w.Abort()
				return zero, err
			}
		}
	}
	if len(pending) > 0 {
		_ = w.Abort()
		return zero, fmt.Errorf("murmur: incomplete blob reassembly (%d rows)", len(pending))
	}
	m, err := w.Finish()
	if err != nil {
		return zero, err
	}
	var bytes uint64
	for i := range m.Files {
		bytes += m.Files[i].Size
	}
	return recoveryBuildOutcome{
		Generation: m.GenerationID,
		Rows:       m.RowCount,
		Bytes:      bytes,
		Seconds:    time.Since(start).Seconds(),
	}, nil
}

// recoveryCipherFor resolves the cache cipher: the live store's
// purpose-separated recovery key when encrypted, else plain mode.
func recoveryCipherFor(store *spool.Store, encrypted bool) (*recovery.Cipher, string, error) {
	if !encrypted {
		c, err := recovery.NewCipher(nil)
		return c, "", err
	}
	id, key, err := store.CurrentRecoveryKey()
	if err != nil {
		return nil, "", err
	}
	defer func() {
		for i := range key {
			key[i] = 0
		}
	}()
	c, err := recovery.NewCipher(key[:])
	if err != nil {
		return nil, "", err
	}
	return c, formatRecoveryKeyRef(id), nil
}

// recoveryRowFromRecord validates one checkpoint row record and maps
// it into a cache row. The payload stays byte-identical to the
// authoritative value (no decode/re-encode drift); Sort reuses the
// encoded primary key already carried by RowMeta.
func recoveryRowFromRecord(key, value []byte, seq uint64, tables map[string]*Table) (recovery.Row, error) {
	var zero recovery.Row
	rest, ok := strings.CutPrefix(string(key), recoveryRowPrefix)
	if !ok {
		return zero, fmt.Errorf("murmur: bad row key %q", key)
	}
	table, _, ok := strings.Cut(rest, "/")
	if !ok || table == "" || len(table) > recovery.MaxTableIDLen {
		return zero, fmt.Errorf("murmur: bad row table %q", key)
	}
	if _, ok := tables[table]; !ok {
		return zero, fmt.Errorf("murmur: row for unknown table %q", table)
	}
	meta, err := DecodeRowMeta(value)
	if err != nil {
		return zero, err
	}
	if len(meta.Key) == 0 || len(meta.Key) > recovery.MaxKeyLen {
		return zero, fmt.Errorf("murmur: bad row key payload")
	}
	primary, err := DecodeValues(meta.Key)
	if err != nil {
		return zero, fmt.Errorf("murmur: bad primary key: %w", err)
	}
	// Same collision check as RecoverCurrent: a mismatch fails the
	// build so the legacy error surfaces instead of diverging.
	if !bytes.Equal(RowKey(table, EncodeValues(primary)), key) {
		return zero, fmt.Errorf("murmur: metadata key collision")
	}
	return recovery.Row{Table: table, Sort: meta.Key, Key: key, Seq: seq, Data: value}, nil
}

// resolveRowValue maps a checkpoint row value to its cache payload:
// raw values pass through, inline descriptors unwrap, and split
// values report split=true with their descriptor for reassembly.
func resolveRowValue(value []byte) (resolved []byte, split bool, desc *blobDescriptor, err error) {
	desc, err = splitDescriptor(value)
	if err != nil {
		return nil, false, nil, err
	}
	if desc == nil {
		return value, false, nil, nil
	}
	if desc.inline {
		return desc.value, false, nil, nil
	}
	return nil, true, desc, nil
}

// pendingBlob accumulates one split row value during the checkpoint
// scan. Piece keys sort contiguously per row, so at most one row's
// pieces are ever buffered.
type pendingBlob struct {
	key    []byte
	seq    uint64
	desc   blobDescriptor
	pieces [][]byte
	filled int
}

func stashBlobDescriptor(pending map[string]*pendingBlob, key []byte, seq uint64, desc *blobDescriptor) error {
	if desc.count != (desc.bytes+blobPiece-1)/blobPiece || desc.bytes > blobMax || desc.bytes <= 0 {
		return fmt.Errorf("murmur: blob descriptor bounds")
	}
	id := hashHex(key)
	if _, dup := pending[id]; dup {
		return fmt.Errorf("murmur: duplicate blob descriptor")
	}
	pending[id] = &pendingBlob{key: append([]byte(nil), key...), seq: seq,
		desc: *desc, pieces: make([][]byte, desc.count)}
	return nil
}

// stashBlobPiece files one piece; when its row completes it returns
// the reassembled value with done=true. Pieces whose descriptor is
// not yet known are parked in orphans (nil drops them); digest
// mismatches (stale pieces) are always ignored.
func stashBlobPiece(pending map[string]*pendingBlob, orphans map[string][]byte, pieceKey, piece []byte) (key, value []byte, seq uint64, done bool, err error) {
	rest, ok := strings.CutPrefix(string(pieceKey), recoveryBlobPrefix)
	if !ok {
		return nil, nil, 0, false, nil
	}
	parts := strings.Split(rest, "/")
	if len(parts) != 3 {
		return nil, nil, 0, false, nil
	}
	pend, ok := pending[parts[0]]
	if !ok {
		if orphans != nil {
			orphans[string(pieceKey)] = append([]byte(nil), piece...)
		}
		return nil, nil, 0, false, nil
	}
	if parts[1] != hexEncode(pend.desc.digest[:]) {
		return nil, nil, 0, false, nil
	}
	index, aerr := strconv.Atoi(parts[2])
	if aerr != nil || index < 0 || index >= pend.desc.count || pend.pieces[index] != nil {
		return nil, nil, 0, false, fmt.Errorf("murmur: bad blob piece")
	}
	pend.pieces[index] = append([]byte(nil), piece...)
	pend.filled++
	if pend.filled < pend.desc.count {
		return nil, nil, 0, false, nil
	}
	out := make([]byte, 0, pend.desc.bytes)
	for i := 0; i < pend.desc.count; i++ {
		want := blobPiece
		if rest := pend.desc.bytes - i*blobPiece; rest < want {
			want = rest
		}
		if len(pend.pieces[i]) != want {
			return nil, nil, 0, false, fmt.Errorf("murmur: blob piece length")
		}
		out = append(out, pend.pieces[i]...)
	}
	if hash(out) != pend.desc.digest {
		return nil, nil, 0, false, fmt.Errorf("murmur: blob digest")
	}
	delete(pending, parts[0])
	return pend.key, out, pend.seq, true, nil
}
