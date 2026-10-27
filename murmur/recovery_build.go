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
	collect := func(records []spool.Record) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		for i := range records {
			rec := &records[i]
			if rec.Deleted || !strings.HasPrefix(string(rec.Key), recoveryRowPrefix) {
				continue
			}
			row, err := recoveryRowFromRecord(rec.Key, rec.Value, rec.Sequence, tables)
			if err != nil {
				return err
			}
			if err := w.Add(row); err != nil {
				return err
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
