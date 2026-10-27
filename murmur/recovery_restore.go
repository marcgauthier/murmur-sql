package murmur

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync/atomic"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql/murmur/recovery"
	"github.com/marcgauthier/murmur-sql/murmur/spool"
)

// RecoveryOutcome names how an open restored SQLite state.
type RecoveryOutcome int

const (
	// RecoveryLegacyDisabled served the legacy path with the feature off.
	RecoveryLegacyDisabled RecoveryOutcome = iota
	// RecoveryLegacyFallback served the legacy path after the cache
	// proved missing, corrupt, or incompatible.
	RecoveryLegacyFallback
	// RecoveryCacheHit served the fast cache path.
	RecoveryCacheHit
	// RecoveryFailed marks an authoritative failure (no SQLite state).
	RecoveryFailed
)

// String renders the outcome name.
func (o RecoveryOutcome) String() string {
	switch o {
	case RecoveryLegacyDisabled:
		return "disabled"
	case RecoveryLegacyFallback:
		return "legacy-fallback"
	case RecoveryCacheHit:
		return "cache-hit"
	case RecoveryFailed:
		return "failed"
	}
	return "unknown"
}

// RecoveryRestoreOutcome details one fast-or-legacy restore.
type RecoveryRestoreOutcome struct {
	Outcome     RecoveryOutcome
	Reason      string // cache miss reason; set on legacy-fallback
	Generation  uint64
	ChangedRows uint64
	TotalRows   uint64
	// CacheBytes, CaptureCursor and CacheCreatedUnixNano describe the
	// generation used on a cache hit; zero otherwise.
	CacheBytes           uint64
	CaptureCursor        uint64
	CacheCreatedUnixNano uint64
}

// RecoveryStatus is the Status-surface restore summary.
type RecoveryStatus struct {
	Outcome     string
	Reason      string
	Generation  uint64
	ChangedRows uint64
	TotalRows   uint64
	// CacheBytes and CaptureCursor describe the generation used on a
	// cache hit. CacheAgeSeconds is the generation's age at open.
	CacheBytes      uint64
	CaptureCursor   uint64
	CacheAgeSeconds float64
	// LastBuild is the latest background build status, refreshed on
	// every Status call. Nil when the scheduler never ran.
	LastBuild *RecoveryBuildStatus
}

// recoveryFallbackTotal counts cache misses that routed to legacy
// recovery in this process.
var recoveryFallbackTotal atomic.Uint64

// RecoveryFallbackTotal reports how many opens fell back to legacy
// recovery because the cache was missing or unusable.
func RecoveryFallbackTotal() uint64 {
	return recoveryFallbackTotal.Load()
}

// recoveryStaleThreshold is the changed-row ratio above which a
// cache-hit open still queues a rebuild.
const recoveryStaleThreshold = 0.25

// recoveryNeedsBuild reports whether an open outcome warrants a
// background rebuild: every fallback, plus cache hits whose overlay
// grew past the stale threshold.
func recoveryNeedsBuild(o RecoveryRestoreOutcome) bool {
	if o.Outcome != RecoveryCacheHit {
		return true
	}
	if o.TotalRows == 0 {
		return false
	}
	return float64(o.ChangedRows)/float64(o.TotalRows) > recoveryStaleThreshold
}

// fastRestoreError marks cache-local failures eligible for legacy
// fallback. Anything else is authoritative and must surface.
type fastRestoreError struct{ err error }

func (e *fastRestoreError) Error() string { return e.err.Error() }
func (e *fastRestoreError) Unwrap() error { return e.err }

// RecoverFastOrLegacy restores SQLite state through the recovery cache
// when enabled and usable, else through RecoverCurrent. Cache
// failures (missing, corrupt, incompatible, unreadable key, oversized
// overlay) fall back transparently; authoritative Spool failures never
// mask as cache misses.
func RecoverFastOrLegacy(opened *Opened, cfg *RecoveryOptions, recoveryDir string, progress *OpenProgressReporter) (*sqlite.Conn, RecoveryRestoreOutcome, error) {
	if !cfg.IsEnabled() {
		conn, err := RecoverCurrent(opened, progress)
		return conn, RecoveryRestoreOutcome{Outcome: RecoveryLegacyDisabled}, err
	}
	conn, outcome, err := recoverFast(opened, cfg.WithDefaults(), recoveryDir, progress)
	if err == nil {
		return conn, outcome, nil
	}
	var fastErr *fastRestoreError
	if !errors.As(err, &fastErr) {
		return nil, RecoveryRestoreOutcome{Outcome: RecoveryFailed}, err
	}
	conn, legacyErr := RecoverCurrent(opened, progress)
	recoveryFallbackTotal.Add(1)
	return conn, RecoveryRestoreOutcome{Outcome: RecoveryLegacyFallback, Reason: fastErr.Error()}, legacyErr
}

// recoverFast validates the best cache generation, reconciles it
// against current Spool winners, and streams SQLite state. All cache
// problems return *fastRestoreError; authoritative Spool errors
// return bare.
func recoverFast(opened *Opened, opts RecoveryOptions, recoveryDir string, progress *OpenProgressReporter) (*sqlite.Conn, RecoveryRestoreOutcome, error) {
	var zero RecoveryRestoreOutcome
	fail := func(err error) (*sqlite.Conn, RecoveryRestoreOutcome, error) {
		return nil, zero, &fastRestoreError{err: err}
	}
	diffStart := time.Now()
	digest := sha256.Sum256(EncodeSchema(opened.Schema))
	encrypted := len(opened.Opts.MasterKey) > 0 || opened.Opts.Passphrase != ""
	gen, err := recovery.Discover(recoveryDir, opened.Identity.Database, restoreOpener(opened.Store, encrypted))
	if err != nil {
		return fail(err)
	}
	// Conservative v1 policy: exact schema match or legacy.
	if gen.Manifest.SchemaDigest != digest {
		return fail(fmt.Errorf("recovery: schema changed since capture"))
	}
	if gen.Manifest.RowCount != indexRowCount(gen.Manifest) || gen.Manifest.RowCount != runRowCount(gen.Manifest) {
		return fail(fmt.Errorf("recovery: manifest row counts disagree"))
	}
	current, err := opened.Store.LiveKeyVersions([]byte(recoveryRowPrefix))
	if err != nil {
		return nil, zero, err // authoritative: do not mask
	}
	overlay, err := reconcileKeys(gen, current, opened.Store, opts.MaxBuildMemory)
	if err != nil {
		return fail(err)
	}
	progress.RecordStage(StageRecoveryDiff, time.Since(diffStart))
	conn, totalRows, err := loadSQLiteFromCache(opened, gen, overlay, progress)
	if err != nil {
		return nil, zero, err // already *fastRestoreError from the loader
	}
	return conn, RecoveryRestoreOutcome{
		Outcome:              RecoveryCacheHit,
		Generation:           gen.Manifest.GenerationID,
		ChangedRows:          uint64(len(overlay)),
		TotalRows:            totalRows,
		CacheBytes:           manifestBytes(gen.Manifest),
		CaptureCursor:        gen.Manifest.CaptureCursor,
		CacheCreatedUnixNano: gen.Manifest.CreatedUnixNano,
	}, nil
}

func manifestBytes(m *recovery.Manifest) uint64 {
	var total uint64
	for i := range m.Files {
		total += m.Files[i].Size
	}
	return total
}

func indexRowCount(m *recovery.Manifest) uint64 {
	var total uint64
	for i := range m.Files {
		if m.Files[i].Type == recovery.FileIndex {
			total += m.Files[i].Rows
		}
	}
	return total
}

func runRowCount(m *recovery.Manifest) uint64 {
	var total uint64
	for i := range m.Files {
		if m.Files[i].Type == recovery.FileRun {
			total += m.Files[i].Rows
		}
	}
	return total
}

// restoreOpener resolves cache ciphers from the live store's retained
// data keys. Unknown (pruned) references fail so the generation is
// skipped, never failed open.
func restoreOpener(store *LogicalStore, encrypted bool) *recovery.Opener {
	return &recovery.Opener{
		Less: recoveryRowLess,
		CipherFor: func(ref string, enc bool) (*recovery.Cipher, error) {
			if !enc {
				if encrypted {
					return nil, fmt.Errorf("recovery: plain cache on encrypted store")
				}
				return recovery.NewCipher(nil)
			}
			if !encrypted {
				return nil, fmt.Errorf("recovery: encrypted cache on plain store")
			}
			id, err := parseRecoveryKeyRef(ref)
			if err != nil {
				return nil, err
			}
			key, err := store.Physical().RecoveryKey(id)
			if err != nil {
				return nil, err
			}
			defer func() {
				for i := range key {
					key[i] = 0
				}
			}()
			return recovery.NewCipher(key[:])
		},
	}
}

// overlayEntry is one reconciled row: current metadata, or a deletion
// marker for cached-only keys.
type overlayEntry struct {
	table   string
	meta    *RowMeta
	deleted bool
}

// reconcileKeys merge-joins the cached key index against current
// Spool winners. Same key and sequence reuses cached bytes with no
// value read; anything else resolves to the current RowMeta (or a
// deletion) in the overlay. The overlay is capped by budgetBytes;
// beyond it the caller falls back to legacy.
func reconcileKeys(gen *recovery.Generation, current []spool.LiveKeyVersion, store *LogicalStore, budgetBytes int64) (map[string]*overlayEntry, error) {
	fail := func(err error) (map[string]*overlayEntry, error) {
		return nil, &fastRestoreError{err: err}
	}
	is, err := gen.OpenIndex()
	if err != nil {
		return fail(err)
	}
	defer is.Close()
	overlay := map[string]*overlayEntry{}
	var overlayBytes int64
	var cachedTotal uint64
	charge := func(n int64) error {
		overlayBytes += n
		if budgetBytes > 0 && overlayBytes > budgetBytes {
			return &fastRestoreError{err: fmt.Errorf("recovery: overlay exceeds memory budget")}
		}
		return nil
	}
	fetch := func(key []byte) (*overlayEntry, error) {
		table, err := parseRecoveryRowTable(key)
		if err != nil {
			return nil, &fastRestoreError{err: err}
		}
		raw, err := store.Get(key)
		if err != nil {
			return nil, &fastRestoreError{err: err}
		}
		if raw == nil {
			return nil, &fastRestoreError{err: fmt.Errorf("recovery: missing row record")}
		}
		meta, err := DecodeRowMeta(raw)
		if err != nil {
			return nil, &fastRestoreError{err: err}
		}
		if err := charge(int64(len(key) + len(raw) + 64)); err != nil {
			return nil, err
		}
		return &overlayEntry{table: table, meta: &meta}, nil
	}
	nextCached := func() (*recovery.IndexEntry, error) {
		e, err := is.Next()
		if err == io.EOF {
			return nil, nil
		}
		return e, err
	}
	cached, err := nextCached()
	if err != nil {
		return fail(err)
	}
	i := 0
	for cached != nil || i < len(current) {
		var c int
		switch {
		case cached == nil:
			c = 1
		case i >= len(current):
			c = -1
		default:
			c = bytes.Compare(cached.Key, current[i].Key)
		}
		switch {
		case c == 0:
			cachedTotal++
			if cached.Seq != current[i].Sequence {
				e, err := fetch(current[i].Key)
				if err != nil {
					return nil, err
				}
				overlay[string(current[i].Key)] = e
			}
			cached, err = nextCached()
			if err != nil {
				return fail(err)
			}
			i++
		case c < 0:
			cachedTotal++
			if err := charge(int64(len(cached.Key) + 64)); err != nil {
				return nil, err
			}
			overlay[string(cached.Key)] = &overlayEntry{deleted: true}
			cached, err = nextCached()
			if err != nil {
				return fail(err)
			}
		default:
			e, err := fetch(current[i].Key)
			if err != nil {
				return nil, err
			}
			overlay[string(current[i].Key)] = e
			i++
		}
	}
	if cachedTotal != gen.Manifest.RowCount {
		return fail(fmt.Errorf("recovery: index holds %d rows, manifest claims %d", cachedTotal, gen.Manifest.RowCount))
	}
	return overlay, nil
}

// parseRecoveryRowTable extracts the table id from a row Spool key.
func parseRecoveryRowTable(key []byte) (string, error) {
	rest, ok := bytes.CutPrefix(key, []byte(recoveryRowPrefix))
	if !ok {
		return "", fmt.Errorf("murmur: bad row key %q", key)
	}
	table, _, ok := bytes.Cut(rest, []byte("/"))
	if !ok || len(table) == 0 || len(table) > recovery.MaxTableIDLen {
		return "", fmt.Errorf("murmur: bad row table %q", key)
	}
	return string(table), nil
}

// loadSQLiteFromCache rebuilds :memory: SQLite by streaming cached
// runs (skipping overlay-covered rows) plus current overlay rows. It
// mirrors RecoverCurrent's DDL, insert validation, progress, index,
// and integrity sequence exactly; any failure closes the partial
// connection for legacy fallback.
func loadSQLiteFromCache(opened *Opened, gen *recovery.Generation, overlay map[string]*overlayEntry, progress *OpenProgressReporter) (*sqlite.Conn, uint64, error) {
	progress.Phase(PhaseRebuilding)
	c, err := sqlite.OpenMemory()
	if err != nil {
		return nil, 0, &fastRestoreError{err: sqlErr(err)}
	}
	if err := RegisterKeyFunctions(c); err != nil {
		c.Close()
		return nil, 0, &fastRestoreError{err: err}
	}
	if err := c.ExecBatch("BEGIN"); err != nil {
		c.Close()
		return nil, 0, &fastRestoreError{err: sqlErr(err)}
	}
	fail := func(err error) (*sqlite.Conn, uint64, error) {
		c.Close()
		return nil, 0, &fastRestoreError{err: err}
	}
	tables := make(map[string]*Table, len(opened.Schema.Tables))
	for i := range opened.Schema.Tables {
		tables[opened.Schema.Tables[i].ID] = &opened.Schema.Tables[i]
	}
	insertStart := time.Now()
	for _, table := range opened.Schema.Tables {
		if err := c.ExecBatch(table.SQL); err != nil {
			return fail(sqlErr(err))
		}
	}
	var totalRows uint64
	var stmt *sqlite.Stmt
	var stmtTable string
	defer func() {
		if stmt != nil {
			stmt.Close()
		}
	}()
	// SetTable follows cache table order (sorted by id), which may
	// differ from schema order on the legacy path. Phases and row
	// counts match exactly.
	useTable := func(tableID string) (*Table, error) {
		table, ok := tables[tableID]
		if !ok {
			return nil, corruptErr("unknown table %q", tableID)
		}
		if stmt == nil || stmtTable != tableID {
			if stmt != nil {
				stmt.Close()
			}
			s, err := c.Prepare(RowInsertSQL(table))
			if err != nil {
				stmt = nil
				return nil, sqlErr(err)
			}
			stmt = s
			stmtTable = tableID
			progress.SetTable(table.Name)
		}
		return table, nil
	}
	closeStmt := func() {
		if stmt != nil {
			stmt.Close()
			stmt = nil
		}
	}
	insert := func(tableID string, spoolKey []byte, meta *RowMeta) error {
		table, err := useTable(tableID)
		if err != nil {
			closeStmt()
			return err
		}
		if err := meta.ExtendNullable(table); err != nil {
			closeStmt()
			return err
		}
		primary, err := DecodeValues(meta.Key)
		if err != nil {
			closeStmt()
			return err
		}
		if !bytes.Equal(RowKey(table.ID, EncodeValues(primary)), spoolKey) {
			closeStmt()
			return corruptErr("metadata key collision")
		}
		values, err := CellValues(table, meta.Cells)
		if err != nil {
			closeStmt()
			return err
		}
		args := make([]any, len(values))
		for i, v := range values {
			args[i] = sqlArg(v)
		}
		if err := stmt.Bind(args...); err != nil {
			closeStmt()
			return sqlErr(err)
		}
		if _, err := stmt.Step(); err != nil {
			closeStmt()
			return sqlErr(err)
		}
		if err := stmt.Reset(); err != nil {
			closeStmt()
			return sqlErr(err)
		}
		progress.AddRows(1, 0)
		totalRows++
		return nil
	}
	rs, err := gen.OpenRuns()
	if err != nil {
		return fail(err)
	}
	streamErr := func() error {
		for {
			r, err := rs.Next()
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
			if _, ok := overlay[string(r.Key)]; ok {
				continue // deleted, or applied from current meta below
			}
			meta, err := DecodeRowMeta(r.Data)
			if err != nil {
				return err
			}
			if err := insert(r.Table, r.Key, &meta); err != nil {
				return err
			}
		}
	}()
	cerr := rs.Close()
	if streamErr != nil {
		return fail(streamErr)
	}
	if cerr != nil {
		return fail(cerr)
	}
	// Overlay rows, table-grouped and PK-ordered for sequential insert.
	type overlayRow struct {
		table string
		key   []byte
		meta  *RowMeta
	}
	var pending []overlayRow
	for key, e := range overlay {
		if e.deleted {
			continue
		}
		pending = append(pending, overlayRow{table: e.table, key: []byte(key), meta: e.meta})
	}
	sort.Slice(pending, func(i, j int) bool {
		a := &recovery.Row{Table: pending[i].table, Sort: pending[i].meta.Key, Key: pending[i].key}
		b := &recovery.Row{Table: pending[j].table, Sort: pending[j].meta.Key, Key: pending[j].key}
		return recoveryRowLess(a, b)
	})
	for i := range pending {
		if err := insert(pending[i].table, pending[i].key, pending[i].meta); err != nil {
			return fail(err)
		}
	}
	if stmt != nil {
		stmt.Close()
		stmt = nil
	}
	progress.RecordStage(StageRowInsert, time.Since(insertStart))
	progress.Phase(PhaseIndexing)
	indexStart := time.Now()
	for _, sql := range opened.Schema.Indexes {
		if err := c.ExecBatch(sql); err != nil {
			return fail(sqlErr(err))
		}
	}
	if err := c.ExecBatch("COMMIT"); err != nil {
		return fail(sqlErr(err))
	}
	progress.RecordStage(StageIndexBuild, time.Since(indexStart))
	checkStart := time.Now()
	row, err := c.QueryRow("PRAGMA integrity_check")
	if err != nil {
		return fail(sqlErr(err))
	}
	progress.RecordStage(StageIntegrityCheck, time.Since(checkStart))
	if len(row) == 0 || row[0].Str != "ok" {
		check := ""
		if len(row) > 0 {
			check = row[0].Str
		}
		return fail(corruptErr("%s", check))
	}
	return c, totalRows, nil
}
