// Package spool is an embedded persistence engine optimized for very
// fast concurrent writes.
//
// There is deliberately no Get: the application keeps current values
// in RAM, spool durably journals every mutation and rebuilds state at
// startup through Load callbacks. Writes are sharded in memory,
// sealed into blocks, compressed and encrypted in parallel, and
// appended sequentially to segment files.
//
// Basic use:
//
//	store, err := spool.OpenAndLoad(spool.Options{
//	    Path:      "./data",
//	    MasterKey: masterKey, // 32 bytes
//	}, func(records []spool.Record) error {
//	    // populate application memory; each key arrives once
//	    // with its current version (tombstone if deleted)
//	    return nil
//	})
//	if err != nil { ... }
//	defer store.Close()
//
//	store.Put([]byte("k"), []byte("v"))
//	store.Delete([]byte("old"))
//	store.Flush() // explicit durability point
package spool

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Sequence layout: 16-bit epoch in the high bits, 48-bit counter
// below. The epoch in the manifest is bumped on every Open, so
// sequences never repeat across restarts even when buffered writes
// were lost to a crash.
const (
	epochShift    = 48
	seqCounterMax = uint64(1) << epochShift
	lockFileName  = "LOCK"
)

// errNoCurrentKey reports a missing current data key. Internal: Open
// and RotateKey guarantee the invariant, so callers only see it on
// corruption.
var errNoCurrentKey = errors.New("spool: no current data key")

// Store is an open spool database.
type Store struct {
	opts Options
	dir  string // store root; segments live in dir/segments
	seg  *segmentWriter

	// Sequence state.
	seqBase uint64 // epoch << epochShift
	seqNext atomic.Uint64

	// Write buffering.
	shards         []writeShard
	shardMask      uint64
	pendingBytes   atomic.Int64
	pendingRecords atomic.Int64
	bpMu           sync.Mutex
	bpCond         *sync.Cond

	// Group admission. admissionMu serializes Commit, flush seals
	// and compaction admissions in FIFO order; sequences and group
	// ids follow admission order. It is also the checkpoint-capture
	// exclusion mutex: capture holds it from the fence through the
	// final link, while rotation, compaction publication, and
	// maintenance start take it briefly, so no publication can
	// interleave a captured cut. Lock order:
	// admissionMu -> sealMu, admissionMu -> queueMu,
	// admissionMu -> manifestMu, admissionMu -> rotateMu,
	// admissionMu -> maintExcl.
	admissionMu sync.Mutex
	sealMu      sync.Mutex
	groupSeq    atomic.Uint64 // last admitted group id
	lastGroup   atomic.Uint64 // last admitted group id (fence target)
	// stagedCount counts buffered individual writes; every shard
	// append increments it inside the shard section, before the
	// bytes land. Seals record the pre-collect count in
	// lastSealed (admissionMu-guarded): a later seal re-collects
	// unless the counter is unchanged, which orders buffered
	// writes before subsequently admitted groups without putting
	// admissionMu on the Put hot path.
	stagedCount atomic.Uint64
	lastSealed  uint64

	// Ordered commit queue, drained by commitLoop.
	queueMu          sync.Mutex
	commitCond       *sync.Cond
	queue            []*commitGroup
	completedThrough uint64 // under queueMu
	drained          bool   // Close finished admitting (under queueMu)

	// Terminal storage failure: sticky cause plus an atomic flag so
	// wait loops can predicate on it without lock nesting. Lock
	// order: storageMu -> queueMu, storageMu -> bpMu.
	storageMu  sync.Mutex
	storageErr error
	terminal   atomic.Bool
	faults     *FaultHooks

	// Pipeline.
	comp    *compressor
	crypt   blockCrypt
	ring    *keyring // nil when unencrypted
	master  []byte   // copy for keys.enc rewrites; nil with passphrase
	pass    string
	ctx     [16]byte // database context binding block AAD and KDF
	wrapID  string   // authenticated wrapping-key id ("" when plain)
	idx     *index
	fstats  *fileStats
	workers int

	// Hot block cache for point reads. Disposable decoded data;
	// the segments stay authoritative.
	readCacheMu sync.Mutex
	readCache   *readCache

	nextBlockID   atomic.Uint64
	appendedGroup atomic.Uint64 // last appended group id (clean marker)

	// Manifest state.
	manifestMu sync.Mutex
	man        *manifest
	rotateMu   sync.Mutex

	// Key-reference tracking for pruning. sealKeys counts sealed
	// but unpublished frames per data key; ckptKeyRefs counts
	// registered checkpoint dependencies. ckptMu also guards the
	// checkpoint registry.
	sealKeyMu   sync.Mutex
	sealKeys    map[uint32]int
	ckptMu      sync.Mutex
	ckptKeyRefs map[uint32]int
	checkpoints map[string]*Checkpoint
	// sealQuiesceMu makes key pruning observe a quiescent seal
	// set: seals hold it shared from key acquisition through
	// untrack, and pruning holds it exclusively across its whole
	// scan-and-delete, so no seal can publish and untrack between
	// the segment scan and the reference snapshot. Pruning is an
	// explicit maintenance call; in-flight seals drain first and
	// new seals wait, so writes stall for one prune scan. Lock
	// order: maintExcl -> rotateMu -> sealQuiesceMu -> admissionMu
	// (compaction publication only), sealQuiesceMu -> manifestMu,
	// sealQuiesceMu -> ring.mu. The sealQuiesceMu -> admissionMu
	// edge is acyclic because admissionMu holders never wait on
	// sealQuiesceMu or rotateMu (maintBegin TryLocks maintExcl).
	sealQuiesceMu sync.RWMutex

	// Maintenance state for diagnostics and rewrite/reseed gating.
	// maintExcl is held across a whole operation (exclusivity, Close
	// barrier); maintMu guards only the short phase/progress
	// critical sections so diagnostics stay available mid-run.
	maintExcl   sync.Mutex
	maintMu     sync.Mutex
	maintPhase  string // "idle" when no maintenance runs
	maintDone   uint64
	maintTotal  uint64
	maintActive atomic.Bool

	// Background loops.
	flushCh chan struct{}
	stopCh  chan struct{}
	closeWG sync.WaitGroup

	lastFlush atomic.Int64

	// Stats counters.
	blocksWritten     atomic.Uint64
	bytesWritten      atomic.Uint64
	bytesUncompressed atomic.Uint64
	compactions       atomic.Uint64
	compactionBytes   atomic.Uint64
	flushErrors       atomic.Uint64
	truncatedTails    atomic.Uint64
	reclaimErrors     atomic.Uint64
	groupsAccepted    atomic.Uint64
	groupsWritten     atomic.Uint64
	groupsSynced      atomic.Uint64
	groupsDiscarded   atomic.Uint64
	pendingGroups     atomic.Int64
	commits           atomic.Uint64
	syncs             atomic.Uint64
	storageFailures   atomic.Uint64
	rotations         atomic.Uint64
	commitNanos       atomic.Uint64
	syncNanos         atomic.Uint64
	flushes           atomic.Uint64
	flushNanos        atomic.Uint64
	reclaims          atomic.Uint64
	reclaimNanos      atomic.Uint64
	compactionNanos   atomic.Uint64
	flushErrMu        sync.Mutex
	lastFlushErr      error
	lastReclaimErr    error

	// reclaimMu serializes Reclaim passes.
	reclaimMu sync.Mutex

	closed atomic.Bool
	unlock func()
}

// checkTerminal reports ErrStorageFailed once the store has failed
// terminally, nil otherwise.
func (s *Store) checkTerminal() error {
	if !s.terminal.Load() {
		return nil
	}
	s.storageMu.Lock()
	defer s.storageMu.Unlock()
	if s.storageErr == nil {
		return ErrStorageFailed
	}
	return fmt.Errorf("spool: %v: %w", s.storageErr, ErrStorageFailed)
}

// StorageError returns the terminal storage failure cause, or nil
// when the store is healthy.
func (s *Store) StorageError() error {
	s.storageMu.Lock()
	defer s.storageMu.Unlock()
	return s.storageErr
}

// setTerminal records a terminal storage failure, wakes every
// waiter, and notifies OnStorageError once outside all locks. Only
// the first cause sticks.
func (s *Store) setTerminal(err error) {
	s.storageMu.Lock()
	if s.storageErr != nil {
		s.storageMu.Unlock()
		return
	}
	s.storageErr = err
	cb := s.opts.OnStorageError
	s.storageMu.Unlock()
	s.terminal.Store(true)
	s.storageFailures.Add(1)
	s.queueMu.Lock()
	s.commitCond.Broadcast()
	s.queueMu.Unlock()
	s.bpMu.Lock()
	s.bpCond.Broadcast()
	s.bpMu.Unlock()
	if cb != nil {
		cb(err)
	}
}

// Open opens (or creates) the store at opts.Path and rebuilds the
// in-memory index from disk. Use OpenAndLoad to also stream every
// record into application memory in the same scan.
func Open(opts Options) (*Store, error) {
	return openStore(opts, nil, false)
}

// openStore implements Open, OpenAndLoad and OpenAndLoadOrdered. A
// non-nil fn streams every scanned record to the caller during the
// rebuild, in key order when ordered.
func openStore(opts Options, fn LoadFunc, ordered bool) (*Store, error) {
	o, err := opts.normalized()
	if err != nil {
		return nil, err
	}
	s := &Store{
		opts:        o,
		dir:         o.Path,
		faults:      o.Faults,
		flushCh:     make(chan struct{}, 1),
		stopCh:      make(chan struct{}),
		sealKeys:    make(map[uint32]int),
		ckptKeyRefs: make(map[uint32]int),
		checkpoints: make(map[string]*Checkpoint),
		maintPhase:  "idle",
	}
	s.bpCond = sync.NewCond(&s.bpMu)
	s.commitCond = sync.NewCond(&s.queueMu)
	s.readCache = newReadCache(o.ReadCacheBytes)
	s.workers = o.Workers
	if s.workers <= 0 {
		s.workers = runtime.NumCPU()
	}
	if s.workers < 2 {
		s.workers = 2
	}
	if s.workers > 32 {
		s.workers = 32
	}
	if err := os.MkdirAll(filepath.Join(o.Path, segmentsDirName), 0o700); err != nil {
		return nil, fmt.Errorf("spool: create store dir: %w", err)
	}
	// The store root contains the encrypted key registry and segment tree.
	// Tighten existing directories as well as newly created ones because
	// callers may pre-create the data path with a broader umask or mode.
	if err := os.Chmod(o.Path, 0o700); err != nil {
		return nil, fmt.Errorf("spool: secure store dir: %w", err)
	}
	unlock, err := lockStore(o.Path)
	if err != nil {
		return nil, err
	}
	s.unlock = unlock
	// From here on, failures must release the lock.
	fail := func(err error) (*Store, error) {
		s.unlock()
		return nil, err
	}

	m, fresh, err := s.loadOrInitManifest()
	if err != nil {
		return fail(err)
	}
	if m.encrypted != (o.Encryption != EncryptionNone) {
		return fail(fmt.Errorf("spool: store encryption (manifest=%v) mismatches options (%s)",
			m.encrypted, o.Encryption))
	}
	if !fresh && m.encrypted {
		// Authenticate before cleanup, maintenance, or manifest
		// publication. The store lock prevents another process
		// changing the envelope between authentication and opening
		// the live store.
		ring, err := s.loadOrInitKeys(o, m)
		if err != nil && errors.Is(err, ErrWrongKey) {
			// A rebind crash between keyring and manifest
			// publication leaves the keyring carrying the target
			// context while the manifest still points at the old
			// one; retry under the intent's target so resume
			// stays reachable. A truly wrong key fails both
			// attempts before anything is modified.
			if r2, terr := s.loadKeysForResume(o, m); terr == nil {
				ring, err = r2, nil
			}
		}
		if err != nil {
			return fail(err)
		}
		if len(o.ContextID) != 0 && string(o.ContextID) != string(ring.context[:]) {
			return fail(fmt.Errorf("spool: key context mismatch: %w", ErrContextMismatch))
		}
		if o.WrappingKeyID != "" && o.WrappingKeyID != ring.wrapID {
			return fail(fmt.Errorf("spool: wrapping id mismatch: %w", ErrWrongKey))
		}
	}
	segDir := filepath.Join(o.Path, segmentsDirName)
	if !fresh {
		// Clear crashed staging and unlisted segments before a
		// possible maintenance resume reads the members.
		if err := sweepUnlisted(segDir, o.Path, m.members, s.faults); err != nil {
			return fail(err)
		}
	}
	// Resume an interrupted rewrite/rebind to completion before
	// any verification or writer state initializes; without an
	// intent this is a no-op. Reload afterwards: resume may have
	// published the manifest and keyring.
	if err := resumeMaintenance(o.Path, o.MasterKey, o.Passphrase, o.Compression, o.allCodecs(), s.workers, o.Faults); err != nil {
		return fail(err)
	}
	if mraw, err := os.ReadFile(filepath.Join(o.Path, manifestFileName)); err != nil {
		return fail(fmt.Errorf("spool: reread manifest: %w", err))
	} else if m, err = parseManifest(mraw); err != nil {
		return fail(err)
	}
	// A provided context must match the persisted one before any
	// writer state initializes. Fresh stores adopt the provided
	// context (or the store id) at creation. The comparison runs
	// against post-resume state so a rebind crash reopens with the
	// target context in one step.
	if !fresh && len(o.ContextID) == 16 && string(o.ContextID) != string(m.context[:]) {
		return fail(fmt.Errorf("spool: supplied database context does not match store: %w", ErrContextMismatch))
	}
	// A clean marker promises an exact on-disk shape; remember it
	// before this open dirties the manifest.
	verifyClean := !fresh && m.clean
	want := cleanMarker{bytes: m.cleanBytes, files: m.cleanFiles, gen: m.cleanGen, group: m.cleanGroup}
	m.clean = false
	m.cleanBytes = 0
	m.cleanFiles = 0
	m.cleanGen = 0
	m.cleanGroup = 0
	// Bump the epoch first (the manifest persist itself is deferred
	// until the load phase below succeeds, so a refused or failed
	// open leaves the store content-untouched; it still lands before
	// any data handling).
	useEpoch := m.nextEpoch
	m.nextEpoch++
	if m.nextEpoch == 0 {
		return fail(fmt.Errorf("spool: epoch space exhausted after 65535 opens"))
	}
	s.man = m
	s.seqBase = uint64(useEpoch) << epochShift

	if m.encrypted {
		ring, err := s.loadOrInitKeys(o, m)
		if err != nil {
			return fail(err)
		}
		s.ring = ring
		s.master = append([]byte(nil), o.MasterKey...)
		s.pass = o.Passphrase
		s.ctx = ring.context
		s.wrapID = ring.wrapID
		if _, ok := ring.byID(m.currentKeyID); !ok {
			return fail(fmt.Errorf("spool: manifest key id %d missing from keys.enc: %w", m.currentKeyID, ErrCorrupt))
		}
		if o.WrappingKeyID != "" && o.WrappingKeyID != ring.wrapID {
			return fail(fmt.Errorf("spool: wrapping-key id %q != envelope %q: %w",
				o.WrappingKeyID, ring.wrapID, ErrWrongKey))
		}
		// Age rotation on open (single-threaded: no coordinator
		// locks needed): an expired current key rotates before
		// any write can reference it.
		if !s.keyFresh() {
			s.rotateMu.Lock()
			err := s.rotateDataKey()
			s.rotateMu.Unlock()
			if err != nil {
				return fail(err)
			}
		}
	} else {
		s.ctx = m.context
	}
	crypt, err := blockCryptFor(o.Encryption)
	if err != nil {
		return fail(err)
	}
	s.crypt = crypt
	comp, err := newCompressor(o.Compression, o.allCodecs())
	if err != nil {
		return fail(err)
	}
	s.comp = comp
	s.idx = newIndex(o.IndexShards)
	s.fstats = newFileStats()
	s.shards = make([]writeShard, o.WriteShards)
	s.shardMask = uint64(o.WriteShards - 1)

	// The manifest member list is authoritative (already swept
	// above, before resume). Listed members that are missing fail
	// the open.
	ids := append([]uint64(nil), m.members...)
	if fresh {
		// The manifest was just created with member 1, but no
		// segment file exists yet; the writer creates it below.
		ids = nil
	} else {
		for _, id := range ids {
			if _, err := os.Stat(filepath.Join(segDir, segmentFileName(id))); err != nil {
				return fail(fmt.Errorf("spool: member segment %d: %w: %w", id, err, ErrCorrupt))
			}
		}
	}
	var maxID uint64
	for _, id := range ids {
		s.fstats.register(id, segmentModTime(segDir, id))
		if id > maxID {
			maxID = id
		}
	}
	nextFile := maxID + 1
	if nextFile < m.nextFileID {
		nextFile = m.nextFileID
	}
	if nextFile == 0 {
		nextFile = 1
	}
	// Rebuild index and statistics from disk before serving writes.
	maxBlock, maxGroup, ends, discarded, err := s.rebuild(fn, ids, ordered)
	if err != nil {
		return fail(err)
	}
	s.groupsDiscarded.Add(discarded)
	if verifyClean {
		if err := checkCleanMarker(segDir, ids, m.generation, want, maxGroup); err != nil {
			return fail(err)
		}
	}
	// The load callback and clean verification above accepted the
	// store, so publish the bumped epoch now, still before any data
	// handling (tail truncation and writer open below). A refused
	// open returns before this point and leaves every existing file
	// byte-identical.
	if err := s.persistManifest(m); err != nil {
		return fail(err)
	}
	if next := maxBlock + 1; next > m.nextBlockID {
		s.nextBlockID.Store(next)
	} else {
		s.nextBlockID.Store(m.nextBlockID)
	}
	// Group ids repair like block ids: persisted next id wins
	// unless the scan saw further. Everything at or below the last
	// used id is trivially complete, so fences on this session's
	// first (possibly empty) seal return immediately.
	if next := maxGroup + 1; next > m.nextGroupID {
		s.groupSeq.Store(next - 1)
	} else {
		s.groupSeq.Store(m.nextGroupID - 1)
	}
	s.lastGroup.Store(s.groupSeq.Load())
	s.queueMu.Lock()
	s.completedThrough = s.groupSeq.Load()
	s.queueMu.Unlock()
	s.appendedGroup.Store(maxGroup)
	active := maxID
	if active == 0 {
		active = 1
		nextFile = 2
		s.fstats.register(1, time.Now())
	}
	// Restore lifecycle states: registration leaves the zero value
	// (Active) everywhere, which would exempt every old segment
	// from compaction. Only the append target stays active.
	for _, id := range s.fstats.ids() {
		if id == active {
			s.fstats.setState(id, SegmentActive)
		} else {
			s.fstats.setState(id, SegmentSealed)
		}
	}
	// Truncate a torn tail on the active segment before any new
	// append could bury it mid-file.
	if err := s.truncateActive(active, ends[active]); err != nil {
		return fail(err)
	}
	seg, err := openSegmentWriter(segDir, o.MaxSegmentSize, active, o.Faults)
	if err != nil {
		return fail(err)
	}
	seg.onRotate = s.onRotate
	s.seg = seg
	s.man.nextFileID = nextFile
	s.man.activeFileID = active
	s.man.nextGroupID = s.groupSeq.Load() + 1
	if err := s.persistManifest(s.man); err != nil {
		seg.close()
		return fail(err)
	}

	s.lastFlush.Store(time.Now().UnixNano())
	s.closeWG.Add(1)
	go s.commitLoop()
	s.closeWG.Add(1)
	go s.flushLoop()
	if o.ReclaimInterval > 0 {
		s.closeWG.Add(1)
		go s.reclaimLoop()
	}
	return s, nil
}

// sweepUnlisted removes unreferenced segment files (present on disk,
// absent from the authoritative member list) and stale temp files
// from crashed atomic writes. The caller must hold the store lock.
func sweepUnlisted(segDir, root string, members []uint64, faults *FaultHooks) error {
	want := make(map[uint64]bool, len(members))
	for _, id := range members {
		want[id] = true
	}
	entries, err := os.ReadDir(segDir)
	if err != nil {
		return fmt.Errorf("spool: list segments: %w", err)
	}
	removed := false
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() {
			continue
		}
		if id, ok := parseSegmentName(name); ok {
			if !want[id] {
				if err := faults.trip("delete"); err != nil {
					return fmt.Errorf("spool: remove unlisted segment %s: %w", name, err)
				}
				if err := os.Remove(filepath.Join(segDir, name)); err != nil && !os.IsNotExist(err) {
					return fmt.Errorf("spool: remove unlisted segment %s: %w", name, err)
				}
				removed = true
			}
			continue
		}
		if strings.HasPrefix(name, ".") && strings.Contains(name, ".tmp-") {
			if err := faults.trip("delete"); err != nil {
				return fmt.Errorf("spool: remove temp file %s: %w", name, err)
			}
			if err := os.Remove(filepath.Join(segDir, name)); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("spool: remove temp file %s: %w", name, err)
			}
			removed = true
		}
	}
	if removed {
		if err := faults.trip("dirsync"); err != nil {
			return fmt.Errorf("spool: sync segments dir: %w", err)
		}
		if err := dirSync(segDir); err != nil {
			return fmt.Errorf("spool: sync segments dir: %w", err)
		}
	}
	roots, err := os.ReadDir(root)
	if err != nil {
		return fmt.Errorf("spool: list store dir: %w", err)
	}
	for _, e := range roots {
		name := e.Name()
		if !e.IsDir() && strings.HasPrefix(name, ".") && strings.Contains(name, ".tmp-") {
			if err := faults.trip("delete"); err != nil {
				return fmt.Errorf("spool: remove temp file %s: %w", name, err)
			}
			if err := os.Remove(filepath.Join(root, name)); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("spool: remove temp file %s: %w", name, err)
			}
			if err := faults.trip("dirsync"); err != nil {
				return fmt.Errorf("spool: sync store dir: %w", err)
			}
			if err := dirSync(root); err != nil {
				return fmt.Errorf("spool: sync store dir: %w", err)
			}
		}
	}
	return nil
}

// loadOrInitManifest reads the manifest or creates a fresh store.
func (s *Store) loadOrInitManifest() (*manifest, bool, error) {
	path := filepath.Join(s.dir, manifestFileName)
	raw, err := os.ReadFile(path)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, false, fmt.Errorf("spool: read manifest: %w", err)
		}
		id, err := newStoreID()
		if err != nil {
			return nil, false, err
		}
		m := &manifest{
			encrypted:    s.opts.Encryption != EncryptionNone,
			storeID:      id,
			context:      id, // standalone default; options override below
			nextFileID:   1,
			nextBlockID:  1,
			nextEpoch:    1,
			currentKeyID: 0,
			activeFileID: 1,
			generation:   1,
			nextGroupID:  1,
			members:      []uint64{1},
		}
		if len(s.opts.ContextID) == 16 {
			copy(m.context[:], s.opts.ContextID)
		}
		if m.encrypted {
			m.currentKeyID = 1
		}
		if err := s.persistManifest(m); err != nil {
			return nil, false, err
		}
		return m, true, nil
	}
	m, err := parseManifest(raw)
	if err != nil {
		return nil, false, err
	}
	return m, false, nil
}

// persistManifest atomically replaces the manifest.
func (s *Store) persistManifest(m *manifest) error {
	s.manifestMu.Lock()
	defer s.manifestMu.Unlock()
	return s.writeManifestFile(*m)
}

// assertManifestInvariants checks structural and semantic invariants of a manifest
// before publication.
func assertManifestInvariants(m *manifest) error {
	if !sort.SliceIsSorted(m.members, func(i, j int) bool { return m.members[i] < m.members[j] }) {
		return fmt.Errorf("spool: manifest members unsorted: %w", ErrCorrupt)
	}
	for i := 1; i < len(m.members); i++ {
		if m.members[i] == m.members[i-1] || m.members[i] == 0 {
			return fmt.Errorf("spool: manifest members invalid: %w", ErrCorrupt)
		}
	}
	if len(m.members) == 0 {
		return fmt.Errorf("spool: manifest has empty members: %w", ErrCorrupt)
	}
	found := false
	for _, id := range m.members {
		if id == m.activeFileID {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("spool: active segment %d missing from members: %w", m.activeFileID, ErrCorrupt)
	}
	return nil
}

// writeManifestFile publishes one manifest encoding. All manifest
// publications funnel through here so fault injection sees them.
func (s *Store) writeManifestFile(m manifest) error {
	if err := assertManifestInvariants(&m); err != nil {
		return err
	}
	if err := s.faults.trip("manifest"); err != nil {
		return fmt.Errorf("spool: injected manifest fault: %w", err)
	}
	return atomicWriteFile(s.dir, manifestFileName, m.encode())
}

// loadOrInitKeys opens keys.enc or creates it with a fresh data key.
// The manifest supplies the store id and database context binding
// the envelope; fresh envelopes adopt the manifest's context and the
// requested wrapping-key id.
// loadKeysForResume retries key unlock under a pending rebind
// intent's target context. It reports an error unless the retry
// applies and succeeds; callers keep their original error then.
func (s *Store) loadKeysForResume(o Options, m *manifest) (*keyring, error) {
	raw, err := os.ReadFile(filepath.Join(s.dir, intentFileName))
	if err != nil {
		return nil, err
	}
	intent, err := parseIntent(raw)
	if err != nil {
		return nil, err
	}
	if intent.op != maintOpRebind || intent.targetContext == m.context {
		return nil, ErrWrongKey
	}
	ring, err := readKeys(s.dir, o.MasterKey, o.Passphrase, m.storeID, intent.targetContext)
	if err != nil {
		return nil, err
	}
	ring.faults = o.Faults
	return ring, nil
}

func (s *Store) loadOrInitKeys(o Options, m *manifest) (*keyring, error) {
	if _, err := os.Stat(filepath.Join(s.dir, keysFileName)); err == nil {
		ring, err := readKeys(s.dir, o.MasterKey, o.Passphrase, m.storeID, m.context)
		if err != nil {
			return nil, err
		}
		ring.faults = o.Faults
		return ring, nil
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("spool: stat keys.enc: %w", err)
	}
	ring := newKeyring()
	ring.context = m.context
	ring.wrapID = o.WrappingKeyID
	ring.faults = o.Faults
	if _, err := ring.rotate(time.Now()); err != nil {
		return nil, err
	}
	if err := ring.writeKeys(s.dir, m.storeID, o.MasterKey, o.Passphrase); err != nil {
		return nil, err
	}
	return ring, nil
}

// allocateFileID reserves the next segment file id from the
// manifest allocator. Writer rotation and compaction replacement
// share this allocator so a successor id can never collide with a
// live replacement. The reservation is in-memory only: a crash
// before publication leaves the id unused, and open recomputes
// the allocator from the maximum id on disk.
func (s *Store) allocateFileID() uint64 {
	s.manifestMu.Lock()
	defer s.manifestMu.Unlock()
	id := s.man.nextFileID
	if id == 0 {
		id = 1
	}
	s.man.nextFileID = id + 1
	return id
}

// onRotate records rotation side effects: the sealed file leaves the
// active state, the successor joins the authoritative members, and
// the manifest generation advances.
func (s *Store) onRotate(sealedID, newID uint64) error {
	s.fstats.setState(sealedID, SegmentSealed)
	s.fstats.register(newID, time.Now())
	s.fstats.setState(newID, SegmentActive)
	s.manifestMu.Lock()
	if newID+1 > s.man.nextFileID {
		s.man.nextFileID = newID + 1
	}
	s.man.activeFileID = newID
	s.man.members = append(s.man.members, newID)
	sort.Slice(s.man.members, func(i, j int) bool { return s.man.members[i] < s.man.members[j] })
	s.man.generation++
	s.man.nextGroupID = s.groupSeq.Load() + 1
	m := *s.man
	m.members = append([]uint64(nil), s.man.members...)
	s.manifestMu.Unlock()
	return s.writeManifestFile(m)
}

// publishReplacement atomically swaps victimID for newID in the
// authoritative members under one generation. The replacement file
// must already be fully written and synced; the victim is unlinked
// afterwards. A crash between publish and unlink leaves an unlisted
// victim that open sweeps.
func (s *Store) publishReplacement(newID, victimID uint64) error {
	// Admission-serialized against checkpoint capture: the
	// snapshot-plus-links section either fully precedes this swap
	// (victim linked and pinned) or fully follows it (replacement
	// linked instead). Callers must not hold admissionMu.
	s.admissionMu.Lock()
	defer s.admissionMu.Unlock()
	s.manifestMu.Lock()
	kept := make([]uint64, 0, len(s.man.members))
	for _, m := range s.man.members {
		if m != victimID {
			kept = append(kept, m)
		}
	}
	kept = append(kept, newID)
	sort.Slice(kept, func(i, j int) bool { return kept[i] < kept[j] })
	s.man.members = kept
	if newID+1 > s.man.nextFileID {
		s.man.nextFileID = newID + 1
	}
	s.man.generation++
	s.man.nextGroupID = s.groupSeq.Load() + 1
	m := *s.man
	m.members = append([]uint64(nil), kept...)
	s.manifestMu.Unlock()
	return s.writeManifestFile(m)
}

// publishMemberRemoval drops id from the authoritative members and
// persists the new generation. Callers unlink the file afterwards;
// a crash between publish and unlink leaves an unlisted file that
// open sweeps.
func (s *Store) publishMemberRemoval(id uint64) error {
	// Admission-serialized against checkpoint capture like
	// publishReplacement. Callers must not hold admissionMu.
	s.admissionMu.Lock()
	defer s.admissionMu.Unlock()
	s.manifestMu.Lock()
	kept := make([]uint64, 0, len(s.man.members))
	for _, m := range s.man.members {
		if m != id {
			kept = append(kept, m)
		}
	}
	s.man.members = kept
	s.man.generation++
	s.man.nextGroupID = s.groupSeq.Load() + 1
	m := *s.man
	m.members = append([]uint64(nil), kept...)
	s.manifestMu.Unlock()
	return s.writeManifestFile(m)
}

// currentDataKey snapshots the ring's current key for a batch. The
// zero key with id 0 serves unencrypted stores.
func (s *Store) currentDataKey() (uint32, [32]byte, bool) {
	if s.ring == nil {
		return 0, [32]byte{}, true
	}
	return s.ring.currentKey()
}

// recordFlushErr stashes the latest background flush error.
func (s *Store) recordFlushErr(err error) {
	s.flushErrMu.Lock()
	s.lastFlushErr = err
	s.flushErrMu.Unlock()
}

// LastFlushErr reports the most recent background flush error, if any.
func (s *Store) LastFlushErr() error {
	s.flushErrMu.Lock()
	defer s.flushErrMu.Unlock()
	return s.lastFlushErr
}

// Put buffers a key/value write. It returns once the mutation is
// staged in memory; durability follows the configured mode and Flush.
func (s *Store) Put(key, value []byte) error {
	return s.put(key, value, false, true)
}

// TryPut behaves like Put but returns ErrBackpressure instead of
// blocking when the pending buffer is full.
func (s *Store) TryPut(key, value []byte) error {
	return s.put(key, value, false, false)
}

// Delete buffers a tombstone for key.
func (s *Store) Delete(key []byte) error {
	return s.put(key, nil, true, true)
}

// PutBatch buffers many key/value writes with one backpressure wait
// and one lock per touched shard. Admission is all-or-nothing: a
// size violation rejects the whole batch before anything is
// buffered. Commits follow the normal flush path and are not atomic
// as a group.
func (s *Store) PutBatch(pairs []KV) error {
	if s.closed.Load() {
		return ErrClosed
	}
	if err := s.checkTerminal(); err != nil {
		return err
	}
	if len(pairs) == 0 {
		return nil
	}
	items := make([]pendingRecord, len(pairs))
	var total int64
	for i := range pairs {
		k, v := pairs[i].Key, pairs[i].Value
		if len(k) > s.opts.MaxKeySize {
			return ErrKeyTooLarge
		}
		if len(v) > s.opts.MaxValueSize {
			return ErrValueTooLarge
		}
		size := len(k) + len(v) + recordOverhead
		if size > s.opts.MaxBlockBytes {
			return ErrRecordTooLarge
		}
		items[i] = pendingRecord{key: cloneBytes(k), value: cloneBytes(v), size: size}
		total += int64(size)
	}
	return s.bufferItems(items, total)
}

// DeleteBatch buffers one tombstone per key. Admission and commit
// semantics match PutBatch.
func (s *Store) DeleteBatch(keys [][]byte) error {
	if s.closed.Load() {
		return ErrClosed
	}
	if err := s.checkTerminal(); err != nil {
		return err
	}
	if len(keys) == 0 {
		return nil
	}
	items := make([]pendingRecord, len(keys))
	var total int64
	for i := range keys {
		k := keys[i]
		if len(k) > s.opts.MaxKeySize {
			return ErrKeyTooLarge
		}
		size := len(k) + recordOverhead
		if size > s.opts.MaxBlockBytes {
			return ErrRecordTooLarge
		}
		items[i] = pendingRecord{key: cloneBytes(k), tomb: true, size: size}
		total += int64(size)
	}
	return s.bufferItems(items, total)
}

// Commit atomically commits a mixed group of mutations at the
// requested durability: all validation precedes admission, operation
// order within the group is preserved (the final operation on a
// repeated key wins), and recovery retains the complete group or
// discards it as a whole, never a partial mutation set.
//
// Async acknowledges on acceptance into ordered pending memory.
// Flush and Sync acknowledge once the complete group (plus, for
// Sync, required file/directory metadata) reaches the OS/disk.
// Empty groups are no-ops and establish no barrier; use Sync for
// that. Spool treats keys and values as opaque bytes.
func (s *Store) Commit(mutations []Mutation, d Durability) error {
	if len(mutations) == 0 {
		return nil
	}
	if s.closed.Load() {
		return ErrClosed
	}
	if err := s.checkTerminal(); err != nil {
		return err
	}
	if err := s.checkMaint(); err != nil {
		return err
	}
	switch d {
	case DurabilityAsync, DurabilityFlush, DurabilitySync:
	default:
		return fmt.Errorf("spool: unknown Durability %d", int(d))
	}
	recs := make([]pendingRecord, len(mutations))
	var total int64
	for i := range mutations {
		m := &mutations[i]
		if m.Deleted && len(m.Value) != 0 {
			return ErrBadMutation
		}
		if len(m.Key) > s.opts.MaxKeySize {
			return ErrKeyTooLarge
		}
		if !m.Deleted && len(m.Value) > s.opts.MaxValueSize {
			return ErrValueTooLarge
		}
		size := len(m.Key) + len(m.Value) + recordOverhead
		if size > s.opts.MaxBlockBytes {
			return ErrRecordTooLarge
		}
		recs[i] = pendingRecord{key: cloneBytes(m.Key), value: cloneBytes(m.Value), tomb: m.Deleted, size: size}
		total += int64(size)
	}
	// Exact plaintext bound before admission: no wire-size surprise.
	maxBody := s.opts.MaxBlockBytes - blockHeaderLen - s.crypt.overhead() - 1024
	if plain := plaintextGroupSize(recs, s.opts.TargetBlockBytes, maxBody, s.opts.MaxRecordsPerBlock); plain > uint64(s.opts.MaxAtomicBatchBytes) {
		return ErrGroupTooLarge
	}
	// Groups never use the large-single-record bypass: a group that
	// cannot fit pending capacity is rejected, not waited on.
	if total > s.opts.MaxPendingBytes {
		return ErrGroupTooLarge
	}
	start := time.Now()
	defer func() { s.commitNanos.Add(uint64(time.Since(start))) }()
	s.bpMu.Lock()
	for s.pendingBytes.Load()+total > s.opts.MaxPendingBytes && !s.closed.Load() && !s.terminal.Load() {
		s.bpCond.Wait()
	}
	closed := s.closed.Load()
	s.bpMu.Unlock()
	if closed {
		return ErrClosed
	}
	if err := s.checkTerminal(); err != nil {
		return err
	}
	if err := s.ensureFreshKey(); err != nil {
		return err
	}
	s.admissionMu.Lock()
	if s.closed.Load() {
		s.admissionMu.Unlock()
		return ErrClosed
	}
	if err := s.checkTerminal(); err != nil {
		s.admissionMu.Unlock()
		return err
	}
	// Seal buffered individual writes first so they order before
	// this group: a Put accepted earlier must never outrank a
	// later Commit. The staged counter reduces this to one atomic
	// load for commit-only callers.
	if _, err := s.flushLocked(DurabilityAsync); err != nil {
		s.admissionMu.Unlock()
		return err
	}
	if s.seqNext.Load()+uint64(len(recs)) >= seqCounterMax {
		s.admissionMu.Unlock()
		return ErrSeqExhausted
	}
	g := s.admitLocked(recs, d)
	s.admissionMu.Unlock()
	s.pendingBytes.Add(total)
	s.pendingRecords.Add(int64(len(recs)))
	s.commits.Add(1)
	if d == DurabilityAsync {
		return nil
	}
	return <-g.done
}

// Sync fences every write accepted before the fence is captured —
// buffered individual writes, queued groups, in-flight workers —
// and fsyncs, so the data survives process and OS crashes (barring
// disk failure). Writes accepted afterward belong to the next fence.
func (s *Store) Sync() error {
	return s.syncBarrier()
}

// Flush is the same full syncing barrier as Sync, kept for
// compatibility.
func (s *Store) Flush() error {
	return s.syncBarrier()
}

func (s *Store) syncBarrier() error {
	if s.closed.Load() {
		return ErrClosed
	}
	if err := s.checkTerminal(); err != nil {
		return err
	}
	start := time.Now()
	defer func() { s.syncNanos.Add(uint64(time.Since(start))) }()
	t0 := time.Now()
	id, err := s.flushInternal(DurabilitySync, false)
	if err != nil {
		return err
	}
	t1 := time.Now()
	if err := s.waitForGroup(id, false); err != nil {
		return err
	}
	t2 := time.Now()
	if err := s.seg.sync(); err != nil {
		s.setTerminal(err)
		return err
	}
	if _, ok := os.LookupEnv("MURMUR_SPOOL_TIMING"); ok {
		if total := time.Since(t0); total >= 20*time.Millisecond {
			fmt.Fprintf(os.Stderr, "[spool-barrier] total=%dms seal=%dms groupwait=%dms segsync=%dms\n",
				total.Milliseconds(), t1.Sub(t0).Milliseconds(), t2.Sub(t1).Milliseconds(), time.Since(t2).Milliseconds())
		}
	}
	s.syncs.Add(1)
	return nil
}

// RotateKey generates a successor data key, persists it to keys.enc
// and makes it current for new blocks. Old blocks stay readable;
// compaction gradually re-encrypts them. The successor is durable
// before any group can reference it. It fails on unencrypted
// stores; envelope or manifest publication failures are terminal.
func (s *Store) RotateKey() error {
	if s.closed.Load() {
		return ErrClosed
	}
	if err := s.checkTerminal(); err != nil {
		return err
	}
	if s.ring == nil {
		return fmt.Errorf("spool: key rotation requires encryption")
	}
	if err := s.checkMaint(); err != nil {
		return err
	}
	// Admission-serialized against checkpoint capture: no keyring
	// or manifest publication interleaves a captured cut.
	s.admissionMu.Lock()
	defer s.admissionMu.Unlock()
	s.rotateMu.Lock()
	defer s.rotateMu.Unlock()
	return s.rotateDataKey()
}

// rotateDataKey persists a successor data key and installs it as
// current, then records it in the manifest. The caller must hold
// rotateMu. Publication failures are terminal: authoritative key
// state is uncertain.
func (s *Store) rotateDataKey() error {
	// keys.enc first: the manifest must never reference a key that is
	// not yet durable.
	s.manifestMu.Lock()
	storeID := s.man.storeID
	s.manifestMu.Unlock()
	id, err := s.ring.rotatePersisted(time.Now(), s.dir, storeID, s.master, s.pass)
	if err != nil {
		s.setTerminal(err)
		return err
	}
	s.rotations.Add(1)
	s.manifestMu.Lock()
	s.man.currentKeyID = id
	m := *s.man
	s.manifestMu.Unlock()
	if err := s.writeManifestFile(m); err != nil {
		err = fmt.Errorf("spool: publish rotation: %w", err)
		s.setTerminal(err)
		return err
	}
	return nil
}

// keyFresh reports whether the current data key is within its
// configured age. A missing key or disabled aging reads fresh; the
// seal path reports the missing key itself.
func (s *Store) keyFresh() bool {
	if s.ring == nil || s.opts.DataKeyMaxAge <= 0 {
		return true
	}
	id, _, ok := s.ring.currentKey()
	if !ok {
		return true
	}
	created, ok := s.ring.createdAt(id)
	if !ok {
		return true
	}
	return time.Since(created) <= s.opts.DataKeyMaxAge
}

// ensureFreshKey rotates an expired data key before new groups
// reference it. Admission paths call it before admitting; it is
// safe for concurrent use. Rotation failures are terminal.
func (s *Store) ensureFreshKey() error {
	if s.keyFresh() {
		return nil
	}
	s.admissionMu.Lock()
	defer s.admissionMu.Unlock()
	s.rotateMu.Lock()
	defer s.rotateMu.Unlock()
	if s.keyFresh() {
		return nil
	}
	return s.rotateDataKey()
}

// RotateMasterKey re-protects keys.enc under new key material without
// rewriting data segments: historical and current data keys are
// untouched, only their envelope changes. Exactly one of
// newMasterKey (32 bytes) and newPassphrase must be set; the choice
// may switch the store between master-key and passphrase protection.
// It fails on unencrypted stores.
func (s *Store) RotateMasterKey(newMasterKey []byte, newPassphrase string) error {
	if s.closed.Load() {
		return ErrClosed
	}
	if err := s.checkTerminal(); err != nil {
		return err
	}
	if s.ring == nil {
		return fmt.Errorf("spool: master-key rotation requires encryption")
	}
	if err := s.checkMaint(); err != nil {
		return err
	}
	if (len(newMasterKey) > 0) == (newPassphrase != "") {
		return fmt.Errorf("spool: set exactly one of newMasterKey and newPassphrase")
	}
	if len(newMasterKey) > 0 && len(newMasterKey) != 32 {
		return fmt.Errorf("spool: newMasterKey must be 32 bytes, got %d", len(newMasterKey))
	}
	// Admission-serialized against checkpoint capture.
	s.admissionMu.Lock()
	defer s.admissionMu.Unlock()
	s.rotateMu.Lock()
	defer s.rotateMu.Unlock()
	// Persist first: on failure the old protector still unlocks the
	// untouched container.
	s.manifestMu.Lock()
	storeID := s.man.storeID
	s.manifestMu.Unlock()
	if err := s.ring.writeKeys(s.dir, storeID, newMasterKey, newPassphrase); err != nil {
		err = fmt.Errorf("spool: publish master rotation: %w", err)
		s.setTerminal(err)
		return err
	}
	wipe(s.master)
	s.master = append([]byte(nil), newMasterKey...)
	s.pass = newPassphrase
	return nil
}

// RotateWrappingKey re-protects keys.enc under new 32-byte wrapping
// material and records its identifier in the authenticated envelope:
// the named-key rotation entrypoint for provider-managed keys. The
// data keys are untouched, only their envelope changes. On failure
// the old protector and id still unlock the untouched container.
// It fails on unencrypted stores; publication failures are terminal.
func (s *Store) RotateWrappingKey(newKey []byte, newID string) error {
	if s.closed.Load() {
		return ErrClosed
	}
	if err := s.checkTerminal(); err != nil {
		return err
	}
	if s.ring == nil {
		return fmt.Errorf("spool: wrapping-key rotation requires encryption")
	}
	if err := s.checkMaint(); err != nil {
		return err
	}
	if len(newKey) != 32 {
		return fmt.Errorf("spool: newKey must be 32 bytes, got %d", len(newKey))
	}
	if newID == "" {
		return fmt.Errorf("spool: new wrapping-key id is required")
	}
	if len(newID) > maxWrapIDLen {
		return fmt.Errorf("spool: wrapping-key id of %d bytes exceeds %d", len(newID), maxWrapIDLen)
	}
	// Admission-serialized against checkpoint capture.
	s.admissionMu.Lock()
	defer s.admissionMu.Unlock()
	s.rotateMu.Lock()
	defer s.rotateMu.Unlock()
	s.manifestMu.Lock()
	storeID := s.man.storeID
	s.manifestMu.Unlock()
	s.ring.mu.Lock()
	old := s.ring.wrapID
	s.ring.wrapID = newID
	err := s.ring.writeKeysLocked(s.dir, storeID, newKey, "", nil)
	if err != nil {
		s.ring.wrapID = old
	}
	s.ring.mu.Unlock()
	if err != nil {
		err = fmt.Errorf("spool: publish wrapping rotation: %w", err)
		s.setTerminal(err)
		return err
	}
	wipe(s.master)
	s.master = append([]byte(nil), newKey...)
	s.pass = ""
	s.wrapID = newID
	return nil
}

// trackSealKey records a sealed-but-unpublished frame's data key so
// pruning never drops a key still in flight. Key 0 (plaintext) needs
// no tracking.
func (s *Store) trackSealKey(id uint32) {
	if id == 0 {
		return
	}
	s.sealKeyMu.Lock()
	s.sealKeys[id]++
	s.sealKeyMu.Unlock()
}

// untrackSealKey releases a trackSealKey registration.
func (s *Store) untrackSealKey(id uint32) {
	if id == 0 {
		return
	}
	s.sealKeyMu.Lock()
	if s.sealKeys[id] <= 1 {
		delete(s.sealKeys, id)
	} else {
		s.sealKeys[id]--
	}
	s.sealKeyMu.Unlock()
}

// sealDataKey returns the active data key registered as
// sealed-but-unpublished, holding sealQuiesceMu shared until the
// caller releases it. It re-reads the active key after
// registering: a rotation landing between the read and the
// registration would otherwise hand the seal orphaned material.
// On a race it unregisters and retries with the new key;
// rotations persist per attempt, so retries converge. Callers
// must pair every success with releaseSealKey.
func (s *Store) sealDataKey() (uint32, [32]byte, bool) {
	s.sealQuiesceMu.RLock()
	for {
		id, key, ok := s.currentDataKey()
		if !ok {
			s.sealQuiesceMu.RUnlock()
			return 0, [32]byte{}, false
		}
		s.trackSealKey(id)
		id2, _, ok2 := s.currentDataKey()
		if ok2 && id2 == id {
			return id, key, true
		}
		s.untrackSealKey(id)
		if !ok2 {
			s.sealQuiesceMu.RUnlock()
			return 0, [32]byte{}, false
		}
	}
}

// releaseSealKey ends a sealDataKey hold: it unregisters the key
// and releases the shared quiescence lock.
func (s *Store) releaseSealKey(id uint32) {
	s.untrackSealKey(id)
	s.sealQuiesceMu.RUnlock()
}

// Close stops background work, drains accepted groups through a
// syncing barrier, records a clean shutdown, and releases the store
// lock. It is idempotent. A failed close releases resources without
// clearing failure state: the manifest stays unmarked and
// StorageError keeps reporting the cause.
func (s *Store) Close() error {
	if s.closed.Swap(true) {
		return nil
	}
	// Wait out any maintenance: it holds maintExcl throughout,
	// and its staged files must settle before the drain measures
	// them.
	s.maintExcl.Lock()
	s.maintExcl.Unlock()
	// Wake queue waiters; the committer stays until drained.
	s.queueMu.Lock()
	s.commitCond.Broadcast()
	s.queueMu.Unlock()
	close(s.stopCh)
	// Drain everything accepted so far through the still-running
	// committer.
	id, err := s.flushInternal(DurabilitySync, true)
	if err == nil {
		err = s.waitForGroup(id, true)
	}
	s.queueMu.Lock()
	s.drained = true
	s.commitCond.Broadcast()
	s.queueMu.Unlock()
	s.closeWG.Wait()
	if err != nil {
		// Unusable after this; still release resources, keep the
		// terminal state and no clean marker.
		s.seg.close()
		s.unlock()
		return err
	}
	if err := s.seg.close(); err != nil {
		s.unlock()
		return err
	}
	// All segment bytes are synced: record the clean-shutdown shape.
	// Measuring failures abort the close; the manifest then stays
	// unmarked and the next open skips the extra verification.
	s.manifestMu.Lock()
	segIDs := append([]uint64(nil), s.man.members...)
	s.manifestMu.Unlock()
	var segBytes uint64
	for _, id := range segIDs {
		st, err := os.Stat(filepath.Join(s.dir, segmentsDirName, segmentFileName(id)))
		if err != nil {
			s.unlock()
			return fmt.Errorf("spool: stat segment %d at close: %w", id, err)
		}
		segBytes += uint64(st.Size())
	}
	s.manifestMu.Lock()
	s.man.nextBlockID = s.nextBlockID.Load()
	s.man.nextGroupID = s.groupSeq.Load() + 1
	s.man.clean = true
	s.man.cleanBytes = segBytes
	s.man.cleanFiles = uint32(len(segIDs))
	s.man.cleanGen = s.man.generation
	s.man.cleanGroup = s.appendedGroup.Load()
	m := *s.man
	m.members = append([]uint64(nil), s.man.members...)
	s.manifestMu.Unlock()
	if err := s.writeManifestFile(m); err != nil {
		s.unlock()
		return err
	}
	clear(s.master)
	s.unlock()
	return nil
}

// segmentModTime reads a segment's mtime for FileStats.CreatedAt,
// falling back to now on any error.
func segmentModTime(segDir string, id uint64) time.Time {
	if st, err := os.Stat(filepath.Join(segDir, segmentFileName(id))); err == nil {
		return st.ModTime()
	}
	return time.Now()
}

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

func nowUnixNano() int64 { return time.Now().UnixNano() }
