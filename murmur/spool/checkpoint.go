package spool

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// Backup checkpoints capture transactionally consistent
// point-in-time copies for streaming (Murmur backups, reseed
// sources). A checkpoint is a caller-owned destination directory of
// hard links to sealed segment inodes plus copies of the manifest
// and keyring bytes, so it stays immutable and readable while the
// live store keeps appending, compacting, rewriting, rebinding,
// rotating, and pruning. Releasing one checkpoint never invalidates
// another: shared inodes drop by link count only, and every
// checkpoint carries its own keyring copy.
//
// Capture holds group admission, drains admitted groups, flushes
// the writer buffer, seals the active segment with a rotation, and
// then links every member except the fresh empty active. The
// checkpoint manifest is the post-rotation manifest minus that
// empty member, with the sealed tail as its active file and the
// clean flag cleared (a crash-consistent cut). Rotation, compaction
// publication, and maintenance start serialize against the capture
// section under admissionMu, so the linked set always matches the
// copied manifest and keyring. Commits block (then proceed) while a
// capture holds admission; live writes resume before the caller
// streams the destination. Pruning needs no exclusion: it scans
// live files for usage, exclusion keeps the linked members live,
// and retired keys gain no new references, so a keyring copy taken
// inside the section is complete for every linked segment.
//
// Destinations must be new directories on the same filesystem (for
// hard links). Cross-filesystem destinations fail at the first link
// with no fallback to copying. Failed or canceled captures remove
// their partial destination; completed destinations belong to the
// caller, and Release only drops the handle's retention
// registrations. Retention registrations are process-local: a
// reopen does not rediscover destinations, which open standalone
// regardless.

const (
	checkpointCompleteName = "COMPLETE"
	checkpointMagic        = "spool-checkpoint/1"
)

// Checkpoint is a captured point-in-time copy. Files lists the
// ordered stream set: manifest, keyring (encrypted stores), then
// segments ascending. All paths are absolute.
type Checkpoint struct {
	store      *Store
	ID         string
	Dir        string
	Generation uint64
	Members    []uint64
	CreatedAt  time.Time
	Encrypted  bool
	keyIDs     []uint32 // referenced data keys, for release
	files      []string // ordered stream set
}

// Files returns the ordered stream set: manifest, keyring when
// present, then segments ascending.
func (c *Checkpoint) Files() []string {
	return append([]string(nil), c.files...)
}

// Release drops the handle's retention registrations so pruning
// can proceed. It is idempotent: releasing twice, or releasing a
// checkpoint the store never registered, succeeds. The destination
// directory belongs to the caller and is never removed here.
func (c *Checkpoint) Release() error {
	if c.store == nil {
		return fmt.Errorf("spool: checkpoint %q has no store", c.ID)
	}
	return c.store.ReleaseCheckpoint(c.ID)
}

// checkpointFiles builds the ordered stream set for a checkpoint
// layout.
func checkpointFiles(dir string, members []uint64, encrypted bool) []string {
	files := make([]string, 0, 2+len(members))
	files = append(files, filepath.Join(dir, manifestFileName))
	if encrypted {
		files = append(files, filepath.Join(dir, keysFileName))
	}
	segDir := filepath.Join(dir, segmentsDirName)
	for _, id := range members {
		files = append(files, filepath.Join(segDir, segmentFileName(id)))
	}
	return files
}

// Checkpoint captures a transactionally consistent point-in-time
// copy into destination and registers a handle owning its
// retention registrations. Destination must be a new directory on
// the same filesystem; anything else fails without touching
// existing data. A nil ctx captures without cancellation.
// Canceling ctx, or any capture failure, removes the partial
// destination and releases nothing (nothing was registered).
func (s *Store) Checkpoint(ctx context.Context, destination string) (*Checkpoint, error) {
	if destination == "" {
		return nil, fmt.Errorf("spool: checkpoint destination is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("spool: checkpoint canceled: %w", err)
	}
	if s.closed.Load() {
		return nil, ErrClosed
	}
	if err := s.checkTerminal(); err != nil {
		return nil, err
	}
	// Admission blocks for the capture: no new groups can start,
	// and rotation, compaction publication, and maintenance start
	// serialize here too, so the drain, rotation, snapshot, links,
	// and metadata copies below are atomic against every mutating
	// path. The committer needs no admissionMu, so the drain
	// cannot deadlock. The maintenance flag is checked inside
	// this section while maintBegin sets it inside the same
	// section, which serializes the ownership decision in both
	// directions.
	s.admissionMu.Lock()
	failed := true
	defer func() {
		if failed {
			s.admissionMu.Unlock()
		}
	}()
	if s.closed.Load() {
		return nil, ErrClosed
	}
	if err := s.checkTerminal(); err != nil {
		return nil, err
	}
	if err := s.checkMaint(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("spool: checkpoint canceled: %w", err)
	}
	// Seal buffered individual writes into admitted groups first:
	// the cut must include every accepted write, not just groups
	// admitted before the capture started. The seal uses the
	// current key; freshness rotation stays excluded for the
	// section.
	if _, err := s.flushLocked(DurabilityAsync); err != nil {
		return nil, err
	}
	fence := s.lastGroup.Load()
	if err := s.waitForGroup(fence, false); err != nil {
		return nil, err
	}
	// Push async-buffered bytes to the file: the linked segments
	// must include every fenced group.
	if err := s.seg.commit(DurabilityFlush); err != nil {
		return nil, err
	}
	// Seal the active segment so every linked inode is immutable;
	// later appends go to the fresh empty active, which the
	// checkpoint excludes. The fence already drained every
	// admitted group and no new group can start, so no completer
	// rotation can interleave the links below.
	if err := s.seg.rotateTo(s.allocateFileID()); err != nil {
		return nil, err
	}
	s.manifestMu.Lock()
	man := *s.man
	active := s.man.activeFileID
	s.manifestMu.Unlock()
	members := make([]uint64, 0, len(man.members))
	for _, id := range man.members {
		if id != active {
			members = append(members, id)
		}
	}
	if len(members) == 0 {
		return nil, fmt.Errorf("spool: checkpoint of empty membership: %w", ErrCorrupt)
	}
	if err := os.Mkdir(destination, 0o700); err != nil {
		return nil, fmt.Errorf("spool: checkpoint destination must be a new directory: %w", err)
	}
	// From here the destination is ours (it did not exist), so
	// failure cleanup removes exactly what capture created.
	cpSegDir := filepath.Join(destination, segmentsDirName)
	fail := func(format string, args ...any) (*Checkpoint, error) {
		os.RemoveAll(destination)
		return nil, fmt.Errorf(format, args...)
	}
	if err := os.Mkdir(cpSegDir, 0o700); err != nil {
		return fail("spool: create checkpoint segments dir: %w", err)
	}
	// Mkdir honors umask; force private dirs regardless.
	if err := os.Chmod(destination, 0o700); err != nil {
		return fail("spool: secure checkpoint dir: %w", err)
	}
	if err := os.Chmod(cpSegDir, 0o700); err != nil {
		return fail("spool: secure checkpoint segments dir: %w", err)
	}
	segDir := filepath.Join(s.dir, segmentsDirName)
	for _, mID := range members {
		if err := ctx.Err(); err != nil {
			return fail("spool: checkpoint canceled: %w", err)
		}
		if err := s.faults.trip("checkpoint"); err != nil {
			return fail("spool: link checkpoint segment %d: %w", mID, err)
		}
		// A cross-filesystem destination fails here with no
		// fallback to copying: hard links are the consistency
		// mechanism, not an optimization.
		if err := os.Link(filepath.Join(segDir, segmentFileName(mID)), filepath.Join(cpSegDir, segmentFileName(mID))); err != nil {
			return fail("spool: link checkpoint segment %d: %w", mID, err)
		}
	}
	// The checkpoint manifest is the post-rotation manifest minus
	// the excluded empty active, with the sealed tail as its
	// active file and no clean flag.
	ckman := man
	ckman.members = members
	ckman.activeFileID = members[len(members)-1]
	ckman.clean = false
	if err := writeSyncFile(filepath.Join(destination, manifestFileName), ckman.encode(), 0o600); err != nil {
		return fail("spool: write checkpoint manifest: %w", err)
	}
	if man.encrypted {
		kraw, err := os.ReadFile(filepath.Join(s.dir, keysFileName))
		if err != nil {
			return fail("spool: read keyring for checkpoint: %w", err)
		}
		if err := writeSyncFile(filepath.Join(destination, keysFileName), kraw, 0o600); err != nil {
			return fail("spool: write checkpoint keyring: %w", err)
		}
	}
	created := time.Now()
	complete := fmt.Sprintf("%s\n%d\n%d\n", checkpointMagic, man.generation, created.UnixNano())
	if err := writeSyncFile(filepath.Join(destination, checkpointCompleteName), []byte(complete), 0o600); err != nil {
		return fail("spool: write checkpoint marker: %w", err)
	}
	if err := s.faults.trip("dirsync"); err != nil {
		return fail("spool: sync checkpoint segments dir: %w", err)
	}
	if err := dirSync(cpSegDir); err != nil {
		return fail("spool: sync checkpoint segments dir: %w", err)
	}
	if err := s.faults.trip("dirsync"); err != nil {
		return fail("spool: sync checkpoint dir: %w", err)
	}
	if err := dirSync(destination); err != nil {
		return fail("spool: sync checkpoint dir: %w", err)
	}
	if err := s.faults.trip("dirsync"); err != nil {
		return fail("spool: sync checkpoint parent dir: %w", err)
	}
	if err := dirSync(filepath.Dir(destination)); err != nil {
		return fail("spool: sync checkpoint parent dir: %w", err)
	}
	generation := man.generation
	encrypted := man.encrypted
	membersCopy := append([]uint64(nil), members...)
	// Release admission before the slow key scan: the checkpoint
	// inodes are immutable and the destination is complete, so
	// writers resume while registration finishes.
	s.admissionMu.Unlock()
	failed = false
	// Key references pin the checkpoint's data keys against
	// pruning until release. Header key ids are exactly the keys
	// decryption reads, so a strict header scan neither misses nor
	// invents references. A scan failure removes the destination;
	// nothing was registered yet.
	keySet := make(map[uint32]bool)
	for _, mID := range membersCopy {
		ids, err := scanFileKeyIDs(filepath.Join(cpSegDir, segmentFileName(mID)))
		if err != nil {
			os.RemoveAll(destination)
			return nil, fmt.Errorf("spool: scan checkpoint segment %d: %w", mID, err)
		}
		for _, k := range ids {
			keySet[k] = true
		}
	}
	cp := &Checkpoint{
		store: s, Dir: destination, Generation: generation,
		Members: membersCopy, CreatedAt: created, Encrypted: encrypted,
		files: checkpointFiles(destination, membersCopy, encrypted),
	}
	for k := range keySet {
		if k != 0 {
			cp.keyIDs = append(cp.keyIDs, k)
		}
	}
	sort.Slice(cp.keyIDs, func(i, j int) bool { return cp.keyIDs[i] < cp.keyIDs[j] })
	s.ckptMu.Lock()
	if s.checkpoints == nil {
		s.checkpoints = make(map[string]*Checkpoint)
	}
	id, err := checkpointID()
	for err == nil {
		if _, exists := s.checkpoints[id]; !exists {
			break
		}
		id, err = checkpointID()
	}
	if err != nil {
		s.ckptMu.Unlock()
		os.RemoveAll(destination)
		return nil, err
	}
	cp.ID = id
	s.checkpoints[id] = cp
	for _, k := range cp.keyIDs {
		s.ckptKeyRefs[k]++
	}
	s.ckptMu.Unlock()
	return cp.copy(), nil
}

// copy duplicates a checkpoint handle with copied slices.
func (c *Checkpoint) copy() *Checkpoint {
	dup := *c
	dup.Members = append([]uint64(nil), c.Members...)
	dup.keyIDs = append([]uint32(nil), c.keyIDs...)
	dup.files = append([]string(nil), c.files...)
	return &dup
}

// ReleaseCheckpoint drops a checkpoint by id, freeing its key
// references. It is idempotent: unknown ids succeed. The
// destination directory belongs to the caller and is never removed
// here. Release stays available on closed, failed, and
// maintenance-held stores: it only touches the registry.
func (s *Store) ReleaseCheckpoint(id string) error {
	s.ckptMu.Lock()
	cp, ok := s.checkpoints[id]
	if !ok {
		s.ckptMu.Unlock()
		return nil
	}
	keyIDs := append([]uint32(nil), cp.keyIDs...)
	delete(s.checkpoints, id)
	for _, k := range keyIDs {
		if s.ckptKeyRefs[k] <= 1 {
			delete(s.ckptKeyRefs, k)
		} else {
			s.ckptKeyRefs[k]--
		}
	}
	s.ckptMu.Unlock()
	return nil
}

// ListCheckpoints returns every registered checkpoint of this
// store handle. Registrations are process-local: destinations
// survive close and crash (they open standalone), but only
// unreleased handles of a live store are listed. Handles are
// copies: releasing through any of them works.
func (s *Store) ListCheckpoints() []*Checkpoint {
	s.ckptMu.Lock()
	defer s.ckptMu.Unlock()
	out := make([]*Checkpoint, 0, len(s.checkpoints))
	for _, cp := range s.checkpoints {
		out = append(out, cp.copy())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// checkpointID mints a registration id for a captured checkpoint.
func checkpointID() (string, error) {
	var rnd [6]byte
	if _, err := rand.Read(rnd[:]); err != nil {
		return "", fmt.Errorf("spool: checkpoint entropy: %w", err)
	}
	return fmt.Sprintf("%d-%x", time.Now().UnixNano(), rnd), nil
}

// writeSyncFile writes bytes and fsyncs the file (not its dir).
func writeSyncFile(path string, data []byte, perm os.FileMode) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// scanFileKeyIDs returns every block's data-key id in file order.
// Any framing error fails the scan: checkpoints only capture
// healthy files.
func scanFileKeyIDs(path string) ([]uint32, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var ids []uint32
	for off := 0; off < len(raw); {
		if len(raw)-off < blockHeaderLen {
			return nil, fmt.Errorf("spool: torn checkpoint frame at %d: %w", off, ErrCorrupt)
		}
		hdr, err := parseBlockHeader(raw[off : off+blockHeaderLen])
		if err != nil {
			return nil, err
		}
		ids = append(ids, hdr.keyID)
		next := off + blockHeaderLen + int(hdr.sealedLen)
		if next > len(raw) {
			return nil, fmt.Errorf("spool: torn checkpoint frame at %d: %w", off, ErrCorrupt)
		}
		off = next
	}
	return ids, nil
}
