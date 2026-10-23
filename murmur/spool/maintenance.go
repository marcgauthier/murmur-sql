package spool

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"

	"github.com/marcgauthier/murmur-sql/murmur/compression"
)

// Maintenance rewrites every authoritative segment: re-encrypting
// under the current data key (RewriteDataKeys) or additionally
// rebinding the database context for reseed (RebindContext).
// Checkpoints keep working throughout: rewrites stage replacement
// inodes and rename over originals, so hard-linked checkpoint files
// keep their original bytes; archived keyrings are never touched.
//
// The operation is exclusive (one at a time, no concurrent Close
// return), gates all mutating operations with ErrMaintenance, and
// records a recoverable intent before staging anything. A crash at
// any point resumes to completion at the next open before writes
// are admitted. Failures before manifest/keyring publication are
// ordinary errors (nothing published); publication failures are
// terminal.

// Intent file layout (all integers little-endian, 40 bytes):
//
//	off  size  field
//	0    4     magic "SPLI"
//	4    2     format version (1)
//	6    1     op (1 = rewrite, 2 = rebind)
//	7    1     phase (1 = staging, 2 = segments done,
//	                 3 = keyring done, 4 = manifest done)
//	8    16    target context (rebind; rewrite: zeros)
//	24   8     manifest generation target
//	32   4     total files (progress aid)
//	36   4     CRC32-Castagnoli over bytes [0,36)

const (
	intentFileName = "maintenance.intent"
	intentLen      = 40

	maintOpRewrite = 1
	maintOpRebind  = 2

	maintPhaseStaging      = 1
	maintPhaseSegmentsDone = 2
	maintPhaseKeyringDone  = 3
	maintPhaseManifestDone = 4
)

// maintIntent is a parsed maintenance intent.
type maintIntent struct {
	op             uint8
	phase          uint8
	targetContext  [16]byte
	manifestTarget uint64
	totalFiles     uint32
}

func encodeIntent(m *maintIntent) []byte {
	out := make([]byte, intentLen)
	copy(out[0:4], "SPLI")
	binary.LittleEndian.PutUint16(out[4:6], 1)
	out[6] = m.op
	out[7] = m.phase
	copy(out[8:24], m.targetContext[:])
	binary.LittleEndian.PutUint64(out[24:32], m.manifestTarget)
	binary.LittleEndian.PutUint32(out[32:36], m.totalFiles)
	binary.LittleEndian.PutUint32(out[36:40], crc32.Checksum(out[:36], castagnoli))
	return out
}

func parseIntent(raw []byte) (*maintIntent, error) {
	if len(raw) != intentLen {
		return nil, fmt.Errorf("spool: intent size %d: %w", len(raw), ErrCorrupt)
	}
	if string(raw[0:4]) != "SPLI" {
		return nil, fmt.Errorf("spool: bad intent magic: %w", ErrCorrupt)
	}
	if v := binary.LittleEndian.Uint16(raw[4:6]); v != 1 {
		return nil, fmt.Errorf("spool: intent version %d: %w", v, ErrUnsupportedVersion)
	}
	if got, want := crc32.Checksum(raw[:36], castagnoli), binary.LittleEndian.Uint32(raw[36:40]); got != want {
		return nil, fmt.Errorf("spool: intent checksum mismatch: %w", ErrCorrupt)
	}
	m := &maintIntent{op: raw[6], phase: raw[7]}
	if m.op != maintOpRewrite && m.op != maintOpRebind {
		return nil, fmt.Errorf("spool: intent op %d: %w", m.op, ErrCorrupt)
	}
	if m.phase < maintPhaseStaging || m.phase > maintPhaseManifestDone {
		return nil, fmt.Errorf("spool: intent phase %d: %w", m.phase, ErrCorrupt)
	}
	copy(m.targetContext[:], raw[8:24])
	m.manifestTarget = binary.LittleEndian.Uint64(raw[24:32])
	m.totalFiles = binary.LittleEndian.Uint32(raw[32:36])
	if m.manifestTarget == 0 {
		return nil, fmt.Errorf("spool: intent lacks a manifest target: %w", ErrCorrupt)
	}
	return m, nil
}

// checkMaint rejects mutating operations while maintenance holds
// the store.
func (s *Store) checkMaint() error {
	if s.maintActive.Load() {
		return ErrMaintenance
	}
	return nil
}

// setMaintPhase records visible maintenance progress.
func (s *Store) setMaintPhase(phase string, done, total uint64) {
	s.maintMu.Lock()
	s.maintPhase = phase
	s.maintDone = done
	s.maintTotal = total
	s.maintMu.Unlock()
}

// maintBegin takes exclusive maintenance ownership: it fails when
// another maintenance holds the lock, gates mutating operations,
// and drains admitted groups so segment files stay stable for
// staging. Buffered-but-unadmitted puts stay buffered and flush
// normally after the ungate. The exclusivity lock stays held until
// maintEnd; phase reporting uses the separate short maintMu.
func (s *Store) maintBegin(op string, total uint64) error {
	if s.closed.Load() {
		return ErrClosed
	}
	if err := s.checkTerminal(); err != nil {
		return err
	}
	// Serialize against checkpoint capture under admissionMu:
	// this blocks while a capture holds the section, then observes
	// a stable store to gate. Capture checks maintActive inside
	// the same section, so the ownership decision is mutual in
	// both directions. Lock order is admissionMu -> maintExcl.
	s.admissionMu.Lock()
	if !s.maintExcl.TryLock() {
		s.admissionMu.Unlock()
		return ErrMaintenance
	}
	if s.closed.Load() {
		s.maintExcl.Unlock()
		s.admissionMu.Unlock()
		return ErrClosed
	}
	if err := s.checkTerminal(); err != nil {
		s.maintExcl.Unlock()
		s.admissionMu.Unlock()
		return err
	}
	s.maintActive.Store(true)
	s.admissionMu.Unlock()
	s.setMaintPhase(op, 0, total)
	// No new admissions past the gate; wait out everything admitted
	// before it so staging reads stable files.
	fence := s.lastGroup.Load()
	if err := s.waitForGroup(fence, false); err != nil {
		s.maintActive.Store(false)
		s.setMaintPhase("idle", 0, 0)
		s.maintExcl.Unlock()
		return err
	}
	// Push async-buffered bytes to the file: staging reads segment
	// files, not the writer buffer.
	if err := s.seg.commit(DurabilityFlush); err != nil {
		s.maintActive.Store(false)
		s.setMaintPhase("idle", 0, 0)
		s.maintExcl.Unlock()
		return err
	}
	return nil
}

// maintEnd releases maintenance ownership.
func (s *Store) maintEnd() {
	s.maintActive.Store(false)
	s.setMaintPhase("idle", 0, 0)
	s.maintExcl.Unlock()
}

// RewriteDataKeys re-encrypts every authoritative segment under the
// current data key with fresh nonces. Logical sequences, group ids
// and record contents are unchanged; only the physical sealing is
// renewed. Historical keys become prunable once their files are
// rewritten (see PruneDataKeys). Plain stores have nothing to
// rewrite and return nil.
func (s *Store) RewriteDataKeys() error {
	if s.ring == nil {
		if s.closed.Load() {
			return ErrClosed
		}
		return s.checkTerminal()
	}
	s.manifestMu.Lock()
	members := append([]uint64(nil), s.man.members...)
	genTarget := s.man.generation + 1
	s.manifestMu.Unlock()
	if err := s.maintBegin("rewrite", uint64(len(members))); err != nil {
		return err
	}
	done := false
	defer func() {
		if !done {
			s.maintEnd()
		}
	}()
	keyID, key, ok := s.currentDataKey()
	if !ok {
		return errNoCurrentKey
	}
	intent := &maintIntent{op: maintOpRewrite, phase: maintPhaseStaging,
		manifestTarget: genTarget, totalFiles: uint32(len(members))}
	if err := s.writeIntentFile(intent); err != nil {
		return fmt.Errorf("spool: write rewrite intent: %w", err)
	}
	env := s.maintEnv()
	rebased := make(map[uint64]int64)
	for i, id := range members {
		offsets, err := stageSegmentFile(env, id, keyID, key, s.ctx, s.ctx, [16]byte{}, false)
		if err != nil {
			return err
		}
		for seq, off := range offsets {
			rebased[seq] = off
		}
		s.setMaintPhase("rewrite", uint64(i+1), uint64(len(members)))
	}
	// The active file was renamed over: rebind the writer to the
	// new inode before anything can append again. Failure here is
	// terminal (appends cannot safely continue on this handle),
	// though the on-disk state stays consistent for a reopen.
	if err := s.seg.reopenActive(); err != nil {
		err = fmt.Errorf("spool: reopen active segment: %w", err)
		s.setTerminal(err)
		return err
	}
	// Completion frames may have shifted block offsets: rebase the
	// live index to the staged layout. Nothing reads the index
	// while gated, and a reopen rebuilds it from disk.
	if missing, ok := s.idx.rebaseBlocks(rebased); !ok {
		err := fmt.Errorf("spool: rewrite missing block %d in rebase map: %w", missing, ErrCorrupt)
		s.setTerminal(err)
		return err
	}
	intent.phase = maintPhaseSegmentsDone
	if err := s.writeIntentFile(intent); err != nil {
		return fmt.Errorf("spool: write rewrite intent: %w", err)
	}
	// Publish the rewrite generation. Members are unchanged; the
	// bump separates pre/post-rewrite content for checkpoint cuts.
	s.manifestMu.Lock()
	s.man.generation = genTarget
	s.manifestMu.Unlock()
	if err := s.persistManifest(s.man); err != nil {
		err = fmt.Errorf("spool: publish rewrite: %w", err)
		s.setTerminal(err)
		return err
	}
	if err := removeIntentFile(s.dir, s.faults); err != nil {
		return err
	}
	done = true
	s.maintEnd()
	return nil
}

// RebindContext rewrites every authoritative segment bound to a new
// database context and adopts it, for reseed under a new identity.
// The wrapping material is unchanged; the keyring rebinds to the
// new context under the same protector. Plain stores only update
// the recorded context. The previous context stops opening the
// store once the rebind publishes.
func (s *Store) RebindContext(newContext [16]byte) error {
	if s.closed.Load() {
		return ErrClosed
	}
	if err := s.checkTerminal(); err != nil {
		return err
	}
	if s.ring == nil {
		return s.rebindPlain(newContext)
	}
	s.manifestMu.Lock()
	members := append([]uint64(nil), s.man.members...)
	genTarget := s.man.generation + 1
	s.manifestMu.Unlock()
	if err := s.maintBegin("rebind", uint64(len(members))); err != nil {
		return err
	}
	done := false
	defer func() {
		if !done {
			s.maintEnd()
		}
	}()
	keyID, key, ok := s.currentDataKey()
	if !ok {
		return errNoCurrentKey
	}
	intent := &maintIntent{op: maintOpRebind, phase: maintPhaseStaging,
		targetContext:  newContext,
		manifestTarget: genTarget, totalFiles: uint32(len(members))}
	if err := s.writeIntentFile(intent); err != nil {
		return fmt.Errorf("spool: write rebind intent: %w", err)
	}
	env := s.maintEnv()
	rebased := make(map[uint64]int64)
	for i, id := range members {
		offsets, err := stageSegmentFile(env, id, keyID, key, s.ctx, newContext, [16]byte{}, false)
		if err != nil {
			return err
		}
		for seq, off := range offsets {
			rebased[seq] = off
		}
		s.setMaintPhase("rebind", uint64(i+1), uint64(len(members)))
	}
	// The active file was renamed over: rebind the writer to the
	// new inode before anything can append again. Failure here is
	// terminal (appends cannot safely continue on this handle),
	// though the on-disk state stays consistent for a reopen.
	if err := s.seg.reopenActive(); err != nil {
		err = fmt.Errorf("spool: reopen active segment: %w", err)
		s.setTerminal(err)
		return err
	}
	// Completion frames may have shifted block offsets: rebase the
	// live index to the staged layout. Nothing reads the index
	// while gated, and a reopen rebuilds it from disk.
	if missing, ok := s.idx.rebaseBlocks(rebased); !ok {
		err := fmt.Errorf("spool: rebind missing block %d in rebase map: %w", missing, ErrCorrupt)
		s.setTerminal(err)
		return err
	}
	intent.phase = maintPhaseSegmentsDone
	if err := s.writeIntentFile(intent); err != nil {
		return fmt.Errorf("spool: write rebind intent: %w", err)
	}
	// Keyring first (context C2 under the same wrapping material),
	// then the manifest pointing at it. Either publication failing
	// is terminal: the live handle cannot tell which side won, but
	// the intent lets the next open resume to completion.
	if err := s.publishRebindKeyring(newContext); err != nil {
		return err
	}
	intent.phase = maintPhaseKeyringDone
	if err := s.writeIntentFile(intent); err != nil {
		return fmt.Errorf("spool: write rebind intent: %w", err)
	}
	s.manifestMu.Lock()
	s.man.context = newContext
	s.man.generation = genTarget
	s.manifestMu.Unlock()
	if err := s.persistManifest(s.man); err != nil {
		err = fmt.Errorf("spool: publish rebind: %w", err)
		s.setTerminal(err)
		return err
	}
	s.ctx = newContext
	if err := removeIntentFile(s.dir, s.faults); err != nil {
		return err
	}
	done = true
	s.maintEnd()
	return nil
}

// rebindPlain rebinds a plaintext store: nothing binds the context
// on disk, so only the recorded context changes.
func (s *Store) rebindPlain(newContext [16]byte) error {
	s.manifestMu.Lock()
	genTarget := s.man.generation + 1
	s.manifestMu.Unlock()
	if err := s.maintBegin("rebind", 0); err != nil {
		return err
	}
	done := false
	defer func() {
		if !done {
			s.maintEnd()
		}
	}()
	intent := &maintIntent{op: maintOpRebind, phase: maintPhaseSegmentsDone,
		targetContext: newContext, manifestTarget: genTarget}
	if err := s.writeIntentFile(intent); err != nil {
		return fmt.Errorf("spool: write rebind intent: %w", err)
	}
	s.manifestMu.Lock()
	s.man.context = newContext
	s.man.generation = genTarget
	s.manifestMu.Unlock()
	if err := s.persistManifest(s.man); err != nil {
		err = fmt.Errorf("spool: publish rebind: %w", err)
		s.setTerminal(err)
		return err
	}
	s.ctx = newContext
	if err := removeIntentFile(s.dir, s.faults); err != nil {
		return err
	}
	done = true
	s.maintEnd()
	return nil
}

// writeIntentFile persists one maintenance intent. All intent writes
// funnel through here so fault injection sees them.
func (s *Store) writeIntentFile(intent *maintIntent) error {
	if err := s.faults.trip("intent"); err != nil {
		return fmt.Errorf("spool: injected intent fault: %w", err)
	}
	return atomicWriteFile(s.dir, intentFileName, encodeIntent(intent))
}

// publishRebindKeyring flips the envelope context and persists it.
// Publication failure is terminal.
func (s *Store) publishRebindKeyring(newContext [16]byte) error {
	s.manifestMu.Lock()
	storeID := s.man.storeID
	s.manifestMu.Unlock()
	s.ring.mu.Lock()
	old := s.ring.context
	s.ring.context = newContext
	err := s.ring.writeKeysLocked(s.dir, storeID, s.master, s.pass, nil)
	if err != nil {
		s.ring.context = old
	}
	s.ring.mu.Unlock()
	if err != nil {
		err = fmt.Errorf("spool: publish rebind keyring: %w", err)
		s.setTerminal(err)
		return err
	}
	return nil
}

// maintEnv snapshots the staging parameters: usable while the live
// handle is gated, and independently at resume. Staging preserves
// each block's original codec rather than the current option, so no
// compression setting travels here.
type maintEnv struct {
	dir     string
	storeID [16]byte
	man     *manifest
	ring    *keyring
	comp    *compressor
	crypt   blockCrypt
	workers int
	faults  *FaultHooks
}

func (s *Store) maintEnv() maintEnv {
	return maintEnv{
		dir: s.dir, storeID: s.man.storeID, man: s.man, ring: s.ring,
		comp: s.comp, crypt: s.crypt, workers: s.workers, faults: s.faults,
	}
}

// stageSegmentFile re-seals one segment's validated groups under key
// and sealCtx, preserving group ids, block sequences, record
// sequences and contents. Groups are read under readCtx; rebind
// resume additionally retries fallbackCtx on authentication failure
// because a crash can strand mixed-context files (live runs pass no
// fallback). The staged replacement is verified before it renames
// over the original, so checkpoint hardlinks keep the original
// inode. It returns the staged data-block offsets by block sequence
// for the live index rebase.
//
// Data frames keep their exact lengths (same plaintext, same codec),
// but completion frames may shift by bytes: their plaintext embeds
// the content digest, which changes with every fresh seal. The live
// index rebase absorbs the resulting offset shifts.
func stageSegmentFile(env maintEnv, id uint64, keyID uint32, key [32]byte, readCtx, sealCtx, fallbackCtx [16]byte, allowFallback bool) (map[uint64]int64, error) {
	offsets := make(map[uint64]int64)
	segDir := filepath.Join(env.dir, segmentsDirName)
	path := filepath.Join(segDir, segmentFileName(id))
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("spool: stage opens segment %d: %w", id, err)
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("spool: stage stats segment %d: %w", id, err)
	}
	groups, _, _, _, err := assembleFile(f, st.Size(), id, false)
	f.Close()
	if err != nil {
		return nil, fmt.Errorf("spool: stage assembles segment %d: %w", id, err)
	}
	l := &loader{dir: env.dir, man: env.man, ring: env.ring, comp: env.comp, ctx: readCtx}
	workers := env.workers
	if workers < 1 {
		workers = 1
	}
	staged, err := os.CreateTemp(segDir, fmt.Sprintf(".rew-%d-*.tmp-", id))
	if err != nil {
		return nil, fmt.Errorf("spool: stage segment %d: %w", id, err)
	}
	stagedName := staged.Name()
	defer os.Remove(stagedName) // success path renames away
	var stagedOff int64
	for _, g := range groups {
		blocks, c, err := validateStagedGroup(l, id, g, fallbackCtx, allowFallback)
		if err != nil {
			staged.Close()
			return nil, err
		}
		frames, bodyLens, err := resealDataBlocks(env, c, blocks, g, keyID, key, sealCtx, workers)
		if err != nil {
			staged.Close()
			return nil, fmt.Errorf("spool: stage segment %d: %w", id, err)
		}
		compFrame, err := resealCompletion(env, c, g, frames, bodyLens, keyID, key, sealCtx)
		if err != nil {
			staged.Close()
			return nil, fmt.Errorf("spool: stage segment %d: %w", id, err)
		}
		for i, frame := range append(frames, compFrame) {
			if i < len(frames) {
				offsets[g.data[i].hdr.blockSeq] = stagedOff
			}
			if _, err := staged.Write(frame); err != nil {
				staged.Close()
				return nil, fmt.Errorf("spool: stage segment %d: %w", id, err)
			}
			stagedOff += int64(len(frame))
		}
	}
	if err := staged.Sync(); err != nil {
		staged.Close()
		return nil, fmt.Errorf("spool: sync staged segment %d: %w", id, err)
	}
	if err := staged.Close(); err != nil {
		return nil, fmt.Errorf("spool: close staged segment %d: %w", id, err)
	}
	if err := verifyStagedFile(env, stagedName, id, sealCtx); err != nil {
		return nil, err
	}
	if err := env.faults.trip("rename"); err != nil {
		return nil, fmt.Errorf("spool: publish staged segment %d: %w", id, err)
	}
	if err := os.Rename(stagedName, path); err != nil {
		return nil, fmt.Errorf("spool: publish staged segment %d: %w", id, err)
	}
	if err := env.faults.trip("dirsync"); err != nil {
		return nil, fmt.Errorf("spool: sync segments dir: %w", err)
	}
	if err := dirSync(segDir); err != nil {
		return nil, fmt.Errorf("spool: sync segments dir: %w", err)
	}
	return offsets, nil
}

// validateStagedGroup validates one group under the loader's
// context, retrying the fallback context once on authentication
// failure when allowed. A working fallback sticks for sibling
// groups (a file holds a single context); a failed one restores
// the primary.
func validateStagedGroup(l *loader, id uint64, g *assembledGroup, fallbackCtx [16]byte, allowFallback bool) ([]validatedRecords, completion, error) {
	primary := l.ctx
	blocks, c, err := l.validateGroup(id, g)
	if err == nil || !allowFallback || !errors.Is(err, ErrAuthFailed) {
		return blocks, c, err
	}
	l.ctx = fallbackCtx
	blocks, c, err = l.validateGroup(id, g)
	if err != nil {
		l.ctx = primary
	}
	return blocks, c, err
}

// resealDataBlocks re-encodes one validated group's data blocks with
// identical framing and fresh sealing. Block order, sequences,
// record contents, and each block's original codec are preserved
// exactly; codec preservation keeps every frame length identical,
// which the live index offsets depend on. Any length drift aborts
// the maintenance before anything publishes.
func resealDataBlocks(env maintEnv, c completion, blocks []validatedRecords, g *assembledGroup, keyID uint32, key [32]byte, sealCtx [16]byte, workers int) ([][]byte, []int, error) {
	type result struct {
		idx     int
		frame   []byte
		bodyLen int
		err     error
	}
	results := make(chan result, len(blocks))
	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup
	for i := range blocks {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			pending := make([]pendingRecord, len(blocks[idx].recs))
			for j := range blocks[idx].recs {
				r := &blocks[idx].recs[j]
				pending[j] = pendingRecord{key: r.Key, value: r.Value, tomb: r.Deleted, seq: r.Sequence}
			}
			mode, err := modeForCompressionID(g.data[idx].hdr.compression)
			if err != nil {
				results <- result{idx: idx, err: err}
				return
			}
			body := encodeBody(c.groupID, uint32(idx), c.blockCount, pending)
			frame, bodyLen, err := sealFrame(env.comp, env.crypt, mode, env.storeID,
				body, false, uint32(len(pending)), keyID, key, g.data[idx].hdr.blockSeq, sealCtx)
			if err == nil {
				want := blockHeaderLen + int(g.data[idx].hdr.sealedLen)
				if len(frame) != want {
					err = fmt.Errorf("spool: reseal length drift %d != %d: %w",
						len(frame), want, ErrCorrupt)
				}
			}
			results <- result{idx: idx, frame: frame, bodyLen: bodyLen, err: err}
		}(i)
	}
	go func() {
		wg.Wait()
		close(results)
	}()
	frames := make([][]byte, len(blocks))
	bodyLens := make([]int, len(blocks))
	for r := range results {
		if r.err != nil {
			return nil, nil, r.err
		}
		frames[r.idx] = r.frame
		bodyLens[r.idx] = r.bodyLen
	}
	return frames, bodyLens, nil
}

// resealCompletion re-seals a group's completion over its fresh data
// frames, preserving group identity with a recomputed digest. The
// original codec is preserved, but the frame length may legitimately
// shift by bytes: the plaintext embeds the fresh content digest.
func resealCompletion(env maintEnv, c completion, g *assembledGroup, frames [][]byte, bodyLens []int, keyID uint32, key [32]byte, sealCtx [16]byte) ([]byte, error) {
	seqs := make([]uint64, len(frames))
	sealed := make([][]byte, len(frames))
	var totalPlain uint64
	for i, f := range frames {
		seqs[i] = g.data[i].hdr.blockSeq
		sealed[i] = f[blockHeaderLen:]
		totalPlain += uint64(bodyLens[i])
	}
	digest := groupDigest(c.groupID, c.blockCount, seqs, sealed)
	body := encodeCompletion(completion{
		groupID:      c.groupID,
		blockCount:   c.blockCount,
		totalRecords: c.totalRecords,
		totalPlain:   totalPlain,
		blockSeqs:    seqs,
		digest:       digest,
	})
	mode, err := modeForCompressionID(g.completion.hdr.compression)
	if err != nil {
		return nil, err
	}
	frame, _, err := sealFrame(env.comp, env.crypt, mode, env.storeID,
		body, true, 0, keyID, key, g.completion.hdr.blockSeq, sealCtx)
	return frame, err
}

// verifyStagedFile assembles and validates a staged replacement
// before it is allowed to rename over its original.
func verifyStagedFile(env maintEnv, stagedName string, id uint64, sealCtx [16]byte) error {
	f, err := os.Open(stagedName)
	if err != nil {
		return fmt.Errorf("spool: verify opens staged segment %d: %w", id, err)
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return fmt.Errorf("spool: verify stats staged segment %d: %w", id, err)
	}
	groups, _, _, _, err := assembleFile(f, st.Size(), id, false)
	f.Close()
	if err != nil {
		return fmt.Errorf("spool: verify assembles staged segment %d: %w", id, err)
	}
	l := &loader{dir: env.dir, man: env.man, ring: env.ring, comp: env.comp, ctx: sealCtx}
	for _, g := range groups {
		if _, _, err := l.validateGroup(id, g); err != nil {
			return fmt.Errorf("spool: verify validates staged segment %d: %w", id, err)
		}
	}
	return nil
}

func removeIntentFile(dir string, faults *FaultHooks) error {
	if err := faults.trip("delete"); err != nil {
		return fmt.Errorf("spool: remove maintenance intent: %w", err)
	}
	if err := os.Remove(filepath.Join(dir, intentFileName)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("spool: remove maintenance intent: %w", err)
	}
	if err := faults.trip("dirsync"); err != nil {
		return fmt.Errorf("spool: sync store dir: %w", err)
	}
	if err := dirSync(dir); err != nil {
		return fmt.Errorf("spool: sync store dir: %w", err)
	}
	return nil
}

// resumeMaintenance completes an interrupted rewrite or rebind. It
// runs under the store lock before the open admits writes, and every
// step is idempotent: staging re-seals whole files, keyring
// publication is skipped when the unlocked envelope already carries
// the target context, and manifest publication is skipped at the
// target generation. A corrupt or unreadable intent fails the open;
// resume never guesses.
func resumeMaintenance(dir string, masterKey []byte, passphrase string, compression Compression, codecs []compression.Codec, workers int, faults *FaultHooks) error {
	raw, err := os.ReadFile(filepath.Join(dir, intentFileName))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("spool: read maintenance intent: %w", err)
	}
	intent, err := parseIntent(raw)
	if err != nil {
		return err
	}
	mraw, err := os.ReadFile(filepath.Join(dir, manifestFileName))
	if err != nil {
		return fmt.Errorf("spool: resume reads manifest: %w", err)
	}
	man, err := parseManifest(mraw)
	if err != nil {
		return err
	}
	if man.generation > intent.manifestTarget {
		return fmt.Errorf("spool: manifest generation %d past intent target %d: %w",
			man.generation, intent.manifestTarget, ErrCorrupt)
	}
	comp, err := newCompressor(compression, codecs)
	if err != nil {
		return err
	}
	crypt := blockCrypt{algo: algoNone}
	if man.encrypted {
		crypt = blockCrypt{algo: algoAESGCM}
	}
	var ring *keyring
	if man.encrypted {
		ring, err = readKeys(dir, masterKey, passphrase, man.storeID, man.context)
		if err != nil && intent.op == maintOpRebind && errors.Is(err, ErrWrongKey) {
			// The keyring may already carry the target
			// context while the manifest still points at
			// the old one (crash between publications).
			ring, err = readKeys(dir, masterKey, passphrase, man.storeID, intent.targetContext)
		}
		if err != nil {
			return fmt.Errorf("spool: resume unlocks keys.enc: %w", err)
		}
		ring.faults = faults
	}
	if workers < 1 {
		workers = 2
	}
	env := maintEnv{dir: dir, storeID: man.storeID, man: man, ring: ring,
		comp: comp, crypt: crypt, workers: workers, faults: faults}
	sealCtx := man.context
	if intent.op == maintOpRebind {
		sealCtx = intent.targetContext
	}
	if intent.phase < maintPhaseSegmentsDone {
		var keyID uint32
		var key [32]byte
		if ring != nil {
			var ok bool
			keyID, key, ok = ring.currentKey()
			if !ok {
				return errNoCurrentKey
			}
		}
		// Files are read under the manifest's (pre-rewrite)
		// context: the keyring may already carry the target
		// while files still await staging. Partially staged
		// rebinds retry the target per file.
		readCtx := man.context
		allowFallback := intent.op == maintOpRebind && ring != nil
		for _, id := range man.members {
			if _, err := stageSegmentFile(env, id, keyID, key, readCtx, sealCtx, intent.targetContext, allowFallback); err != nil {
				return err
			}
		}
		intent.phase = maintPhaseSegmentsDone
		if err := faults.trip("intent"); err != nil {
			return fmt.Errorf("spool: injected intent fault: %w", err)
		}
		if err := atomicWriteFile(dir, intentFileName, encodeIntent(intent)); err != nil {
			return fmt.Errorf("spool: resume writes intent: %w", err)
		}
	}
	if intent.op == maintOpRebind && ring != nil && intent.phase < maintPhaseKeyringDone &&
		ring.context != intent.targetContext {
		ring.context = sealCtx
		if err := ring.writeKeys(dir, man.storeID, masterKey, passphrase); err != nil {
			return fmt.Errorf("spool: resume publishes keyring: %w", err)
		}
		intent.phase = maintPhaseKeyringDone
		if err := faults.trip("intent"); err != nil {
			return fmt.Errorf("spool: injected intent fault: %w", err)
		}
		if err := atomicWriteFile(dir, intentFileName, encodeIntent(intent)); err != nil {
			return fmt.Errorf("spool: resume writes intent: %w", err)
		}
	}
	if man.generation != intent.manifestTarget {
		man.context = sealCtx
		man.generation = intent.manifestTarget
		// Resume's renames changed the on-disk shape a close-time
		// clean marker describes, so the marker must not survive
		// this publish: the post-rebuild check would compare the
		// pre-resume shape against post-resume state. Integrity
		// still holds: resume verified every staged file, and the
		// rebuild validates every group of every member.
		man.clean = false
		man.cleanBytes = 0
		man.cleanFiles = 0
		man.cleanGen = 0
		man.cleanGroup = 0
		if err := faults.trip("manifest"); err != nil {
			return fmt.Errorf("spool: injected manifest fault: %w", err)
		}
		if err := atomicWriteFile(dir, manifestFileName, man.encode()); err != nil {
			return fmt.Errorf("spool: resume publishes manifest: %w", err)
		}
	}
	return removeIntentFile(dir, faults)
}
