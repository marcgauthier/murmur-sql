package spool

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// DataKeyInfo is non-secret metadata for one envelope data key.
type DataKeyInfo struct {
	ID        uint32
	CreatedAt time.Time
	Status    KeyStatus
}

// KeyInventory is a non-secret snapshot of envelope identity, data
// keys, and maintenance progress for diagnostics and rotation APIs.
// It carries no key material.
type KeyInventory struct {
	// Encrypted reports whether the store uses key material at all.
	Encrypted bool
	// WrappingKeyID is the envelope's authenticated wrapping id.
	WrappingKeyID string
	// ActiveDataKeyID is the current key for new blocks.
	ActiveDataKeyID uint32
	// DataKeys lists every envelope key by id, current and retired.
	DataKeys []DataKeyInfo
	// KeyringSeq is the envelope's persist counter.
	KeyringSeq uint64
	// ManifestGeneration is the authoritative membership generation.
	ManifestGeneration uint64
	// MaintenancePhase names the running maintenance operation
	// ("idle" when none), with Done/Total progress in files.
	MaintenancePhase string
	MaintenanceDone  uint64
	MaintenanceTotal uint64
}

// KeyInventory snapshots non-secret key and maintenance metadata. It
// touches memory only; per-key segment references need the separate
// KeyReferences scan.
func (s *Store) KeyInventory() KeyInventory {
	inv := KeyInventory{}
	s.maintMu.Lock()
	inv.MaintenancePhase = s.maintPhase
	inv.MaintenanceDone = s.maintDone
	inv.MaintenanceTotal = s.maintTotal
	s.maintMu.Unlock()
	if s.ring == nil {
		return inv
	}
	inv.Encrypted = true
	s.ring.mu.RLock()
	inv.WrappingKeyID = s.ring.wrapID
	inv.ActiveDataKeyID = s.ring.current
	inv.KeyringSeq = s.ring.seq
	inv.DataKeys = make([]DataKeyInfo, 0, len(s.ring.keys))
	for _, k := range s.ring.keys {
		inv.DataKeys = append(inv.DataKeys, DataKeyInfo{ID: k.ID, CreatedAt: time.Unix(0, k.CreatedAt), Status: k.Status})
	}
	s.ring.mu.RUnlock()
	sort.Slice(inv.DataKeys, func(i, j int) bool { return inv.DataKeys[i].ID < inv.DataKeys[j].ID })
	s.manifestMu.Lock()
	inv.ManifestGeneration = s.man.generation
	s.manifestMu.Unlock()
	return inv
}

// KeyReference counts one data key's live references: authoritative
// segments and validated blocks using it, sealed-but-unpublished
// frames, and registered checkpoint dependencies.
type KeyReference struct {
	KeyID        uint32
	SegmentFiles uint64
	Blocks       uint64
	Inflight     bool
	Checkpoints  uint64
}

// KeyReferences scans authoritative segments with full group
// validation and reports per-key references. Validation failures
// abort the scan with an error and change nothing. The scan reads
// every segment, so it costs about as much as a startup load;
// pruning and diagnostics call it, not the write path.
func (s *Store) KeyReferences() ([]KeyReference, error) {
	if s.closed.Load() {
		return nil, ErrClosed
	}
	if err := s.checkTerminal(); err != nil {
		return nil, err
	}
	if err := s.checkMaint(); err != nil {
		return nil, err
	}
	if s.ring == nil {
		return nil, nil
	}
	return s.scanKeyReferences()
}

// scanKeyReferences implements KeyReferences without lifecycle
// checks so pruning can share it under the rotation lock. It
// flushes staged bytes first: Async seals leave up to a megabyte
// in the segment buffer after publication, and a scan of the file
// alone would miss those groups' references. Pruning calls this
// under seal quiescence, where no seal can append, so the flush
// makes the scan observe every published group; direct
// KeyReferences callers get best-effort freshness.
func (s *Store) scanKeyReferences() ([]KeyReference, error) {
	if err := s.seg.commit(DurabilityFlush); err != nil {
		return nil, err
	}
	s.manifestMu.Lock()
	members := append([]uint64(nil), s.man.members...)
	s.manifestMu.Unlock()
	l := &loader{dir: s.dir, man: s.man, ring: s.ring, comp: s.comp, ctx: s.ctx}
	segDir := filepath.Join(s.dir, segmentsDirName)
	blocks := make(map[uint32]uint64)
	files := make(map[uint32]map[uint64]bool)
	for _, id := range members {
		path := filepath.Join(segDir, segmentFileName(id))
		f, err := os.Open(path)
		if err != nil {
			return nil, fmt.Errorf("spool: reference scan opens segment %d: %w", id, err)
		}
		st, err := f.Stat()
		if err != nil {
			f.Close()
			return nil, fmt.Errorf("spool: reference scan stats segment %d: %w", id, err)
		}
		groups, _, _, _, err := assembleFile(f, st.Size(), id, false)
		f.Close()
		if err != nil {
			return nil, fmt.Errorf("spool: reference scan assembles segment %d: %w", id, err)
		}
		for _, g := range groups {
			if _, _, err := l.validateGroup(id, g); err != nil {
				return nil, fmt.Errorf("spool: reference scan validates segment %d: %w", id, err)
			}
			touch := func(keyID uint32) {
				blocks[keyID]++
				set := files[keyID]
				if set == nil {
					set = make(map[uint64]bool)
					files[keyID] = set
				}
				set[id] = true
			}
			for _, b := range g.data {
				touch(b.hdr.keyID)
			}
			touch(g.completion.hdr.keyID)
		}
	}
	s.sealKeyMu.Lock()
	inflight := make(map[uint32]int, len(s.sealKeys))
	for k, v := range s.sealKeys {
		inflight[k] = v
	}
	s.sealKeyMu.Unlock()
	s.ckptMu.Lock()
	ckpt := make(map[uint32]int, len(s.ckptKeyRefs))
	for k, v := range s.ckptKeyRefs {
		ckpt[k] = v
	}
	s.ckptMu.Unlock()
	union := make(map[uint32]bool)
	for k := range blocks {
		union[k] = true
	}
	for k := range inflight {
		union[k] = true
	}
	for k := range ckpt {
		union[k] = true
	}
	out := make([]KeyReference, 0, len(union))
	for k := range union {
		out = append(out, KeyReference{
			KeyID:        k,
			SegmentFiles: uint64(len(files[k])),
			Blocks:       blocks[k],
			Inflight:     inflight[k] > 0,
			Checkpoints:  uint64(ckpt[k]),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].KeyID < out[j].KeyID })
	return out, nil
}

// PruneDataKeys drops retired data keys that nothing references: no
// authoritative segment block, no sealed-but-unpublished frame, and
// no registered checkpoint. The active key is never pruned. The
// surviving envelope persists atomically; publication failures are
// terminal. It returns the pruned key ids.
//
// A validation failure during the reference scan aborts pruning
// with an ordinary error: the corruption will fail the next open
// loudly, but this handle stays usable.
//
// Pruning quiesces seals: it takes maintExcl (blocking; maintenance
// seals do not take the quiescence lock) and holds sealQuiesceMu
// exclusively across the whole scan-and-delete, so the reference
// snapshot observes every seal that could publish an unreferenced
// key. In-flight seals drain first and new seals wait; writes
// stall for one prune scan.
func (s *Store) PruneDataKeys() ([]uint32, error) {
	if s.closed.Load() {
		return nil, ErrClosed
	}
	if err := s.checkTerminal(); err != nil {
		return nil, err
	}
	if err := s.checkMaint(); err != nil {
		return nil, err
	}
	if s.ring == nil {
		return nil, nil
	}
	s.maintExcl.Lock()
	defer s.maintExcl.Unlock()
	s.rotateMu.Lock()
	defer s.rotateMu.Unlock()
	s.sealQuiesceMu.Lock()
	defer s.sealQuiesceMu.Unlock()
	refs, err := s.scanKeyReferences()
	if err != nil {
		return nil, err
	}
	used := make(map[uint32]bool)
	for _, r := range refs {
		if r.SegmentFiles > 0 || r.Blocks > 0 || r.Inflight || r.Checkpoints > 0 {
			used[r.KeyID] = true
		}
	}
	s.manifestMu.Lock()
	storeID := s.man.storeID
	s.manifestMu.Unlock()
	s.ring.mu.Lock()
	var dropped []*DataKey
	for id, k := range s.ring.keys {
		if k.Status == KeyStatusRetired && !used[id] {
			dropped = append(dropped, k)
			delete(s.ring.keys, id)
		}
	}
	if len(dropped) == 0 {
		s.ring.mu.Unlock()
		return nil, nil
	}
	if err := s.ring.writeKeysLocked(s.dir, storeID, s.master, s.pass, nil); err != nil {
		for _, k := range dropped {
			s.ring.keys[k.ID] = k
		}
		s.ring.mu.Unlock()
		err = fmt.Errorf("spool: publish key pruning: %w", err)
		s.setTerminal(err)
		return nil, err
	}
	s.ring.mu.Unlock()
	ids := make([]uint32, 0, len(dropped))
	for _, k := range dropped {
		ids = append(ids, k.ID)
		clear(k.Key[:])
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, nil
}
