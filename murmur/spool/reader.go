package spool

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// Point reads over committed segments. Pending (unflushed) puts are
// excluded. Only key locations live in the index; values are
// authenticated and decoded from disk, with decoded blocks retained
// in the byte-bounded hot cache.

// Get reads the latest committed value for key. It returns (nil, nil)
// when the key has no live value. It serializes with maintenance
// that replaces segment files.
func (s *Store) Get(key []byte) ([]byte, error) {
	s.maintExcl.Lock()
	defer s.maintExcl.Unlock()
	if s.closed.Load() {
		return nil, ErrClosed
	}
	loc, ok := s.idx.location(key)
	if !ok {
		return nil, nil
	}
	ck := cacheKey{file: loc.FileID, offset: loc.BlockOffset}
	s.readCacheMu.Lock()
	blk := s.readCache.get(ck)
	s.readCacheMu.Unlock()
	if blk == nil {
		var err error
		blk, err = s.readCachedBlock(loc.FileID, loc.BlockOffset)
		if err != nil {
			return nil, err
		}
		s.readCacheMu.Lock()
		s.readCache.insert(ck, blk)
		s.readCacheMu.Unlock()
	}
	if int(loc.RecordIndex) >= len(blk.records) {
		return nil, fmt.Errorf("spool: invalid indexed record: %w", ErrCorrupt)
	}
	rec := blk.records[loc.RecordIndex]
	if !bytes.Equal(rec.Key, key) || rec.Deleted {
		return nil, fmt.Errorf("spool: indexed key mismatch: %w", ErrCorrupt)
	}
	return append([]byte(nil), rec.Value...), nil
}

// readCachedBlock opens, authenticates and decodes one indexed block.
// The caller must hold maintExcl: maintenance replaces segment files.
func (s *Store) readCachedBlock(fileID uint64, offset int64) (*cacheBlock, error) {
	s.manifestMu.Lock()
	man := s.man
	ctx := s.ctx
	s.manifestMu.Unlock()
	l := &loader{dir: s.dir, man: man, ring: s.ring, comp: s.comp, ctx: ctx}
	path := filepath.Join(s.dir, segmentsDirName, segmentFileName(fileID))
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("spool: open indexed segment: %w", err)
	}
	hdr, frame, _, err := readBlockFrame(f, offset, absMaxBlockBytes)
	cerr := f.Close()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("spool: missing indexed block: %w", ErrCorrupt)
		}
		return nil, err
	}
	if cerr != nil {
		return nil, fmt.Errorf("spool: close indexed segment: %w", cerr)
	}
	plain, err := l.openPayload(hdr, frame[blockHeaderLen:])
	if err != nil {
		return nil, err
	}
	_, _, _, recs, err := decodeBody(plain, hdr.recordCount)
	if err != nil {
		return nil, err
	}
	return &cacheBlock{frame: frame, plain: plain, records: recs}, nil
}

// KeysWithPrefix returns every committed live key with prefix,
// sorted. Only key locations live in the index; values are read
// from disk with Get.
func (s *Store) KeysWithPrefix(prefix []byte) [][]byte {
	return s.idx.keysWithPrefix(prefix)
}

// KeysPage returns at most limit sorted committed keys with prefix,
// exclusively after after (nil starts at the beginning). Values
// remain in authenticated blocks; no value scan occurs. Use a
// checkpoint when a stable multi-page boundary is required.
func (s *Store) KeysPage(prefix, after []byte, limit int) [][]byte {
	return s.idx.keysPage(prefix, after, limit)
}

// LiveKeyVersion pairs a committed live key with its latest logical
// sequence. Sequences order versions of a key: every new committed
// mutation advances them, while compaction, key rotation, and reopen
// preserve them.
type LiveKeyVersion struct {
	Key      []byte
	Sequence uint64
}

// LiveKeyVersions returns every committed live key with prefix and its
// latest logical sequence, sorted by raw key bytes. Deleted keys are
// excluded; no value is read. Key bytes are copies the caller owns.
//
// Each shard is copied under its own read lock, so concurrent commits
// may interleave between shards. Callers that need a global snapshot
// must quiesce writers (as during open, before the worker starts) or
// read from a Checkpoint. Under concurrency every returned pair is a
// real committed version, but the set may mix moments.
//
// The scan is one pass over the index and holds O(N) memory for N
// matching keys. For bounded memory over huge key spaces use
// ScanLiveKeyVersions and sort externally.
func (s *Store) LiveKeyVersions(prefix []byte) ([]LiveKeyVersion, error) {
	if s.closed.Load() {
		return nil, ErrClosed
	}
	return s.idx.liveKeyVersions(prefix), nil
}

// ScanLiveKeyVersions calls fn once per committed live key with
// prefix, in shard order (not sorted), copying one shard at a time so
// peak memory tracks the largest shard's matches rather than the
// total. The same stability caveats as LiveKeyVersions apply. A
// non-nil fn error aborts the scan and is returned; key bytes are
// fresh per call and must be copied to retain.
func (s *Store) ScanLiveKeyVersions(prefix []byte, fn func(LiveKeyVersion) error) error {
	if s.closed.Load() {
		return ErrClosed
	}
	if fn == nil {
		return fmt.Errorf("spool: nil scan callback")
	}
	return s.idx.scanLiveKeys(prefix, fn)
}
