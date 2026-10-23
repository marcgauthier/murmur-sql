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
