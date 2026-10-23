package spool

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// segmentFileName formats a segment leaf name: zero-padded id plus
// the .spool suffix.
func segmentFileName(id uint64) string {
	return fmt.Sprintf("%012d.spool", id)
}

// parseSegmentName extracts the id from a segment leaf name.
func parseSegmentName(name string) (uint64, bool) {
	if !strings.HasSuffix(name, ".spool") {
		return 0, false
	}
	id, err := strconv.ParseUint(strings.TrimSuffix(name, ".spool"), 10, 64)
	if err != nil || id == 0 {
		return 0, false
	}
	return id, true
}

// listSegments returns sorted segment ids present in dir,
// ignoring non-segment files.
func listSegments(dir string) ([]uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("spool: list segments: %w", err)
	}
	var out []uint64
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if id, ok := parseSegmentName(e.Name()); ok {
			out = append(out, id)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

// segmentWriter appends block frames to the active segment and rolls
// to a new segment past maxSize. All methods serialize on mu, which
// is the single commit point for physical appends: flushes and
// compactions share it.
//
// Lock order: appendMu -> fileStats. Never acquire appendMu while
// holding the stats lock.
type segmentWriter struct {
	dir     string
	maxSize int64

	mu     sync.Mutex
	file   *os.File
	buf    *bufio.Writer
	id     uint64
	offset int64 // logical end (includes buffered bytes)

	// onRotate runs under mu after the old file is sealed and before
	// the new file accepts appends. It persists rotation side effects
	// (manifest, stats, index of liveness).
	onRotate func(sealedID, newID uint64) error

	faults *FaultHooks
}

// openSegmentWriter opens (or creates) the active segment id. A
// missing file is created; an existing one is opened for append and
// its size becomes the starting offset.
func openSegmentWriter(dir string, maxSize int64, activeID uint64, faults *FaultHooks) (*segmentWriter, error) {
	w := &segmentWriter{dir: dir, maxSize: maxSize, id: activeID, faults: faults}
	if err := w.openActive(); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *segmentWriter) openActive() error {
	path := filepath.Join(w.dir, segmentFileName(w.id))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("spool: open segment %d: %w", w.id, err)
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return fmt.Errorf("spool: stat segment %d: %w", w.id, err)
	}
	w.file = f
	w.buf = bufio.NewWriterSize(f, 1<<20)
	w.offset = st.Size()
	return nil
}

// activeID reports the current segment id.
func (w *segmentWriter) activeID() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.id
}

// endOffset reports the logical end of the active segment.
func (w *segmentWriter) endOffset() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.offset
}

// rotate seals the active segment and opens its successor.
// appendGroup writes a commit group's frames contiguously into the
// active segment without rotating: groups never split across files
// (the caller reserves space by rotating first). It returns the home
// file id and each frame's starting offset.
func (w *segmentWriter) appendGroup(frames [][]byte) (uint64, []int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return 0, nil, fmt.Errorf("spool: segment writer closed: %w", ErrClosed)
	}
	if err := w.faults.trip("append"); err != nil {
		return 0, nil, fmt.Errorf("spool: injected append fault: %w", err)
	}
	offs := make([]int64, len(frames))
	for i, frame := range frames {
		offs[i] = w.offset
		if _, err := w.buf.Write(frame); err != nil {
			return 0, nil, fmt.Errorf("spool: append group frame %d: %w", i, err)
		}
		w.offset += int64(len(frame))
	}
	return w.id, offs, nil
}

// rotateTo seals the active segment and opens the successor with
// the given id. The id must come from allocateFileID: successor
// ids are manifest-allocated, never sealed+1, because compaction
// reserves replacement ids from the same allocator and a
// sealed+1 successor could collide with a live replacement. The
// successor file is created before the old file seals, so a
// creation failure leaves the writer usable on the old file.
func (w *segmentWriter) rotateTo(next uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return fmt.Errorf("spool: segment writer closed: %w", ErrClosed)
	}
	sealed := w.id
	path := filepath.Join(w.dir, segmentFileName(next))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("spool: create segment %d: %w", next, err)
	}
	abort := func(format string, args ...any) error {
		f.Close()
		os.Remove(path)
		return fmt.Errorf(format, args...)
	}
	if err := w.buf.Flush(); err != nil {
		return abort("spool: flush rotated segment: %w", err)
	}
	// Fsync on rotation bounds Async-mode loss to the active segment;
	// one fsync per segment is negligible.
	if err := w.file.Sync(); err != nil {
		return abort("spool: sync rotated segment: %w", err)
	}
	if err := w.file.Close(); err != nil {
		return abort("spool: close rotated segment: %w", err)
	}
	w.file = f
	w.buf = bufio.NewWriterSize(f, 1<<20)
	w.id = next
	w.offset = 0
	if err := w.faults.trip("dirsync"); err != nil {
		return fmt.Errorf("spool: sync segments dir: %w", err)
	}
	if err := dirSync(w.dir); err != nil {
		return fmt.Errorf("spool: sync segments dir: %w", err)
	}
	if w.onRotate != nil {
		if err := w.onRotate(sealed, next); err != nil {
			return err
		}
	}
	return nil
}

// commit applies the durability level to staged bytes: Async stages
// only, Flush pushes bytes to the OS, Sync additionally fsyncs.
func (w *segmentWriter) commit(d Durability) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return fmt.Errorf("spool: segment writer closed: %w", ErrClosed)
	}
	switch d {
	case DurabilityAsync:
		return nil
	case DurabilityFlush:
		if err := w.buf.Flush(); err != nil {
			return fmt.Errorf("spool: flush segment: %w", err)
		}
		return nil
	case DurabilitySync:
		if err := w.buf.Flush(); err != nil {
			return fmt.Errorf("spool: flush segment: %w", err)
		}
		if err := w.faults.trip("sync"); err != nil {
			return fmt.Errorf("spool: injected sync fault: %w", err)
		}
		if err := w.file.Sync(); err != nil {
			return fmt.Errorf("spool: sync segment: %w", err)
		}
		return nil
	default:
		return fmt.Errorf("spool: unknown durability %d", int(d))
	}
}

// sync flushes and fsyncs the active segment.
func (w *segmentWriter) sync() error {
	return w.commit(DurabilitySync)
}

// reopenActive drops the current file description and re-opens the
// same active segment path. Maintenance renames replacements over
// the active file, which orphans the writer's old inode; without a
// reopen, subsequent appends would land invisibly on the orphan.
func (w *segmentWriter) reopenActive() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return fmt.Errorf("spool: segment writer closed: %w", ErrClosed)
	}
	if err := w.buf.Flush(); err != nil {
		return fmt.Errorf("spool: flush segment: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("spool: sync segment: %w", err)
	}
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("spool: close segment: %w", err)
	}
	path := filepath.Join(w.dir, segmentFileName(w.id))
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		w.file = nil
		return fmt.Errorf("spool: reopen segment %d: %w", w.id, err)
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		w.file = nil
		return fmt.Errorf("spool: stat segment %d: %w", w.id, err)
	}
	w.file = f
	w.buf = bufio.NewWriterSize(f, 1<<20)
	w.offset = st.Size()
	return nil
}

// close flushes, syncs and closes the active segment.
func (w *segmentWriter) close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	if err := w.buf.Flush(); err != nil {
		return fmt.Errorf("spool: close flush: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("spool: close sync: %w", err)
	}
	err := w.file.Close()
	w.file = nil
	w.buf = nil
	return err
}

// readBlockFrame reads one framed block at offset: header plus sealed
// payload. Short reads (clean end or torn tail) return io.EOF; the
// caller stops with the current valid end. Real I/O failures and
// corrupt headers are returned as errors.
// peekBlockNext returns the offset past the framed block at offset
// without retaining its payload. Short reads report io.EOF for the
// torn-tail rule; anything else failing is corruption.
func peekBlockNext(f *os.File, offset int64) (int64, error) {
	raw := make([]byte, blockHeaderLen)
	if _, err := f.ReadAt(raw, offset); err != nil {
		if err == io.EOF {
			return offset, io.EOF
		}
		return offset, fmt.Errorf("spool: read block header: %w", err)
	}
	hdr, err := parseBlockHeader(raw)
	if err != nil {
		return offset, err
	}
	if hdr.sealedLen > absMaxBlockBytes {
		return offset, fmt.Errorf("spool: sealed size %d exceeds %d: %w", hdr.sealedLen, absMaxBlockBytes, ErrCorrupt)
	}
	// The payload is fully present iff its last byte reads.
	var probe [1]byte
	end := offset + blockHeaderLen + int64(hdr.sealedLen)
	if _, err := f.ReadAt(probe[:], end-1); err != nil {
		if err == io.EOF {
			return offset, io.EOF
		}
		return offset, fmt.Errorf("spool: read block payload: %w", err)
	}
	return end, nil
}

func readBlockFrame(f *os.File, offset int64, maxSealed uint32) (hdr *blockHeader, frame []byte, next int64, err error) {
	raw := make([]byte, blockHeaderLen)
	if _, err := f.ReadAt(raw, offset); err != nil {
		if err == io.EOF {
			return nil, nil, offset, io.EOF
		}
		return nil, nil, offset, fmt.Errorf("spool: read block header: %w", err)
	}
	hdr, err = parseBlockHeader(raw)
	if err != nil {
		return nil, nil, offset, err
	}
	if hdr.sealedLen > maxSealed {
		return nil, nil, offset, fmt.Errorf("spool: sealed size %d exceeds %d: %w", hdr.sealedLen, maxSealed, ErrCorrupt)
	}
	frame = make([]byte, blockHeaderLen+hdr.sealedLen)
	copy(frame, hdr.raw)
	if _, err := f.ReadAt(frame[blockHeaderLen:], offset+blockHeaderLen); err != nil {
		if err == io.EOF {
			return nil, nil, offset, io.EOF
		}
		return nil, nil, offset, fmt.Errorf("spool: read block payload: %w", err)
	}
	return hdr, frame, offset + int64(len(frame)), nil
}
