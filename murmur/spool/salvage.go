package spool

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/marcgauthier/murmur-sql/murmur/compression"
)

// SalvageGapReason names why a source byte range was unrecoverable.
type SalvageGapReason string

// Salvage gap reasons.
const (
	// GapCorrupt marks bytes that fail block framing while valid
	// frames follow: genuine mid-file corruption, never truncation.
	GapCorrupt SalvageGapReason = "corrupt"
	// GapAuth marks a structurally complete group whose
	// authentication, digest, or framing verification failed. Its
	// bytes frame correctly but their content is untrustworthy.
	GapAuth SalvageGapReason = "auth"
	// GapTornTail marks trailing bytes no valid frame follows: a
	// crash interrupted an append (or garbage was appended).
	GapTornTail SalvageGapReason = "torn-tail"
	// GapIncomplete marks a trailing group of individually valid
	// frames that never got its completion: nothing after it can
	// prove it committed, so the whole group is dropped.
	GapIncomplete SalvageGapReason = "incomplete-group"
	// GapMissing marks a manifest member with no segment file on
	// disk. End is -1: the size is unknown.
	GapMissing SalvageGapReason = "missing-file"
)

// SalvageGap is one unrecoverable source byte range.
type SalvageGap struct {
	// File is the segment id holding the gap.
	File uint64
	// Start and End bound the gap in bytes. End is -1 for
	// GapMissing: the file is gone so its size is unknown.
	Start int64
	End   int64
	// Reason names why the range was unrecoverable.
	Reason SalvageGapReason
	// Detail carries the underlying failure text.
	Detail string
}

// SalvageReport summarizes a Salvage run.
type SalvageReport struct {
	// Records counts salvaged records, including superseded
	// versions and tombstones: every authenticated record is
	// copied, and the destination resolves winners by sequence.
	Records int64
	// Files counts scanned manifest members.
	Files int
	// Gaps lists every unrecoverable byte range in scan order.
	// Empty for a clean store.
	Gaps []SalvageGap
}

// SalvageOptions configures Salvage.
type SalvageOptions struct {
	// SrcDir is the damaged store directory. It is opened
	// read-only and locked for the whole scan; Salvage never
	// writes to it.
	SrcDir string
	// DestDir receives the salvaged store. It must not exist or
	// must be empty; it must differ from SrcDir.
	DestDir string
	// MasterKey is the 32-byte store key. Exactly one of
	// MasterKey and Passphrase must be set for encrypted stores;
	// both must be empty for plaintext stores (a plaintext source
	// salvages to a plaintext destination).
	MasterKey []byte
	// Passphrase unlocks passphrase-protected stores.
	Passphrase string
	// WrappingKeyID, when set, must match the source envelope's
	// authenticated wrapping id.
	WrappingKeyID string
	// ContextID, when 16 bytes, must match the source store's
	// persisted database context. Empty accepts it.
	ContextID []byte
	// Codecs registers custom compression codecs (ids 128-255)
	// needed to read source blocks written under them.
	Codecs []compression.Codec
	// Codec, when non-nil, is the block codec used for
	// destination blocks. Nil writes the spool default. Murmur
	// stores pass their zstd codec so salvaged blocks match the
	// store's standard shape (any codec reads back regardless).
	Codec compression.Codec
}

// salvageBatchBytes bounds one destination commit group: salvage
// streams the whole store, so verified records commit in bounded
// batches instead of one unbounded admission.
const salvageBatchBytes = 32 << 20

// Salvage copies every authenticated record from a damaged store
// into a fresh store, skipping what it cannot verify and reporting
// each skipped byte range in the report. It is the offline
// corruption-repair primitive behind murmur-admin repair.
//
// The source is locked and scanned read-only; it is never
// modified. Each segment's framing is walked block by block: a
// framing failure resyncs to the next valid header (or records a
// torn tail when nothing valid follows), and each structurally
// complete group is authenticated exactly like a normal load. Only
// verified records reach the destination; failures become gaps,
// never silent drops and never unverified data.
//
// Records keep their original sequences, so winners resolve
// exactly as in the source (compaction relocates old versions into
// newer files; fresh sequences in scan order would elect the wrong
// winners). The destination's sequence epoch advances past the
// highest salvaged epoch, so later writes sort after salvaged data.
// Encryption mirrors the source: an encrypted source needs its key
// and salvages to an encrypted destination; a plaintext source
// salvages to plaintext. The destination keeps the source's
// database context but gets a fresh store identity.
//
// Salvage fails closed on a wrong key, a locked (live) source, a
// non-empty destination, an unreadable manifest, and I/O errors.
// A missing member file is a gap, not a failure: everything else
// still salvages.
func Salvage(o SalvageOptions) (SalvageReport, error) {
	var rep SalvageReport
	if o.SrcDir == "" {
		return rep, fmt.Errorf("spool: salvage: source directory is empty")
	}
	if o.DestDir == "" {
		return rep, fmt.Errorf("spool: salvage: destination directory is empty")
	}
	srcAbs, err := filepath.Abs(o.SrcDir)
	if err != nil {
		return rep, fmt.Errorf("spool: salvage: source directory: %w", err)
	}
	dstAbs, err := filepath.Abs(o.DestDir)
	if err != nil {
		return rep, fmt.Errorf("spool: salvage: destination directory: %w", err)
	}
	if srcAbs == dstAbs {
		return rep, fmt.Errorf("spool: salvage: source and destination are the same directory")
	}
	if entries, err := os.ReadDir(o.DestDir); err == nil {
		if len(entries) != 0 {
			return rep, fmt.Errorf("spool: salvage: destination %q is not empty", o.DestDir)
		}
	} else if !os.IsNotExist(err) {
		return rep, fmt.Errorf("spool: salvage: destination directory: %w", err)
	}
	// Lock the source before reading anything: a live store could
	// rotate or unlink segments mid-scan.
	unlock, err := lockStore(o.SrcDir)
	if err != nil {
		return rep, err
	}
	defer unlock()

	l, err := openLoader(LoadOptions{
		Path:          o.SrcDir,
		MasterKey:     o.MasterKey,
		Passphrase:    o.Passphrase,
		WrappingKeyID: o.WrappingKeyID,
		ContextID:     o.ContextID,
		Codecs:        o.Codecs,
	})
	if err != nil {
		return rep, err
	}

	dopts := DefaultOptions(o.DestDir)
	if l.man.encrypted {
		dopts.MasterKey = o.MasterKey
		dopts.Passphrase = o.Passphrase
	} else {
		dopts.MasterKey = nil
		dopts.Passphrase = ""
		dopts.Encryption = EncryptionNone
	}
	dopts.ContextID = append([]byte(nil), l.ctx[:]...)
	dopts.ReclaimInterval = -1 // no background compaction during bulk salvage
	if o.Codec != nil {
		dopts.Codec = o.Codec
	}
	dest, err := Open(dopts)
	if err != nil {
		return rep, err
	}
	closeDest := func() {
		_ = dest.Close()
	}

	var staged []pendingRecord
	var stagedBytes int64
	var maxEpoch uint64
	flush := func() error {
		if len(staged) == 0 {
			return nil
		}
		dest.admissionMu.Lock()
		g := dest.admitLocked(staged, DurabilitySync)
		dest.admissionMu.Unlock()
		staged = nil
		stagedBytes = 0
		return dest.waitForGroup(g.id, false)
	}
	emit := func(r Record) {
		pr := pendingRecord{
			key:  cloneBytes(r.Key),
			seq:  r.Sequence,
			tomb: r.Deleted,
			size: len(r.Key) + len(r.Value) + recordOverhead,
		}
		if !r.Deleted {
			pr.value = cloneBytes(r.Value)
		}
		if epoch := r.Sequence >> epochShift; epoch > maxEpoch {
			maxEpoch = epoch
		}
		stagedBytes += int64(pr.size + 32)
		staged = append(staged, pr)
	}

	for _, id := range l.man.members {
		gaps, count, serr := l.salvageFile(id, emit)
		rep.Gaps = append(rep.Gaps, gaps...)
		rep.Records += count
		rep.Files++
		if serr != nil {
			closeDest()
			return rep, serr
		}
		if stagedBytes >= salvageBatchBytes {
			if ferr := flush(); ferr != nil {
				closeDest()
				return rep, ferr
			}
		}
	}
	if err := flush(); err != nil {
		closeDest()
		return rep, err
	}
	if rep.Records > 0 {
		if err := dest.adoptSalvageEpoch(maxEpoch); err != nil {
			closeDest()
			return rep, err
		}
	}
	if err := dest.Close(); err != nil {
		return rep, err
	}
	return rep, nil
}

// adoptSalvageEpoch advances the sequence epoch past the highest
// salvaged epoch so later writes sort after salvaged data, and
// persists the reservation in the manifest. The caller must own the
// store exclusively with no admitted-but-incomplete groups: no
// rotation or manifest write may interleave. It fails when the
// source consumed the epoch space: a destination that cannot take
// another open is a brick, so salvage refuses to produce one.
func (s *Store) adoptSalvageEpoch(maxEpoch uint64) error {
	if maxEpoch > 0xfffd {
		return fmt.Errorf("spool: salvage: source consumed the sequence epoch space; refusing a destination that could never reopen")
	}
	newEpoch := uint16(maxEpoch + 1)
	s.seqBase = uint64(newEpoch) << epochShift
	s.seqNext.Store(0)
	s.manifestMu.Lock()
	if next := newEpoch + 1; next > s.man.nextEpoch {
		s.man.nextEpoch = next
	}
	s.man.generation++
	s.man.nextGroupID = s.groupSeq.Load() + 1
	m := *s.man
	m.members = append([]uint64(nil), s.man.members...)
	s.manifestMu.Unlock()
	return s.writeManifestFile(m)
}

// salvageFile streams one member's verified records to emit,
// recording a gap for every byte range it cannot verify. Record
// values borrow decrypted block buffers and are valid only during
// the emit call.
func (l *loader) salvageFile(id uint64, emit func(Record)) ([]SalvageGap, int64, error) {
	path := filepath.Join(l.dir, segmentsDirName, segmentFileName(id))
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return []SalvageGap{{
				File: id, Start: 0, End: -1,
				Reason: GapMissing,
				Detail: fmt.Sprintf("manifest member %d has no segment file: %v", id, err),
			}}, 0, nil
		}
		return nil, 0, fmt.Errorf("spool: open segment %d: %w", id, err)
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, 0, fmt.Errorf("spool: stat segment %d: %w", id, err)
	}
	size := st.Size()
	var gaps []SalvageGap
	var count int64
	var open *assembledGroup
	var openStart int64
	var off int64
	for off < size {
		next, rerr := peekBlockNext(f, off)
		if rerr == nil {
			var hdr *blockHeader
			var frame []byte
			hdr, frame, next, rerr = readBlockFrame(f, off, absMaxBlockBytes)
			if rerr == nil {
				fb := &framedBlock{hdr: hdr, sealed: frame[blockHeaderLen:], off: off}
				off = next
				if !hdr.completion {
					if open == nil {
						open = &assembledGroup{firstOff: fb.off}
						openStart = fb.off
					}
					open.data = append(open.data, fb)
					continue
				}
				if open == nil {
					gaps = append(gaps, SalvageGap{
						File: id, Start: fb.off, End: off,
						Reason: GapCorrupt, Detail: "orphan completion",
					})
					continue
				}
				open.completion = fb
				gstart := openStart
				g := open
				open = nil
				blocks, _, verr := l.validateGroup(id, g)
				if verr != nil {
					gaps = append(gaps, SalvageGap{
						File: id, Start: gstart, End: off,
						Reason: GapAuth, Detail: verr.Error(),
					})
					continue
				}
				for _, b := range blocks {
					for j := range b.recs {
						emit(b.recs[j])
						count++
					}
				}
				continue
			}
		}
		if rerr == io.EOF {
			break
		}
		// Resync past off: the header at off is unusable, and a
		// header that reads valid but unusable (insane sealed
		// length with a matching CRC) would otherwise resync
		// onto itself and loop forever.
		roff, ok, rserr := resyncFrame(f, off+1, size)
		if rserr != nil {
			return gaps, count, fmt.Errorf("spool: segment %d offset %d: %w (resync failed: %v)", id, off, rerr, rserr)
		}
		start := off
		if open != nil {
			// The open group's earlier frames were valid but
			// the group can never complete; its bytes are
			// lost with the unreadable span.
			start = openStart
			open = nil
		}
		if !ok {
			gaps = append(gaps, SalvageGap{
				File: id, Start: start, End: size,
				Reason: GapTornTail, Detail: rerr.Error(),
			})
			return gaps, count, nil
		}
		gaps = append(gaps, SalvageGap{
			File: id, Start: start, End: roff,
			Reason: GapCorrupt, Detail: rerr.Error(),
		})
		off = roff
	}
	if open != nil {
		reason := GapIncomplete
		detail := "trailing group missing its completion"
		if off < size {
			// Short bytes end the file: the crash caught
			// this append mid-write.
			reason = GapTornTail
			detail = "trailing group missing its completion in a torn tail"
		}
		gaps = append(gaps, SalvageGap{
			File: id, Start: openStart, End: size,
			Reason: reason, Detail: detail,
		})
	} else if off < size {
		gaps = append(gaps, SalvageGap{
			File: id, Start: off, End: size,
			Reason: GapTornTail, Detail: "trailing bytes hold no complete frame",
		})
	}
	return gaps, count, nil
}
