package spool

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
)

// Manifest v3 layout (all integers little-endian, variable length):
//
//	off  size  field
//	0    4     magic "SPLM"
//	4    2     format version (3)
//	6    2     flags (bit0 = encrypted, bit1 = clean shutdown)
//	8    4     body length N
//	12   4     header CRC32-Castagnoli over bytes [0,12)
//	16   N     body (see below)
//	16+N 4     body CRC32-Castagnoli over body bytes
//
// Body:
//
//	off    size  field
//	0      16    random store id
//	16     16    database context id (authenticated copy lives in keys.enc)
//	32     8     next file id
//	40     8     next block id
//	48     2     next epoch (sequence epoch to use on the NEXT open)
//	50     4     current data key id (0 when unencrypted)
//	54     8     active file id
//	62     8     manifest generation (bumped on membership change)
//	70     8     next group id (repaired from scan, see below)
//	78     8     required feature flags (unknown bits rejected)
//	86     8     optional feature flags (ignored)
//	94     4     member count M
//	98     8*M   member file ids, ascending (authoritative segments)
//	98+8M  8     clean total segment bytes (clean close only)
//	106+8M 4     clean segment file count (clean close only)
//	110+8M 8     clean manifest generation (clean close only)
//	118+8M 8     clean last appended group id (clean close only)
//
// The manifest is written atomically (temp file, fsync, rename,
// directory fsync). The member list is authoritative: segment files
// present on disk but absent from members are unreferenced garbage
// (crashed creations, superseded replacements) and are removed at
// open; listed members that are missing fail the open. Directory
// listings alone never decide membership.
//
// nextGroupID is persisted lazily (membership persists and close);
// open repairs it to max(persisted, scannedMax+1) so reused ids are
// impossible. The clean marker only adds a verification: a clean
// close records the exact committed shape, and the next open
// requires the scan to reproduce it. It never skips integrity
// checks.
//
// Versions 1-2 (pre-transactional and pre-context stores) are
// rejected without migration: conversion is outside the migration.

const (
	manifestMagic   = "SPLM"
	manifestVersion = 3

	manifestFlagEncrypted = 1 << 0
	manifestFlagClean     = 1 << 1

	// featureGroupsV2 marks transactional commit-group framing. It
	// is always set by this writer; readers reject manifests with
	// required bits they do not know.
	featureGroupsV2 = 1 << 0
	// featureCryptoV3 marks store/context-bound authenticated
	// encryption (block AAD, keys.enc KDF and envelope).
	featureCryptoV3 = 1 << 1

	manifestFileName = "manifest"
	segmentsDirName  = "segments"

	// manifestBodyBase is the fixed body size before members: 16
	// store + 16 context + counters + features + count + clean tail.
	manifestBodyBase = 126
)

// manifest is the parsed store-level state.
type manifest struct {
	encrypted    bool
	storeID      [16]byte // random identity, standard library only
	context      [16]byte // database context (standalone: store id)
	nextFileID   uint64
	nextBlockID  uint64
	nextEpoch    uint16
	currentKeyID uint32
	activeFileID uint64
	generation   uint64
	nextGroupID  uint64
	reqFeatures  uint64
	members      []uint64 // authoritative segments, ascending
	clean        bool
	cleanBytes   uint64
	cleanFiles   uint32
	cleanGen     uint64
	cleanGroup   uint64
}

func (m *manifest) encode() []byte {
	body := make([]byte, 0, manifestBodyBase+8*len(m.members))
	var tmp [16]byte
	body = append(body, m.storeID[:]...)
	body = append(body, m.context[:]...)
	binary.LittleEndian.PutUint64(tmp[:8], m.nextFileID)
	body = append(body, tmp[:8]...)
	binary.LittleEndian.PutUint64(tmp[:8], m.nextBlockID)
	body = append(body, tmp[:8]...)
	binary.LittleEndian.PutUint16(tmp[:2], m.nextEpoch)
	body = append(body, tmp[:2]...)
	binary.LittleEndian.PutUint32(tmp[:4], m.currentKeyID)
	body = append(body, tmp[:4]...)
	binary.LittleEndian.PutUint64(tmp[:8], m.activeFileID)
	body = append(body, tmp[:8]...)
	binary.LittleEndian.PutUint64(tmp[:8], m.generation)
	body = append(body, tmp[:8]...)
	binary.LittleEndian.PutUint64(tmp[:8], m.nextGroupID)
	body = append(body, tmp[:8]...)
	binary.LittleEndian.PutUint64(tmp[:8], m.reqFeatures|featureGroupsV2|featureCryptoV3)
	body = append(body, tmp[:8]...)
	binary.LittleEndian.PutUint64(tmp[:8], 0) // optional features
	body = append(body, tmp[:8]...)
	binary.LittleEndian.PutUint32(tmp[:4], uint32(len(m.members)))
	body = append(body, tmp[:4]...)
	for _, id := range m.members {
		binary.LittleEndian.PutUint64(tmp[:8], id)
		body = append(body, tmp[:8]...)
	}
	binary.LittleEndian.PutUint64(tmp[:8], m.cleanBytes)
	body = append(body, tmp[:8]...)
	binary.LittleEndian.PutUint32(tmp[:4], m.cleanFiles)
	body = append(body, tmp[:4]...)
	binary.LittleEndian.PutUint64(tmp[:8], m.cleanGen)
	body = append(body, tmp[:8]...)
	binary.LittleEndian.PutUint64(tmp[:8], m.cleanGroup)
	body = append(body, tmp[:8]...)

	out := make([]byte, 0, 16+len(body)+4)
	out = append(out, manifestMagic...)
	binary.LittleEndian.PutUint16(tmp[:2], manifestVersion)
	out = append(out, tmp[:2]...)
	var flags uint16
	if m.encrypted {
		flags |= manifestFlagEncrypted
	}
	if m.clean {
		flags |= manifestFlagClean
	}
	binary.LittleEndian.PutUint16(tmp[:2], flags)
	out = append(out, tmp[:2]...)
	binary.LittleEndian.PutUint32(tmp[:4], uint32(len(body)))
	out = append(out, tmp[:4]...)
	binary.LittleEndian.PutUint32(tmp[:4], crc32.Checksum(out[:12], castagnoli))
	out = append(out, tmp[:4]...)
	out = append(out, body...)
	binary.LittleEndian.PutUint32(tmp[:4], crc32.Checksum(body, castagnoli))
	out = append(out, tmp[:4]...)
	return out
}

// newStoreID draws a random 16-byte store identity.
func newStoreID() ([16]byte, error) {
	var id [16]byte
	if _, err := io.ReadFull(rand.Reader, id[:]); err != nil {
		return id, fmt.Errorf("spool: store id entropy: %w", err)
	}
	return id, nil
}

func parseManifest(raw []byte) (*manifest, error) {
	if len(raw) < 16 {
		return nil, fmt.Errorf("spool: manifest size %d: %w", len(raw), ErrCorrupt)
	}
	if string(raw[0:4]) != manifestMagic {
		return nil, fmt.Errorf("spool: bad manifest magic: %w", ErrCorrupt)
	}
	v := binary.LittleEndian.Uint16(raw[4:6])
	if v != manifestVersion {
		return nil, fmt.Errorf("spool: manifest version %d rejected without migration: %w", v, ErrUnsupportedVersion)
	}
	if got, want := crc32.Checksum(raw[:12], castagnoli), binary.LittleEndian.Uint32(raw[12:16]); got != want {
		return nil, fmt.Errorf("spool: manifest header checksum mismatch: %w", ErrCorrupt)
	}
	n := binary.LittleEndian.Uint32(raw[8:12])
	if uint64(len(raw)) != uint64(16+n+4) {
		return nil, fmt.Errorf("spool: manifest size %d != body %d: %w", len(raw), n, ErrCorrupt)
	}
	body := raw[16 : 16+n]
	if got, want := crc32.Checksum(body, castagnoli), binary.LittleEndian.Uint32(raw[16+n:]); got != want {
		return nil, fmt.Errorf("spool: manifest body checksum mismatch: %w", ErrCorrupt)
	}
	if len(body) < manifestBodyBase {
		return nil, fmt.Errorf("spool: manifest body %d: %w", len(body), ErrCorrupt)
	}
	m := &manifest{}
	copy(m.storeID[:], body[0:16])
	copy(m.context[:], body[16:32])
	m.nextFileID = binary.LittleEndian.Uint64(body[32:40])
	m.nextBlockID = binary.LittleEndian.Uint64(body[40:48])
	m.nextEpoch = binary.LittleEndian.Uint16(body[48:50])
	m.currentKeyID = binary.LittleEndian.Uint32(body[50:54])
	m.activeFileID = binary.LittleEndian.Uint64(body[54:62])
	m.generation = binary.LittleEndian.Uint64(body[62:70])
	m.nextGroupID = binary.LittleEndian.Uint64(body[70:78])
	m.reqFeatures = binary.LittleEndian.Uint64(body[78:86])
	// body[86:94] optional features: ignored.
	mc := binary.LittleEndian.Uint32(body[94:98])
	if uint64(len(body)) != uint64(manifestBodyBase+8*mc) {
		return nil, fmt.Errorf("spool: manifest members %d != body %d: %w", mc, len(body), ErrCorrupt)
	}
	m.members = make([]uint64, mc)
	for i := range m.members {
		m.members[i] = binary.LittleEndian.Uint64(body[98+8*i : 106+8*i])
	}
	tail := body[98+8*mc:]
	m.cleanBytes = binary.LittleEndian.Uint64(tail[0:8])
	m.cleanFiles = binary.LittleEndian.Uint32(tail[8:12])
	m.cleanGen = binary.LittleEndian.Uint64(tail[12:20])
	m.cleanGroup = binary.LittleEndian.Uint64(tail[20:28])

	flags := binary.LittleEndian.Uint16(raw[6:8])
	m.encrypted = flags&manifestFlagEncrypted != 0
	m.clean = flags&manifestFlagClean != 0
	if m.reqFeatures&^(featureGroupsV2|featureCryptoV3) != 0 {
		return nil, fmt.Errorf("spool: manifest requires unknown features %#x: %w", m.reqFeatures, ErrUnsupportedVersion)
	}
	if !sort.SliceIsSorted(m.members, func(i, j int) bool { return m.members[i] < m.members[j] }) {
		return nil, fmt.Errorf("spool: manifest members unsorted: %w", ErrCorrupt)
	}
	for i := 1; i < len(m.members); i++ {
		if m.members[i] == m.members[i-1] || m.members[i] == 0 {
			return nil, fmt.Errorf("spool: manifest members invalid: %w", ErrCorrupt)
		}
	}
	if m.nextFileID == 0 || m.nextBlockID == 0 || m.activeFileID == 0 || m.nextGroupID == 0 {
		return nil, fmt.Errorf("spool: manifest has zero counters: %w", ErrCorrupt)
	}
	if m.encrypted && m.currentKeyID == 0 {
		return nil, fmt.Errorf("spool: encrypted manifest lacks a key id: %w", ErrCorrupt)
	}
	found := false
	for _, id := range m.members {
		if id == m.activeFileID {
			found = true
		}
	}
	if !found {
		return nil, fmt.Errorf("spool: manifest active file %d not a member: %w", m.activeFileID, ErrCorrupt)
	}
	return m, nil
}

// cleanMarker is the recorded clean-shutdown shape a reopen must
// reproduce.
type cleanMarker struct {
	bytes uint64
	files uint32
	gen   uint64
	group uint64
}

// checkCleanMarker requires a cleanly closed store to still have
// its recorded shape: same manifest generation, file count, last
// group, and no fewer bytes. Less means committed data vanished
// outside the store (tampering, filesystem loss) and fails loudly.
// Extra bytes are tolerated: the torn-tail path truncates them as
// usual.
func checkCleanMarker(segDir string, ids []uint64, curGen uint64, want cleanMarker, maxGroup uint64) error {
	if curGen != want.gen {
		return fmt.Errorf("spool: clean-shutdown marker generation %d != manifest %d: %w",
			want.gen, curGen, ErrCorrupt)
	}
	if uint64(len(ids)) != uint64(want.files) {
		return fmt.Errorf("spool: clean-shutdown marker expects %d segment files, found %d: %w",
			want.files, len(ids), ErrCorrupt)
	}
	if maxGroup != want.group {
		return fmt.Errorf("spool: clean-shutdown marker group %d != scanned %d: %w",
			want.group, maxGroup, ErrCorrupt)
	}
	var total uint64
	for _, id := range ids {
		st, err := os.Stat(filepath.Join(segDir, segmentFileName(id)))
		if err != nil {
			return fmt.Errorf("spool: stat segment %d: %w", id, err)
		}
		total += uint64(st.Size())
	}
	if total < want.bytes {
		return fmt.Errorf("spool: clean-shutdown marker expects %d segment bytes, found %d: %w",
			want.bytes, total, ErrCorrupt)
	}
	return nil
}

// atomicWriteFile durably replaces dir/name: temp file in the same
// directory, content fsync, rename, directory fsync.
func atomicWriteFile(dir, name string, data []byte) error {
	tmp, err := os.CreateTemp(dir, "."+name+".tmp-*")
	if err != nil {
		return fmt.Errorf("spool: temp file: %w", err)
	}
	tmpName := tmp.Name()
	// Best-effort cleanup; success path renames away.
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("spool: write temp file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("spool: sync temp file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("spool: close temp file: %w", err)
	}
	if err := os.Rename(tmpName, filepath.Join(dir, name)); err != nil {
		return fmt.Errorf("spool: rename %s: %w", name, err)
	}
	if err := dirSync(dir); err != nil {
		return fmt.Errorf("spool: sync dir: %w", err)
	}
	return nil
}

// dirSync fsyncs a directory so file creations, renames and removals
// survive a crash. Windows cannot fsync directories; there it is a
// no-op.
func dirSync(dir string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
