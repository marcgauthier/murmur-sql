package recovery

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"strings"
)

// FileDescriptor describes one data file in a generation.
type FileDescriptor struct {
	Name string
	Type uint8 // FileRun or FileIndex
	Size uint64
	Hash [HashLen]byte
	Rows uint64
	// MinKey/MaxKey bound the run's sort-key range. Empty for the index.
	MinKey []byte
	MaxKey []byte
}

// Manifest is the authenticated descriptor of one cache generation.
type Manifest struct {
	DatabaseID      [16]byte
	GenerationID    uint64
	CaptureCursor   uint64
	CaptureSequence uint64
	SchemaDigest    [32]byte
	CreatedUnixNano uint64
	CodecID         uint8
	Encrypted       bool
	KeyRef          string
	RowCount        uint64
	TableCount      uint32
	Files           []FileDescriptor
}

// encodeManifest renders the deterministic manifest plaintext.
func encodeManifest(m *Manifest) []byte {
	var out []byte
	out = append(out, MagicManifest...)
	var tmp [8]byte
	binary.LittleEndian.PutUint16(tmp[:2], FormatVersion)
	out = append(out, tmp[:2]...)
	out = append(out, m.DatabaseID[:]...)
	binary.LittleEndian.PutUint64(tmp[:], m.GenerationID)
	out = append(out, tmp[:]...)
	binary.LittleEndian.PutUint64(tmp[:], m.CaptureCursor)
	out = append(out, tmp[:]...)
	binary.LittleEndian.PutUint64(tmp[:], m.CaptureSequence)
	out = append(out, tmp[:]...)
	out = append(out, m.SchemaDigest[:]...)
	binary.LittleEndian.PutUint64(tmp[:], m.CreatedUnixNano)
	out = append(out, tmp[:]...)
	out = append(out, m.CodecID)
	if m.Encrypted {
		out = append(out, 1)
	} else {
		out = append(out, 0)
	}
	out = appendBytes(out, []byte(m.KeyRef))
	out = appendUvarint(out, m.RowCount)
	var t4 [4]byte
	binary.LittleEndian.PutUint32(t4[:], m.TableCount)
	out = append(out, t4[:]...)
	out = appendUvarint(out, uint64(len(m.Files)))
	for i := range m.Files {
		f := &m.Files[i]
		out = appendBytes(out, []byte(f.Name))
		out = append(out, f.Type)
		binary.LittleEndian.PutUint64(tmp[:], f.Size)
		out = append(out, tmp[:]...)
		out = append(out, f.Hash[:]...)
		out = appendUvarint(out, f.Rows)
		out = appendBytes(out, f.MinKey)
		out = appendBytes(out, f.MaxKey)
	}
	return out
}

// decodeManifest parses and bounds-checks manifest plaintext.
func decodeManifest(raw []byte) (*Manifest, error) {
	r := &byteReader{buf: raw}
	if magic := string(r.fixed(len(MagicManifest))); r.err == nil && magic != MagicManifest {
		return nil, corruptErr("bad manifest magic %q", magic)
	}
	ver := r.fixed(2)
	if r.err != nil {
		return nil, r.err
	}
	if v := binary.LittleEndian.Uint16(ver); v != FormatVersion {
		return nil, corruptErr("unsupported manifest format %d", v)
	}
	m := &Manifest{}
	copy(m.DatabaseID[:], r.fixed(16))
	if r.err != nil {
		return nil, r.err
	}
	gen := r.fixed(8)
	cur := r.fixed(8)
	seq := r.fixed(8)
	if r.err != nil {
		return nil, r.err
	}
	m.GenerationID = binary.LittleEndian.Uint64(gen)
	m.CaptureCursor = binary.LittleEndian.Uint64(cur)
	m.CaptureSequence = binary.LittleEndian.Uint64(seq)
	copy(m.SchemaDigest[:], r.fixed(32))
	if r.err != nil {
		return nil, r.err
	}
	ts := r.fixed(8)
	if r.err != nil {
		return nil, r.err
	}
	m.CreatedUnixNano = binary.LittleEndian.Uint64(ts)
	head := r.fixed(2)
	if r.err != nil {
		return nil, r.err
	}
	m.CodecID = head[0]
	switch head[1] {
	case 0:
		m.Encrypted = false
	case 1:
		m.Encrypted = true
	default:
		return nil, corruptErr("bad manifest encryption flag %d", head[1])
	}
	m.KeyRef = string(r.bytes(MaxKeyRefLen, "key ref"))
	if r.err != nil {
		return nil, r.err
	}
	m.RowCount = r.uvarint()
	tc := r.fixed(4)
	if r.err != nil {
		return nil, r.err
	}
	m.TableCount = binary.LittleEndian.Uint32(tc)
	nFiles := r.uvarint()
	if r.err != nil {
		return nil, r.err
	}
	if nFiles > MaxFilesPerManifest {
		return nil, corruptErr("file count %d exceeds bound", nFiles)
	}
	m.Files = make([]FileDescriptor, 0, min(nFiles, 1024))
	for i := uint64(0); i < nFiles; i++ {
		var f FileDescriptor
		f.Name = string(r.bytes(MaxFileNameLen, "file name"))
		if r.err != nil {
			return nil, r.err
		}
		if err := checkFileName(f.Name); err != nil {
			return nil, err
		}
		typ := r.fixed(1)
		if r.err != nil {
			return nil, r.err
		}
		f.Type = typ[0]
		if f.Type != FileRun && f.Type != FileIndex {
			return nil, corruptErr("bad file type %d", f.Type)
		}
		sz := r.fixed(8)
		if r.err != nil {
			return nil, r.err
		}
		f.Size = binary.LittleEndian.Uint64(sz)
		if f.Size == 0 || f.Size > uint64(MaxFileBytes) {
			return nil, corruptErr("bad file size %d", f.Size)
		}
		copy(f.Hash[:], r.fixed(HashLen))
		if r.err != nil {
			return nil, r.err
		}
		f.Rows = r.uvarint()
		if r.err != nil {
			return nil, r.err
		}
		f.MinKey = r.bytes(MaxKeyLen, "min key")
		if r.err != nil {
			return nil, r.err
		}
		f.MaxKey = r.bytes(MaxKeyLen, "max key")
		if r.err != nil {
			return nil, r.err
		}
		m.Files = append(m.Files, f)
	}
	if len(r.buf) != 0 {
		return nil, corruptErr("manifest has %d trailing bytes", len(r.buf))
	}
	return m, nil
}

// checkFileName rejects traversal, separators, and hidden names so
// manifest entries can never escape the generation directory.
func checkFileName(name string) error {
	if name == "" || len(name) > MaxFileNameLen {
		return corruptErr("bad file name length")
	}
	if name != filepath.Base(name) || strings.HasPrefix(name, ".") {
		return corruptErr("bad file name %q", name)
	}
	for _, c := range name {
		if c == '/' || c == '\\' || c < 0x20 || c == 0x7f {
			return corruptErr("bad file name %q", name)
		}
	}
	return nil
}

// manifestHeader renders the plaintext cipher-selection header:
// magic, format, encryption flag, and key reference. Everything else
// stays sealed; CURRENT's manifest hash binds these bytes too.
func manifestHeader(encrypted bool, keyRef string) []byte {
	var out []byte
	out = append(out, MagicManifest...)
	var tmp [2]byte
	binary.LittleEndian.PutUint16(tmp[:], FormatVersion)
	out = append(out, tmp[:]...)
	if encrypted {
		out = append(out, 1)
	} else {
		out = append(out, 0)
	}
	return appendBytes(out, []byte(keyRef))
}

// peekManifestHeader reads the cipher-selection header, returning the
// encryption flag, key reference, and header length.
func peekManifestHeader(file []byte) (encrypted bool, keyRef string, err error) {
	r := &byteReader{buf: file}
	if magic := string(r.fixed(len(MagicManifest))); r.err == nil && magic != MagicManifest {
		return false, "", corruptErr("bad manifest file magic %q", magic)
	}
	ver := r.fixed(2)
	if r.err != nil {
		return false, "", r.err
	}
	if v := binary.LittleEndian.Uint16(ver); v != FormatVersion {
		return false, "", corruptErr("unsupported manifest file format %d", v)
	}
	flag := r.fixed(1)
	if r.err != nil {
		return false, "", r.err
	}
	switch flag[0] {
	case 0:
		encrypted = false
	case 1:
		encrypted = true
	default:
		return false, "", corruptErr("bad manifest encryption flag %d", flag[0])
	}
	keyRef = string(r.bytes(MaxKeyRefLen, "key ref"))
	if r.err != nil {
		return false, "", r.err
	}
	return encrypted, keyRef, nil
}

// headerLen returns the plaintext header length embedded in file.
func headerLen(file []byte) (int, error) {
	r := &byteReader{buf: file}
	r.fixed(len(MagicManifest) + 2 + 1)
	if r.err != nil {
		return 0, r.err
	}
	r.bytes(MaxKeyRefLen, "key ref")
	if r.err != nil {
		return 0, r.err
	}
	return len(file) - len(r.buf), nil
}

// sealManifest encodes and protects m. The returned bytes are the
// complete MANIFEST file: plaintext header + nonce + sealed plaintext.
func sealManifest(c *Cipher, m *Manifest) ([]byte, error) {
	head := manifestHeader(c.Encrypted(), m.KeyRef)
	plain := encodeManifest(m)
	nonce, sealed, err := c.Seal(plain, ManifestAAD(m.DatabaseID))
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, len(head)+len(nonce)+len(sealed))
	out = append(out, head...)
	out = append(out, nonce...)
	return append(out, sealed...), nil
}

// openManifest verifies file bytes against the expected database and
// decodes the manifest. The header claims must match the sealed body.
func openManifest(c *Cipher, dbID [16]byte, file []byte) (*Manifest, error) {
	claimedEncrypted, claimedRef, err := peekManifestHeader(file)
	if err != nil {
		return nil, err
	}
	off, err := headerLen(file)
	if err != nil {
		return nil, err
	}
	rest := file[off:]
	if len(rest) < NonceLen+1 {
		return nil, corruptErr("truncated manifest file")
	}
	if len(file) > len(manifestHeader(true, claimedRef))+MaxManifestPlain+c.Overhead()+NonceLen {
		return nil, corruptErr("manifest file too large")
	}
	plain, err := c.Open(rest[:NonceLen], rest[NonceLen:], ManifestAAD(dbID))
	if err != nil {
		return nil, err
	}
	m, err := decodeManifest(plain)
	if err != nil {
		return nil, err
	}
	if m.DatabaseID != dbID {
		return nil, corruptErr("manifest database mismatch")
	}
	if m.Encrypted != claimedEncrypted || m.KeyRef != claimedRef {
		return nil, corruptErr("manifest header mismatch")
	}
	return m, nil
}

// GenerationDirName renders generation-N directory names.
func GenerationDirName(id uint64) string {
	return fmt.Sprintf("%s%08d", GenerationPrefix, id)
}

// ParseGenerationDir extracts the id from a generation directory base
// name, or reports false.
func ParseGenerationDir(name string) (uint64, bool) {
	if !strings.HasPrefix(name, GenerationPrefix) {
		return 0, false
	}
	rest := strings.TrimPrefix(name, GenerationPrefix)
	if len(rest) != 8 {
		return 0, false
	}
	var id uint64
	for i := 0; i < 8; i++ {
		if rest[i] < '0' || rest[i] > '9' {
			return 0, false
		}
		id = id*10 + uint64(rest[i]-'0')
	}
	return id, true
}

// RunFileName renders per-table run names. The table id is hex-encoded
// so arbitrary ids stay valid file names.
func RunFileName(tableID string, seq int) string {
	return fmt.Sprintf("table-%x-%05d.rrun", tableID, seq)
}

// FormatCurrent renders the CURRENT pointer: generation name plus the
// manifest file hash binding it to exact manifest bytes.
func FormatCurrent(dirName string, manifestHash [HashLen]byte) []byte {
	return []byte(dirName + "\n" + hex.EncodeToString(manifestHash[:]) + "\n")
}

// ParseCurrent extracts the generation name and manifest hash. Any
// deviation is corruption (the caller falls back to scanning).
func ParseCurrent(raw []byte) (string, [HashLen]byte, error) {
	var zero [HashLen]byte
	lines := strings.Split(string(raw), "\n")
	if len(lines) != 3 || lines[2] != "" {
		return "", zero, corruptErr("malformed CURRENT")
	}
	if _, ok := ParseGenerationDir(lines[0]); !ok {
		return "", zero, corruptErr("bad CURRENT generation %q", lines[0])
	}
	hash, err := hex.DecodeString(lines[1])
	if err != nil || len(hash) != HashLen {
		return "", zero, corruptErr("bad CURRENT hash")
	}
	var out [HashLen]byte
	copy(out[:], hash)
	return lines[0], out, nil
}

// HashBytes returns the SHA-256 of b.
func HashBytes(b []byte) [HashLen]byte { return sha256.Sum256(b) }
