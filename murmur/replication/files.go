package replication

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"io"
	"io/fs"
	"net/netip"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"hash"

	"github.com/marcgauthier/murmur-sql-go/murmur/internal/postcard"
	"golang.org/x/crypto/hkdf"
)

// FileChunk is the file transfer chunk size.
const FileChunk = 64 << 10

// FilesConfig tunes node-local file objects.
type FilesConfig struct {
	Directory         string
	Key               [32]byte
	PreviousKey       *[32]byte
	Generation        uint64
	FetchBind         netip.AddrPort
	MaxFileBytes      uint64
	MaxStagingBytes   uint64
	ConcurrentFetches int
	FetchInterval     time.Duration
}

// NewFilesConfig returns the default file configuration.
func NewFilesConfig(directory string, key [32]byte, fetchBind netip.AddrPort) *FilesConfig {
	return &FilesConfig{Directory: directory, Key: key, FetchBind: fetchBind,
		MaxFileBytes: 4 << 30, MaxStagingBytes: 1 << 30,
		ConcurrentFetches: 2, FetchInterval: 30 * time.Second}
}

// Validate rejects inconsistent file limits.
func (c *FilesConfig) Validate() error {
	if c.MaxFileBytes == 0 || c.MaxStagingBytes < FileChunk ||
		c.ConcurrentFetches == 0 || c.FetchInterval <= 0 {
		return invalidErr("file limits")
	}
	return nil
}

// FileMetadata describes one stored file.
type FileMetadata struct {
	Name       string
	Digest     [32]byte
	Bytes      uint64
	Generation uint64
	Millis     uint64
	Node       NodeId
	Deleted    bool
}

// FileAvailabilityKind classifies file availability.
type FileAvailabilityKind int

const (
	// FileLocal serves the file from this node.
	FileLocal FileAvailabilityKind = iota
	// FileRemote serves the file from listed peers.
	FileRemote
	// FileUnavailable cannot serve the file.
	FileUnavailable
)

// FileAvailability reports where a file can be fetched.
type FileAvailability struct {
	Kind   FileAvailabilityKind
	Remote []NodeId
}

// FileKeyRotationStatus reports key rotation progress.
type FileKeyRotationStatus struct {
	CurrentGeneration                   uint64
	OldGenerationObjects                uint64
	OldGenerationMetadataMissingLocally uint64
	StagedTransfers                     uint64
	PreviousKeyUnusedLocally            bool
}

func encodeFileMetadata(e *postcard.Encoder, m *FileMetadata) {
	e.String(m.Name)
	e.Raw(m.Digest[:])
	e.U64(m.Bytes)
	e.U64(m.Generation)
	e.U64(m.Millis)
	e.Raw(m.Node[:])
	e.Bool(m.Deleted)
}

func decodeFileMetadata(d *postcard.Decoder) FileMetadata {
	var m FileMetadata
	m.Name = d.String()
	copy(m.Digest[:], d.Raw(32))
	m.Bytes = d.U64()
	m.Generation = d.U64()
	m.Millis = d.U64()
	copy(m.Node[:], d.Raw(16))
	m.Deleted = d.Bool()
	return m
}

// EncodeFileMetadata serializes file metadata.
func EncodeFileMetadata(m *FileMetadata) []byte {
	return encode(func(e *postcard.Encoder) { encodeFileMetadata(e, m) })
}

// DecodeFileMetadata parses file metadata.
func DecodeFileMetadata(b []byte) (FileMetadata, error) { return decodeOne(b, decodeFileMetadata) }

func encodeFileAvailability(e *postcard.Encoder, a *FileAvailability) {
	e.Discriminant(uint32(a.Kind))
	if a.Kind == FileRemote {
		e.Len(len(a.Remote))
		for _, n := range a.Remote {
			e.Raw(n[:])
		}
	}
}

func decodeFileAvailability(d *postcard.Decoder) FileAvailability {
	var a FileAvailability
	switch d.Discriminant() {
	case 0:
		a.Kind = FileLocal
	case 1:
		a.Kind = FileRemote
		for n := d.Len(); n > 0; n-- {
			var id NodeId
			copy(id[:], d.Raw(16))
			a.Remote = append(a.Remote, id)
		}
	case 2:
		a.Kind = FileUnavailable
	default:
		d.Fail(protocolErr("bad file availability"))
	}
	return a
}

// EncodeFileAvailability serializes file availability.
func EncodeFileAvailability(a *FileAvailability) []byte {
	return encode(func(e *postcard.Encoder) { encodeFileAvailability(e, a) })
}

// DecodeFileAvailability parses file availability.
func DecodeFileAvailability(b []byte) (FileAvailability, error) {
	return decodeOne(b, decodeFileAvailability)
}

func encodeFileKeyRotationStatus(e *postcard.Encoder, s *FileKeyRotationStatus) {
	e.U64(s.CurrentGeneration)
	e.U64(s.OldGenerationObjects)
	e.U64(s.OldGenerationMetadataMissingLocally)
	e.U64(s.StagedTransfers)
	e.Bool(s.PreviousKeyUnusedLocally)
}

func decodeFileKeyRotationStatus(d *postcard.Decoder) FileKeyRotationStatus {
	var s FileKeyRotationStatus
	s.CurrentGeneration = d.U64()
	s.OldGenerationObjects = d.U64()
	s.OldGenerationMetadataMissingLocally = d.U64()
	s.StagedTransfers = d.U64()
	s.PreviousKeyUnusedLocally = d.Bool()
	return s
}

// EncodeFileKeyRotationStatus serializes rotation status.
func EncodeFileKeyRotationStatus(s *FileKeyRotationStatus) []byte {
	return encode(func(e *postcard.Encoder) { encodeFileKeyRotationStatus(e, s) })
}

// DecodeFileKeyRotationStatus parses rotation status.
func DecodeFileKeyRotationStatus(b []byte) (FileKeyRotationStatus, error) {
	return decodeOne(b, decodeFileKeyRotationStatus)
}

// FileObjects stores immutable authenticated file objects. Metadata
// belongs to the provided backend.
type FileObjects struct {
	config       FilesConfig
	backend      Backend
	errMu        sync.Mutex
	lastErr      *string
	gate         sync.Mutex
	stagedPaths  sync.Mutex
	staged       map[string]bool
	stagingUsage *stagingUsage
}

type stagingUsage struct {
	mu   sync.Mutex
	used uint64
}

// OpenFileObjects opens an object store, refusing to start when
// old-generation metadata lacks its key.
func OpenFileObjects(config FilesConfig, backend Backend) (*FileObjects, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	keys, err := backend.Keys([]byte("08/files/meta/"))
	if err != nil {
		return nil, err
	}
	for _, key := range keys {
		raw, err := backend.Get(key)
		if err != nil {
			return nil, err
		}
		if raw == nil {
			continue
		}
		meta, err := DecodeFileMetadata(raw)
		if err != nil {
			return nil, err
		}
		if !meta.Deleted && meta.Generation < config.Generation {
			if config.PreviousKey == nil {
				return nil, invalidErr("previous file key is required while old-generation metadata exists")
			}
			if meta.Generation+1 != config.Generation {
				return nil, invalidErr("file keys must rotate one generation at a time")
			}
		}
	}
	if err := os.MkdirAll(config.Directory, 0o700); err != nil {
		return nil, ioErr(err)
	}
	objects := &FileObjects{config: config, backend: backend,
		staged: make(map[string]bool), stagingUsage: &stagingUsage{}}
	if err := objects.reconcileStaging(); err != nil {
		return nil, err
	}
	return objects, nil
}

// reconcileStaging drops staging entries that can never resume: sidecars
// without a partial (publish completed, or the attempt died between
// rename and sidecar removal), partials without a sidecar (unresumable
// by the fileResumeOffset rule), and partials whose object is already
// published. Runs at open, when no fetcher is active, so nothing here
// is protected or in flight; valid resumable pairs are kept.
func (o *FileObjects) reconcileStaging() error {
	dir := filepath.Join(o.config.Directory, "staging")
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return ioErr(err)
	}
	present := make(map[string]bool, len(entries))
	for _, e := range entries {
		if info, err := e.Info(); err == nil && !info.IsDir() {
			present[e.Name()] = true
		}
	}
	for name := range present {
		full := filepath.Join(dir, name)
		switch {
		case strings.HasSuffix(name, ".identity"):
			if !present[strings.TrimSuffix(name, ".identity")+".part"] {
				if err := os.Remove(full); err != nil {
					return ioErr(err)
				}
			}
		case strings.HasSuffix(name, ".part"):
			sidecar := strings.TrimSuffix(name, ".part") + ".identity"
			if !present[sidecar] {
				if err := os.Remove(full); err != nil {
					return ioErr(err)
				}
				continue
			}
			if digest, generation, ok := parseStagingName(name); ok {
				if st, err := os.Stat(o.path(digest, generation)); err == nil && !st.IsDir() {
					if err := os.Remove(full); err != nil {
						return ioErr(err)
					}
					if err := os.Remove(filepath.Join(dir, sidecar)); err != nil {
						return ioErr(err)
					}
				}
			}
		}
	}
	return nil
}

// parseStagingName parses "<sourcehex>-<digesthex>.g<generation>.part".
func parseStagingName(name string) ([32]byte, uint64, bool) {
	var zero [32]byte
	stem, ok := strings.CutSuffix(name, ".part")
	if !ok {
		return zero, 0, false
	}
	_, rest, found := strings.Cut(stem, "-")
	if !found {
		return zero, 0, false
	}
	i := strings.LastIndex(rest, ".g")
	if i < 0 {
		return zero, 0, false
	}
	var generation uint64
	for _, c := range rest[i+2:] {
		if c < '0' || c > '9' {
			return zero, 0, false
		}
		generation = generation*10 + uint64(c-'0')
	}
	if len(rest[:i]) != 64 {
		return zero, 0, false
	}
	var digest [32]byte
	for j := 0; j < 32; j++ {
		hi, ok1 := hexVal(rest[2*j])
		lo, ok2 := hexVal(rest[2*j+1])
		if !ok1 || !ok2 {
			return zero, 0, false
		}
		digest[j] = hi<<4 | lo
	}
	return digest, generation, true
}

// StagedTransfer pins staging paths until released.
type StagedTransfer struct {
	objects *FileObjects
	paths   []string
	once    sync.Once
}

// ProtectStaging pins a path and its identity sidecar.
func (o *FileObjects) ProtectStaging(path string) *StagedTransfer {
	o.gate.Lock()
	defer o.gate.Unlock()
	paths := []string{path, stagingIdentityPath(path)}
	o.stagedPaths.Lock()
	for _, p := range paths {
		o.staged[p] = true
	}
	o.stagedPaths.Unlock()
	return &StagedTransfer{objects: o, paths: paths}
}

// Release unpins the staging paths. It is idempotent.
func (s *StagedTransfer) Release() {
	s.once.Do(func() {
		s.objects.stagedPaths.Lock()
		for _, p := range s.paths {
			delete(s.objects.staged, p)
		}
		s.objects.stagedPaths.Unlock()
	})
}

// LastFetchError reports the last background fetch error.
func (o *FileObjects) LastFetchError() *string {
	o.errMu.Lock()
	defer o.errMu.Unlock()
	if o.lastErr == nil {
		return nil
	}
	cp := *o.lastErr
	return &cp
}

func (o *FileObjects) fetchError(err error) {
	s := err.Error()
	o.errMu.Lock()
	o.lastErr = &s
	o.errMu.Unlock()
}

// UploadOne stores one object from a reader.
func (o *FileObjects) UploadOne(name string, input io.Reader) (*FileMetadata, error) {
	o.gate.Lock()
	defer o.gate.Unlock()
	return o.uploadInner(name, input)
}

func (o *FileObjects) uploadInner(name string, input io.Reader) (*FileMetadata, error) {
	upload, err := newFileUpload(o, name)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, FileChunk)
	for {
		n, err := input.Read(buf)
		if n > 0 {
			if aerr := upload.Append(buf[:n]); aerr != nil {
				upload.Abort()
				return nil, aerr
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			upload.Abort()
			return nil, ioErr(err)
		}
	}
	meta, err := upload.finishInner(o)
	if err != nil {
		return nil, err
	}
	return meta, nil
}

// BeginUpload starts a bounded encrypted upload. No metadata is
// published until FinishUpload. Abandoned uploads are swept by Collect;
// call Abort for prompt cleanup.
func (o *FileObjects) BeginUpload(name string) (*FileUpload, error) {
	o.gate.Lock()
	defer o.gate.Unlock()
	return newFileUpload(o, name)
}

// FinishUpload publishes a completed upload atomically with respect to
// local mutations.
func (o *FileObjects) FinishUpload(upload *FileUpload) (*FileMetadata, error) {
	if upload.used != o.stagingUsage {
		return nil, invalidErr("upload belongs to another object store")
	}
	o.gate.Lock()
	defer o.gate.Unlock()
	meta, err := upload.finishInner(o)
	if err != nil {
		return nil, err
	}
	return meta, nil
}

// StagingReservation holds shared disk staging capacity.
type StagingReservation struct {
	used  *stagingUsage
	bytes uint64
	once  sync.Once
}

// Release returns the reserved capacity. It is idempotent.
func (r *StagingReservation) Release() {
	r.once.Do(func() {
		r.used.mu.Lock()
		if r.used.used < r.bytes {
			r.used.used = 0
		} else {
			r.used.used -= r.bytes
		}
		r.used.mu.Unlock()
	})
}

// ReserveStaging reserves shared disk staging capacity for an upload or
// peer fetch.
func (o *FileObjects) ReserveStaging(n uint64) (*StagingReservation, error) {
	o.stagingUsage.mu.Lock()
	defer o.stagingUsage.mu.Unlock()
	if n > satSub(o.config.MaxStagingBytes, o.stagingUsage.used) {
		return nil, ErrOverloaded
	}
	o.stagingUsage.used += n
	return &StagingReservation{used: o.stagingUsage, bytes: n}, nil
}

// ListBounded collects a bounded metadata snapshot without
// materializing all backend keys.
func (o *FileObjects) ListBounded(maxBytes int) ([]FileMetadata, error) {
	var out []FileMetadata
	used := 0
	var after []byte
	for {
		keys, err := o.backend.KeysPage([]byte("08/files/meta/"), after, 256)
		if err != nil {
			return nil, err
		}
		if len(keys) == 0 {
			break
		}
		for _, key := range keys {
			raw, err := o.backend.Get(key)
			if err != nil {
				return nil, err
			}
			if raw == nil {
				continue
			}
			meta, err := DecodeFileMetadata(raw)
			if err != nil {
				return nil, err
			}
			if !meta.Deleted {
				used += len(meta.Name) + 256
				if used < len(meta.Name)+256 {
					return nil, ErrOverloaded
				}
				if used > maxBytes {
					return nil, ErrOverloaded
				}
				out = append(out, meta)
			}
		}
		after = keys[len(keys)-1]
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// Download pins and authenticates one immutable object before streaming
// it to a client. Close the download when done.
func (o *FileObjects) Download(name string) (*FileDownload, error) {
	o.gate.Lock()
	defer o.gate.Unlock()
	meta, err := o.Metadata(name)
	if err != nil {
		return nil, err
	}
	if meta == nil || meta.Deleted {
		return nil, unavailableErr("file metadata missing")
	}
	file, err := os.Open(o.path(meta.Digest, meta.Generation))
	if err != nil {
		return nil, unavailableErr("file payload unavailable; retry after background fetching")
	}
	if err := o.verifyInput(file, meta, io.Discard); err != nil {
		file.Close()
		return nil, err
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		file.Close()
		return nil, ioErr(err)
	}
	var header [40]byte
	if _, err := io.ReadFull(file, header[:]); err != nil {
		file.Close()
		return nil, ioErr(err)
	}
	var key *[32]byte
	if meta.Generation == o.config.Generation {
		key = &o.config.Key
	} else if o.config.PreviousKey != nil {
		key = o.config.PreviousKey
	} else {
		file.Close()
		return nil, authErr("object key unavailable")
	}
	var salt [16]byte
	copy(salt[:], header[16:32])
	derived := objectKey(key, &salt)
	block, err := aes.NewCipher(derived[:])
	if err != nil {
		file.Close()
		return nil, ioErr(err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		file.Close()
		return nil, ioErr(err)
	}
	var id NodeId
	if _, err := rand.Read(id[:]); err != nil {
		file.Close()
		return nil, ioErr(err)
	}
	// A unique hard link pins the container across replacement/GC on all platforms.
	pin := filepath.Join(o.config.Directory, ".download-"+Hex(id[:]))
	if err := os.Link(o.path(meta.Digest, meta.Generation), pin); err != nil {
		file.Close()
		return nil, ioErr(err)
	}
	o.stagedPaths.Lock()
	o.staged[pin] = true
	o.stagedPaths.Unlock()
	return &FileDownload{meta: *meta, file: file, cipher: gcm, header: header, pin: pin, objects: o}, nil
}

// Metadata reads one file's metadata.
func (o *FileObjects) Metadata(name string) (*FileMetadata, error) {
	if err := checkFileName(name); err != nil {
		return nil, err
	}
	raw, err := o.backend.Get(metaKey(name))
	if err != nil {
		return nil, err
	}
	if raw == nil {
		return nil, nil
	}
	meta, err := DecodeFileMetadata(raw)
	if err != nil {
		return nil, err
	}
	return &meta, nil
}

// List returns all live file metadata by name.
func (o *FileObjects) List() ([]FileMetadata, error) {
	keys, err := o.backend.Keys([]byte("08/files/meta/"))
	if err != nil {
		return nil, err
	}
	var out []FileMetadata
	for _, key := range keys {
		raw, err := o.backend.Get(key)
		if err != nil {
			return nil, err
		}
		if raw == nil {
			continue
		}
		meta, err := DecodeFileMetadata(raw)
		if err != nil {
			return nil, err
		}
		if !meta.Deleted {
			out = append(out, meta)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// Available reports whether the payload is stored locally.
func (o *FileObjects) Available(name string) (bool, error) {
	meta, err := o.Metadata(name)
	if err != nil {
		return false, err
	}
	if meta == nil || meta.Deleted {
		return false, nil
	}
	st, err := os.Stat(o.path(meta.Digest, meta.Generation))
	return err == nil && !st.IsDir(), nil
}

// Availability reports where a file can be fetched.
func (o *FileObjects) Availability(name string) (*FileAvailability, error) {
	meta, err := o.Metadata(name)
	if err != nil {
		return nil, err
	}
	if meta == nil || meta.Deleted {
		return &FileAvailability{Kind: FileUnavailable}, nil
	}
	if st, err := os.Stat(o.path(meta.Digest, meta.Generation)); err == nil && !st.IsDir() {
		return &FileAvailability{Kind: FileLocal}, nil
	}
	prefix := availabilityPrefix(meta.Digest, meta.Generation)
	cutoff := satSub(filesNow(), 7*86400_000)
	keys, err := o.backend.Keys(prefix)
	if err != nil {
		return nil, err
	}
	var peers []NodeId
	for _, key := range keys {
		raw, err := o.backend.Get(key)
		if err != nil {
			return nil, err
		}
		if raw == nil {
			continue
		}
		seen, err := decodeOne(raw, func(d *postcard.Decoder) uint64 { return d.U64() })
		if err != nil {
			return nil, err
		}
		if seen >= cutoff {
			parts := bytes.Split(key, []byte("/"))
			if node, ok := decodeHexNode(parts[len(parts)-1]); ok {
				peers = append(peers, node)
			}
		}
	}
	sort.Slice(peers, func(i, j int) bool { return bytes.Compare(peers[i][:], peers[j][:]) < 0 })
	peers = peers[:uniqueNodes(peers)]
	if len(peers) == 0 {
		return &FileAvailability{Kind: FileUnavailable}, nil
	}
	return &FileAvailability{Kind: FileRemote, Remote: peers}, nil
}

func uniqueNodes(peers []NodeId) int {
	n := 0
	for i, p := range peers {
		if i == 0 || p != peers[i-1] {
			peers[n] = p
			n++
		}
	}
	return n
}

func (o *FileObjects) recordPeerAvailability(digest [32]byte, generation uint64, node NodeId, available bool) error {
	key := append(availabilityPrefix(digest, generation), Hex(node[:])...)
	var mutation Record
	if available {
		mutation = PutRecord(key, encode(func(e *postcard.Encoder) { e.U64(filesNow()) }))
	} else {
		mutation = DeleteRecord(key)
	}
	return o.backend.Commit([]Record{mutation})
}

// Delete tombstones one file's metadata.
func (o *FileObjects) Delete(name string) error {
	o.gate.Lock()
	defer o.gate.Unlock()
	meta, err := o.Metadata(name)
	if err != nil {
		return err
	}
	if meta != nil {
		meta.Deleted = true
		meta.Millis = filesNow()
		_, meta.Node = o.backend.Identity()
		return o.backend.Publish([]Record{PutRecord(metaKey(name), EncodeFileMetadata(meta))})
	}
	return nil
}

// Read verifies one object and streams its plaintext.
func (o *FileObjects) Read(name string, output io.Writer) (*FileMetadata, error) {
	meta, err := o.Metadata(name)
	if err != nil {
		return nil, err
	}
	if meta == nil || meta.Deleted {
		return nil, unavailableErr("file metadata missing")
	}
	if err := o.Verify(o.path(meta.Digest, meta.Generation), meta, output); err != nil {
		return nil, err
	}
	return meta, nil
}

// StreamPlainChunks verifies and streams plaintext in bounded chunks,
// omitting an already exported prefix when resuming an external transfer.
func (o *FileObjects) StreamPlainChunks(name string, skip uint64, chunkSize int, emit func(uint64, []byte) error) (*FileMetadata, uint64, error) {
	if chunkSize == 0 {
		return nil, 0, invalidErr("zero file chunk size")
	}
	meta, err := o.Metadata(name)
	if err != nil {
		return nil, 0, err
	}
	if meta == nil || meta.Deleted {
		return nil, 0, unavailableErr("file metadata missing")
	}
	sink := &chunkWriter{skip: skip, position: skip, chunkSize: chunkSize, emit: emit}
	verified := o.Verify(o.path(meta.Digest, meta.Generation), meta, sink)
	if sink.err != nil {
		return nil, 0, sink.err
	}
	if verified != nil {
		return nil, 0, verified
	}
	if len(sink.buffer) > 0 {
		if err := emit(sink.position, sink.buffer); err != nil {
			return nil, 0, err
		}
		sink.position += uint64(len(sink.buffer))
	}
	return meta, sink.total, nil
}

type chunkWriter struct {
	skip      uint64
	position  uint64
	total     uint64
	chunkSize int
	buffer    []byte
	emit      func(uint64, []byte) error
	err       error
}

func (w *chunkWriter) Write(p []byte) (int, error) {
	original := len(p)
	w.total += uint64(original)
	discard := w.skip
	if discard > uint64(len(p)) {
		discard = uint64(len(p))
	}
	w.skip -= discard
	p = p[discard:]
	for len(p) > 0 {
		take := w.chunkSize - len(w.buffer)
		if take > len(p) {
			take = len(p)
		}
		w.buffer = append(w.buffer, p[:take]...)
		p = p[take:]
		if len(w.buffer) == w.chunkSize {
			if err := w.emit(w.position, w.buffer); err != nil {
				w.err = err
				return 0, io.ErrUnexpectedEOF
			}
			w.position += uint64(len(w.buffer))
			w.buffer = w.buffer[:0]
		}
	}
	return original, nil
}

// Verify authenticates one container and streams its plaintext.
func (o *FileObjects) Verify(path string, meta *FileMetadata, output io.Writer) error {
	input, err := os.Open(path)
	if err != nil {
		return unavailableErr("file payload unavailable")
	}
	defer input.Close()
	return o.verifyInput(input, meta, output)
}

func (o *FileObjects) verifyInput(input io.Reader, meta *FileMetadata, output io.Writer) error {
	var header [40]byte
	if _, err := io.ReadFull(input, header[:]); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return authErr("object header")
		}
		return ioErr(err)
	}
	if string(header[:8]) != "SPFO0001" {
		return authErr("object header")
	}
	generation := binary.BigEndian.Uint64(header[8:16])
	var key *[32]byte
	switch {
	case generation == o.config.Generation:
		key = &o.config.Key
	case generation+1 == o.config.Generation && o.config.PreviousKey != nil:
		key = o.config.PreviousKey
	case generation+1 == o.config.Generation:
		return authErr("object key unavailable")
	default:
		return authErr("object key generation")
	}
	var salt [16]byte
	copy(salt[:], header[16:32])
	derived := objectKey(key, &salt)
	block, err := aes.NewCipher(derived[:])
	if err != nil {
		return ioErr(err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return ioErr(err)
	}
	var prefix [8]byte
	copy(prefix[:], header[32:40])
	var index uint32
	hash := sha256.New()
	var total uint64
	for {
		var nb [4]byte
		if _, err := io.ReadFull(input, nb[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return authErr("object chunk length")
			}
			return ioErr(err)
		}
		size := int(binary.BigEndian.Uint32(nb[:]))
		finalChunk := size == 0xFFFFFFFF
		if finalChunk {
			if _, err := io.ReadFull(input, nb[:]); err != nil {
				if err == io.EOF || err == io.ErrUnexpectedEOF {
					return authErr("object chunk length")
				}
				return ioErr(err)
			}
			size = int(binary.BigEndian.Uint32(nb[:]))
		}
		if size > FileChunk+16 || size < 16 {
			return authErr("object chunk length")
		}
		encrypted := make([]byte, size)
		if _, err := io.ReadFull(input, encrypted); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return authErr("object chunk length")
			}
			return ioErr(err)
		}
		aad := append(append([]byte(nil), header[:]...), uint32be(index)...)
		if finalChunk {
			aad = append(aad, []byte("final")...)
		}
		nonce := fileNonce(prefix, index)
		plain, err := gcm.Open(nil, nonce[:], encrypted, aad)
		if err != nil {
			return authErr("object authentication")
		}
		if finalChunk {
			var length uint64
			var digest [32]byte
			_, derr := decodeOne(plain, func(d *postcard.Decoder) uint64 {
				length = d.U64()
				copy(digest[:], d.Raw(32))
				return 0
			})
			if derr != nil {
				return derr
			}
			var sum [32]byte
			copy(sum[:], hash.Sum(nil))
			var one [1]byte
			n, _ := input.Read(one[:])
			if length != total || length != meta.Bytes || digest != meta.Digest || sum != digest || n != 0 {
				return authErr("object length or digest")
			}
			return nil
		}
		if total += uint64(len(plain)); total < uint64(len(plain)) {
			return ErrOverloaded
		}
		if total > meta.Bytes || total > o.config.MaxFileBytes {
			return ErrOverloaded
		}
		hash.Write(plain)
		if _, err := output.Write(plain); err != nil {
			return ioErr(err)
		}
		if index == 0xFFFFFFFF {
			return ErrOverloaded
		}
		index++
	}
}

func uint32be(v uint32) []byte {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], v)
	return b[:]
}

func (o *FileObjects) path(digest [32]byte, generation uint64) string {
	return filepath.Join(o.config.Directory, Hex(digest[:])+".g"+strconv.FormatUint(generation, 10)+".spfo")
}

// PublishFetched publishes only a fully authenticated staged container.
func (o *FileObjects) PublishFetched(staged string, meta *FileMetadata) error {
	o.gate.Lock()
	defer o.gate.Unlock()
	if err := o.Verify(staged, meta, io.Discard); err != nil {
		return err
	}
	path := o.path(meta.Digest, meta.Generation)
	if _, err := os.Stat(path); err != nil {
		if err := os.Rename(staged, path); err != nil {
			return ioErr(err)
		}
		if err := syncDirectory(o.config.Directory); err != nil {
			return err
		}
	}
	return nil
}

// RotateKeys re-encrypts up to limit old-generation objects with the
// current key. Metadata generation is the durable progress marker, so a
// restart resumes by scanning for the next stale entry.
func (o *FileObjects) RotateKeys(limit int) (int, error) {
	rotated := 0
	listed, err := o.List()
	if err != nil {
		return 0, err
	}
	for _, entry := range listed {
		if rotated >= limit {
			break
		}
		if entry.Generation == o.config.Generation {
			continue
		}
		if st, err := os.Stat(o.path(entry.Digest, entry.Generation)); err != nil || st.IsDir() {
			continue
		}
		o.gate.Lock()
		current, err := o.Metadata(entry.Name)
		if err != nil || current == nil || current.Deleted || current.Digest != entry.Digest || current.Generation == o.config.Generation {
			o.gate.Unlock()
			if err != nil {
				return rotated, err
			}
			continue
		}
		var id NodeId
		if _, err := rand.Read(id[:]); err != nil {
			o.gate.Unlock()
			return rotated, ioErr(err)
		}
		raw := filepath.Join(o.config.Directory, ".rotate-"+Hex(id[:]))
		plain, err := privateFile(raw)
		if err != nil {
			o.gate.Unlock()
			return rotated, err
		}
		verr := o.Verify(o.path(current.Digest, current.Generation), current, plain)
		if verr == nil {
			verr = plain.Sync()
		}
		plain.Close()
		var opened *os.File
		if verr == nil {
			opened, verr = os.Open(raw)
		}
		var meta *FileMetadata
		if verr == nil {
			meta, verr = o.uploadInner(current.Name, opened)
			opened.Close()
		}
		_ = meta
		_ = os.Remove(raw)
		o.gate.Unlock()
		if verr != nil {
			return rotated, verr
		}
		rotated++
	}
	return rotated, nil
}

// KeyRotationStatus reports whether this store still contains any object
// or staged transfer that may need an older file key. Local-store check.
func (o *FileObjects) KeyRotationStatus() (*FileKeyRotationStatus, error) {
	var missingLocally uint64
	listed, err := o.List()
	if err != nil {
		return nil, err
	}
	for _, meta := range listed {
		if meta.Generation != o.config.Generation {
			if st, err := os.Stat(o.path(meta.Digest, meta.Generation)); err != nil || st.IsDir() {
				missingLocally++
			}
		}
	}
	var oldObjects, staged uint64
	entries, err := os.ReadDir(o.config.Directory)
	if err != nil {
		return nil, ioErr(err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if name == "staging" {
			if info, err := entry.Info(); err == nil && info.IsDir() {
				stagedEntries, err := os.ReadDir(filepath.Join(o.config.Directory, name))
				if err != nil {
					return nil, ioErr(err)
				}
				for _, s := range stagedEntries {
					if info, err := s.Info(); err == nil && !info.IsDir() {
						staged++
					}
				}
			}
			continue
		}
		if _, generation, ok := parseObjectName(name); ok && generation != o.config.Generation {
			oldObjects++
		}
	}
	return &FileKeyRotationStatus{
		CurrentGeneration:                   o.config.Generation,
		OldGenerationObjects:                oldObjects,
		OldGenerationMetadataMissingLocally: missingLocally,
		StagedTransfers:                     staged,
		PreviousKeyUnusedLocally:            oldObjects == 0 && missingLocally == 0 && staged == 0,
	}, nil
}

// Collect removes unreferenced encrypted objects and abandoned staging
// files after a grace period. Metadata, including deletion tombstones,
// remains in the backend.
func (o *FileObjects) Collect(grace time.Duration) (int, error) {
	o.gate.Lock()
	defer o.gate.Unlock()
	var graceMs uint64
	if ms := grace.Milliseconds(); ms > 0 {
		graceMs = uint64(ms)
	}
	cutoff := satSub(filesNow(), graceMs)
	live := make(map[[40]byte]bool)
	keys, err := o.backend.Keys([]byte("08/files/meta/"))
	if err != nil {
		return 0, err
	}
	for _, key := range keys {
		raw, err := o.backend.Get(key)
		if err != nil {
			return 0, err
		}
		if raw == nil {
			continue
		}
		meta, err := DecodeFileMetadata(raw)
		if err != nil {
			return 0, err
		}
		if !meta.Deleted {
			var k [40]byte
			copy(k[:32], meta.Digest[:])
			binary.BigEndian.PutUint64(k[32:], meta.Generation)
			live[k] = true
		}
	}
	removed := 0
	entries, err := os.ReadDir(o.config.Directory)
	if err != nil {
		return 0, ioErr(err)
	}
	for _, entry := range entries {
		name := entry.Name()
		info, err := entry.Info()
		if err != nil {
			return 0, ioErr(err)
		}
		full := filepath.Join(o.config.Directory, name)
		if name == "staging" && info.IsDir() {
			staged, err := os.ReadDir(full)
			if err != nil {
				return 0, ioErr(err)
			}
			for _, s := range staged {
				sp := filepath.Join(full, s.Name())
				o.stagedPaths.Lock()
				protected := o.staged[sp]
				o.stagedPaths.Unlock()
				if protected {
					continue
				}
				sinfo, err := s.Info()
				if err != nil {
					return 0, ioErr(err)
				}
				if modifiedMillis(sinfo) <= cutoff && !sinfo.IsDir() {
					if err := os.Remove(sp); err != nil {
						return 0, ioErr(err)
					}
					removed++
				}
			}
			continue
		}
		if modifiedMillis(info) > cutoff {
			continue
		}
		o.stagedPaths.Lock()
		protected := o.staged[full]
		o.stagedPaths.Unlock()
		if protected {
			continue
		}
		if strings.HasPrefix(name, ".upload-") || strings.HasPrefix(name, ".rotate-") || strings.HasPrefix(name, ".download-") {
			if !info.IsDir() {
				if err := os.Remove(full); err != nil {
					return 0, ioErr(err)
				}
				removed++
			}
			continue
		}
		if digest, generation, ok := parseObjectName(name); ok {
			var k [40]byte
			copy(k[:32], digest[:])
			binary.BigEndian.PutUint64(k[32:], generation)
			if !live[k] {
				if err := os.Remove(full); err != nil {
					return 0, ioErr(err)
				}
				removed++
			}
		}
	}
	if removed > 0 {
		if err := syncDirectory(o.config.Directory); err != nil {
			return removed, err
		}
	}
	return removed, nil
}

func modifiedMillis(info fs.FileInfo) uint64 {
	ms := info.ModTime().UnixMilli()
	if ms < 0 {
		return filesNow()
	}
	return uint64(ms)
}

// FileUpload is an incremental encrypted object writer. Abort without
// finishing to discard it; Collect sweeps abandoned uploads after grace.
type FileUpload struct {
	name       string
	path       string
	file       *os.File
	cipher     cipher.AEAD
	header     []byte
	prefix     [8]byte
	hash       hash.Hash
	total      uint64
	index      uint32
	generation uint64
	max        uint64
	pending    []byte
	reserve    *StagingReservation
	used       *stagingUsage
	stagingMax uint64
	objects    *FileObjects
}

func newFileUpload(objects *FileObjects, name string) (*FileUpload, error) {
	if err := checkFileName(name); err != nil {
		return nil, err
	}
	reservation, err := objects.ReserveStaging(128)
	if err != nil {
		return nil, err
	}
	var id NodeId
	if _, err := rand.Read(id[:]); err != nil {
		reservation.Release()
		return nil, ioErr(err)
	}
	path := filepath.Join(objects.config.Directory, ".upload-"+Hex(id[:]))
	file, err := privateFile(path)
	if err != nil {
		reservation.Release()
		return nil, err
	}
	var salt [16]byte
	if _, err := rand.Read(salt[:]); err != nil {
		file.Close()
		os.Remove(path)
		reservation.Release()
		return nil, ioErr(err)
	}
	derived := objectKey(&objects.config.Key, &salt)
	var prefix [8]byte
	if _, err := rand.Read(prefix[:]); err != nil {
		file.Close()
		os.Remove(path)
		reservation.Release()
		return nil, ioErr(err)
	}
	header := append([]byte(nil), []byte("SPFO0001")...)
	var gb [8]byte
	binary.BigEndian.PutUint64(gb[:], objects.config.Generation)
	header = append(header, gb[:]...)
	header = append(header, salt[:]...)
	header = append(header, prefix[:]...)
	if _, err := file.Write(header); err != nil {
		file.Close()
		os.Remove(path)
		reservation.Release()
		return nil, ioErr(err)
	}
	block, err := aes.NewCipher(derived[:])
	if err != nil {
		file.Close()
		os.Remove(path)
		reservation.Release()
		return nil, ioErr(err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		file.Close()
		os.Remove(path)
		reservation.Release()
		return nil, ioErr(err)
	}
	objects.stagedPaths.Lock()
	objects.staged[path] = true
	objects.stagedPaths.Unlock()
	return &FileUpload{name: name, path: path, file: file, cipher: gcm, header: header,
		prefix: prefix, hash: sha256.New(), generation: objects.config.Generation,
		max: objects.config.MaxFileBytes, pending: make([]byte, 0, FileChunk),
		reserve: reservation, used: objects.stagingUsage,
		stagingMax: objects.config.MaxStagingBytes, objects: objects}, nil
}

// Append adds plaintext to the upload.
func (u *FileUpload) Append(p []byte) error {
	total := u.total + uint64(len(p))
	if total < u.total {
		return ErrOverloaded
	}
	if total > u.max {
		return ErrOverloaded
	}
	reserve := uint64(len(p)) + (divCeil(total, FileChunk)-divCeil(u.total, FileChunk))*20
	u.used.mu.Lock()
	if reserve > satSub(u.stagingMax, u.used.used) {
		u.used.mu.Unlock()
		return ErrOverloaded
	}
	u.used.used += reserve
	u.used.mu.Unlock()
	u.reserve.bytes += reserve
	u.hash.Write(p)
	u.total = total
	for len(p) > 0 {
		n := FileChunk - len(u.pending)
		if n > len(p) {
			n = len(p)
		}
		u.pending = append(u.pending, p[:n]...)
		p = p[n:]
		if len(u.pending) == FileChunk {
			if err := u.flushChunk(); err != nil {
				return err
			}
		}
	}
	return nil
}

func divCeil(a, b uint64) uint64 {
	q := a / b
	if a%b != 0 {
		q++
	}
	return q
}

func (u *FileUpload) flushChunk() error {
	if len(u.pending) == 0 {
		return nil
	}
	aad := append(append([]byte(nil), u.header...), uint32be(u.index)...)
	nonce := fileNonce(u.prefix, u.index)
	encrypted := u.cipher.Seal(nil, nonce[:], u.pending, aad)
	if _, err := u.file.Write(uint32be(uint32(len(encrypted)))); err != nil {
		return ioErr(err)
	}
	if _, err := u.file.Write(encrypted); err != nil {
		return ioErr(err)
	}
	if u.index == 0xFFFFFFFF {
		return ErrOverloaded
	}
	u.index++
	for i := range u.pending {
		u.pending[i] = 0
	}
	u.pending = u.pending[:0]
	return nil
}

func (u *FileUpload) finishInner(objects *FileObjects) (*FileMetadata, error) {
	defer u.cleanup()
	if err := u.flushChunk(); err != nil {
		return nil, err
	}
	sum := u.hash.Sum(nil)
	var digest [32]byte
	copy(digest[:], sum)
	terminal := encode(func(e *postcard.Encoder) {
		e.U64(u.total)
		e.Raw(digest[:])
	})
	aad := append(append(append([]byte(nil), u.header...), uint32be(u.index)...), []byte("final")...)
	nonce := fileNonce(u.prefix, u.index)
	encrypted := u.cipher.Seal(nil, nonce[:], terminal, aad)
	if _, err := u.file.Write(uint32be(0xFFFFFFFF)); err != nil {
		return nil, ioErr(err)
	}
	if _, err := u.file.Write(uint32be(uint32(len(encrypted)))); err != nil {
		return nil, ioErr(err)
	}
	if _, err := u.file.Write(encrypted); err != nil {
		return nil, ioErr(err)
	}
	if err := u.file.Sync(); err != nil {
		return nil, ioErr(err)
	}
	u.file.Close()
	u.file = nil
	path := objects.path(digest, u.generation)
	if _, err := os.Stat(path); err == nil {
		os.Remove(u.path)
	} else {
		if err := os.Rename(u.path, path); err != nil {
			return nil, ioErr(err)
		}
		if err := syncDirectory(objects.config.Directory); err != nil {
			return nil, err
		}
	}
	_, node := objects.backend.Identity()
	meta := &FileMetadata{Name: u.name, Digest: digest, Bytes: u.total,
		Generation: u.generation, Millis: filesNow(), Node: node}
	if err := objects.backend.Publish([]Record{PutRecord(metaKey(meta.Name), EncodeFileMetadata(meta))}); err != nil {
		return nil, err
	}
	return meta, nil
}

func (u *FileUpload) cleanup() {
	if u.file != nil {
		u.file.Close()
		u.file = nil
	}
	os.Remove(u.path)
	u.objects.stagedPaths.Lock()
	delete(u.objects.staged, u.path)
	u.objects.stagedPaths.Unlock()
	for i := range u.pending {
		u.pending[i] = 0
	}
	u.reserve.Release()
}

// Abort discards the upload.
func (u *FileUpload) Abort() { u.cleanup() }

// FileDownload is a verified, pinned immutable object consumed in
// bounded plaintext chunks. Close it when done.
type FileDownload struct {
	meta       FileMetadata
	file       *os.File
	cipher     cipher.AEAD
	header     [40]byte
	index      uint32
	offset     uint64
	pending    []byte
	pendingPos int
	pin        string
	objects    *FileObjects
	once       sync.Once
}

// Meta returns the download's metadata.
func (d *FileDownload) Meta() *FileMetadata { return &d.meta }

// Next returns up to limit plaintext bytes with the start offset and a
// done flag.
func (d *FileDownload) Next(limit int) (uint64, []byte, bool, error) {
	if limit == 0 || limit > 1<<20 {
		return 0, nil, false, invalidErr("file chunk limit")
	}
	start := d.offset
	out := make([]byte, 0, limit)
	for len(out) < limit && d.offset < d.meta.Bytes {
		if len(d.pending) == 0 {
			var nb [4]byte
			if _, err := io.ReadFull(d.file, nb[:]); err != nil {
				return 0, nil, false, ioErr(err)
			}
			n := int(binary.BigEndian.Uint32(nb[:]))
			if n < 16 || n > FileChunk+16 {
				return 0, nil, false, authErr("object chunk length")
			}
			encrypted := make([]byte, n)
			if _, err := io.ReadFull(d.file, encrypted); err != nil {
				return 0, nil, false, ioErr(err)
			}
			aad := append(append([]byte(nil), d.header[:]...), uint32be(d.index)...)
			var prefix [8]byte
			copy(prefix[:], d.header[32:40])
			nonce := fileNonce(prefix, d.index)
			plain, err := d.cipher.Open(nil, nonce[:], encrypted, aad)
			if err != nil {
				return 0, nil, false, authErr("object authentication")
			}
			for i := range d.pending {
				d.pending[i] = 0
			}
			d.pending = plain
			d.pendingPos = 0
			d.index++
		}
		n := limit - len(out)
		if n > len(d.pending)-d.pendingPos {
			n = len(d.pending) - d.pendingPos
		}
		out = append(out, d.pending[d.pendingPos:d.pendingPos+n]...)
		d.pendingPos += n
		d.offset += uint64(n)
		if d.pendingPos == len(d.pending) {
			for i := range d.pending {
				d.pending[i] = 0
			}
			d.pending = nil
			d.pendingPos = 0
		}
	}
	return start, out, d.offset == d.meta.Bytes, nil
}

// Close releases the download pin. It is idempotent.
func (d *FileDownload) Close() {
	d.once.Do(func() {
		d.file.Close()
		os.Remove(d.pin)
		d.objects.stagedPaths.Lock()
		delete(d.objects.staged, d.pin)
		d.objects.stagedPaths.Unlock()
	})
}

func parseObjectName(name string) ([32]byte, uint64, bool) {
	var zero [32]byte
	stem, ok := strings.CutSuffix(name, ".spfo")
	if !ok {
		return zero, 0, false
	}
	i := strings.LastIndex(stem, ".g")
	if i < 0 {
		return zero, 0, false
	}
	var generation uint64
	for _, c := range stem[i+2:] {
		if c < '0' || c > '9' {
			return zero, 0, false
		}
		generation = generation*10 + uint64(c-'0')
	}
	if stem[:i] == "" || len(stem[:i]) != 64 {
		return zero, 0, false
	}
	var digest [32]byte
	for j := 0; j < 32; j++ {
		hi, ok1 := hexVal(stem[2*j])
		lo, ok2 := hexVal(stem[2*j+1])
		if !ok1 || !ok2 {
			return zero, 0, false
		}
		digest[j] = hi<<4 | lo
	}
	return digest, generation, true
}

func hexVal(b byte) (byte, bool) {
	switch {
	case b >= '0' && b <= '9':
		return b - '0', true
	case b >= 'a' && b <= 'f':
		return b - 'a' + 10, true
	}
	return 0, false
}

func availabilityPrefix(digest [32]byte, generation uint64) []byte {
	return []byte("08/files/availability/" + Hex(digest[:]) + "/" + strconv.FormatUint(generation, 10) + "/")
}

func decodeHexNode(b []byte) (NodeId, bool) {
	var zero NodeId
	if len(b) != 32 {
		return zero, false
	}
	var node NodeId
	for i := 0; i < 16; i++ {
		hi, ok1 := hexVal(b[2*i])
		lo, ok2 := hexVal(b[2*i+1])
		if !ok1 || !ok2 {
			return zero, false
		}
		node[i] = hi<<4 | lo
	}
	return node, true
}

func fileNonce(prefix [8]byte, index uint32) [12]byte {
	var n [12]byte
	copy(n[:8], prefix[:])
	binary.BigEndian.PutUint32(n[8:], index)
	return n
}

func metaKey(name string) []byte {
	sum := sha256.Sum256([]byte(name))
	return []byte("08/files/meta/" + Hex(sum[:]))
}

func checkFileName(name string) error {
	if name == "" || len(name) > 4096 || strings.ContainsRune(name, 0) {
		return invalidErr("file name")
	}
	return nil
}

func filesNow() uint64 {
	ms := time.Now().UnixMilli()
	if ms < 0 {
		return 0
	}
	return uint64(ms)
}

func privateFile(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, ioErr(err)
	}
	return f, nil
}

func syncDirectory(path string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	d, err := os.Open(path)
	if err != nil {
		return ioErr(err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return ioErr(err)
	}
	return nil
}

func objectKey(master *[32]byte, salt *[16]byte) [32]byte {
	var key [32]byte
	r := hkdf.New(sha256.New, master[:], salt[:], []byte("murmur/sqlite-file-object/v1"))
	_, _ = io.ReadFull(r, key[:])
	return key
}
