package replication

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/marcgauthier/murmur-sql/murmur/internal/postcard"
	"golang.org/x/crypto/chacha20poly1305"
	"golang.org/x/crypto/hkdf"
)

// BridgeRole selects the bridge direction.
type BridgeRole int

const (
	// BridgeLowExporter seals artifacts for the high side.
	BridgeLowExporter BridgeRole = iota
	// BridgeHighReceiver imports sealed artifacts.
	BridgeHighReceiver
)

// BridgeConfig tunes the one-way logical bridge.
type BridgeConfig struct {
	Domain          NodeId
	TrustedDomains  map[NodeId]bool
	TrustedOrigins  map[NodeId][32]byte
	Role            BridgeRole
	Directory       string
	Stream          string
	Recipient       [32]byte
	ReceiverSecret  *[32]byte
	Signer          *[32]byte
	TrustedSigners  map[NodeId][32]byte
	MaxBundleBytes  int
	MaxPayloadBytes int
	StagingBytes    int
}

// Validate rejects inconsistent bridge configuration.
func (c *BridgeConfig) Validate() error {
	streamOK := len(c.Stream) > 0 && len(c.Stream) <= 64
	for i := 0; i < len(c.Stream) && streamOK; i++ {
		b := c.Stream[i]
		if !(b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9' || b == '-' || b == '_') {
			streamOK = false
		}
	}
	if c.Domain == ([16]byte{}) ||
		c.Role == BridgeHighReceiver && len(c.TrustedDomains) == 0 ||
		!streamOK ||
		c.MaxBundleBytes == 0 || c.MaxBundleBytes > 16<<20 ||
		c.MaxPayloadBytes == 0 || c.MaxPayloadBytes > 64<<20 ||
		c.StagingBytes < c.MaxBundleBytes ||
		c.Role == BridgeLowExporter && c.Signer == nil ||
		c.Role == BridgeHighReceiver && c.ReceiverSecret == nil {
		return invalidErr("bridge configuration")
	}
	return nil
}

// NewBridgeExporter returns an exporter configuration.
func NewBridgeExporter(domain NodeId, directory, stream string, signer, recipient [32]byte) *BridgeConfig {
	return &BridgeConfig{Domain: domain, Role: BridgeLowExporter, Directory: directory,
		Stream: stream, Recipient: recipient, Signer: &signer,
		MaxBundleBytes: 16 << 20, MaxPayloadBytes: 64 << 20, StagingBytes: 1 << 30}
}

// NewBridgeReceiver returns a receiver configuration.
func NewBridgeReceiver(domain NodeId, directory, stream string, secret [32]byte, trustedDomains map[NodeId]bool, trustedSigners, trustedOrigins map[NodeId][32]byte) *BridgeConfig {
	return &BridgeConfig{Domain: domain, TrustedDomains: trustedDomains, TrustedOrigins: trustedOrigins,
		Role: BridgeHighReceiver, Directory: directory, Stream: stream,
		Recipient: RecipientPublic(&secret), ReceiverSecret: &secret,
		TrustedSigners: trustedSigners,
		MaxBundleBytes: 16 << 20, MaxPayloadBytes: 64 << 20, StagingBytes: 1 << 30}
}

// Origin proves the source transaction of a bridged payload.
type Origin struct {
	Node        NodeId
	Sequence    uint64
	Transaction NodeId
	Digest      [32]byte
}

// Header is a sealed bundle envelope.
type Header struct {
	Origin         *Origin
	SourceDatabase NodeId
	SourceNode     NodeId
	Stream         string
	Sequence       uint64
	Bundle         NodeId
	Recipient      [32]byte
	Ephemeral      [32]byte
	Nonce          [24]byte
	Digest         [32]byte
}

// Bundle is one sealed artifact.
type Bundle struct {
	Version    uint16
	Header     Header
	Ciphertext []byte
	Signature  []byte
}

// FilePart is one chunk of a bridged file.
type FilePart struct {
	Name      string
	Digest    [32]byte
	Bytes     uint64
	Offset    uint64
	Data      []byte
	FinalPart bool
}

func encodeOrigin(e *postcard.Encoder, o *Origin) {
	e.Raw(o.Node[:])
	e.U64(o.Sequence)
	e.Raw(o.Transaction[:])
	e.Raw(o.Digest[:])
}

func decodeOrigin(d *postcard.Decoder) Origin {
	var o Origin
	copy(o.Node[:], d.Raw(16))
	o.Sequence = d.U64()
	copy(o.Transaction[:], d.Raw(16))
	copy(o.Digest[:], d.Raw(32))
	return o
}

func encodeHeader(e *postcard.Encoder, h *Header) {
	if h.Origin == nil {
		e.None()
	} else {
		e.Some()
		encodeOrigin(e, h.Origin)
	}
	e.Raw(h.SourceDatabase[:])
	e.Raw(h.SourceNode[:])
	e.String(h.Stream)
	e.U64(h.Sequence)
	e.Raw(h.Bundle[:])
	e.Raw(h.Recipient[:])
	e.Raw(h.Ephemeral[:])
	e.Raw(h.Nonce[:])
	e.Raw(h.Digest[:])
}

func decodeHeader(d *postcard.Decoder) Header {
	var h Header
	if d.Present() {
		o := decodeOrigin(d)
		h.Origin = &o
	}
	copy(h.SourceDatabase[:], d.Raw(16))
	copy(h.SourceNode[:], d.Raw(16))
	h.Stream = d.String()
	h.Sequence = d.U64()
	copy(h.Bundle[:], d.Raw(16))
	copy(h.Recipient[:], d.Raw(32))
	copy(h.Ephemeral[:], d.Raw(32))
	copy(h.Nonce[:], d.Raw(24))
	copy(h.Digest[:], d.Raw(32))
	return h
}

// EncodeHeader serializes a bundle header.
func EncodeHeader(h *Header) []byte { return encode(func(e *postcard.Encoder) { encodeHeader(e, h) }) }

// DecodeHeader parses a bundle header.
func DecodeHeader(b []byte) (Header, error) { return decodeOne(b, decodeHeader) }

func encodeBundle(e *postcard.Encoder, b *Bundle) {
	e.U16(b.Version)
	encodeHeader(e, &b.Header)
	e.ByteString(b.Ciphertext)
	e.ByteString(b.Signature)
}

func decodeBundle(d *postcard.Decoder) Bundle {
	var b Bundle
	b.Version = d.U16()
	b.Header = decodeHeader(d)
	b.Ciphertext = append([]byte(nil), d.Bytes()...)
	b.Signature = append([]byte(nil), d.Bytes()...)
	return b
}

// EncodeBundle serializes a bundle.
func EncodeBundle(b *Bundle) []byte { return encode(func(e *postcard.Encoder) { encodeBundle(e, b) }) }

// DecodeBundle parses a bundle.
func DecodeBundle(b []byte) (Bundle, error) { return decodeOne(b, decodeBundle) }

func encodeFilePart(e *postcard.Encoder, p *FilePart) {
	e.String(p.Name)
	e.Raw(p.Digest[:])
	e.U64(p.Bytes)
	e.U64(p.Offset)
	e.ByteString(p.Data)
	e.Bool(p.FinalPart)
}

func decodeFilePart(d *postcard.Decoder) FilePart {
	var p FilePart
	p.Name = d.String()
	copy(p.Digest[:], d.Raw(32))
	p.Bytes = d.U64()
	p.Offset = d.U64()
	p.Data = append([]byte(nil), d.Bytes()...)
	p.FinalPart = d.Bool()
	return p
}

// EncodeFilePart serializes a file part.
func EncodeFilePart(p *FilePart) []byte {
	return encode(func(e *postcard.Encoder) { encodeFilePart(e, p) })
}

// DecodeFilePart parses a file part.
func DecodeFilePart(b []byte) (FilePart, error) { return decodeOne(b, decodeFilePart) }

// Bridge moves sealed artifacts one way between domains.
type Bridge struct {
	config  BridgeConfig
	backend Backend
	gate    sync.Mutex
	filesMu sync.Mutex
	files   *FileObjects
}

var bridgeFileMagic = []byte("MURMUR-BRIDGE-FILE-1\x00")

func bridgeFilePayload(part *FilePart) ([]byte, error) {
	out := append([]byte(nil), bridgeFileMagic...)
	return append(out, EncodeFilePart(part)...), nil
}

// OpenBridge opens a bridge over a backend.
func OpenBridge(config BridgeConfig, backend Backend) (*Bridge, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	db, _ := backend.Identity()
	if config.Domain != db {
		return nil, invalidErr("bridge domain mismatch")
	}
	if err := os.MkdirAll(config.Directory, 0o700); err != nil {
		return nil, ioErr(err)
	}
	return &Bridge{config: config, backend: backend}, nil
}

// AttachFiles attaches file storage for file bridging.
func (b *Bridge) AttachFiles(objects *FileObjects) {
	b.filesMu.Lock()
	b.files = objects
	b.filesMu.Unlock()
}

// Export seals one payload artifact.
func (b *Bridge) Export(payload []byte) (string, error) {
	return b.exportOrigin(payload, nil)
}

func (b *Bridge) exportOrigin(payload []byte, origin *Origin) (string, error) {
	if b.config.Role != BridgeLowExporter {
		return "", invalidErr("bridge direction")
	}
	if len(payload) > b.config.MaxPayloadBytes {
		return "", ErrOverloaded
	}
	db, node := b.backend.Identity()
	sequenceKey := []byte("09/bridge/export/" + b.config.Stream + "/sequence")
	var sequence uint64
	if raw, err := b.backend.Get(sequenceKey); err != nil {
		return "", err
	} else if raw != nil {
		v, err := decodeOne(raw, func(d *postcard.Decoder) uint64 { return d.U64() })
		if err != nil {
			return "", err
		}
		sequence = v
	}
	if sequence == ^uint64(0) {
		return "", ErrOverloaded
	}
	sequence++
	var ephemeralSeed [32]byte
	if _, err := rand.Read(ephemeralSeed[:]); err != nil {
		return "", ioErr(err)
	}
	ephemeralPublic := RecipientPublic(&ephemeralSeed)
	secret, err := Agree(&ephemeralSeed, &b.config.Recipient)
	if err != nil {
		return "", err
	}
	var bundle NodeId
	if _, err := rand.Read(bundle[:]); err != nil {
		return "", ioErr(err)
	}
	var nonce [24]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", ioErr(err)
	}
	header := Header{Origin: origin, SourceDatabase: db, SourceNode: node,
		Stream: b.config.Stream, Sequence: sequence, Bundle: bundle,
		Recipient: b.config.Recipient, Ephemeral: ephemeralPublic,
		Nonce: nonce, Digest: sha256.Sum256(payload)}
	aad := EncodeHeader(&header)
	key := bridgeDerive(&secret, aad)
	compressed, err := frameZstd.Compress(nil, payload)
	if err != nil {
		return "", protocolErr("%v", err)
	}
	aead, err := chacha20poly1305.NewX(key[:])
	if err != nil {
		return "", ioErr(err)
	}
	ciphertext := aead.Seal(nil, nonce[:], compressed, aad)
	bundleMsg := &Bundle{Version: 1, Header: header, Ciphertext: ciphertext}
	bundleMsg.Signature = Sign(b.config.Signer, bridgeSigned(bundleMsg))
	raw := EncodeBundle(bundleMsg)
	if len(raw) > b.config.MaxBundleBytes {
		return "", ErrOverloaded
	}
	nodeHex := Hex(node[:])
	path := filepath.Join(b.config.Directory, nodeHex+"-"+zeroPad(sequence, 20)+".bundle")
	pending := filepath.Join(b.config.Directory, "."+Hex(bundle[:])+".pending")
	file, err := privateFile(pending)
	if err != nil {
		return "", err
	}
	if _, err := file.Write(raw); err != nil {
		file.Close()
		return "", ioErr(err)
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return "", ioErr(err)
	}
	file.Close()
	if err := b.backend.Commit([]Record{
		PutRecord([]byte("09/bridge/outbox/"+Hex(bundle[:])), raw),
		PutRecord(sequenceKey, encode(func(e *postcard.Encoder) { e.U64(sequence) })),
	}); err != nil {
		return "", err
	}
	if err := os.Rename(pending, path); err != nil {
		return "", ioErr(err)
	}
	if err := syncDirectory(b.config.Directory); err != nil {
		return "", err
	}
	return path, nil
}

// OpenBundle authenticates and decrypts one artifact.
func (b *Bridge) OpenBundle(raw []byte) (*Header, []byte, error) {
	if b.config.Role != BridgeHighReceiver || len(raw) > b.config.MaxBundleBytes {
		return nil, nil, invalidErr("bridge role or artifact size")
	}
	bundle, err := DecodeBundle(raw)
	if err != nil {
		return nil, nil, err
	}
	if bundle.Version != 1 || !b.config.TrustedDomains[bundle.Header.SourceDatabase] ||
		bundle.Header.Sequence == 0 || bundle.Header.Stream != b.config.Stream {
		return nil, nil, protocolErr("bridge envelope")
	}
	public, ok := b.config.TrustedSigners[bundle.Header.SourceNode]
	if !ok {
		return nil, nil, authErr("untrusted bridge signer")
	}
	if err := Verify(&public, bridgeSigned(&bundle), bundle.Signature); err != nil {
		return nil, nil, err
	}
	secret := b.config.ReceiverSecret
	if RecipientPublic(secret) != bundle.Header.Recipient {
		return nil, nil, authErr("bridge recipient mismatch")
	}
	shared, err := Agree(secret, &bundle.Header.Ephemeral)
	if err != nil {
		return nil, nil, err
	}
	aad := EncodeHeader(&bundle.Header)
	key := bridgeDerive(&shared, aad)
	aead, err := chacha20poly1305.NewX(key[:])
	if err != nil {
		return nil, nil, ioErr(err)
	}
	compressed, err := aead.Open(nil, bundle.Header.Nonce[:], bundle.Ciphertext, aad)
	if err != nil {
		return nil, nil, authErr("bridge authentication")
	}
	payload, err := frameZstd.Decompress(nil, compressed, b.config.MaxPayloadBytes+1)
	if err != nil || len(payload) > b.config.MaxPayloadBytes {
		return nil, nil, authErr("bridge payload digest or size")
	}
	if sha256.Sum256(payload) != bundle.Header.Digest {
		return nil, nil, authErr("bridge payload digest or size")
	}
	return &bundle.Header, payload, nil
}

// Stage authenticates before staging; preserves gaps and identity
// conflicts across restart.
func (b *Bridge) Stage(path string) (*Header, error) {
	st, err := os.Stat(path)
	if err != nil {
		return nil, ioErr(err)
	}
	if st.Size() > int64(b.config.MaxBundleBytes) {
		return nil, ErrOverloaded
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, ioErr(err)
	}
	header, err := func() (*Header, error) {
		h, _, err := b.OpenBundle(raw)
		return h, err
	}()
	if err != nil {
		return nil, err
	}
	key := []byte("09/bridge/inbox/" + Hex(header.SourceDatabase[:]) + "/" + header.Stream + "/" + zeroPad(header.Sequence, 20))
	if old, err := b.backend.Get(key); err != nil {
		return nil, err
	} else if old != nil {
		previous, _, err := b.OpenBundle(old)
		if err != nil {
			return nil, err
		}
		if previous.Bundle != header.Bundle || previous.Digest != header.Digest {
			return nil, authErr("conflicting bridge sequence")
		}
	} else {
		keys, err := b.backend.Keys([]byte("09/bridge/inbox/"))
		if err != nil {
			return nil, err
		}
		total := len(raw)
		for _, k := range keys {
			if v, err := b.backend.Get(k); err != nil {
				return nil, err
			} else if v != nil {
				total += len(v)
			}
			if total > b.config.StagingBytes {
				return nil, ErrOverloaded
			}
		}
		if err := b.backend.Commit([]Record{PutRecord(key, raw)}); err != nil {
			return nil, err
		}
	}
	return header, nil
}

// Release releases bridged ownership back to the backend.
func (b *Bridge) Release(table string, key []byte, column *string) (bool, error) {
	if b.config.Role != BridgeHighReceiver {
		return false, invalidErr("bridge direction")
	}
	return b.backend.ReleaseOwnership(table, key, column)
}

// ExportFile exports one named file as bounded sealed bridge bundles.
// The current offset is durable in Spool and resumes safely after restart.
func (b *Bridge) ExportFile(name string) error {
	b.gate.Lock()
	defer b.gate.Unlock()
	return b.exportFileLocked(name)
}

func (b *Bridge) exportFileLocked(name string) error {
	b.filesMu.Lock()
	objects := b.files
	b.filesMu.Unlock()
	if objects == nil {
		return invalidErr("bridge file storage is not configured")
	}
	meta, err := objects.Metadata(name)
	if err != nil {
		return err
	}
	if meta == nil || meta.Deleted {
		return unavailableErr("file metadata missing")
	}
	sum := sha256.Sum256([]byte(name))
	progressKey := []byte("09/bridge/file-export/" + b.config.Stream + "/" + Hex(sum[:]))
	type progress struct {
		digest [32]byte
		gen    uint64
		offset uint64
		done   bool
	}
	var saved *progress
	if raw, err := b.backend.Get(progressKey); err != nil {
		return err
	} else if raw != nil {
		var p progress
		_, err = decodeOne(raw, func(d *postcard.Decoder) uint64 {
			copy(p.digest[:], d.Raw(32))
			p.gen = d.U64()
			p.offset = d.U64()
			p.done = d.Bool()
			return 0
		})
		if err != nil {
			return err
		}
		saved = &p
	}
	encodeProgress := func(digest [32]byte, gen, offset uint64, done bool) []byte {
		return encode(func(e *postcard.Encoder) {
			e.Raw(digest[:])
			e.U64(gen)
			e.U64(offset)
			e.Bool(done)
		})
	}
	if saved != nil && saved.digest == meta.Digest && saved.gen == meta.Generation && saved.done {
		return nil
	}
	var skip uint64
	if saved != nil && saved.digest == meta.Digest && saved.gen == meta.Generation {
		skip = saved.offset
	}
	var offset uint64 = skip
	overhead := len(name) + 256
	maxChunk := satSub(uint64(b.config.MaxPayloadBytes), uint64(overhead))
	if maxChunk > 64<<10 {
		maxChunk = 64 << 10
	}
	if maxChunk == 0 {
		return ErrOverloaded
	}
	current, total, err := objects.StreamPlainChunks(name, skip, int(maxChunk), func(at uint64, data []byte) error {
		part := &FilePart{Name: name, Digest: meta.Digest, Bytes: meta.Bytes, Offset: at, Data: data}
		payload, err := bridgeFilePayload(part)
		if err != nil {
			return err
		}
		if _, err := b.exportOrigin(payload, nil); err != nil {
			return err
		}
		offset = at + uint64(len(data))
		return b.backend.Commit([]Record{PutRecord(progressKey, encodeProgress(meta.Digest, meta.Generation, offset, false))})
	})
	if err != nil {
		return err
	}
	if current.Digest != meta.Digest || current.Generation != meta.Generation {
		return unavailableErr("file changed during bridge export")
	}
	if total != meta.Bytes {
		return authErr("bridge file length")
	}
	finalPart := &FilePart{Name: name, Digest: meta.Digest, Bytes: meta.Bytes, Offset: total, FinalPart: true}
	payload, err := bridgeFilePayload(finalPart)
	if err != nil {
		return err
	}
	if _, err := b.exportOrigin(payload, nil); err != nil {
		return err
	}
	return b.backend.Commit([]Record{PutRecord(progressKey, encodeProgress(meta.Digest, meta.Generation, total, true))})
}

func (b *Bridge) captureFileLocked() (bool, error) {
	b.filesMu.Lock()
	objects := b.files
	b.filesMu.Unlock()
	if objects == nil {
		return false, nil
	}
	listed, err := objects.List()
	if err != nil {
		return false, err
	}
	for _, meta := range listed {
		avail, err := objects.Available(meta.Name)
		if err != nil {
			return false, err
		}
		if !avail {
			continue
		}
		sum := sha256.Sum256([]byte(meta.Name))
		key := []byte("09/bridge/file-export/" + b.config.Stream + "/" + Hex(sum[:]))
		done := false
		if raw, err := b.backend.Get(key); err != nil {
			return false, err
		} else if raw != nil {
			var digest [32]byte
			var gen uint64
			var finished bool
			_, err = decodeOne(raw, func(d *postcard.Decoder) uint64 {
				copy(digest[:], d.Raw(32))
				gen = d.U64()
				_ = d.U64()
				finished = d.Bool()
				return 0
			})
			if err != nil {
				return false, err
			}
			done = digest == meta.Digest && gen == meta.Generation && finished
		}
		if !done {
			if err := b.exportFileLocked(meta.Name); err != nil {
				return false, err
			}
			return true, nil
		}
	}
	return false, nil
}

// Capture reads immutable signed transactions, including forwarded origins.
func (b *Bridge) Capture() (int, error) {
	if b.config.Role != BridgeLowExporter {
		return 0, nil
	}
	b.gate.Lock()
	defer b.gate.Unlock()
	count := 0
	keys, err := b.backend.Keys([]byte("07/replication/log/"))
	if err != nil {
		return 0, err
	}
	for _, key := range keys {
		if count >= 4 {
			break
		}
		raw, err := b.backend.Get(key)
		if err != nil {
			return 0, err
		}
		if raw == nil {
			continue
		}
		tx, err := DecodeTransaction(raw)
		if err != nil {
			return 0, err
		}
		progressKey := []byte("09/bridge/exported/" + b.config.Stream + "/" + Hex(tx.Origin[:]))
		var previous uint64
		if praw, err := b.backend.Get(progressKey); err != nil {
			return 0, err
		} else if praw != nil {
			v, err := decodeOne(praw, func(d *postcard.Decoder) uint64 { return d.U64() })
			if err != nil {
				return 0, err
			}
			previous = v
		}
		if tx.Sequence <= previous {
			continue
		}
		digest := tx.Digest()
		if _, err := b.exportOrigin(raw, &Origin{Node: tx.Origin, Sequence: tx.Sequence, Transaction: tx.ID, Digest: digest}); err != nil {
			return 0, err
		}
		if err := b.backend.Commit([]Record{PutRecord(progressKey,
			encode(func(e *postcard.Encoder) { e.U64(tx.Sequence) }))}); err != nil {
			return 0, err
		}
		count++
	}
	if count < 4 {
		more, err := b.captureFileLocked()
		if err != nil {
			return 0, err
		}
		if more {
			count++
		}
	}
	return count, nil
}

// quarantine moves a rejected artifact to quarantine/ with a bounded
// Spool record. The record stores a short prefix plus digest and
// length: staging the full bytes would exceed Spool's value cap for
// oversized artifacts and wedge the importer retrying the same file
// forever. Reads stream in chunks so a hostile multi-gigabyte file
// cannot OOM the importer either. No reader consumes these records;
// the moved file is the quarantine signal.
func (b *Bridge) quarantine(path, reason string) error {
	file, err := os.Open(path)
	if err != nil {
		return ioErr(err)
	}
	defer file.Close()
	hash := sha256.New()
	var prefix []byte
	var length uint64
	buf := make([]byte, 64<<10)
	for {
		n, err := file.Read(buf)
		if n > 0 {
			hash.Write(buf[:n])
			length += uint64(n)
			if len(prefix) < 65536 {
				take := n
				if take > 65536-len(prefix) {
					take = 65536 - len(prefix)
				}
				prefix = append(prefix, buf[:take]...)
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return ioErr(err)
		}
	}
	sum := hash.Sum(nil)
	digest := Hex(sum)
	key := []byte("09/bridge/quarantine/" + digest)
	value := encode(func(e *postcard.Encoder) {
		e.String(reason)
		e.U64(length)
		e.String(digest)
		e.ByteString(prefix)
	})
	if err := b.backend.Commit([]Record{PutRecord(key, value)}); err != nil {
		return err
	}
	quarantine := filepath.Join(b.config.Directory, "quarantine")
	if err := os.MkdirAll(quarantine, 0o700); err != nil {
		return ioErr(err)
	}
	if name := filepath.Base(path); name != "" && name != "." && name != "/" {
		if err := os.Rename(path, filepath.Join(quarantine, name)); err != nil {
			return ioErr(err)
		}
	}
	return nil
}

// Process imports staged artifacts in sequence order.
func (b *Bridge) Process() (int, error) {
	if b.config.Role != BridgeHighReceiver {
		return 0, nil
	}
	b.gate.Lock()
	defer b.gate.Unlock()
	entries, err := os.ReadDir(b.config.Directory)
	if err != nil {
		return 0, ioErr(err)
	}
	var artifacts []string
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".bundle" {
			artifacts = append(artifacts, filepath.Join(b.config.Directory, e.Name()))
		}
	}
	sort.Strings(artifacts)
	if len(artifacts) > 256 {
		artifacts = artifacts[:256]
	}
	for _, path := range artifacts {
		if _, err := b.Stage(path); err != nil {
			if qerr := b.quarantine(path, err.Error()); qerr != nil {
				return 0, qerr
			}
		}
	}
	count := 0
	keys, err := b.backend.Keys([]byte("09/bridge/inbox/"))
	if err != nil {
		return 0, err
	}
	for _, key := range keys {
		if count >= 4 {
			break
		}
		raw, err := b.backend.Get(key)
		if err != nil {
			return 0, err
		}
		if raw == nil {
			continue
		}
		// Peek at the still-unverified envelope to skip already-applied or
		// gapped sequences before re-verification. Inbox bytes were
		// authenticated at stage time, so only the next sequence pays for
		// (or can fail) verification at apply time; a retired signer must
		// not wedge the importer.
		peek, err := DecodeBundle(raw)
		if err != nil {
			return 0, err
		}
		progressKey := []byte("09/bridge/applied/" + Hex(peek.Header.SourceDatabase[:]) + "/" + peek.Header.Stream)
		var applied uint64
		if praw, err := b.backend.Get(progressKey); err != nil {
			return 0, err
		} else if praw != nil {
			v, err := decodeOne(praw, func(d *postcard.Decoder) uint64 { return d.U64() })
			if err != nil {
				return 0, err
			}
			applied = v
		}
		if peek.Header.Sequence <= applied || peek.Header.Sequence != applied+1 {
			continue
		}
		header, payload, err := b.OpenBundle(raw)
		if err != nil {
			return 0, err
		}
		if header.Origin != nil {
			tx, err := DecodeTransaction(payload)
			if err != nil {
				return 0, err
			}
			if err := tx.Verify(header.SourceDatabase, b.config.TrustedOrigins, b.config.MaxPayloadBytes); err != nil {
				return 0, err
			}
			if tx.Origin != header.Origin.Node || tx.Sequence != header.Origin.Sequence ||
				tx.ID != header.Origin.Transaction || tx.Digest() != header.Origin.Digest {
				return 0, authErr("bridge origin proof mismatch")
			}
			payload = tx.Payload
		}
		markApplied := func() error {
			return b.backend.Commit([]Record{PutRecord(progressKey,
				encode(func(e *postcard.Encoder) { e.U64(header.Sequence) }))})
		}
		holdErr := func(err error) error {
			return b.backend.Commit([]Record{PutRecord([]byte("09/bridge/hold/"+Hex(header.Bundle[:])),
				encode(func(e *postcard.Encoder) { e.String(err.Error()) }))})
		}
		if bytes.HasPrefix(payload, bridgeFileMagic) {
			cleanup, err := b.processFilePart(header, payload)
			if err != nil {
				if herr := holdErr(err); herr != nil {
					return 0, herr
				}
				continue
			}
			if err := markApplied(); err != nil {
				return 0, err
			}
			if cleanup != "" {
				_ = os.Remove(cleanup)
			}
			count++
		} else {
			ok, err := b.backend.ImportBridge(*header, payload)
			if err != nil {
				// Identity collisions fail closed with a visible
				// error; everything else is held for retry.
				if errors.Is(err, ErrBridgeIdentityCollision) {
					return 0, err
				}
				if herr := holdErr(err); herr != nil {
					return 0, herr
				}
				continue
			}
			if ok {
				if err := markApplied(); err != nil {
					return 0, err
				}
				count++
			}
		}
	}
	return count, nil
}

func (b *Bridge) processFilePart(header *Header, payload []byte) (string, error) {
	b.filesMu.Lock()
	objects := b.files
	b.filesMu.Unlock()
	if objects == nil {
		return "", invalidErr("bridge file storage is not configured")
	}
	part, err := DecodeFilePart(payload[len(bridgeFileMagic):])
	if err != nil {
		return "", err
	}
	if part.Name == "" || len(part.Name) > 4096 || strings.ContainsRune(part.Name, 0) ||
		part.Bytes > uint64(b.config.StagingBytes) || part.Offset > part.Bytes ||
		part.FinalPart && (part.Offset != part.Bytes || len(part.Data) != 0) {
		return "", protocolErr("bridge file chunk bounds")
	}
	if end := part.Offset + uint64(len(part.Data)); end < part.Offset || end > part.Bytes {
		return "", protocolErr("bridge file chunk bounds")
	}
	transferRaw := encode(func(e *postcard.Encoder) {
		e.Raw(header.SourceDatabase[:])
		e.String(header.Stream)
		e.String(part.Name)
		e.Raw(part.Digest[:])
	})
	transferSum := sha256.Sum256(transferRaw)
	transfer := Hex(transferSum[:])
	path := filepath.Join(b.config.Directory, ".file-"+transfer+".part")
	receivedKey := []byte("09/bridge/file-received/" + transfer)
	if part.FinalPart {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			if raw, err := b.backend.Get(receivedKey); err != nil {
				return "", err
			} else if raw != nil {
				return "", nil
			}
		}
	}
	if _, err := os.Stat(path); os.IsNotExist(err) {
		f, err := privateFile(path)
		if err != nil {
			return "", err
		}
		f.Close()
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		return "", ioErr(err)
	}
	defer file.Close()
	st, err := file.Stat()
	if err != nil {
		return "", ioErr(err)
	}
	length := uint64(st.Size())
	var staged uint64
	entries, err := os.ReadDir(b.config.Directory)
	if err != nil {
		return "", ioErr(err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".file-") {
			if info, err := e.Info(); err != nil {
				return "", ioErr(err)
			} else {
				staged += uint64(info.Size())
			}
		}
	}
	var additional uint64
	if length == part.Offset {
		additional = uint64(len(part.Data))
	}
	if staged+additional < staged || staged+additional > uint64(b.config.StagingBytes) {
		return "", ErrOverloaded
	}
	if length == part.Offset {
		if _, err := file.Seek(int64(part.Offset), io.SeekStart); err != nil {
			return "", ioErr(err)
		}
		if _, err := file.Write(part.Data); err != nil {
			return "", ioErr(err)
		}
		if err := file.Sync(); err != nil {
			return "", ioErr(err)
		}
	} else if length >= part.Offset+uint64(len(part.Data)) {
		if _, err := file.Seek(int64(part.Offset), io.SeekStart); err != nil {
			return "", ioErr(err)
		}
		old := make([]byte, len(part.Data))
		if _, err := io.ReadFull(file, old); err != nil {
			return "", ioErr(err)
		}
		if !bytes.Equal(old, part.Data) {
			return "", authErr("conflicting bridge file chunk")
		}
	} else {
		return "", protocolErr("bridge file chunk offset")
	}
	if !part.FinalPart {
		return "", nil
	}
	st, err = file.Stat()
	if err != nil {
		return "", ioErr(err)
	}
	if uint64(st.Size()) != part.Bytes {
		return "", unavailableErr("incomplete bridge file")
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return "", ioErr(err)
	}
	hash := sha256.New()
	buf := make([]byte, 64<<10)
	for {
		n, err := file.Read(buf)
		if n > 0 {
			hash.Write(buf[:n])
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", ioErr(err)
		}
	}
	var sum [32]byte
	copy(sum[:], hash.Sum(nil))
	if sum != part.Digest {
		return "", authErr("bridge file digest")
	}
	file.Close()
	plain, err := os.Open(path)
	if err != nil {
		return "", ioErr(err)
	}
	uploaded, err := objects.UploadOne(part.Name, plain)
	plain.Close()
	if err != nil {
		return "", err
	}
	if uploaded.Digest != part.Digest || uploaded.Bytes != part.Bytes {
		return "", authErr("bridge file content mismatch")
	}
	if err := b.backend.Commit([]Record{PutRecord(receivedKey,
		encode(func(e *postcard.Encoder) {
			e.Raw(part.Digest[:])
			e.U64(part.Bytes)
		}))}); err != nil {
		return "", err
	}
	return path, nil
}

// PublishHTTP uploads one artifact over HTTP(S) PUT.
func (b *Bridge) PublishHTTP(path, url string) error {
	if b.config.Role != BridgeLowExporter || !(strings.HasPrefix(url, "https://") || strings.HasPrefix(url, "http://")) {
		return invalidErr("bridge export URL")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return ioErr(err)
	}
	if len(data) > b.config.MaxBundleBytes {
		return ErrOverloaded
	}
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(data))
	if err != nil {
		return unavailableErr("%v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return unavailableErr("%v", err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return unavailableErr("bridge export status %d", resp.StatusCode)
	}
	return nil
}

// PublishFTP uploads one artifact over FTP(S).
func (b *Bridge) PublishFTP(path, url string, tls *FtpOptions) error {
	if b.config.Role != BridgeLowExporter || !(strings.HasPrefix(url, "ftp://") || strings.HasPrefix(url, "ftps://")) {
		return invalidErr("bridge export URL")
	}
	name := filepath.Base(path)
	if name == "" || name == "." || name == "/" {
		return invalidErr("bridge artifact name")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return ioErr(err)
	}
	if len(data) == 0 {
		return invalidErr("bridge artifact has no payload")
	}
	if len(data) > b.config.MaxBundleBytes {
		return ErrOverloaded
	}
	return FtpUpload(url, name, data, tls)
}

func bridgeSigned(bundle *Bundle) []byte {
	sum := sha256.Sum256(bundle.Ciphertext)
	body := encode(func(e *postcard.Encoder) {
		e.U16(bundle.Version)
		encodeHeader(e, &bundle.Header)
		e.ByteString(sum[:])
	})
	return append(append([]byte(nil), []byte("murmur/sqlite-bridge/v1")...), body...)
}

func bridgeDerive(secret *[32]byte, aad []byte) [32]byte {
	var key [32]byte
	r := hkdf.New(sha256.New, secret[:], []byte("murmur/sqlite-bridge/v1"), aad)
	_, _ = io.ReadFull(r, key[:])
	return key
}

// RecipientPublicForBridge derives the X25519 public key of a seed.
func RecipientPublicForBridge(secret [32]byte) [32]byte {
	return RecipientPublic(&secret)
}
