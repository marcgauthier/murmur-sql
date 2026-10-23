package replication

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"math/bits"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/marcgauthier/murmur-sql-go/murmur/internal/postcard"
)

// Manifest describes one durable snapshot transfer.
type Manifest struct {
	Database  NodeId
	Donor     NodeId
	Recipient NodeId
	Transfer  NodeId
	Expires   uint64
	Bytes     uint64
	Digest    [32]byte
	Schema    [32]byte
	Progress  []Progress
	Chunks    [][32]byte
	Signature []byte
}

func encodeManifest(e *postcard.Encoder, m *Manifest) {
	e.Raw(m.Database[:])
	e.Raw(m.Donor[:])
	e.Raw(m.Recipient[:])
	e.Raw(m.Transfer[:])
	e.U64(m.Expires)
	e.U64(m.Bytes)
	e.Raw(m.Digest[:])
	e.Raw(m.Schema[:])
	e.Len(len(m.Progress))
	for i := range m.Progress {
		encodeProgress(e, &m.Progress[i])
	}
	e.Len(len(m.Chunks))
	for _, c := range m.Chunks {
		e.Raw(c[:])
	}
	e.ByteString(m.Signature)
}

func decodeManifest(d *postcard.Decoder) Manifest {
	var m Manifest
	copy(m.Database[:], d.Raw(16))
	copy(m.Donor[:], d.Raw(16))
	copy(m.Recipient[:], d.Raw(16))
	copy(m.Transfer[:], d.Raw(16))
	m.Expires = d.U64()
	m.Bytes = d.U64()
	copy(m.Digest[:], d.Raw(32))
	copy(m.Schema[:], d.Raw(32))
	for n := d.Len(); n > 0; n-- {
		m.Progress = append(m.Progress, decodeProgress(d))
	}
	for n := d.Len(); n > 0; n-- {
		var c [32]byte
		copy(c[:], d.Raw(32))
		m.Chunks = append(m.Chunks, c)
	}
	m.Signature = append([]byte(nil), d.Bytes()...)
	return m
}

// EncodeManifest serializes a manifest.
func EncodeManifest(m *Manifest) []byte {
	return encode(func(e *postcard.Encoder) { encodeManifest(e, m) })
}

// Pins is the snapshot pin prefix.
var Pins = []byte("07/replication/snapshot-v1/pins/")

// Pin freezes a pruning boundary for one recipient.
type Pin struct {
	Recipient NodeId
	Boundary  map[NodeId]uint64
	Target    map[NodeId]uint64
	HasTarget bool
	Expires   uint64
	Digest    [32]byte
}

func encodeIdMap(e *postcard.Encoder, m map[NodeId]uint64) {
	keys := make([]NodeId, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i][:], keys[j][:]) < 0 })
	e.Len(len(keys))
	for _, k := range keys {
		e.Raw(k[:])
		e.U64(m[k])
	}
}

func decodeIdMap(d *postcard.Decoder) map[NodeId]uint64 {
	out := make(map[NodeId]uint64)
	for n := d.Len(); n > 0; n-- {
		var k NodeId
		copy(k[:], d.Raw(16))
		out[k] = d.U64()
	}
	return out
}

func encodePin(e *postcard.Encoder, p *Pin) {
	e.Raw(p.Recipient[:])
	encodeIdMap(e, p.Boundary)
	if !p.HasTarget {
		e.None()
	} else {
		e.Some()
		encodeIdMap(e, p.Target)
	}
	e.U64(p.Expires)
	e.Raw(p.Digest[:])
}

func decodePin(d *postcard.Decoder) Pin {
	var p Pin
	copy(p.Recipient[:], d.Raw(16))
	p.Boundary = decodeIdMap(d)
	if d.Present() {
		p.HasTarget = true
		p.Target = decodeIdMap(d)
	}
	p.Expires = d.U64()
	copy(p.Digest[:], d.Raw(32))
	return p
}

// EncodePin serializes a pin.
func EncodePin(p *Pin) []byte { return encode(func(e *postcard.Encoder) { encodePin(e, p) }) }

// DecodePin parses a pin.
func DecodePin(b []byte) (Pin, error) { return decodeOne(b, decodePin) }

// Snapshot transfer prefixes.
var (
	SnapshotOut      = []byte("07/replication/snapshot-v1/out/")
	SnapshotIn       = []byte("07/replication/snapshot-v1/in/")
	SnapshotReceipts = []byte("07/replication/snapshot-v1/receipts/")
)

var snapshotDomain = []byte("murmur/sqlite-snapshot/v1")

// StreamMagic heads record-stream snapshots.
var StreamMagic = []byte("MURMUR-SNAPSHOT\x00\x02")

// Build grace constants for streaming snapshot builds.
const (
	BuildGraceMillis       uint64 = 3_600_000
	BuildStaleMillis       uint64 = 300_000
	BuildFreshMarginMillis uint64 = BuildGraceMillis - BuildStaleMillis
)

// Incoming is one staged inbound transfer.
type Incoming struct {
	Manifest Manifest
	Deadline uint64
}

func encodeIncoming(e *postcard.Encoder, in *Incoming) {
	encodeManifest(e, &in.Manifest)
	e.U64(in.Deadline)
}

func decodeIncoming(d *postcard.Decoder) Incoming {
	return Incoming{Manifest: decodeManifest(d), Deadline: d.U64()}
}

// EncodeIncoming serializes an incoming transfer.
func EncodeIncoming(in *Incoming) []byte {
	return encode(func(e *postcard.Encoder) { encodeIncoming(e, in) })
}

// DecodeIncoming parses an incoming transfer.
func DecodeIncoming(b []byte) (Incoming, error) { return decodeOne(b, decodeIncoming) }

// SnapshotHeaderKey renders a transfer header key.
func SnapshotHeaderKey(prefix []byte, transfer NodeId) []byte {
	return []byte(string(prefix) + Hex(transfer[:]) + "/header")
}

// SnapshotChunkKey renders a transfer chunk key.
func SnapshotChunkKey(prefix []byte, transfer NodeId, index uint32) []byte {
	return []byte(string(prefix) + Hex(transfer[:]) + "/chunk/" + zeroPad(uint64(index), 8))
}

// SnapshotPinKey renders a transfer pin key.
func SnapshotPinKey(transfer NodeId) []byte {
	return append(append([]byte(nil), Pins...), Hex(transfer[:])...)
}

// SnapshotReceiptKey renders a transfer receipt key.
func SnapshotReceiptKey(transfer NodeId) []byte {
	return append(append([]byte(nil), SnapshotReceipts...), Hex(transfer[:])...)
}

func snapshotInstallKey(transfer NodeId) []byte {
	return []byte(string(SnapshotIn) + Hex(transfer[:]) + "/install")
}

// SnapshotDeadline renders a transfer deadline from the configured lifetime.
func SnapshotDeadline(config *Config, now uint64) uint64 {
	lifetime := config.SnapshotTransferLifetime.Milliseconds()
	var ms uint64
	if lifetime > 0 {
		ms = uint64(lifetime)
	}
	if sum, carry := bits.Add64(now, ms, 0); carry == 0 {
		return sum
	}
	return ^uint64(0)
}

func (m *Manifest) signedBytes() []byte {
	body := encode(func(e *postcard.Encoder) {
		e.U16(Version)
		e.Raw(m.Database[:])
		e.Raw(m.Donor[:])
		e.Raw(m.Recipient[:])
		e.Raw(m.Transfer[:])
		e.U64(m.Expires)
		e.U64(m.Bytes)
		e.Raw(m.Digest[:])
		e.Raw(m.Schema[:])
		e.Len(len(m.Progress))
		for i := range m.Progress {
			encodeProgress(e, &m.Progress[i])
		}
		e.Len(len(m.Chunks))
		for _, c := range m.Chunks {
			e.Raw(c[:])
		}
	})
	return append(append([]byte(nil), snapshotDomain...), body...)
}

// Verify checks identity, trust, expiry, limits, progress, and signature.
func (m *Manifest) Verify(backend Backend, config *Config, source NodeId, now uint64) error {
	database, recipient := backend.Identity()
	if m.Database != database || m.Donor != source || m.Recipient != recipient ||
		!config.TrustedSnapshotSources[source] || m.Expires <= now {
		return authErr("snapshot identity, trust or expiry")
	}
	want := m.Bytes / uint64(ChunkBytes)
	if m.Bytes%uint64(ChunkBytes) != 0 {
		want++
	}
	if m.Bytes > uint64(config.SnapshotBytes) || m.Bytes > uint64(config.SnapshotStagingBytes) ||
		len(m.Progress) > 4096 || len(m.Chunks) != int(want) {
		return ErrOverloaded
	}
	origins := make(map[NodeId]bool)
	for _, p := range m.Progress {
		if origins[p.Origin] || p.Observed < p.Applied ||
			(p.RetainedFrom > p.RetainedThrough && p.RetainedThrough != 0) {
			return protocolErr("snapshot progress")
		}
		origins[p.Origin] = true
	}
	public, ok := config.TrustedOrigins[source]
	if !ok {
		return authErr("snapshot signer")
	}
	return Verify(&public, m.signedBytes(), m.Signature)
}

// Build captures an immutable snapshot transfer and its retention pin.
// Called inside the serialized capture operation.
func Build(identity [2]NodeId, recipient NodeId, data []byte, progress []Progress, schema [32]byte, config *Config, now uint64) (*Manifest, []Record, error) {
	if len(data) > config.SnapshotBytes || len(data) > config.SnapshotStagingBytes || len(progress) > 4096 {
		return nil, nil, ErrOverloaded
	}
	var transfer NodeId
	if _, err := rand.Read(transfer[:]); err != nil {
		return nil, nil, ioErr(err)
	}
	m := &Manifest{
		Database: identity[0], Donor: identity[1], Recipient: recipient, Transfer: transfer,
		Expires: SnapshotDeadline(config, now), Bytes: uint64(len(data)),
		Digest: sha256.Sum256(data), Schema: schema, Progress: progress,
	}
	for i := 0; i < len(data); i += ChunkBytes {
		end := i + ChunkBytes
		if end > len(data) {
			end = len(data)
		}
		m.Chunks = append(m.Chunks, sha256.Sum256(data[i:end]))
	}
	m.Signature = Sign(&config.SigningKey, m.signedBytes())
	pin := &Pin{Recipient: recipient, Boundary: make(map[NodeId]uint64),
		Expires: m.Expires, Digest: m.Digest}
	for _, p := range m.Progress {
		pin.Boundary[p.Origin] = p.Applied
	}
	var records []Record
	for i := range m.Chunks {
		start := i * ChunkBytes
		end := start + ChunkBytes
		if end > len(data) {
			end = len(data)
		}
		records = append(records, PutRecord(SnapshotChunkKey(SnapshotOut, transfer, uint32(i)), append([]byte(nil), data[start:end]...)))
	}
	records = append(records, PutRecord(SnapshotPinKey(transfer), EncodePin(pin)))
	records = append(records, PutRecord(SnapshotHeaderKey(SnapshotOut, transfer), EncodeManifest(m)))
	return m, records, nil
}

// BuildStream builds a record-stream snapshot without materializing its
// complete payload. next yields encoded Content pages until done. emit
// persists bounded groups; only the final group publishes the signed
// manifest and retention pin.
func BuildStream(identity [2]NodeId, recipient NodeId, next func() (page []byte, err error, done bool), progress []Progress, schema [32]byte, config *Config, now uint64, emit func([]Record) error) (*Manifest, error) {
	if len(progress) > 4096 {
		return nil, ErrOverloaded
	}
	var transfer NodeId
	if _, err := rand.Read(transfer[:]); err != nil {
		return nil, ioErr(err)
	}
	// Phase 1: preliminary pin+header. Chunkless by construction (a real
	// snapshot always carries at least the stream-magic chunk), so
	// receivers and reuse can tell it from a servable build.
	prelim := &Manifest{Database: identity[0], Donor: identity[1], Recipient: recipient,
		Transfer: transfer, Expires: satAdd(now, BuildGraceMillis), Schema: schema, Progress: progress}
	prelim.Signature = Sign(&config.SigningKey, prelim.signedBytes())
	prePin := &Pin{Recipient: recipient, Boundary: make(map[NodeId]uint64), Expires: prelim.Expires}
	for _, p := range progress {
		prePin.Boundary[p.Origin] = p.Applied
	}
	if err := emit([]Record{
		PutRecord(SnapshotPinKey(transfer), EncodePin(prePin)),
		PutRecord(SnapshotHeaderKey(SnapshotOut, transfer), EncodeManifest(prelim)),
	}); err != nil {
		return nil, err
	}
	var hashes [][32]byte
	digest := sha256.New()
	buffer := make([]byte, 0, ChunkBytes)
	var records []Record
	total := 0
	push := func(b []byte) error {
		if total += len(b); total < len(b) {
			return ErrOverloaded
		}
		if total > config.SnapshotBytes || total > config.SnapshotStagingBytes {
			return ErrOverloaded
		}
		digest.Write(b)
		rest := b
		for len(rest) > 0 {
			n := ChunkBytes - len(buffer)
			if n > len(rest) {
				n = len(rest)
			}
			buffer = append(buffer, rest[:n]...)
			rest = rest[n:]
			if len(buffer) == ChunkBytes {
				hashes = append(hashes, sha256.Sum256(buffer))
				records = append(records, PutRecord(SnapshotChunkKey(SnapshotOut, transfer, uint32(len(hashes)-1)), buffer))
				buffer = make([]byte, 0, ChunkBytes)
				if len(records) == 16 {
					take := records
					records = nil
					if err := emit(take); err != nil {
						return err
					}
				}
			}
		}
		return nil
	}
	if err := push(StreamMagic); err != nil {
		return nil, err
	}
	for {
		page, err, done := next()
		if err != nil {
			return nil, err
		}
		if done {
			break
		}
		if len(page) == 0 || len(page) > config.MaxTransactionBytes {
			return nil, ErrOverloaded
		}
		var lb [4]byte
		binary.BigEndian.PutUint32(lb[:], uint32(len(page)))
		if err := push(lb[:]); err != nil {
			return nil, err
		}
		if err := push(page); err != nil {
			return nil, err
		}
	}
	if len(buffer) > 0 {
		hashes = append(hashes, sha256.Sum256(buffer))
		records = append(records, PutRecord(SnapshotChunkKey(SnapshotOut, transfer, uint32(len(hashes)-1)), buffer))
	}
	var sum [32]byte
	copy(sum[:], digest.Sum(nil))
	manifest := &Manifest{Database: identity[0], Donor: identity[1], Recipient: recipient,
		Transfer: transfer, Expires: SnapshotDeadline(config, now), Bytes: uint64(total),
		Digest: sum, Schema: schema, Progress: progress, Chunks: hashes}
	manifest.Signature = Sign(&config.SigningKey, manifest.signedBytes())
	pin := &Pin{Recipient: recipient, Boundary: make(map[NodeId]uint64), Expires: manifest.Expires, Digest: manifest.Digest}
	for _, p := range manifest.Progress {
		pin.Boundary[p.Origin] = p.Applied
	}
	records = append(records, PutRecord(SnapshotPinKey(transfer), EncodePin(pin)))
	records = append(records, PutRecord(SnapshotHeaderKey(SnapshotOut, transfer), EncodeManifest(manifest)))
	if err := emit(records); err != nil {
		return nil, err
	}
	return manifest, nil
}

func satAdd(a, b uint64) uint64 {
	if sum, carry := bits.Add64(a, b, 0); carry == 0 {
		return sum
	}
	return ^uint64(0)
}

func streamRead(backend Backend, m *Manifest, offset uint64, length int) ([]byte, error) {
	end, carry := bits.Add64(offset, uint64(length), 0)
	if carry != 0 || end > m.Bytes {
		return nil, protocolErr("snapshot stream bounds")
	}
	out := make([]byte, 0, length)
	at := int(offset)
	for len(out) < length {
		index := at / ChunkBytes
		raw, err := backend.Get(SnapshotChunkKey(SnapshotIn, m.Transfer, uint32(index)))
		if err != nil {
			return nil, err
		}
		if raw == nil {
			return nil, unavailableErr("snapshot stream chunk missing")
		}
		if err := validateChunk(m, m.Donor, uint32(index), raw); err != nil {
			return nil, err
		}
		off := at % ChunkBytes
		n := length - len(out)
		if n > len(raw)-off {
			n = len(raw) - off
		}
		out = append(out, raw[off:off+n]...)
		at += n
	}
	return out, nil
}

// Outgoing reads a servable outbound transfer.
func Outgoing(backend Backend, recipient, transfer NodeId, now uint64) (*Manifest, error) {
	raw, err := backend.Get(SnapshotHeaderKey(SnapshotOut, transfer))
	if err != nil {
		return nil, err
	}
	if raw == nil {
		return nil, nil
	}
	m, err := DecodeManifest(raw)
	if err != nil {
		return nil, err
	}
	if m.Recipient != recipient {
		return nil, authErr("snapshot recipient")
	}
	schema, err := backend.Schema()
	if err != nil {
		return nil, err
	}
	if m.Schema != SchemaDigest(schema) {
		return nil, nil
	}
	pinRaw, err := backend.Get(SnapshotPinKey(transfer))
	if err != nil {
		return nil, err
	}
	var pin *Pin
	if pinRaw != nil {
		p, err := DecodePin(pinRaw)
		if err != nil {
			return nil, err
		}
		pin = &p
	}
	if m.Expires > now && pin != nil && !pin.HasTarget {
		return &m, nil
	}
	return nil, nil
}

// Cancel releases an unapplied transfer after explicit recipient
// abandonment. Once acknowledged, its catch-up obligation must instead
// be satisfied or expire.
func Cancel(backend Backend, recipient, transfer NodeId) error {
	raw, err := backend.Get(SnapshotPinKey(transfer))
	if err != nil {
		return err
	}
	if raw == nil {
		return nil
	}
	pin, err := DecodePin(raw)
	if err != nil {
		return err
	}
	if pin.Recipient != recipient {
		return authErr("snapshot cancellation recipient")
	}
	if pin.HasTarget {
		return nil
	}
	return backend.Commit([]Record{
		DeleteRecord(SnapshotPinKey(transfer)),
		DeleteRecord(SnapshotHeaderKey(SnapshotOut, transfer)),
	})
}

// historyDefinitivelyMissing reports whether a missing log is gone for
// good rather than merely ahead of this node.
func historyDefinitivelyMissing(backend Backend, origin NodeId, seq uint64) (bool, error) {
	seen := uint64(0)
	if raw, err := backend.Get(ProgressKey(origin)); err != nil {
		return false, err
	} else if raw != nil {
		v, err := decodeOne(raw, func(d *postcard.Decoder) uint64 { return d.U64() })
		if err != nil {
			return false, err
		}
		seen = v
	}
	if raw, err := backend.Get(RetainedKey(origin)); err != nil {
		return false, err
	} else if raw != nil {
		head, err := DecodeProgress(raw)
		if err != nil {
			return false, err
		}
		if head.Applied > seen {
			seen = head.Applied
		}
		if head.RetainedThrough > seen {
			seen = head.RetainedThrough
		}
	}
	return seq <= seen, nil
}

// ReadIncoming reads the staged inbound transfer, if any.
func ReadIncoming(backend Backend) (*Incoming, error) {
	keys, err := backend.Keys(SnapshotIn)
	if err != nil {
		return nil, err
	}
	for _, key := range keys {
		if !bytes.HasSuffix(key, []byte("/header")) {
			continue
		}
		raw, err := backend.Get(key)
		if err != nil {
			return nil, err
		}
		if raw != nil {
			in, err := DecodeIncoming(raw)
			if err != nil {
				return nil, err
			}
			return &in, nil
		}
	}
	return nil, nil
}

// Accept stages an inbound manifest.
func Accept(backend Backend, config *Config, source NodeId, m *Manifest, now uint64) error {
	if err := m.Verify(backend, config, source, now); err != nil {
		return err
	}
	// A chunkless manifest is a donor's in-progress build marker, never a
	// servable snapshot: refuse without prejudice so the requester retries
	// once the build finalizes.
	if len(m.Chunks) == 0 {
		return unavailableErr("snapshot build in progress")
	}
	if raw, err := backend.Get(SnapshotReceiptKey(m.Transfer)); err != nil {
		return err
	} else if raw != nil {
		done, err := DecodeIncoming(raw)
		if err != nil {
			return err
		}
		if !bytes.Equal(EncodeManifest(&done.Manifest), EncodeManifest(m)) {
			return authErr("conflicting snapshot receipt")
		}
		return nil
	}
	if old, err := ReadIncoming(backend); err != nil {
		return err
	} else if old != nil {
		if !bytes.Equal(EncodeManifest(&old.Manifest), EncodeManifest(m)) {
			return unavailableErr("another snapshot is staged")
		}
		return nil // Retransmissions cannot extend the locally persisted deadline.
	}
	receipts, err := backend.Keys(SnapshotReceipts)
	if err != nil {
		return err
	}
	if len(receipts) >= config.StagingTransfers {
		return ErrOverloaded
	}
	// Include leftover chunks awaiting bounded reclamation in the byte budget.
	used := m.Bytes
	keys, err := backend.Keys(SnapshotIn)
	if err != nil {
		return err
	}
	for _, key := range keys {
		if bytes.HasSuffix(key, []byte("/header")) {
			continue
		}
		raw, err := backend.Get(key)
		if err != nil {
			return err
		}
		if raw != nil {
			var n uint64
			if n = uint64(len(raw)); used+n < used {
				return ErrOverloaded
			} else {
				used += n
			}
		}
		if used > uint64(config.SnapshotStagingBytes) {
			return ErrOverloaded
		}
	}
	deadline := m.Expires
	if d := SnapshotDeadline(config, now); d < deadline {
		deadline = d
	}
	return backend.Commit([]Record{PutRecord(
		SnapshotHeaderKey(SnapshotIn, m.Transfer),
		EncodeIncoming(&Incoming{Manifest: *m, Deadline: deadline}),
	)})
}

// Receive stores one inbound chunk.
func Receive(backend Backend, source, transfer NodeId, index uint32, data []byte, now uint64) error {
	// Late identical chunks after durable completion are harmless.
	if raw, err := backend.Get(SnapshotReceiptKey(transfer)); err != nil {
		return err
	} else if raw != nil {
		receipt, err := DecodeIncoming(raw)
		if err != nil {
			return err
		}
		return validateChunk(&receipt.Manifest, source, index, data)
	}
	raw, err := backend.Get(SnapshotHeaderKey(SnapshotIn, transfer))
	if err != nil {
		return err
	}
	if raw == nil {
		return unavailableErr("snapshot manifest missing")
	}
	state, err := DecodeIncoming(raw)
	if err != nil {
		return err
	}
	if state.Deadline <= now {
		return unavailableErr("snapshot expired")
	}
	if err := validateChunk(&state.Manifest, source, index, data); err != nil {
		return err
	}
	key := SnapshotChunkKey(SnapshotIn, transfer, index)
	if old, err := backend.Get(key); err != nil {
		return err
	} else if old != nil {
		if !bytes.Equal(old, data) {
			return authErr("conflicting snapshot chunk")
		}
		return nil
	}
	return backend.Commit([]Record{PutRecord(key, data)})
}

func validateChunk(m *Manifest, source NodeId, index uint32, data []byte) error {
	i := int(index)
	if source != m.Donor || i >= len(m.Chunks) {
		return authErr("snapshot chunk identity")
	}
	expected := int(m.Bytes) - i*ChunkBytes
	if expected > ChunkBytes {
		expected = ChunkBytes
	}
	sum := sha256.Sum256(data)
	if len(data) != expected || sum != m.Chunks[i] {
		return authErr("snapshot chunk length or hash")
	}
	return nil
}

// Missing lists up to 128 missing chunk indexes using the key index only.
func Missing(backend Backend, state *Incoming) ([]uint32, error) {
	prefix := string(SnapshotIn) + Hex(state.Manifest.Transfer[:]) + "/chunk/"
	keys, err := backend.Keys([]byte(prefix))
	if err != nil {
		return nil, err
	}
	saved := make(map[string]bool, len(keys))
	for _, k := range keys {
		saved[string(k)] = true
	}
	var indexes []uint32
	for i := 0; i < len(state.Manifest.Chunks); i++ {
		if !saved[string(SnapshotChunkKey(SnapshotIn, state.Manifest.Transfer, uint32(i)))] {
			indexes = append(indexes, uint32(i))
			if len(indexes) == 128 {
				break
			}
		}
	}
	return indexes, nil
}

// Finish installs a complete transfer, returning false when deferred.
func Finish(backend Backend, config *Config, source, transfer NodeId, now uint64) (bool, error) {
	if raw, err := backend.Get(SnapshotReceiptKey(transfer)); err != nil {
		return false, err
	} else if raw != nil {
		r, err := DecodeIncoming(raw)
		if err != nil {
			return false, err
		}
		if r.Manifest.Donor != source {
			return false, authErr("snapshot receipt donor")
		}
		return true, nil
	}
	raw, err := backend.Get(SnapshotHeaderKey(SnapshotIn, transfer))
	if err != nil {
		return false, err
	}
	if raw == nil {
		return false, nil
	}
	r, err := DecodeIncoming(raw)
	if err != nil {
		return false, err
	}
	if err := r.Manifest.Verify(backend, config, source, now); err != nil {
		return false, err
	}
	if r.Deadline <= now {
		return false, nil
	}
	schema, err := backend.Schema()
	if err != nil {
		return false, err
	}
	if r.Manifest.Schema != SchemaDigest(schema) {
		return false, unavailableErr("snapshot schema mismatch")
	}
	missing, err := Missing(backend, &r)
	if err != nil {
		return false, err
	}
	if len(missing) > 0 {
		return false, nil
	}
	var install *uint64
	if raw, err := backend.Get(snapshotInstallKey(transfer)); err != nil {
		return false, err
	} else if raw != nil {
		v, err := decodeOne(raw, func(d *postcard.Decoder) uint64 { return d.U64() })
		if err != nil {
			return false, err
		}
		install = &v
	}
	streamed := false
	if r.Manifest.Bytes >= uint64(len(StreamMagic)) {
		head, err := streamRead(backend, &r.Manifest, 0, len(StreamMagic))
		if err != nil {
			return false, err
		}
		streamed = bytes.Equal(head, StreamMagic)
	}
	var data []byte
	if install == nil {
		digest := sha256.New()
		var total uint64
		for i := 0; i < len(r.Manifest.Chunks); i++ {
			raw, err := backend.Get(SnapshotChunkKey(SnapshotIn, transfer, uint32(i)))
			if err != nil {
				return false, err
			}
			if raw == nil {
				return false, nil
			}
			if err := validateChunk(&r.Manifest, source, uint32(i), raw); err != nil {
				return false, err
			}
			digest.Write(raw)
			total += uint64(len(raw))
			if !streamed {
				data = append(data, raw...)
			}
		}
		var sum [32]byte
		copy(sum[:], digest.Sum(nil))
		if total != r.Manifest.Bytes || sum != r.Manifest.Digest {
			return false, authErr("snapshot payload digest")
		}
	}
	if streamed {
		cursor := uint64(len(StreamMagic))
		if install != nil {
			cursor = *install
		}
		if cursor < uint64(len(StreamMagic)) || cursor > r.Manifest.Bytes {
			return false, protocolErr("snapshot installation cursor")
		}
		if cursor < r.Manifest.Bytes {
			// Batch install pages per apply call, like normal transaction
			// batches: every apply is one cursor race against concurrent
			// local writes, and installing page-by-page needs dozens of
			// races (usually lost under write load) where a few suffice.
			// A lost race commits nothing, so the next call re-reads from
			// the same cursor.
			var payloads [][]byte
			nbytes := 0
			for cursor < r.Manifest.Bytes && len(payloads) < 16 {
				lb, err := streamRead(backend, &r.Manifest, cursor, 4)
				if err != nil {
					return false, err
				}
				length := int(binary.BigEndian.Uint32(lb))
				if length == 0 || length > config.MaxTransactionBytes {
					return false, ErrOverloaded
				}
				if len(payloads) > 0 && nbytes+length > config.BatchBytes {
					break
				}
				nbytes += length
				page, err := streamRead(backend, &r.Manifest, cursor+4, length)
				if err != nil {
					return false, err
				}
				payloads = append(payloads, page)
				cursor += 4 + uint64(length)
			}
			cursorBytes := encode(func(e *postcard.Encoder) { e.U64(cursor) })
			ok, err := backend.Apply(payloads, []Record{PutRecord(snapshotInstallKey(transfer), cursorBytes)})
			if err != nil {
				return false, err
			}
			if !ok {
				return false, nil
			}
			if cursor < r.Manifest.Bytes {
				return false, nil
			}
		}
	}
	receipt := r
	receipt.Deadline = SnapshotDeadline(config, now)
	records := []Record{
		PutRecord(SnapshotReceiptKey(transfer), EncodeIncoming(&receipt)),
		DeleteRecord(SnapshotHeaderKey(SnapshotIn, transfer)),
		DeleteRecord(snapshotInstallKey(transfer)),
	}
	for _, p := range r.Manifest.Progress {
		old := uint64(0)
		if raw, err := backend.Get(ProgressKey(p.Origin)); err != nil {
			return false, err
		} else if raw != nil {
			v, err := decodeOne(raw, func(d *postcard.Decoder) uint64 { return d.U64() })
			if err != nil {
				return false, err
			}
			old = v
		}
		if p.Applied > old {
			old = p.Applied
		}
		records = append(records, PutRecord(ProgressKey(p.Origin),
			encode(func(e *postcard.Encoder) { e.U64(old) })))
	}
	if streamed {
		return backend.Apply(nil, records)
	}
	return backend.Apply([][]byte{data}, records)
}

// PruneLimit is the minimum allowed pruning boundary. Origins absent at
// capture must retain all newly received history while a transfer pin
// is live.
func PruneLimit(backend Backend, origin NodeId, now uint64) (uint64, error) {
	limit := ^uint64(0)
	keys, err := backend.Keys(Pins)
	if err != nil {
		return 0, err
	}
	for _, key := range keys {
		raw, err := backend.Get(key)
		if err != nil {
			return 0, err
		}
		if raw == nil {
			continue
		}
		pin, err := DecodePin(raw)
		if err != nil {
			return 0, err
		}
		if pin.Expires > now {
			if b, ok := pin.Boundary[origin]; ok {
				if b < limit {
					limit = b
				}
			} else {
				limit = 0
			}
		}
	}
	return limit, nil
}

// DecodeManifest parses a manifest.
func DecodeManifest(b []byte) (Manifest, error) { return decodeOne(b, decodeManifest) }

// Maintain runs bounded cleanup; deleting a pin requires expiry or
// persisted peer ACKs reaching its frozen target. A new manifest cannot
// overwrite this obligation.
func Maintain(backend Backend, now uint64) error {
	var records []Record
	pins, err := backend.Keys(Pins)
	if err != nil {
		return err
	}
	for _, key := range pins {
		raw, err := backend.Get(key)
		if err != nil {
			return err
		}
		if raw == nil {
			continue
		}
		pin, err := DecodePin(raw)
		if err != nil {
			return err
		}
		caughtUp := false
		if pin.HasTarget {
			peer, err := Peer(backend, pin.Recipient)
			if err != nil {
				return err
			}
			if peer != nil {
				caughtUp = true
				for o, s := range pin.Target {
					if peer.Acknowledged[o] < s {
						caughtUp = false
						break
					}
				}
			}
		}
		if pin.Expires <= now || caughtUp {
			records = append(records, DeleteRecord(key))
		}
		if len(records) == 256 {
			break
		}
	}
	if len(records) > 0 {
		if err := backend.Commit(records); err != nil {
			return err
		}
	}
	for _, prefix := range [][]byte{SnapshotIn, SnapshotOut} {
		keys, err := backend.Keys(prefix)
		if err != nil {
			return err
		}
		live := make(map[string]bool)
		records = nil
		for _, key := range keys {
			if !bytes.HasSuffix(key, []byte("/header")) {
				continue
			}
			raw, err := backend.Get(key)
			if err != nil {
				return err
			}
			if raw == nil {
				continue
			}
			var transfer NodeId
			expired := false
			if bytes.Equal(prefix, SnapshotIn) {
				r, err := DecodeIncoming(raw)
				if err != nil {
					return err
				}
				transfer = r.Manifest.Transfer
				expired = r.Deadline <= now
			} else {
				m, err := DecodeManifest(raw)
				if err != nil {
					return err
				}
				transfer = m.Transfer
				pinRaw, err := backend.Get(SnapshotPinKey(m.Transfer))
				if err != nil {
					return err
				}
				if pinRaw == nil {
					expired = true // Missing pin expires the header.
				} else {
					pin, err := DecodePin(pinRaw)
					if err != nil {
						return err
					}
					expired = m.Expires <= now || pin.HasTarget
				}
			}
			if expired && len(records) < 256 {
				records = append(records, DeleteRecord(key))
			} else {
				live[Hex(transfer[:])] = true
			}
		}
		for _, key := range keys {
			if bytes.HasSuffix(key, []byte("/header")) {
				continue
			}
			if len(records) >= 256 {
				break
			}
			if !utf8.Valid(key) {
				return protocolErr("snapshot chunk key")
			}
			id := ""
			if rest, ok := strings.CutPrefix(string(key), string(prefix)); ok {
				if i := strings.IndexByte(rest, '/'); i >= 0 {
					id = rest[:i]
				} else {
					id = rest
				}
			}
			if !live[id] {
				records = append(records, DeleteRecord(key))
			}
			if len(records) >= 256 {
				break
			}
		}
		if len(records) > 0 {
			if err := backend.Commit(records); err != nil {
				return err
			}
		}
	}
	records = nil
	receipts, err := backend.Keys(SnapshotReceipts)
	if err != nil {
		return err
	}
	for _, key := range receipts {
		raw, err := backend.Get(key)
		if err != nil {
			return err
		}
		if raw == nil {
			continue
		}
		r, err := DecodeIncoming(raw)
		if err != nil {
			return err
		}
		if r.Deadline <= now {
			records = append(records, DeleteRecord(key))
		}
		if len(records) == 256 {
			break
		}
	}
	if len(records) > 0 {
		if err := backend.Commit(records); err != nil {
			return err
		}
	}
	// Version 8 partial transfers have no durable sender identity to resume.
	oldKeys, err := backend.Keys([]byte("07/replication/snapshot/"))
	if err != nil {
		return err
	}
	var old []Record
	for i, k := range oldKeys {
		if i >= 256 {
			break
		}
		old = append(old, DeleteRecord(k))
	}
	if len(old) > 0 {
		if err := backend.Commit(old); err != nil {
			return err
		}
	}
	return nil
}

// reapForgottenPins drops transfer pins for forgotten recipients. A pin
// protects the log range its snapshot covers; when the recipient is
// retired, or has been unseen past the offline window with no live
// session, its transfer can never resume (its in-memory transfer state
// died with its session), so the pin would otherwise pin retention until
// pin expiry (default days) while the retention subsystem itself forgot
// the peer after maximum_offline. Recipients with no peer record are
// kept: conservative.
//
// A live recipient that already acknowledges the whole pinned boundary
// also releases a target-less pin: its live peer floor seamlessly takes
// over tail protection, so the pin only over-retains. Without this, a
// lost completion/cancel wedges the donor's retention until pin expiry.
func reapForgottenPins(backend Backend, now uint64, maximumOfflineMillis uint64, online map[NodeId]bool) error {
	var records []Record
	pins, err := backend.Keys(Pins)
	if err != nil {
		return err
	}
	for _, key := range pins {
		raw, err := backend.Get(key)
		if err != nil {
			return err
		}
		if raw == nil {
			continue
		}
		pin, err := DecodePin(raw)
		if err != nil {
			return err
		}
		peer, err := Peer(backend, pin.Recipient)
		if err != nil {
			return err
		}
		forgotten := false
		if peer != nil {
			forgotten = peer.Retired || (!online[pin.Recipient] &&
				satSub(now, peer.LastSeenMillis) > maximumOfflineMillis)
		}
		fulfilled := !pin.HasTarget && len(pin.Boundary) > 0 && peer != nil &&
			!peer.Retired && (online[pin.Recipient] ||
			satSub(now, peer.LastSeenMillis) <= maximumOfflineMillis)
		if fulfilled {
			for o, s := range pin.Boundary {
				if peer.Acknowledged[o] < s {
					fulfilled = false
					break
				}
			}
		}
		if (forgotten || fulfilled) && len(records) < 256 {
			records = append(records, DeleteRecord(key))
		}
		if len(records) >= 256 {
			break
		}
	}
	if len(records) > 0 {
		return backend.Commit(records)
	}
	return nil
}
