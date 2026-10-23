package replication

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"net/netip"

	"github.com/marcgauthier/murmur-sql-go/murmur/compression"
	"github.com/marcgauthier/murmur-sql-go/murmur/internal/postcard"
)

// Canonical SQLite replication profile. Deliberately distinct from RIME v6.
const (
	// Version is the wire protocol version.
	Version uint16 = 13
	// FrameBytes bounds one wire frame.
	FrameBytes = 8 << 20
	// ChunkBytes is the transfer chunk size.
	ChunkBytes = 64 << 10
	// CapSignedSchema advertises signed-schema support.
	CapSignedSchema uint64 = 1 << 7
	// Capabilities advertises all supported capabilities.
	Capabilities uint64 = (1 << 1) | (1 << 2) | (1 << 3) | (1 << 4) | (1 << 5) | (1 << 6) | CapSignedSchema
	// Required capabilities for peering.
	Required uint64 = (1 << 3) | (1 << 4) | (1 << 5) | CapSignedSchema
)

// ALPN is the TLS application protocol.
var ALPN = []byte("murmur-sqlite/1")

var (
	signDomain       = []byte("murmur/sqlite-origin-transaction/v1")
	schemaSignDomain = []byte("murmur/sqlite-schema/v1")
)

func encodeAddr(e *postcard.Encoder, a netip.AddrPort) {
	if a.Addr().Is4() {
		e.Discriminant(0)
		v4 := a.Addr().As4()
		e.Raw(v4[:])
	} else {
		e.Discriminant(1)
		v6 := a.Addr().As16()
		e.Raw(v6[:])
	}
	e.U16(a.Port())
}

func decodeAddr(d *postcard.Decoder) netip.AddrPort {
	switch d.Discriminant() {
	case 0:
		var v4 [4]byte
		copy(v4[:], d.Raw(4))
		return netip.AddrPortFrom(netip.AddrFrom4(v4), d.U16())
	case 1:
		var v6 [16]byte
		copy(v6[:], d.Raw(16))
		return netip.AddrPortFrom(netip.AddrFrom16(v6), d.U16())
	}
	d.Fail(protocolErr("bad socket address"))
	return netip.AddrPort{}
}

func encodeRecord(e *postcard.Encoder, r *Record) {
	e.ByteString(r.Key)
	if r.Value == nil {
		e.None()
	} else {
		e.Some()
		e.ByteString(r.Value)
	}
}

func decodeRecord(d *postcard.Decoder) Record {
	var r Record
	r.Key = append([]byte(nil), d.Bytes()...)
	if d.Present() {
		r.Value = append([]byte(nil), d.Bytes()...)
	}
	return r
}

// EncodeRecord serializes a record.
func EncodeRecord(r *Record) []byte { return encode(func(e *postcard.Encoder) { encodeRecord(e, r) }) }

// DecodeRecord parses a record.
func DecodeRecord(b []byte) (Record, error) { return decodeOne(b, decodeRecord) }

func encodeContent(e *postcard.Encoder, c *Content) {
	if c.SQLite == nil {
		e.None()
	} else {
		e.Some()
		e.ByteString(c.SQLite)
	}
	e.Len(len(c.Records))
	for i := range c.Records {
		encodeRecord(e, &c.Records[i])
	}
	e.Raw(c.Origin[:])
}

func decodeContent(d *postcard.Decoder) Content {
	var c Content
	if d.Present() {
		c.SQLite = append([]byte(nil), d.Bytes()...)
	}
	for n := d.Len(); n > 0; n-- {
		c.Records = append(c.Records, decodeRecord(d))
	}
	copy(c.Origin[:], d.Raw(16))
	return c
}

// EncodeContent serializes publishable content.
func EncodeContent(c *Content) []byte {
	return encode(func(e *postcard.Encoder) { encodeContent(e, c) })
}

// DecodeContent parses publishable content.
func DecodeContent(b []byte) (Content, error) { return decodeOne(b, decodeContent) }

// Transaction is one signed origin transaction.
type Transaction struct {
	Database      NodeId
	Origin        NodeId
	Sequence      uint64
	ID            NodeId
	Millis        uint64
	Logical       uint32
	Schema        [32]byte
	PayloadDigest [32]byte
	Payload       []byte
	Signature     []byte
}

func (t *Transaction) signedBytes() []byte {
	body := encode(func(e *postcard.Encoder) {
		e.Raw(t.Database[:])
		e.Raw(t.Origin[:])
		e.U64(t.Sequence)
		e.Raw(t.ID[:])
		e.U64(t.Millis)
		e.U32(t.Logical)
		e.Raw(t.Schema[:])
		e.Raw(t.PayloadDigest[:])
	})
	return append(append([]byte(nil), signDomain...), body...)
}

// Sign digests the payload and signs the envelope.
func (t *Transaction) Sign(seed *[32]byte) {
	t.PayloadDigest = sha256.Sum256(t.Payload)
	t.Signature = Sign(seed, t.signedBytes())
}

// Verify checks identity, size, payload digest, and signature.
func (t *Transaction) Verify(database NodeId, trusted map[NodeId][32]byte, max int) error {
	if t.Database != database || t.Sequence == 0 || len(t.Payload) > max {
		return protocolErr("transaction identity or size")
	}
	if t.PayloadDigest != sha256.Sum256(t.Payload) {
		return authErr("payload digest mismatch")
	}
	return t.VerifyHeader(database, trusted)
}

// VerifyHeader checks identity and signature only.
func (t *Transaction) VerifyHeader(database NodeId, trusted map[NodeId][32]byte) error {
	if t.Database != database || t.Sequence == 0 {
		return authErr("transaction identity")
	}
	public, ok := trusted[t.Origin]
	if !ok {
		return authErr("untrusted transaction origin")
	}
	return Verify(&public, t.signedBytes(), t.Signature)
}

// Digest hashes the encoded transaction.
func (t *Transaction) Digest() [32]byte {
	return sha256.Sum256(EncodeTransaction(t))
}

func encodeTransaction(e *postcard.Encoder, t *Transaction) {
	e.Raw(t.Database[:])
	e.Raw(t.Origin[:])
	e.U64(t.Sequence)
	e.Raw(t.ID[:])
	e.U64(t.Millis)
	e.U32(t.Logical)
	e.Raw(t.Schema[:])
	e.Raw(t.PayloadDigest[:])
	e.ByteString(t.Payload)
	e.ByteString(t.Signature)
}

func decodeTransaction(d *postcard.Decoder) Transaction {
	var t Transaction
	copy(t.Database[:], d.Raw(16))
	copy(t.Origin[:], d.Raw(16))
	t.Sequence = d.U64()
	copy(t.ID[:], d.Raw(16))
	t.Millis = d.U64()
	t.Logical = d.U32()
	copy(t.Schema[:], d.Raw(32))
	copy(t.PayloadDigest[:], d.Raw(32))
	t.Payload = append([]byte(nil), d.Bytes()...)
	t.Signature = append([]byte(nil), d.Bytes()...)
	return t
}

// EncodeTransaction serializes a transaction.
func EncodeTransaction(t *Transaction) []byte {
	return encode(func(e *postcard.Encoder) { encodeTransaction(e, t) })
}

// DecodeTransaction parses a transaction.
func DecodeTransaction(b []byte) (Transaction, error) { return decodeOne(b, decodeTransaction) }

// SignedSchema is a master-signed schema promise. Schema carries canonical
// replicated-subset bytes; local-only tables are excluded by the sender.
type SignedSchema struct {
	Database  NodeId
	Signer    NodeId
	Sequence  uint64
	Schema    []byte
	Signature []byte
}

func (s *SignedSchema) signedBytes() []byte {
	digest := sha256.Sum256(s.Schema)
	body := encode(func(e *postcard.Encoder) {
		e.Raw(s.Database[:])
		e.Raw(s.Signer[:])
		e.U64(s.Sequence)
		e.ByteString(digest[:])
	})
	return append(append([]byte(nil), schemaSignDomain...), body...)
}

// Sign signs the envelope.
func (s *SignedSchema) Sign(seed *[32]byte) {
	s.Signature = Sign(seed, s.signedBytes())
}

// Verify checks the envelope against one expected signer: the configured
// schema master, or in legacy mode the sender itself (any trusted origin).
func (s *SignedSchema) Verify(database, master NodeId, trusted map[NodeId][32]byte, max int) error {
	if s.Database != database || s.Signer != master || s.Sequence == 0 || len(s.Schema) > max {
		return authErr("schema envelope identity or size")
	}
	public, ok := trusted[s.Signer]
	if !ok {
		return authErr("untrusted schema signer")
	}
	return Verify(&public, s.signedBytes(), s.Signature)
}

// Digest hashes the schema bytes.
func (s *SignedSchema) Digest() [32]byte { return sha256.Sum256(s.Schema) }

func encodeSignedSchema(e *postcard.Encoder, s *SignedSchema) {
	e.Raw(s.Database[:])
	e.Raw(s.Signer[:])
	e.U64(s.Sequence)
	e.ByteString(s.Schema)
	e.ByteString(s.Signature)
}

func decodeSignedSchema(d *postcard.Decoder) SignedSchema {
	var s SignedSchema
	copy(s.Database[:], d.Raw(16))
	copy(s.Signer[:], d.Raw(16))
	s.Sequence = d.U64()
	s.Schema = append([]byte(nil), d.Bytes()...)
	s.Signature = append([]byte(nil), d.Bytes()...)
	return s
}

// EncodeSignedSchema serializes a signed schema.
func EncodeSignedSchema(s *SignedSchema) []byte {
	return encode(func(e *postcard.Encoder) { encodeSignedSchema(e, s) })
}

// DecodeSignedSchema parses a signed schema.
func DecodeSignedSchema(b []byte) (SignedSchema, error) { return decodeOne(b, decodeSignedSchema) }

// Chunk is one piece of a large transaction.
type Chunk struct {
	Proof    Transaction
	Origin   NodeId
	Sequence uint64
	ID       NodeId
	Index    uint32
	Count    uint32
	Total    uint64
	Digest   [32]byte
	Data     []byte
}

// ChunkManifest is the durable per-transfer chunk manifest. Stored on the
// first chunk after the proof signature verifies; later chunks bind to it
// by digest instead of re-verifying. Survives restarts, so repair can
// resume without the sender.
type ChunkManifest struct {
	ID     NodeId
	Count  uint32
	Total  uint64
	Digest [32]byte
}

// ChunkPrefix renders the chunk key prefix.
func ChunkPrefix(origin NodeId, seq uint64) string {
	return fmt.Sprintf("07/replication/chunks/%s/%020d/", Hex(origin[:]), seq)
}

// ChunkKey renders one chunk key.
func ChunkKey(origin NodeId, seq uint64, index uint32) []byte {
	return []byte(fmt.Sprintf("%s%08d", ChunkPrefix(origin, seq), index))
}

// ChunkManifestKey renders the manifest key.
func ChunkManifestKey(origin NodeId, seq uint64) []byte {
	return []byte(ChunkPrefix(origin, seq) + "manifest")
}

func encodeChunk(e *postcard.Encoder, c *Chunk) {
	encodeTransaction(e, &c.Proof)
	e.Raw(c.Origin[:])
	e.U64(c.Sequence)
	e.Raw(c.ID[:])
	e.U32(c.Index)
	e.U32(c.Count)
	e.U64(c.Total)
	e.Raw(c.Digest[:])
	e.ByteString(c.Data)
}

func decodeChunk(d *postcard.Decoder) Chunk {
	var c Chunk
	c.Proof = decodeTransaction(d)
	copy(c.Origin[:], d.Raw(16))
	c.Sequence = d.U64()
	copy(c.ID[:], d.Raw(16))
	c.Index = d.U32()
	c.Count = d.U32()
	c.Total = d.U64()
	copy(c.Digest[:], d.Raw(32))
	c.Data = append([]byte(nil), d.Bytes()...)
	return c
}

// EncodeChunk serializes a chunk.
func EncodeChunk(c *Chunk) []byte { return encode(func(e *postcard.Encoder) { encodeChunk(e, c) }) }

// DecodeChunk parses a chunk.
func DecodeChunk(b []byte) (Chunk, error) { return decodeOne(b, decodeChunk) }

func encodeChunkManifest(e *postcard.Encoder, m *ChunkManifest) {
	e.Raw(m.ID[:])
	e.U32(m.Count)
	e.U64(m.Total)
	e.Raw(m.Digest[:])
}

func decodeChunkManifest(d *postcard.Decoder) ChunkManifest {
	var m ChunkManifest
	copy(m.ID[:], d.Raw(16))
	m.Count = d.U32()
	m.Total = d.U64()
	copy(m.Digest[:], d.Raw(32))
	return m
}

// EncodeChunkManifest serializes a chunk manifest.
func EncodeChunkManifest(m *ChunkManifest) []byte {
	return encode(func(e *postcard.Encoder) { encodeChunkManifest(e, m) })
}

// DecodeChunkManifest parses a chunk manifest.
func DecodeChunkManifest(b []byte) (ChunkManifest, error) {
	return decodeOne(b, decodeChunkManifest)
}

// Progress is durable per-origin replication progress.
type Progress struct {
	Origin          NodeId
	Applied         uint64
	Observed        uint64
	RetainedFrom    uint64
	RetainedThrough uint64
}

func encodeProgress(e *postcard.Encoder, p *Progress) {
	e.Raw(p.Origin[:])
	e.U64(p.Applied)
	e.U64(p.Observed)
	e.U64(p.RetainedFrom)
	e.U64(p.RetainedThrough)
}

func decodeProgress(d *postcard.Decoder) Progress {
	var p Progress
	copy(p.Origin[:], d.Raw(16))
	p.Applied = d.U64()
	p.Observed = d.U64()
	p.RetainedFrom = d.U64()
	p.RetainedThrough = d.U64()
	return p
}

// EncodeProgress serializes progress.
func EncodeProgress(p *Progress) []byte {
	return encode(func(e *postcard.Encoder) { encodeProgress(e, p) })
}

// DecodeProgress parses progress.
func DecodeProgress(b []byte) (Progress, error) { return decodeOne(b, decodeProgress) }

// MsgKind classifies a Message in wire declaration order.
type MsgKind int

const (
	MsgHello MsgKind = iota
	MsgWelcome
	MsgBatches
	MsgAck
	MsgNeed
	MsgSnapshotRequest
	MsgSnapshotManifest
	MsgSnapshotChunk
	MsgSnapshotDone
	MsgSnapshotResume
	MsgSnapshotNeed
	MsgSnapshotApplied
	MsgSnapshotUnavailable
	MsgSnapshotCancel
	MsgPing
	MsgPong
	MsgError
	MsgSchemaRequest
	MsgSchemaManifest
	MsgSchemaAck
	MsgPlumtreeData
	MsgIHave
	MsgPrune
	MsgGraft
	MsgProgressRequest
	MsgProgressPage
	MsgTransactionChunk
	MsgMembersRequest
	MsgMembersPage
	MsgChunkNeed
	MsgChunkAvailabilityRequest
	MsgChunkAvailabilityPage
)

// ChunkAvailability advertises available chunk indexes.
type ChunkAvailability struct {
	Origin   NodeId
	Sequence uint64
	Indexes  []uint32
}

// Message is one wire message. Only the fields of Kind are valid.
type Message struct {
	Kind           MsgKind
	Capabilities   uint64
	Required       uint64
	Version        uint16
	Database       NodeId
	Node           NodeId
	Fetch          *netip.AddrPort
	Schema         []byte
	Mode           Dissemination
	MaxTransaction uint64
	Transactions   []Transaction
	Ack            []Progress
	Origin         NodeId
	From           uint64
	Through        uint64
	Manifest       Manifest
	Transfer       NodeId
	Index          uint32
	Data           []byte
	Indexes        []uint32
	Digest         [32]byte
	Sequence       uint64
	Nonce          uint64
	Code           uint16
	ErrMessage     string
	Signed         SignedSchema
	Tx             Transaction
	Offset         uint32
	More           bool
	Entries        []Progress
	Chunk          Chunk
	Members        []Member
	Availability   []ChunkAvailability
}

// FrameKind renders the wire frame type, distinct from declaration order.
func (m *Message) FrameKind() uint16 {
	switch m.Kind {
	case MsgHello:
		return 1
	case MsgWelcome:
		return 2
	case MsgBatches:
		return 3
	case MsgAck:
		return 4
	case MsgNeed:
		return 5
	case MsgSnapshotRequest:
		return 6
	case MsgSnapshotManifest:
		return 7
	case MsgSnapshotChunk:
		return 8
	case MsgSnapshotDone:
		return 9
	case MsgPing:
		return 10
	case MsgPong:
		return 11
	case MsgError:
		return 12
	case MsgSchemaRequest:
		return 13
	case MsgSchemaManifest:
		return 14
	case MsgSchemaAck:
		return 15
	case MsgPlumtreeData:
		return 16
	case MsgIHave:
		return 17
	case MsgPrune:
		return 18
	case MsgGraft:
		return 19
	case MsgProgressRequest:
		return 20
	case MsgProgressPage:
		return 21
	case MsgTransactionChunk:
		return 22
	case MsgChunkNeed:
		return 23
	case MsgChunkAvailabilityRequest:
		return 24
	case MsgChunkAvailabilityPage:
		return 25
	case MsgMembersRequest:
		return 26
	case MsgMembersPage:
		return 27
	case MsgSnapshotResume:
		return 28
	case MsgSnapshotNeed:
		return 29
	case MsgSnapshotApplied:
		return 30
	case MsgSnapshotUnavailable:
		return 31
	case MsgSnapshotCancel:
		return 32
	}
	return 0
}

func encodeMessage(e *postcard.Encoder, m *Message) {
	e.Discriminant(uint32(m.Kind))
	switch m.Kind {
	case MsgHello:
		e.U64(m.Capabilities)
		e.U64(m.Required)
		e.U16(m.Version)
		e.Raw(m.Database[:])
		e.Raw(m.Node[:])
		if m.Fetch == nil {
			e.None()
		} else {
			e.Some()
			encodeAddr(e, *m.Fetch)
		}
		e.ByteString(m.Schema)
		e.Discriminant(uint32(m.Mode))
		e.U64(m.MaxTransaction)
	case MsgWelcome:
		e.U64(m.Capabilities)
		e.U64(m.Required)
		e.Raw(m.Database[:])
		e.Raw(m.Node[:])
		if m.Fetch == nil {
			e.None()
		} else {
			e.Some()
			encodeAddr(e, *m.Fetch)
		}
		e.ByteString(m.Schema)
		e.Discriminant(uint32(m.Mode))
	case MsgBatches:
		e.Len(len(m.Transactions))
		for i := range m.Transactions {
			encodeTransaction(e, &m.Transactions[i])
		}
	case MsgAck:
		e.Len(len(m.Ack))
		for i := range m.Ack {
			encodeProgress(e, &m.Ack[i])
		}
	case MsgNeed:
		e.Raw(m.Origin[:])
		e.U64(m.From)
		e.U64(m.Through)
	case MsgSnapshotRequest, MsgPrune, MsgSchemaRequest:
	case MsgSnapshotManifest:
		encodeManifest(e, &m.Manifest)
	case MsgSnapshotChunk:
		e.Raw(m.Transfer[:])
		e.U32(m.Index)
		e.ByteString(m.Data)
	case MsgSnapshotDone, MsgSnapshotResume, MsgSnapshotUnavailable, MsgSnapshotCancel:
		e.Raw(m.Transfer[:])
	case MsgSnapshotNeed:
		e.Raw(m.Transfer[:])
		e.Len(len(m.Indexes))
		for _, x := range m.Indexes {
			e.U32(x)
		}
	case MsgChunkNeed:
		e.Raw(m.Origin[:])
		e.U64(m.Sequence)
		e.Len(len(m.Indexes))
		for _, x := range m.Indexes {
			e.U32(x)
		}
	case MsgSnapshotApplied:
		e.Raw(m.Transfer[:])
		e.Raw(m.Digest[:])
	case MsgPing, MsgPong:
		e.U64(m.Nonce)
	case MsgError:
		e.U16(m.Code)
		e.String(m.ErrMessage)
	case MsgSchemaManifest:
		encodeSignedSchema(e, &m.Signed)
	case MsgSchemaAck:
		e.Raw(m.Digest[:])
		e.U64(m.Sequence)
	case MsgPlumtreeData:
		encodeTransaction(e, &m.Tx)
	case MsgIHave, MsgGraft:
		e.Raw(m.Origin[:])
		e.U64(m.Sequence)
	case MsgProgressRequest, MsgMembersRequest, MsgChunkAvailabilityRequest:
		e.U32(m.Offset)
	case MsgProgressPage:
		e.U32(m.Offset)
		e.Bool(m.More)
		e.Len(len(m.Entries))
		for i := range m.Entries {
			encodeProgress(e, &m.Entries[i])
		}
	case MsgTransactionChunk:
		encodeChunk(e, &m.Chunk)
	case MsgMembersPage:
		e.U32(m.Offset)
		e.Bool(m.More)
		e.Len(len(m.Members))
		for i := range m.Members {
			encodeMember(e, &m.Members[i])
		}
	case MsgChunkAvailabilityPage:
		e.Len(len(m.Availability))
		for i := range m.Availability {
			a := &m.Availability[i]
			e.Raw(a.Origin[:])
			e.U64(a.Sequence)
			e.Len(len(a.Indexes))
			for _, x := range a.Indexes {
				e.U32(x)
			}
		}
		e.Bool(m.More)
	}
}

func decodeMessage(d *postcard.Decoder) Message {
	var m Message
	disc := d.Discriminant()
	if disc > uint32(MsgChunkAvailabilityPage) {
		d.Fail(protocolErr("bad message kind"))
		return m
	}
	m.Kind = MsgKind(disc)
	switch m.Kind {
	case MsgHello:
		m.Capabilities = d.U64()
		m.Required = d.U64()
		m.Version = d.U16()
		copy(m.Database[:], d.Raw(16))
		copy(m.Node[:], d.Raw(16))
		if d.Present() {
			a := decodeAddr(d)
			m.Fetch = &a
		}
		m.Schema = append([]byte(nil), d.Bytes()...)
		switch d.Discriminant() {
		case 0:
			m.Mode = DisseminationGossip
		case 1:
			m.Mode = DisseminationPlumtree
		default:
			d.Fail(protocolErr("bad dissemination"))
		}
		m.MaxTransaction = d.U64()
	case MsgWelcome:
		m.Capabilities = d.U64()
		m.Required = d.U64()
		copy(m.Database[:], d.Raw(16))
		copy(m.Node[:], d.Raw(16))
		if d.Present() {
			a := decodeAddr(d)
			m.Fetch = &a
		}
		m.Schema = append([]byte(nil), d.Bytes()...)
		switch d.Discriminant() {
		case 0:
			m.Mode = DisseminationGossip
		case 1:
			m.Mode = DisseminationPlumtree
		default:
			d.Fail(protocolErr("bad dissemination"))
		}
	case MsgBatches:
		for n := d.Len(); n > 0; n-- {
			m.Transactions = append(m.Transactions, decodeTransaction(d))
		}
	case MsgAck:
		for n := d.Len(); n > 0; n-- {
			m.Ack = append(m.Ack, decodeProgress(d))
		}
	case MsgNeed:
		copy(m.Origin[:], d.Raw(16))
		m.From = d.U64()
		m.Through = d.U64()
	case MsgSnapshotRequest, MsgPrune, MsgSchemaRequest:
	case MsgSnapshotManifest:
		m.Manifest = decodeManifest(d)
	case MsgSnapshotChunk:
		copy(m.Transfer[:], d.Raw(16))
		m.Index = d.U32()
		m.Data = append([]byte(nil), d.Bytes()...)
	case MsgSnapshotDone, MsgSnapshotResume, MsgSnapshotUnavailable, MsgSnapshotCancel:
		copy(m.Transfer[:], d.Raw(16))
	case MsgSnapshotNeed:
		copy(m.Transfer[:], d.Raw(16))
		for n := d.Len(); n > 0; n-- {
			m.Indexes = append(m.Indexes, d.U32())
		}
	case MsgChunkNeed:
		copy(m.Origin[:], d.Raw(16))
		m.Sequence = d.U64()
		for n := d.Len(); n > 0; n-- {
			m.Indexes = append(m.Indexes, d.U32())
		}
	case MsgSnapshotApplied:
		copy(m.Transfer[:], d.Raw(16))
		copy(m.Digest[:], d.Raw(32))
	case MsgPing, MsgPong:
		m.Nonce = d.U64()
	case MsgError:
		m.Code = d.U16()
		m.ErrMessage = d.String()
	case MsgSchemaManifest:
		m.Signed = decodeSignedSchema(d)
	case MsgSchemaAck:
		copy(m.Digest[:], d.Raw(32))
		m.Sequence = d.U64()
	case MsgPlumtreeData:
		m.Tx = decodeTransaction(d)
	case MsgIHave, MsgGraft:
		copy(m.Origin[:], d.Raw(16))
		m.Sequence = d.U64()
	case MsgProgressRequest, MsgMembersRequest, MsgChunkAvailabilityRequest:
		m.Offset = d.U32()
	case MsgProgressPage:
		m.Offset = d.U32()
		m.More = d.Bool()
		for n := d.Len(); n > 0; n-- {
			m.Entries = append(m.Entries, decodeProgress(d))
		}
	case MsgTransactionChunk:
		m.Chunk = decodeChunk(d)
	case MsgMembersPage:
		m.Offset = d.U32()
		m.More = d.Bool()
		for n := d.Len(); n > 0; n-- {
			m.Members = append(m.Members, decodeMember(d))
		}
	case MsgChunkAvailabilityPage:
		for n := d.Len(); n > 0; n-- {
			var a ChunkAvailability
			copy(a.Origin[:], d.Raw(16))
			a.Sequence = d.U64()
			for k := d.Len(); k > 0; k-- {
				a.Indexes = append(a.Indexes, d.U32())
			}
			m.Availability = append(m.Availability, a)
		}
		m.More = d.Bool()
	}
	return m
}

// EncodeMessage serializes a message.
func EncodeMessage(m *Message) []byte {
	return encode(func(e *postcard.Encoder) { encodeMessage(e, m) })
}

// DecodeMessage parses a message.
func DecodeMessage(b []byte) (Message, error) { return decodeOne(b, decodeMessage) }

// Prepared is a reusable immutable wire frame. Queue accounting uses
// the uncompressed size.
type Prepared struct {
	Bytes  []byte
	Charge int
}

var frameZstd = compression.MustZstd(3)

// NewPrepared frames one message, compressing large payloads.
func NewPrepared(m *Message) (*Prepared, error) {
	data := EncodeMessage(m)
	if len(data) > FrameBytes {
		return nil, ErrOverloaded
	}
	charge := len(data) + 12
	flags := uint16(0)
	if len(data) > 1024 {
		compressed, err := frameZstd.Compress(nil, data)
		if err != nil {
			return nil, protocolErr("%v", err)
		}
		if len(compressed)+4 < len(data) {
			buf := make([]byte, 4, 4+len(compressed))
			binary.BigEndian.PutUint32(buf, uint32(len(data)))
			data = append(buf, compressed...)
			flags = 0x8002
		}
	}
	out := make([]byte, 12, 12+len(data))
	copy(out[:2], "RD")
	binary.BigEndian.PutUint16(out[2:4], Version)
	binary.BigEndian.PutUint16(out[4:6], m.FrameKind())
	binary.BigEndian.PutUint16(out[6:8], flags)
	binary.BigEndian.PutUint32(out[8:12], uint32(len(data)))
	out = append(out, data...)
	return &Prepared{Bytes: out, Charge: charge}, nil
}

// Write sends the frame.
func (p *Prepared) Write(w io.Writer) error {
	_, err := w.Write(p.Bytes)
	if err != nil {
		return protocolErr("%v", err)
	}
	return nil
}

// WriteMessage frames and sends one message.
func WriteMessage(w io.Writer, m *Message) error {
	p, err := NewPrepared(m)
	if err != nil {
		return err
	}
	return p.Write(w)
}

// ReadMessage reads one framed message.
func ReadMessage(r io.Reader) (Message, error) {
	var m Message
	h := make([]byte, 12)
	if _, err := io.ReadFull(r, h); err != nil {
		return m, protocolErr("%v", err)
	}
	n := int(binary.BigEndian.Uint32(h[8:12]))
	if string(h[:2]) != "RD" || binary.BigEndian.Uint16(h[2:4]) != Version ||
		(binary.BigEndian.Uint16(h[6:8]) != 0 && binary.BigEndian.Uint16(h[6:8]) != 0x8002) ||
		n > FrameBytes {
		return m, protocolErr("invalid frame header")
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(r, b); err != nil {
		return m, protocolErr("%v", err)
	}
	if binary.BigEndian.Uint16(h[6:8]) == 0x8002 {
		if len(b) < 4 {
			return m, protocolErr("compressed frame length")
		}
		expected := int(binary.BigEndian.Uint32(b[:4]))
		if expected > FrameBytes {
			return m, ErrOverloaded
		}
		plain, err := frameZstd.Decompress(nil, b[4:], expected+1)
		if err != nil || len(plain) != expected {
			return m, protocolErr("decompressed frame size")
		}
		b = plain
	}
	m, err := DecodeMessage(b)
	if err != nil {
		return m, err
	}
	if m.FrameKind() != binary.BigEndian.Uint16(h[4:6]) {
		return m, protocolErr("frame type mismatch")
	}
	return m, nil
}

// LogKey renders a retained transaction key.
func LogKey(origin NodeId, seq uint64) []byte {
	return []byte(fmt.Sprintf("07/replication/log/%s/%020d", Hex(origin[:]), seq))
}

// StageKey renders a staged transaction key.
func StageKey(origin NodeId, seq uint64) []byte {
	return []byte(fmt.Sprintf("07/replication/staged/%s/%020d", Hex(origin[:]), seq))
}

// ProgressKey renders an applied-progress key.
func ProgressKey(origin NodeId) []byte {
	return []byte("07/replication/applied/" + Hex(origin[:]))
}

// ReceiptKey renders a receipt key.
func ReceiptKey(origin NodeId, seq uint64) []byte {
	return []byte(fmt.Sprintf("07/replication/receipt/%s/%020d", Hex(origin[:]), seq))
}

// SequenceKey renders an origin sequence key.
func SequenceKey(origin NodeId) []byte {
	return []byte("07/replication/sequence/" + Hex(origin[:]))
}

// SchemaManifestKey renders the node-local outbound manifest cursor key.
func SchemaManifestKey() []byte { return []byte("07/replication/schema/manifest") }

// SchemaAcceptedKey renders the per-signer accepted-schema key.
func SchemaAcceptedKey(signer NodeId) []byte {
	return []byte("07/replication/schema/accepted/" + Hex(signer[:]))
}

// SchemaDigest hashes schema bytes.
func SchemaDigest(b []byte) [32]byte { return sha256.Sum256(b) }

// ObservedKey renders an observed-progress key.
func ObservedKey(origin NodeId) []byte {
	return []byte("07/replication/observed/" + Hex(origin[:]))
}

// RetainedKey renders a retained-progress key.
func RetainedKey(origin NodeId) []byte {
	return []byte("07/replication/retained/" + Hex(origin[:]))
}
