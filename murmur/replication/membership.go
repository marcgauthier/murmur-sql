package replication

import (
	"math/rand/v2"
	"net/netip"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/internal/postcard"
)

// MemberState is bounded SWIM state.
type MemberState int

const (
	MemberAlive MemberState = iota
	MemberSuspect
	MemberDead
	MemberLeft
)

// Member is one cluster member. Relayed identities are discovery
// hints, never credentials.
type Member struct {
	Node        NodeId
	Address     netip.AddrPort
	Incarnation uint64
	State       MemberState
}

// PacketKind classifies a membership packet.
type PacketKind int

const (
	PacketGossip PacketKind = iota
	PacketIndirectAck
	PacketPing
	PacketAck
	PacketPingRequest
)

// Packet is one SWIM message.
type Packet struct {
	Kind     PacketKind
	Database NodeId
	Sender   NodeId
	Target   NodeId
	Nonce    uint64
	Members  []Member
}

// View is the local membership table.
type View struct {
	Members   map[NodeId]Member
	Local     Member
	Excluded  map[NodeId]bool
	Pending   map[NodeId]pendingProbe
	Suspect   map[NodeId]time.Time
	Awareness uint32
}

type pendingProbe struct {
	At      time.Time
	Nonce   uint64
	Proxied bool
}

// NewView builds a membership view around one local member.
func NewView(local Member) *View {
	return &View{
		Members:  make(map[NodeId]Member),
		Local:    local,
		Excluded: make(map[NodeId]bool),
		Pending:  make(map[NodeId]pendingProbe),
		Suspect:  make(map[NodeId]time.Time),
	}
}

// Merge folds one relayed member into the view.
func (v *View) Merge(member Member) {
	if member.Node == v.Local.Node {
		if member.Incarnation >= v.Local.Incarnation && member.State != MemberAlive {
			v.Local.Incarnation = member.Incarnation + 1
		}
		return
	}
	if _, ok := v.Members[member.Node]; v.Excluded[member.Node] || len(v.Members) >= 4096 && !ok {
		return
	}
	rank := func(s MemberState) int {
		switch s {
		case MemberAlive:
			return 0
		case MemberSuspect:
			return 1
		case MemberDead:
			return 2
		}
		return 3
	}
	if old, ok := v.Members[member.Node]; !ok ||
		member.Incarnation > old.Incarnation ||
		member.Incarnation == old.Incarnation && rank(member.State) > rank(old.State) {
		if member.State == MemberAlive {
			delete(v.Suspect, member.Node)
		}
		v.Members[member.Node] = member
	}
}

// Gossip returns the local member plus up to three random peers.
func (v *View) Gossip() []Member {
	out := []Member{v.Local}
	keys := make([]NodeId, 0, len(v.Members))
	for k := range v.Members {
		keys = append(keys, k)
	}
	rand.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
	for i := 0; i < len(keys) && i < 3; i++ {
		out = append(out, v.Members[keys[i]])
	}
	return out
}

// Acknowledged clears a matching pending probe.
func (v *View) Acknowledged(node NodeId, nonce uint64) {
	if p, ok := v.Pending[node]; ok && p.Nonce == nonce {
		delete(v.Pending, node)
		if v.Awareness > 0 {
			v.Awareness--
		}
	}
}

// IndirectProbe asks peers to probe a member on our behalf.
type IndirectProbe struct {
	Node  NodeId
	Nonce uint64
}

// Expire times out pending probes, returning indirect-probe requests,
// and promotes long-suspect members to dead.
func (v *View) Expire() []IndirectProbe {
	now := time.Now()
	timeout := time.Duration(500*(uint64(v.Awareness)+1)) * time.Millisecond
	var lost []NodeId
	for id, p := range v.Pending {
		if now.Sub(p.At) >= timeout {
			lost = append(lost, id)
		}
	}
	var indirect []IndirectProbe
	for _, id := range lost {
		p := v.Pending[id]
		if !p.Proxied {
			p.At = now
			p.Proxied = true
			v.Pending[id] = p
			indirect = append(indirect, IndirectProbe{Node: id, Nonce: p.Nonce})
		} else {
			delete(v.Pending, id)
			if m, ok := v.Members[id]; ok {
				m.State = MemberSuspect
				v.Members[id] = m
			}
			if _, ok := v.Suspect[id]; !ok {
				v.Suspect[id] = now
			}
			if v.Awareness < 8 {
				v.Awareness++
			}
		}
	}
	expiry := time.Duration(5*(uint64(v.Awareness)+1)) * time.Second
	var dead []NodeId
	for id, at := range v.Suspect {
		if now.Sub(at) >= expiry {
			dead = append(dead, id)
		}
	}
	for _, id := range dead {
		if m, ok := v.Members[id]; ok {
			m.State = MemberDead
			v.Members[id] = m
		}
		delete(v.Suspect, id)
	}
	return indirect
}

func encodeMemberState(e *postcard.Encoder, s MemberState) { e.Discriminant(uint32(s)) }

func decodeMemberState(d *postcard.Decoder) MemberState {
	switch d.Discriminant() {
	case 0:
		return MemberAlive
	case 1:
		return MemberSuspect
	case 2:
		return MemberDead
	case 3:
		return MemberLeft
	}
	d.Fail(protocolErr("bad member state"))
	return MemberAlive
}

func encodeMember(e *postcard.Encoder, m *Member) {
	e.Raw(m.Node[:])
	encodeAddr(e, m.Address)
	e.U64(m.Incarnation)
	encodeMemberState(e, m.State)
}

func decodeMember(d *postcard.Decoder) Member {
	var m Member
	copy(m.Node[:], d.Raw(16))
	m.Address = decodeAddr(d)
	m.Incarnation = d.U64()
	m.State = decodeMemberState(d)
	return m
}

// EncodeMember serializes a member.
func EncodeMember(m *Member) []byte { return encode(func(e *postcard.Encoder) { encodeMember(e, m) }) }

// DecodeMember parses a member.
func DecodeMember(b []byte) (Member, error) { return decodeOne(b, decodeMember) }

func encodePacket(e *postcard.Encoder, p *Packet) {
	e.Discriminant(uint32(p.Kind))
	e.Raw(p.Database[:])
	e.Raw(p.Sender[:])
	switch p.Kind {
	case PacketGossip:
		e.Len(len(p.Members))
		for i := range p.Members {
			encodeMember(e, &p.Members[i])
		}
	case PacketIndirectAck, PacketPingRequest:
		e.Raw(p.Target[:])
		e.U64(p.Nonce)
	case PacketPing, PacketAck:
		e.U64(p.Nonce)
		e.Len(len(p.Members))
		for i := range p.Members {
			encodeMember(e, &p.Members[i])
		}
	}
}

func decodePacket(d *postcard.Decoder) Packet {
	var p Packet
	switch d.Discriminant() {
	case 0:
		p.Kind = PacketGossip
	case 1:
		p.Kind = PacketIndirectAck
	case 2:
		p.Kind = PacketPing
	case 3:
		p.Kind = PacketAck
	case 4:
		p.Kind = PacketPingRequest
	default:
		d.Fail(protocolErr("bad packet"))
		return p
	}
	copy(p.Database[:], d.Raw(16))
	copy(p.Sender[:], d.Raw(16))
	switch p.Kind {
	case PacketGossip:
		for n := d.Len(); n > 0; n-- {
			p.Members = append(p.Members, decodeMember(d))
		}
	case PacketIndirectAck, PacketPingRequest:
		copy(p.Target[:], d.Raw(16))
		p.Nonce = d.U64()
	case PacketPing, PacketAck:
		p.Nonce = d.U64()
		for n := d.Len(); n > 0; n-- {
			p.Members = append(p.Members, decodeMember(d))
		}
	}
	return p
}

// EncodePacket serializes a packet.
func EncodePacket(p *Packet) []byte { return encode(func(e *postcard.Encoder) { encodePacket(e, p) }) }

// DecodePacket parses a packet.
func DecodePacket(b []byte) (Packet, error) { return decodeOne(b, decodePacket) }
