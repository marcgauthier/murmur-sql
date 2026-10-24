// Raw-QUIC replication-protocol attacker fixture.
//
// A hostile peer speaking the product wire protocol: Message framing,
// snapshot manifest/chunk protocol, and a QUIC endpoint with the product
// SAN-URI TLS verifier. Everything wire-visible is reused from the
// product murmur/replication package; this file only adds test-side
// dialogue helpers (dial/handshake, patient collection, corrupt-snapshot
// forging). Scenarios must use this fixture instead of reimplementing
// the protocol.
package goharness

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"net/netip"
	"time"

	"github.com/quic-go/quic-go"

	"github.com/marcgauthier/murmur-sql/murmur/replication"
)

const (
	// dialTimeout bounds dial/handshake.
	dialTimeout = 15 * time.Second
	// ioTimeout bounds per-send and Welcome waits.
	ioTimeout = 10 * time.Second
	// maxTransaction is the largest transaction the attacker advertises
	// (product default).
	maxTransaction = 64 << 20
	// collectSlice is the quiet-slice granularity of CollectUntil.
	collectSlice = 500 * time.Millisecond
)

// Attacker is a malicious peer identity: a QUIC endpoint with a
// cluster-CA leaf cert for nodeID and an origin signing key (seed) the
// victim may or may not trust, depending on the leg. It owns everything;
// no borrow of the cluster.
type Attacker struct {
	nodeID   [16]byte
	config   *replication.Config
	tlsConf  *tls.Config
	quicConf *quic.Config
}

// NewAttacker creates an attacker identity nodeID with origin signing
// seed. Use a fixed id distinct from every node id, and register the
// seed's public key in the victim's ExtraTrustedOrigins when the leg
// needs the victim to accept the attacker's signatures (e.g. a forged
// snapshot manifest).
func NewAttacker(cluster *Cluster, nodeID [16]byte, seed [32]byte) (*Attacker, error) {
	issued, err := cluster.IssueCert(nodeID)
	if err != nil {
		return nil, fmt.Errorf("attacker: issue attacker cert: %w", err)
	}
	creds := replication.Credentials{
		Chain:      [][]byte{issued.CertDER},
		PrivateKey: issued.KeyDER,
		Roots:      [][]byte{cluster.CaDER()},
	}
	tlsConf, err := attackerTLS(creds)
	if err != nil {
		return nil, err
	}
	config := replication.NewConfig(netip.AddrPortFrom(netip.AddrFrom4([4]byte{127, 0, 0, 1}), 0), creds, seed)
	return &Attacker{
		nodeID:  nodeID,
		config:  config,
		tlsConf: tlsConf,
		quicConf: &quic.Config{
			MaxIdleTimeout:        30 * time.Second,
			KeepAlivePeriod:       10 * time.Second,
			MaxIncomingStreams:    40,
			MaxIncomingUniStreams: -1,
			EnableDatagrams:       true,
		},
	}, nil
}

// Impersonate builds a relay impersonating cluster node idx: a fresh TLS
// leaf for that node's id (identity is the SAN URI, so it authenticates
// as the node) with origin signing seed (pass NodeOriginSeed for a
// signature the victim accepts).
func Impersonate(cluster *Cluster, idx int, seed [32]byte) (*Attacker, error) {
	return NewAttacker(cluster, cluster.NodeID(idx), seed)
}

// NodeID returns the attacker node id.
func (a *Attacker) NodeID() [16]byte { return a.nodeID }

// Config returns the product replication config (e.g. to shorten
// SnapshotTransferLifetime for a forged manifest).
func (a *Attacker) Config() *replication.Config { return a.config }

// Dial completes a handshake against the victim and returns the session
// plus the victim's schema bytes (revealed by its Welcome). The Hello
// carries an empty schema.
func (a *Attacker) Dial(addr netip.AddrPort, db, expect [16]byte) (*Session, error) {
	return a.DialWithSchema(addr, db, expect, nil)
}

// DialWithSchema completes a handshake against the victim, offering schema
// as the Hello schema bytes, and returns the session plus the victim's
// schema bytes (revealed by its Welcome). Divergent-schema legs pass a
// bogus schema (expecting a SchemaRequest or a divergent Welcome);
// matched-schema control legs pass the victim's real schema bytes
// (expecting no SchemaRequest).
func (a *Attacker) DialWithSchema(addr netip.AddrPort, db, expect [16]byte, schema []byte) (*Session, error) {
	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	conn, err := quic.DialAddr(ctx, addr.String(), a.tlsConf, a.quicConf)
	cancel()
	if err != nil {
		return nil, fmt.Errorf("attacker: connect: %w", err)
	}
	fail := func(err error) (*Session, error) {
		_ = conn.CloseWithError(0, "attack dial failed")
		return nil, err
	}
	openCtx, openCancel := context.WithTimeout(context.Background(), dialTimeout)
	stream, err := conn.OpenStreamSync(openCtx)
	openCancel()
	if err != nil {
		return fail(fmt.Errorf("attacker: open stream: %w", err))
	}
	hello := replication.Message{
		Kind:           replication.MsgHello,
		Capabilities:   replication.Capabilities,
		Required:       replication.Required,
		Version:        replication.Version,
		Database:       db,
		Node:           a.nodeID,
		Schema:         schema,
		Mode:           replication.DisseminationGossip,
		MaxTransaction: maxTransaction,
	}
	if err := writeTimeout(stream, &hello, ioTimeout); err != nil {
		return fail(fmt.Errorf("attacker: write hello: %w", err))
	}
	inbound := make(chan inboundMsg, 1024)
	go func() {
		defer close(inbound)
		for {
			msg, err := replication.ReadMessage(stream)
			inbound <- inboundMsg{msg: msg, err: err}
			if err != nil {
				return
			}
		}
	}()
	select {
	case in, ok := <-inbound:
		if !ok || in.err != nil {
			return fail(fmt.Errorf("attacker: expected welcome, stream died: %v", in.err))
		}
		if in.msg.Kind != replication.MsgWelcome {
			return fail(fmt.Errorf("attacker: expected welcome, got kind %d", int(in.msg.Kind)))
		}
		if in.msg.Database != db {
			return fail(errors.New("attacker: welcome database mismatch"))
		}
		if in.msg.Node != expect {
			return fail(errors.New("attacker: welcome node mismatch"))
		}
		return &Session{conn: conn, stream: stream, welcomeSchema: append([]byte(nil), in.msg.Schema...), inbound: inbound}, nil
	case <-time.After(ioTimeout):
		return fail(errors.New("attacker: welcome timeout"))
	}
}

func attackerTLS(creds replication.Credentials) (*tls.Config, error) {
	key, err := x509.ParsePKCS8PrivateKey(creds.PrivateKey)
	if err != nil {
		return nil, fmt.Errorf("attacker: parse attacker key: %w", err)
	}
	leaf, err := x509.ParseCertificate(creds.Chain[0])
	if err != nil {
		return nil, fmt.Errorf("attacker: parse attacker leaf: %w", err)
	}
	roots := x509.NewCertPool()
	for _, der := range creds.Roots {
		cert, err := x509.ParseCertificate(der)
		if err != nil {
			return nil, fmt.Errorf("attacker: parse trust root: %w", err)
		}
		roots.AddCert(cert)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{{
			Certificate: creds.Chain,
			PrivateKey:  key,
			Leaf:        leaf,
		}},
		ServerName:         "murmur.invalid",
		MinVersion:         tls.VersionTLS13,
		NextProtos:         []string{string(replication.ALPN)},
		InsecureSkipVerify: true,
		VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			if len(rawCerts) == 0 {
				return errors.New("missing peer certificate")
			}
			peer, err := x509.ParseCertificate(rawCerts[0])
			if err != nil {
				return err
			}
			intermediates := x509.NewCertPool()
			for _, raw := range rawCerts[1:] {
				if cert, err := x509.ParseCertificate(raw); err == nil {
					intermediates.AddCert(cert)
				}
			}
			if _, err := peer.Verify(x509.VerifyOptions{
				Roots:         roots,
				Intermediates: intermediates,
				KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
			}); err != nil {
				return err
			}
			_, err = replication.NodeID(rawCerts[0])
			return err
		},
	}, nil
}

type inboundMsg struct {
	msg replication.Message
	err error
}

// Session is one attack QUIC session with its handshake stream open. All
// replication traffic multiplexes over that stream; closing the session
// closes the connection.
type Session struct {
	conn          *quic.Conn
	stream        *quic.Stream
	welcomeSchema []byte
	inbound       chan inboundMsg
}

// WelcomeSchema returns the victim schema bytes from its Welcome (for
// forging schema-matching manifests via replication.SchemaDigest).
func (s *Session) WelcomeSchema() []byte {
	return append([]byte(nil), s.welcomeSchema...)
}

// Send writes one message with the I/O timeout.
func (s *Session) Send(message *replication.Message) error {
	if err := writeTimeout(s.stream, message, ioTimeout); err != nil {
		return fmt.Errorf("attacker: send: %w", err)
	}
	return nil
}

// CollectUntil reads until want matches, total elapses, or the session
// dies. It stays patient across quiet slices instead of returning on the
// first one.
//
// Deviation: a reader goroutine owns the stream (framing is never split
// across read deadlines); the Rust version reads inline with timeout
// slices. Observable behavior matches.
func (s *Session) CollectUntil(total time.Duration, want func(*replication.Message) bool) []replication.Message {
	var out []replication.Message
	deadline := time.Now().Add(total)
	for time.Now().Before(deadline) {
		remaining := time.Until(deadline)
		if remaining > collectSlice {
			remaining = collectSlice
		}
		select {
		case in, ok := <-s.inbound:
			if !ok || in.err != nil {
				return out // session died; the caller interprets
			}
			out = append(out, in.msg)
			if want(&out[len(out)-1]) {
				return out
			}
		case <-time.After(remaining):
			// quiet slice; keep waiting
		}
	}
	return out
}

// Close closes the session's connection.
func (s *Session) Close() {
	_ = s.conn.CloseWithError(0, "attack done")
}

// Hello builds a Hello message for this attacker's identity offering
// schema (replay-handshake legs re-send one over a live session).
func (a *Attacker) Hello(db [16]byte, schema []byte) *replication.Message {
	return &replication.Message{
		Kind:           replication.MsgHello,
		Capabilities:   replication.Capabilities,
		Required:       replication.Required,
		Version:        replication.Version,
		Database:       db,
		Node:           a.nodeID,
		Schema:         schema,
		Mode:           replication.DisseminationGossip,
		MaxTransaction: maxTransaction,
	}
}

// SendRaw writes one raw wire frame: the 12-byte "RD" header (protocol
// version, kind, flags, body length) plus body. Compressed-bomb legs use
// this to bypass message framing.
func (s *Session) SendRaw(kind, flags uint16, body []byte) error {
	hdr := make([]byte, 12)
	copy(hdr[:2], "RD")
	binary.BigEndian.PutUint16(hdr[2:4], replication.Version)
	binary.BigEndian.PutUint16(hdr[4:6], kind)
	binary.BigEndian.PutUint16(hdr[6:8], flags)
	binary.BigEndian.PutUint32(hdr[8:12], uint32(len(body)))
	if err := s.stream.SetWriteDeadline(time.Now().Add(ioTimeout)); err != nil {
		return err
	}
	defer s.stream.SetWriteDeadline(time.Time{})
	if _, err := s.stream.Write(hdr); err != nil {
		return fmt.Errorf("attacker: send raw header: %w", err)
	}
	if _, err := s.stream.Write(body); err != nil {
		return fmt.Errorf("attacker: send raw body: %w", err)
	}
	return nil
}

// SendAllocClaim writes a bare frame header claiming claim bytes with no
// body behind it (allocation-claim legs: the victim must refuse the
// claim instead of allocating).
func (s *Session) SendAllocClaim(kind uint16, claim uint32) error {
	hdr := make([]byte, 12)
	copy(hdr[:2], "RD")
	binary.BigEndian.PutUint16(hdr[2:4], replication.Version)
	binary.BigEndian.PutUint16(hdr[4:6], kind)
	binary.BigEndian.PutUint16(hdr[6:8], 0)
	binary.BigEndian.PutUint32(hdr[8:12], claim)
	if err := s.stream.SetWriteDeadline(time.Now().Add(ioTimeout)); err != nil {
		return err
	}
	defer s.stream.SetWriteDeadline(time.Time{})
	if _, err := s.stream.Write(hdr); err != nil {
		return fmt.Errorf("attacker: send alloc claim: %w", err)
	}
	return nil
}

func writeTimeout(stream *quic.Stream, msg *replication.Message, d time.Duration) error {
	if err := stream.SetWriteDeadline(time.Now().Add(d)); err != nil {
		return err
	}
	defer stream.SetWriteDeadline(time.Time{})
	return replication.WriteMessage(stream, msg)
}

// ForgedSnapshot is a manifest plus chunk payloads whose second chunk was
// bit-flipped after the manifest digests were computed.
type ForgedSnapshot struct {
	Manifest  replication.Manifest
	Transfer  [16]byte
	Chunk0    []byte
	Chunk1Bad []byte
}

// ForgeCorruptSnapshot forges a structurally valid two-chunk snapshot for
// the victim's schema: the manifest passes every structural check, but
// the final chunk's bytes differ from what its chunk hash covers. size
// must span exactly two 64 KiB wire chunks. The manifest is signed with
// config's key for donor, so point config at the attacker identity the
// victim trusts.
//
// Deviation: the Rust fixture asserts its structural guards (panics); this
// port returns errors.
func ForgeCorruptSnapshot(config *replication.Config, donor, db, victim [16]byte, victimSchema []byte, size int) (*ForgedSnapshot, error) {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte('a' + i%26)
	}
	schema := replication.SchemaDigest(victimSchema)
	manifest, _, err := replication.Build([2][16]byte{db, donor}, victim, data, nil, schema, config, replication.NowMillis())
	if err != nil {
		return nil, fmt.Errorf("attacker: forge manifest: %w", err)
	}
	if len(manifest.Chunks) != 2 {
		return nil, fmt.Errorf("attacker: fixture: forged snapshot must span exactly two chunks, got %d", len(manifest.Chunks))
	}

	// Non-vacuous guards: the uncorrupted bytes verify against the
	// manifest, and the corrupted bytes must not.
	if sha256.Sum256(data) != manifest.Digest {
		return nil, errors.New("attacker: fixture: uncorrupted snapshot does not verify locally")
	}
	if len(data) <= replication.ChunkBytes {
		return nil, errors.New("attacker: fixture: snapshot does not span two chunks")
	}
	chunk0 := append([]byte(nil), data[:replication.ChunkBytes]...)
	chunk1Bad := append([]byte(nil), data[replication.ChunkBytes:]...)
	// Flip a byte mid-chunk. An ASCII-to-ASCII flip preserves length and
	// framing; only the digest differs.
	mid := len(chunk1Bad) / 2
	chunk1Bad[mid] ^= 0x01
	if len(chunk1Bad) != len(data)-len(chunk0) {
		return nil, errors.New("attacker: fixture: corruption changed the encoded length")
	}
	if sha256.Sum256(chunk1Bad) == manifest.Chunks[1] {
		return nil, errors.New("attacker: fixture: corrupted snapshot still verifies locally")
	}
	return &ForgedSnapshot{Manifest: *manifest, Transfer: manifest.Transfer, Chunk0: chunk0, Chunk1Bad: chunk1Bad}, nil
}
