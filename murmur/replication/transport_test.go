package replication

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"net/netip"
	"net/url"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
)

func testCert(t *testing.T, uris ...string) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	for _, u := range uris {
		parsed, err := url.Parse(u)
		if err != nil {
			t.Fatal(err)
		}
		tmpl.URIs = append(tmpl.URIs, parsed)
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, key.Public(), key)
	if err != nil {
		t.Fatal(err)
	}
	return der
}

// TestPeerAllowed pins the identity allow-list predicate behind both
// enforcement points (QUIC session admission and the TLS identity
// check): an empty list admits every authenticated identity, a
// non-empty list admits exactly its members.
func TestPeerAllowed(t *testing.T) {
	c := NewConfig(netip.MustParseAddrPort("127.0.0.1:0"), Credentials{}, fill32(1))
	if !peerAllowed(c, idOf(2)) {
		t.Fatal("nil allow-list denies")
	}
	c.AllowedPeers = map[NodeId]bool{}
	if !peerAllowed(c, idOf(2)) {
		t.Fatal("empty allow-list denies")
	}
	c.AllowedPeers = map[NodeId]bool{idOf(1): true}
	if !peerAllowed(c, idOf(1)) {
		t.Fatal("listed identity denied")
	}
	if peerAllowed(c, idOf(2)) {
		t.Fatal("unlisted identity allowed")
	}
	// An explicit false entry denies even when the key is present.
	c.AllowedPeers = map[NodeId]bool{idOf(2): false}
	if peerAllowed(c, idOf(2)) {
		t.Fatal("false entry allowed")
	}
}

// fakeConn stubs the engine-facing connection surface: only the
// admission path (RemoteAddr, PeerCertificates) is functional.
type fakeConn struct {
	addr  net.Addr
	certs []*x509.Certificate
}

func (f *fakeConn) OpenStream() (quicStream, error) { panic("unused") }
func (f *fakeConn) OpenStreamSync(ctx context.Context) (quicStream, error) {
	panic("unused")
}
func (f *fakeConn) AcceptStream(ctx context.Context) (quicStream, error) { panic("unused") }
func (f *fakeConn) SendDatagram(p []byte) error                          { panic("unused") }
func (f *fakeConn) ReceiveDatagram(ctx context.Context) ([]byte, error) {
	panic("unused")
}
func (f *fakeConn) CloseWithError(code quic.ApplicationErrorCode, reason string) error {
	return nil
}
func (f *fakeConn) RemoteAddr() net.Addr                  { return f.addr }
func (f *fakeConn) Context() context.Context              { return context.Background() }
func (f *fakeConn) PeerCertificates() []*x509.Certificate { return f.certs }

// badAddr renders an unparseable remote address.
type badAddr struct{}

func (badAddr) Network() string { return "udp" }
func (badAddr) String() string  { return "not-an-address" }

func testNodeCert(t *testing.T, id NodeId) *x509.Certificate {
	t.Helper()
	der := testCert(t, "replicateddb://node/"+Hex(id[:]))
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return cert
}

func testTransportConfig() *Config {
	return NewConfig(netip.MustParseAddrPort("127.0.0.1:0"), Credentials{}, fill32(1))
}

// TestPeerIDLegs pins connection admission: unparseable remotes,
// denied networks, missing or malformed certificates, and unlisted
// identities are refused before any session state exists.
func udpAddr(ip string, port int) net.Addr {
	return &net.UDPAddr{IP: net.ParseIP(ip), Port: port}
}

func TestPeerIDLegs(t *testing.T) {
	goodAddr := udpAddr("10.1.2.3", 9)
	cert1 := testNodeCert(t, idOf(1))
	if id, err := peerID(&fakeConn{addr: goodAddr, certs: []*x509.Certificate{cert1}}, testTransportConfig()); err != nil || id != idOf(1) {
		t.Fatalf("admit = %v, %v", id, err)
	}
	if _, err := peerID(&fakeConn{addr: badAddr{}, certs: []*x509.Certificate{cert1}}, testTransportConfig()); err == nil {
		t.Fatal("unparseable remote admitted")
	}
	netted := testTransportConfig()
	netted.AllowedNetworks = []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")}
	if _, err := peerID(&fakeConn{addr: udpAddr("11.0.0.1", 9), certs: []*x509.Certificate{cert1}}, netted); err == nil {
		t.Fatal("denied network admitted")
	}
	if _, err := peerID(&fakeConn{addr: goodAddr}, testTransportConfig()); err == nil {
		t.Fatal("missing certificates admitted")
	}
	bad, err := x509.ParseCertificate(testCert(t))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := peerID(&fakeConn{addr: goodAddr, certs: []*x509.Certificate{bad}}, testTransportConfig()); err == nil {
		t.Fatal("non-node certificate admitted")
	}
	listed := testTransportConfig()
	listed.AllowedPeers = map[NodeId]bool{idOf(2): true}
	if _, err := peerID(&fakeConn{addr: goodAddr, certs: []*x509.Certificate{cert1}}, listed); err == nil {
		t.Fatal("unlisted identity admitted")
	}
}

// TestAcceptAllowed pins the inbound floodgate: connections past the
// connection cap or into the gossip/repair session reserve are
// refused, while headroom admits.
func TestAcceptAllowed(t *testing.T) {
	c := testTransportConfig() // Fanout 4, Repairs 2, Sessions 32, Connections 64
	addr := netip.MustParseAddrPort("10.1.2.3:9")
	if !acceptAllowed(c, addr, true, 0, 32) {
		t.Fatal("headroom refused")
	}
	if acceptAllowed(c, addr, false, 0, 32) {
		t.Fatal("unparseable remote admitted")
	}
	netted := testTransportConfig()
	netted.AllowedNetworks = []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")}
	if acceptAllowed(netted, netip.MustParseAddrPort("11.0.0.1:9"), true, 0, 32) {
		t.Fatal("denied network admitted")
	}
	if !acceptAllowed(c, addr, true, 63, 32) {
		t.Fatal("last connection slot refused")
	}
	if acceptAllowed(c, addr, true, 64, 32) {
		t.Fatal("connection past the cap admitted")
	}
	// Session reserve: Fanout+Repairs sessions stay free for gossip
	// and repair; the floodgate closes at the boundary.
	if acceptAllowed(c, addr, true, 0, 6) {
		t.Fatal("reserve floor admitted")
	}
	if !acceptAllowed(c, addr, true, 0, 7) {
		t.Fatal("one past the reserve refused")
	}
}

// TestSessionSemaphore pins the session budget: acquisitions fail
// closed at capacity, releases reopen, and over-release is harmless.
func TestSessionSemaphore(t *testing.T) {
	c := &engineCore{sessions: make(chan struct{}, 2)}
	if got := c.sessionsAvailable(); got != 2 {
		t.Fatalf("available = %d, want 2", got)
	}
	if !c.trySession() || !c.trySession() {
		t.Fatal("acquire under capacity failed")
	}
	if c.trySession() {
		t.Fatal("acquire past capacity succeeded")
	}
	if got := c.sessionsAvailable(); got != 0 {
		t.Fatalf("available = %d, want 0", got)
	}
	c.releaseSession()
	if got := c.sessionsAvailable(); got != 1 {
		t.Fatalf("available = %d, want 1", got)
	}
	if !c.trySession() {
		t.Fatal("reacquire after release failed")
	}
	c.releaseSession()
	c.releaseSession()
	c.releaseSession() // over-release: harmless, never negative
	if got := c.sessionsAvailable(); got != 2 {
		t.Fatalf("available = %d, want 2", got)
	}
}

// TestOutgoingCount counts only outgoing dials toward the dial cap.
func TestOutgoingCount(t *testing.T) {
	c := &engineCore{peers: map[NodeId]*enginePeer{
		idOf(1): {outgoing: true},
		idOf(2): {outgoing: true},
		idOf(3): {},
	}}
	if got := c.outgoingCount(); got != 2 {
		t.Fatalf("outgoing = %d, want 2", got)
	}
}

func TestNodeID(t *testing.T) {
	uri := "replicateddb://node/01010101010101010101010101010101"
	der := testCert(t, "https://example.com", uri)
	if id, err := NodeID(der); err != nil || id != idOf(1) {
		t.Fatalf("id = %v, %v", id, err)
	}
	if _, err := NodeID(testCert(t)); err == nil {
		t.Fatal("missing SAN accepted")
	}
	if _, err := NodeID(testCert(t, uri, uri)); err == nil {
		t.Fatal("duplicate URI accepted")
	}
	if _, err := NodeID(testCert(t, "replicateddb://node/00000000000000000000000000000000")); err == nil {
		t.Fatal("zero id accepted")
	}
	if _, err := NodeID(testCert(t, "replicateddb://node/not-a-node")); err == nil {
		t.Fatal("bad id accepted")
	}
	if _, err := NodeID(der[:len(der)-1]); err == nil {
		t.Fatal("truncated DER accepted")
	}
	if _, err := NodeID(append(append([]byte(nil), der...), 0)); err == nil {
		t.Fatal("trailing bytes accepted")
	}
	// Dashed UUID form parses to the same identity.
	dashed := testCert(t, "replicateddb://node/01010101-0101-0101-0101-010101010101")
	if id, err := NodeID(dashed); err != nil || id != idOf(1) {
		t.Fatalf("dashed id = %v, %v", id, err)
	}
}

func TestNetworkAllowed(t *testing.T) {
	c := NewConfig(netip.MustParseAddrPort("127.0.0.1:0"), Credentials{}, fill32(1))
	any := netip.MustParseAddrPort("10.1.2.3:9")
	if !NetworkAllowed(c, any) {
		t.Fatal("empty allowlist denies")
	}
	c.AllowedNetworks = []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")}
	if !NetworkAllowed(c, any) {
		t.Fatal("listed network denied")
	}
	if NetworkAllowed(c, netip.MustParseAddrPort("11.0.0.1:9")) {
		t.Fatal("unlisted network allowed")
	}
	mapped := netip.MustParseAddrPort("[::ffff:10.9.9.9]:9")
	if !NetworkAllowed(c, mapped) {
		t.Fatal("mapped IPv4 denied")
	}
}
