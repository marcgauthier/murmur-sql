package replication

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net/netip"
	"net/url"
	"testing"
	"time"
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
