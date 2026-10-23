package replication

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/netip"
	"time"

	"github.com/quic-go/quic-go"
)

// quicStream is the engine-facing bidirectional stream surface.
type quicStream interface {
	io.ReadWriteCloser
	CancelRead(code quic.StreamErrorCode)
	CancelWrite(code quic.StreamErrorCode)
}

// quicConn is the engine-facing connection surface.
type quicConn interface {
	OpenStream() (quicStream, error)
	OpenStreamSync(ctx context.Context) (quicStream, error)
	AcceptStream(ctx context.Context) (quicStream, error)
	SendDatagram(p []byte) error
	ReceiveDatagram(ctx context.Context) ([]byte, error)
	CloseWithError(code quic.ApplicationErrorCode, reason string) error
	RemoteAddr() net.Addr
	Context() context.Context
	PeerCertificates() []*x509.Certificate
}

// quicListener is the engine-facing endpoint surface.
type quicListener interface {
	Accept(ctx context.Context) (quicConn, error)
	Dial(ctx context.Context, addr netip.AddrPort) (quicConn, error)
	LocalAddr() netip.AddrPort
	Close() error
}

type quicStreamWrap struct{ *quic.Stream }

func (s quicStreamWrap) CancelRead(code quic.StreamErrorCode)  { s.Stream.CancelRead(code) }
func (s quicStreamWrap) CancelWrite(code quic.StreamErrorCode) { s.Stream.CancelWrite(code) }

type quicConnWrap struct{ *quic.Conn }

func (c quicConnWrap) OpenStream() (quicStream, error) {
	s, err := c.Conn.OpenStream()
	if err != nil {
		return nil, err
	}
	return quicStreamWrap{s}, nil
}

func (c quicConnWrap) OpenStreamSync(ctx context.Context) (quicStream, error) {
	s, err := c.Conn.OpenStreamSync(ctx)
	if err != nil {
		return nil, err
	}
	return quicStreamWrap{s}, nil
}

func (c quicConnWrap) AcceptStream(ctx context.Context) (quicStream, error) {
	s, err := c.Conn.AcceptStream(ctx)
	if err != nil {
		return nil, err
	}
	return quicStreamWrap{s}, nil
}

func (c quicConnWrap) PeerCertificates() []*x509.Certificate {
	return c.Conn.ConnectionState().TLS.PeerCertificates
}

// quicEndpoint bundles a QUIC listener with dial credentials.
type quicEndpoint struct {
	listener *quic.Listener
	dialTLS  *tls.Config
	quicConf *quic.Config
}

func (e *quicEndpoint) Accept(ctx context.Context) (quicConn, error) {
	c, err := e.listener.Accept(ctx)
	if err != nil {
		return nil, err
	}
	return quicConnWrap{c}, nil
}

func (e *quicEndpoint) Dial(ctx context.Context, addr netip.AddrPort) (quicConn, error) {
	c, err := quic.DialAddr(ctx, addr.String(), e.dialTLS, e.quicConf)
	if err != nil {
		return nil, err
	}
	return quicConnWrap{c}, nil
}

func (e *quicEndpoint) LocalAddr() netip.AddrPort {
	addr, _ := netip.ParseAddrPort(e.listener.Addr().String())
	return addr
}

func (e *quicEndpoint) Close() error { return e.listener.Close() }

// duplicateCloseReason marks a dial closed mid-handshake as redundant
// against a live session. It exits quietly instead of latching routine
// churn as an engine error; any other far-side close stays loud.
const duplicateCloseReason = "duplicate connection"

// isDuplicateClose reports whether err (or the connection's close error)
// is a duplicate tie-break close.
func isDuplicateClose(conn quicConn, err error) bool {
	if isDuplicateErr(err) {
		return true
	}
	if conn != nil {
		return isDuplicateErr(conn.Context().Err())
	}
	return false
}

func isDuplicateErr(err error) bool {
	var appErr *quic.ApplicationError
	if errors.As(err, &appErr) {
		return appErr.Remote && appErr.ErrorCode == 0 && appErr.ErrorMessage == duplicateCloseReason
	}
	return false
}

// connClosed reports whether the connection has any close reason.
func connClosed(conn quicConn) bool {
	return conn.Context().Err() != nil
}

// peerID authenticates the far side: network allow-list, leaf SAN node
// id, and the allowed-peers set.
func peerID(conn quicConn, config *Config) (NodeId, error) {
	var zero NodeId
	addr, err := netip.ParseAddrPort(conn.RemoteAddr().String())
	if err != nil {
		return zero, authErr("remote address")
	}
	if !NetworkAllowed(config, addr) {
		return zero, authErr("remote network denied")
	}
	chain := conn.PeerCertificates()
	if len(chain) == 0 {
		return zero, authErr("missing peer certificate")
	}
	id, err := NodeID(chain[0].Raw)
	if err != nil {
		return zero, err
	}
	if !peerAllowed(config, id) {
		return zero, authErr("remote node denied")
	}
	return id, nil
}

// peerAllowed reports whether id passes the identity allow-list. An
// empty list admits every authenticated identity.
func peerAllowed(config *Config, id NodeId) bool {
	return len(config.AllowedPeers) == 0 || config.AllowedPeers[id]
}

func parsePrivateKey(der []byte) (any, error) {
	if key, err := x509.ParsePKCS8PrivateKey(der); err == nil {
		return key, nil
	}
	if key, err := x509.ParseECPrivateKey(der); err == nil {
		return key, nil
	}
	return x509.ParsePKCS1PrivateKey(der)
}

// newEndpoint builds the mTLS QUIC endpoint: TLS 1.3 only, roots-pinned
// chain verification, node SAN identity, datagrams enabled, uni streams
// disabled.
func newEndpoint(config *Config, bind netip.AddrPort, node NodeId, alpn []byte) (*quicEndpoint, error) {
	if len(config.Credentials.Chain) == 0 {
		return nil, authErr("missing certificate chain")
	}
	if id, err := NodeID(config.Credentials.Chain[0]); err != nil || id != node {
		if err != nil {
			return nil, err
		}
		return nil, authErr("certificate is not for this node")
	}
	roots := x509.NewCertPool()
	for _, der := range config.Credentials.Roots {
		cert, err := x509.ParseCertificate(der)
		if err != nil {
			return nil, authErr("invalid trust root: %v", err)
		}
		roots.AddCert(cert)
	}
	key, err := parsePrivateKey(config.Credentials.PrivateKey)
	if err != nil {
		return nil, authErr("invalid private key: %v", err)
	}
	leaf, err := x509.ParseCertificate(config.Credentials.Chain[0])
	if err != nil {
		return nil, authErr("invalid leaf certificate: %v", err)
	}
	cert := tls.Certificate{Certificate: config.Credentials.Chain, PrivateKey: key, Leaf: leaf}

	serverTLS := &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    roots,
		MinVersion:   tls.VersionTLS13,
		NextProtos:   []string{string(alpn)},
	}
	clientTLS := &tls.Config{
		Certificates:       []tls.Certificate{cert},
		ServerName:         "murmur.invalid",
		MinVersion:         tls.VersionTLS13,
		NextProtos:         []string{string(alpn)},
		InsecureSkipVerify: true,
		VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			if len(rawCerts) == 0 {
				return errors.New("missing peer certificate")
			}
			leaf, err := x509.ParseCertificate(rawCerts[0])
			if err != nil {
				return err
			}
			intermediates := x509.NewCertPool()
			for _, raw := range rawCerts[1:] {
				if cert, err := x509.ParseCertificate(raw); err == nil {
					intermediates.AddCert(cert)
				}
			}
			if _, err := leaf.Verify(x509.VerifyOptions{
				Roots:         roots,
				Intermediates: intermediates,
				KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
			}); err != nil {
				return err
			}
			id, err := NodeID(rawCerts[0])
			if err != nil {
				return err
			}
			if !peerAllowed(config, id) {
				return errors.New("peer identity denied")
			}
			return nil
		},
	}
	quicConf := &quic.Config{
		MaxIdleTimeout:        30 * time.Second,
		KeepAlivePeriod:       10 * time.Second,
		MaxIncomingStreams:    int64(config.Sessions + 8),
		MaxIncomingUniStreams: -1,
		EnableDatagrams:       true,
	}
	listener, err := quic.ListenAddr(bind.String(), serverTLS, quicConf)
	if err != nil {
		return nil, err
	}
	return &quicEndpoint{listener: listener, dialTLS: clientTLS, quicConf: quicConf}, nil
}

// refuse closes an unwanted inbound connection immediately. quic-go has
// no pre-handshake refuse; the handshake already ran, so this is the
// closest equivalent.
func refuse(conn quicConn) {
	_ = conn.CloseWithError(1, "refused")
}
