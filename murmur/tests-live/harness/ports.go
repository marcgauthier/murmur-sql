// Ephemeral port helpers.
//
// QUIC replication nodes bind 127.0.0.1:0 directly and need no
// coordination. These best-effort helpers exist only for fixture
// subprocesses that must be told a port up front; the address can be
// stolen between bind and reuse.
package harness

import (
	"net"
	"net/netip"
)

// FreeAddr binds 127.0.0.1:0, reports the address, and releases it.
func FreeAddr() (netip.AddrPort, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return netip.AddrPort{}, err
	}
	defer l.Close()
	addr := l.Addr().(*net.TCPAddr)
	return netip.AddrPortFrom(netip.AddrFrom4([4]byte{127, 0, 0, 1}), uint16(addr.Port)), nil
}

// FreePort returns just the port half of FreeAddr.
func FreePort() (uint16, error) {
	addr, err := FreeAddr()
	if err != nil {
		return 0, err
	}
	return addr.Port(), nil
}
