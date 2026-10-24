// Ephemeral port helpers.
//
// SQL listeners take an up-front free port (best effort: the address can be
// stolen between bind and reuse). Replication binds 127.0.0.1:0 directly
// and needs no coordination; the bound address is discovered via
// MURMUR SHOW REPLICATION.
package goharness

import (
	"net"
	"net/netip"
)

// FreeAddr binds 127.0.0.1:0 (TCP), reports the address, and releases it.
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

// FreeUDPAddr binds 127.0.0.1:0 (UDP), reports the address, and releases
// it. The SQL harness pre-picks replication (QUIC/UDP) ports up front.
func FreeUDPAddr() (netip.AddrPort, error) {
	c, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		return netip.AddrPort{}, err
	}
	defer c.Close()
	addr := c.LocalAddr().(*net.UDPAddr)
	ip := addr.IP.To4()
	if ip == nil {
		return netip.AddrPort{}, net.InvalidAddrError("non-IPv4 UDP address")
	}
	return netip.AddrPortFrom(netip.AddrFrom4([4]byte(ip)), uint16(addr.Port)), nil
}
