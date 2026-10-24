package native

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"strconv"

	"github.com/marcgauthier/murmur-sql/murmur/replication"
)

// resolveSocket parses host:port, resolving DNS names. IP literals take
// the fast path without DNS; failures name the entry.
func resolveSocket(ctx context.Context, s string) (netip.AddrPort, error) {
	if v, e := netip.ParseAddrPort(s); e == nil {
		return v, nil
	}
	host, portStr, e := net.SplitHostPort(s)
	if e != nil {
		return netip.AddrPort{}, fmt.Errorf("invalid seed address %q: %w", s, e)
	}
	port, e := strconv.Atoi(portStr)
	if e != nil || port < 1 || port > 65535 {
		return netip.AddrPort{}, fmt.Errorf("invalid seed address %q: bad port", s)
	}
	addr, e := replication.ResolveSeed(ctx, host)
	if e != nil {
		return netip.AddrPort{}, fmt.Errorf("invalid seed address %q: %w", s, e)
	}
	return netip.AddrPortFrom(addr, uint16(port)), nil
}

// seedHostname returns the DNS hostname for a seed entry, or "" for IP
// literals. Entries must already have passed resolveSocket.
func seedHostname(s string) string {
	if _, e := netip.ParseAddrPort(s); e == nil {
		return ""
	}
	host, _, e := net.SplitHostPort(s)
	if e != nil {
		return ""
	}
	return host
}
