package replication

import (
	"context"
	"net"
	"net/netip"
	"time"
)

// seedRefreshTimeout caps one re-resolution pass over named seeds.
const seedRefreshTimeout = 2 * time.Second

// ResolveSeed resolves one bootstrap hostname to a dialable address,
// preferring IPv4. IP literals never reach the resolver; callers must
// handle them on the fast path.
func ResolveSeed(ctx context.Context, host string) (netip.Addr, error) {
	addrs, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return netip.Addr{}, err
	}
	fallback := netip.Addr{}
	for _, a := range addrs {
		addr, err := netip.ParseAddr(a.IP.String())
		if err != nil || !addr.IsValid() || addr.IsUnspecified() {
			continue
		}
		if addr.Is4() {
			return addr, nil
		}
		if !fallback.IsValid() {
			fallback = addr
		}
	}
	if !fallback.IsValid() {
		return netip.Addr{}, unavailableErr("no addresses for %q", host)
	}
	return fallback, nil
}

// refreshNamedSeeds re-resolves hostname seeds while the node has no
// peers: the only state where stale seed DNS blocks rejoining.
// Failures surface in status, throttled to one report per minute.
func (c *engineCore) refreshNamedSeeds(seeds *[]netip.AddrPort, hosts []string) {
	named := false
	for _, h := range hosts {
		if h != "" {
			named = true
			break
		}
	}
	if !named {
		return
	}
	resolve := c.config.SeedResolver
	if resolve == nil {
		resolve = ResolveSeed
	}
	ctx, cancel := context.WithTimeout(c.ctx, seedRefreshTimeout)
	defer cancel()
	updated, _, failed := refreshSeeds(ctx, *seeds, hosts, resolve)
	*seeds = updated
	if failed > 0 && time.Since(c.lastSeedErrLog) >= time.Minute {
		c.lastSeedErrLog = time.Now()
		c.recordError(unavailableErr("%d seed hostname(s) failed to resolve", failed))
	}
}

// refreshSeeds re-resolves named seeds, returning updated dial addresses
// with refreshed/failed counts. hosts aligns with seeds; "" means a
// literal or runtime address and is kept untouched, as are entries past
// len(hosts). Resolution errors keep the previous address; ports never
// change.
func refreshSeeds(ctx context.Context, seeds []netip.AddrPort, hosts []string, resolve func(context.Context, string) (netip.Addr, error)) (out []netip.AddrPort, refreshed, failed int) {
	out = append([]netip.AddrPort(nil), seeds...)
	for i := range out {
		if i >= len(hosts) || hosts[i] == "" {
			continue
		}
		addr, err := resolve(ctx, hosts[i])
		if err != nil || !addr.IsValid() {
			failed++
			continue
		}
		if fresh := netip.AddrPortFrom(addr, out[i].Port()); fresh != out[i] {
			out[i] = fresh
			refreshed++
		}
	}
	return out, refreshed, failed
}
