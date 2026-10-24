package replication

import (
	"context"
	"errors"
	"net/netip"
	"testing"
	"time"
)

// TestRefreshSeeds pins seed re-resolution: named seeds refresh from the
// resolver keeping their ports, literals and runtime addresses stay,
// and resolution errors keep the previous address.
func TestRefreshSeeds(t *testing.T) {
	seeds := []netip.AddrPort{
		netip.MustParseAddrPort("203.0.113.11:9001"),
		netip.MustParseAddrPort("203.0.113.99:9002"),
		netip.MustParseAddrPort("198.51.100.7:9003"),
	}
	hosts := []string{"", "db0.example", "gone.example"}
	resolve := func(ctx context.Context, host string) (netip.Addr, error) {
		if host == "db0.example" {
			return netip.MustParseAddr("192.0.2.5"), nil
		}
		return netip.Addr{}, errors.New("no such host")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got, refreshed, failed := refreshSeeds(ctx, seeds, hosts, resolve)
	want := []netip.AddrPort{
		netip.MustParseAddrPort("203.0.113.11:9001"),
		netip.MustParseAddrPort("192.0.2.5:9002"),
		netip.MustParseAddrPort("198.51.100.7:9003"),
	}
	if len(got) != len(want) {
		t.Fatalf("seeds = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("seeds[%d] = %v, want %v", i, got[i], want[i])
		}
	}
	if refreshed != 1 || failed != 1 {
		t.Fatalf("refreshed/failed = %d/%d, want 1/1", refreshed, failed)
	}
	// Short hosts stay put.
	if got, refreshed, failed := refreshSeeds(ctx, seeds, nil, resolve); len(got) != len(seeds) || refreshed != 0 || failed != 0 {
		t.Fatalf("short = %v, %d/%d", got, refreshed, failed)
	}
}
