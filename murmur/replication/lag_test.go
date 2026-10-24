package replication

import "testing"

// TestLagBehindComputesWorstAndCount pins per-peer lag math: the worst
// per-origin deficit against local heads, and how many origins lag.
// Peers ahead of (or silent about) an origin never count against it.
func TestLagBehindComputesWorstAndCount(t *testing.T) {
	var a, b, c NodeId
	a[0], b[0], c[0] = 1, 2, 3
	heads := []Progress{
		{Origin: a, Applied: 100},
		{Origin: b, Applied: 50},
		{Origin: c, Applied: 10},
	}
	applied := map[NodeId]uint64{a: 100, b: 30, c: 999}
	worst, behind := lagBehind(heads, applied)
	if worst != 20 || behind != 1 {
		t.Fatalf("lag = (%d, %d), want (20, 1)", worst, behind)
	}
	worst, behind = lagBehind(heads, nil)
	if worst != 100 || behind != 3 {
		t.Fatalf("silent lag = (%d, %d), want (100, 3)", worst, behind)
	}
	worst, behind = lagBehind(nil, applied)
	if worst != 0 || behind != 0 {
		t.Fatalf("empty heads lag = (%d, %d), want (0, 0)", worst, behind)
	}
	caught := map[NodeId]uint64{a: 100, b: 50, c: 10}
	worst, behind = lagBehind(heads, caught)
	if worst != 0 || behind != 0 {
		t.Fatalf("caught-up lag = (%d, %d), want (0, 0)", worst, behind)
	}
}

// TestObservedDeficitComputesStaleness pins the bounded-staleness input:
// the worst deficit of local applied heads against peers' observed
// heads. Origins no peer saw, and peers behind us, add nothing.
func TestObservedDeficitComputesStaleness(t *testing.T) {
	var a, b NodeId
	a[0], b[0] = 1, 2
	heads := []Progress{{Origin: a, Applied: 90}, {Origin: b, Applied: 50}}
	peers := []map[NodeId]uint64{{a: 100, b: 40}, {a: 95}}
	if got := observedDeficit(heads, peers); got != 10 {
		t.Fatalf("deficit = %d, want 10", got)
	}
	if got := observedDeficit(heads, nil); got != 0 {
		t.Fatalf("no-peer deficit = %d, want 0", got)
	}
	// Origins unknown locally count as nothing applied: a fresh node is
	// fully behind what its peers observed, never vacuously fresh.
	if got := observedDeficit(nil, peers); got != 100 {
		t.Fatalf("no-head deficit = %d, want 100", got)
	}
	caught := []map[NodeId]uint64{{a: 90, b: 50}}
	if got := observedDeficit(heads, caught); got != 0 {
		t.Fatalf("caught-up deficit = %d, want 0", got)
	}
}

// TestMemberStateName pins the SHOW PEERS member-state strings,
// including members the view has never seen.
func TestMemberStateName(t *testing.T) {
	cases := []struct {
		state MemberState
		known bool
		want  string
	}{
		{MemberAlive, true, "alive"},
		{MemberSuspect, true, "suspect"},
		{MemberDead, true, "dead"},
		{MemberLeft, true, "left"},
		{MemberAlive, false, "unknown"},
		{MemberState(99), true, "unknown"},
	}
	for _, c := range cases {
		if got := memberStateName(c.state, c.known); got != c.want {
			t.Fatalf("memberStateName(%d, %v) = %q, want %q", c.state, c.known, got, c.want)
		}
	}
}
