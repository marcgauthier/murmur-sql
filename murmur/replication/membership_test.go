package replication

import (
	"net/netip"
	"testing"
	"time"
)

func testMember(node NodeId, incarnation uint64, state MemberState) Member {
	return Member{
		Node:        node,
		Address:     netip.MustParseAddrPort("127.0.0.1:9000"),
		Incarnation: incarnation,
		State:       state,
	}
}

func testView() *View {
	return NewView(testMember(idOf(1), 1, MemberAlive))
}

func TestViewMergeSelfRefutes(t *testing.T) {
	v := testView()
	// A suspect report about self at the local incarnation refutes by
	// bumping past the reported incarnation.
	v.Merge(testMember(idOf(1), 1, MemberSuspect))
	if v.Local.Incarnation != 2 {
		t.Fatalf("local incarnation = %d, want 2", v.Local.Incarnation)
	}
	// An alive report about self changes nothing.
	v.Merge(testMember(idOf(1), 9, MemberAlive))
	if v.Local.Incarnation != 2 {
		t.Fatalf("local incarnation = %d, want 2", v.Local.Incarnation)
	}
	// A stale report (below local) changes nothing.
	v.Merge(testMember(idOf(1), 1, MemberDead))
	if v.Local.Incarnation != 2 {
		t.Fatalf("local incarnation = %d, want 2", v.Local.Incarnation)
	}
	if len(v.Members) != 0 {
		t.Fatal("self merged into member table")
	}
}

func TestViewMergeOrdersByIncarnationThenRank(t *testing.T) {
	v := testView()
	v.Merge(testMember(idOf(2), 3, MemberSuspect))
	if got := v.Members[idOf(2)]; got.Incarnation != 3 || got.State != MemberSuspect {
		t.Fatalf("new member = %+v", got)
	}
	// Lower incarnation loses even with a higher-ranked state.
	v.Merge(testMember(idOf(2), 2, MemberDead))
	if got := v.Members[idOf(2)]; got.Incarnation != 3 || got.State != MemberSuspect {
		t.Fatalf("stale overwrite = %+v", got)
	}
	// Same incarnation: higher rank wins (suspect -> dead).
	v.Merge(testMember(idOf(2), 3, MemberDead))
	if got := v.Members[idOf(2)]; got.State != MemberDead {
		t.Fatalf("rank promotion lost: %+v", got)
	}
	// Same incarnation: lower rank loses (dead -> alive rejected).
	v.Merge(testMember(idOf(2), 3, MemberAlive))
	if got := v.Members[idOf(2)]; got.State != MemberDead {
		t.Fatalf("rank demotion won: %+v", got)
	}
	// Higher incarnation always wins, even alive over dead.
	v.Merge(testMember(idOf(2), 4, MemberAlive))
	if got := v.Members[idOf(2)]; got.Incarnation != 4 || got.State != MemberAlive {
		t.Fatalf("fresh incarnation lost: %+v", got)
	}
}

func TestViewMergeAliveClearsSuspect(t *testing.T) {
	v := testView()
	v.Merge(testMember(idOf(2), 1, MemberSuspect))
	v.Suspect[idOf(2)] = time.Now()
	v.Merge(testMember(idOf(2), 2, MemberAlive))
	if _, ok := v.Suspect[idOf(2)]; ok {
		t.Fatal("alive merge left a suspect timer")
	}
}

func TestViewMergeExcludedAndBounded(t *testing.T) {
	v := testView()
	v.Excluded[idOf(9)] = true
	v.Merge(testMember(idOf(9), 1, MemberAlive))
	if _, ok := v.Members[idOf(9)]; ok {
		t.Fatal("excluded member merged")
	}
	for i := 0; i < 4096; i++ {
		var id NodeId
		id[0], id[1] = byte(i), byte(i>>8)
		if id == idOf(1) || id == idOf(9) {
			continue
		}
		v.Merge(testMember(id, 1, MemberAlive))
	}
	if len(v.Members) > 4096 {
		t.Fatalf("%d members, want at most 4096", len(v.Members))
	}
	overflow := idOf(7)
	if _, ok := v.Members[overflow]; ok {
		t.Fatal("test setup collided with the bound fill")
	}
	v.Merge(testMember(overflow, 1, MemberAlive))
	if _, ok := v.Members[overflow]; ok {
		t.Fatal("member merged past the 4096 bound")
	}
	// Updates to members already in the table still apply at the bound.
	var known NodeId
	for id := range v.Members {
		known = id
		break
	}
	v.Merge(testMember(known, 2, MemberSuspect))
	if got := v.Members[known]; got.Incarnation != 2 || got.State != MemberSuspect {
		t.Fatal("in-table update lost at the bound")
	}
}

func TestViewGossipAlwaysSeedsLocal(t *testing.T) {
	v := testView()
	if got := v.Gossip(); len(got) != 1 || got[0].Node != idOf(1) {
		t.Fatalf("singleton gossip = %+v", got)
	}
	for i := byte(2); i <= 11; i++ {
		v.Merge(testMember(idOf(i), 1, MemberAlive))
	}
	got := v.Gossip()
	if len(got) != 4 {
		t.Fatalf("gossip size = %d, want local plus three", len(got))
	}
	if got[0].Node != idOf(1) {
		t.Fatal("local member not first")
	}
	seen := map[NodeId]bool{idOf(1): true}
	for _, m := range got[1:] {
		if seen[m.Node] {
			t.Fatalf("duplicate gossip member %v", m.Node)
		}
		seen[m.Node] = true
	}
}

func TestViewAcknowledged(t *testing.T) {
	v := testView()
	v.Awareness = 2
	v.Pending[idOf(2)] = pendingProbe{At: time.Now(), Nonce: 7}
	v.Acknowledged(idOf(2), 8) // wrong nonce: kept
	if _, ok := v.Pending[idOf(2)]; !ok {
		t.Fatal("wrong-nonce ack cleared the probe")
	}
	v.Acknowledged(idOf(3), 7) // unknown node: no-op
	v.Acknowledged(idOf(2), 7)
	if _, ok := v.Pending[idOf(2)]; ok {
		t.Fatal("matching ack left the probe")
	}
	if v.Awareness != 1 {
		t.Fatalf("awareness = %d, want 1", v.Awareness)
	}
	// Awareness never drops below zero.
	v.Pending[idOf(2)] = pendingProbe{At: time.Now(), Nonce: 8}
	v.Awareness = 0
	v.Acknowledged(idOf(2), 8)
	if v.Awareness != 0 {
		t.Fatalf("awareness = %d, want 0", v.Awareness)
	}
}

func TestViewExpireIndirectThenSuspect(t *testing.T) {
	v := testView()
	v.Merge(testMember(idOf(2), 1, MemberAlive))
	// Fresh probe: untouched. Timeout at awareness 0 is 500ms.
	v.Pending[idOf(2)] = pendingProbe{At: time.Now(), Nonce: 11}
	if got := v.Expire(); len(got) != 0 {
		t.Fatalf("fresh probe expired: %+v", got)
	}
	// First timeout: re-armed as proxied, indirect probe requested.
	v.Pending[idOf(2)] = pendingProbe{At: time.Now().Add(-time.Second), Nonce: 11}
	got := v.Expire()
	if len(got) != 1 || got[0].Node != idOf(2) || got[0].Nonce != 11 {
		t.Fatalf("indirect = %+v", got)
	}
	p := v.Pending[idOf(2)]
	if !p.Proxied {
		t.Fatal("timed-out probe not marked proxied")
	}
	// Second timeout: suspect, timer set, awareness rises.
	p.At = time.Now().Add(-time.Second)
	v.Pending[idOf(2)] = p
	if got := v.Expire(); len(got) != 0 {
		t.Fatalf("proxied timeout re-requested: %+v", got)
	}
	if _, ok := v.Pending[idOf(2)]; ok {
		t.Fatal("proxied probe left pending")
	}
	if v.Members[idOf(2)].State != MemberSuspect {
		t.Fatal("member not suspected after proxied timeout")
	}
	if _, ok := v.Suspect[idOf(2)]; !ok {
		t.Fatal("suspect timer not set")
	}
	if v.Awareness != 1 {
		t.Fatalf("awareness = %d, want 1", v.Awareness)
	}
}

func TestViewExpireSuspectToDead(t *testing.T) {
	v := testView()
	v.Merge(testMember(idOf(2), 1, MemberSuspect))
	// Suspect expiry at awareness 0 is 5s.
	v.Suspect[idOf(2)] = time.Now().Add(-time.Second)
	v.Expire()
	if v.Members[idOf(2)].State != MemberSuspect {
		t.Fatal("fresh suspect promoted early")
	}
	if _, ok := v.Suspect[idOf(2)]; !ok {
		t.Fatal("fresh suspect timer dropped")
	}
	v.Suspect[idOf(2)] = time.Now().Add(-6 * time.Second)
	v.Expire()
	if v.Members[idOf(2)].State != MemberDead {
		t.Fatal("long suspect not promoted to dead")
	}
	if _, ok := v.Suspect[idOf(2)]; ok {
		t.Fatal("dead member kept a suspect timer")
	}
}

func TestViewExpireAwarenessScalesTimeouts(t *testing.T) {
	v := testView()
	v.Awareness = 3 // probe timeout 2s, suspect expiry 20s
	v.Pending[idOf(2)] = pendingProbe{At: time.Now().Add(-1500 * time.Millisecond), Nonce: 5}
	if got := v.Expire(); len(got) != 0 {
		t.Fatal("probe expired before the scaled timeout")
	}
	v.Pending[idOf(2)] = pendingProbe{At: time.Now().Add(-2500 * time.Millisecond), Nonce: 5}
	if got := v.Expire(); len(got) != 1 {
		t.Fatal("probe survived past the scaled timeout")
	}
	// Awareness caps at 8 no matter how many timeouts stack.
	v.Awareness = 8
	p := v.Pending[idOf(2)]
	p.At = time.Now().Add(-time.Hour)
	v.Pending[idOf(2)] = p
	v.Expire()
	if v.Awareness != 8 {
		t.Fatalf("awareness = %d, want cap 8", v.Awareness)
	}
}
