package server

import (
	"testing"

	"github.com/marcgauthier/murmur-sql/config"
)

// TestRotationSessionBehavior pins session semantics across rotation:
// sessions unlocked before the rotation stay unlocked, while fresh
// sessions must present the new credential.
func TestRotationSessionBehavior(t *testing.T) {
	f := &fixtureEngine{}
	_, addr, _ := startFixture(t, config.DefaultConfig(), f)
	a := mustConnect(t, addr)
	b := mustConnect(t, addr)
	if ms := a.query(t, "MURMUR UNLOCK KEY 'old-secret'"); findCode(ms) != "" {
		t.Fatal(ms)
	}
	if ms := b.query(t, "MURMUR UNLOCK KEY 'old-secret'"); findCode(ms) != "" {
		t.Fatal(ms)
	}
	if ms := a.query(t, "MURMUR ROTATE KEY 'old-secret' 'new-secret'"); findCode(ms) != "" {
		t.Fatal(ms)
	}
	if ms := a.query(t, "MURMUR FLUSH"); findCode(ms) != "" {
		t.Fatalf("rotating session locked: %v", ms)
	}
	if ms := b.query(t, "MURMUR FLUSH"); findCode(ms) != "" {
		t.Fatalf("peer session locked: %v", ms)
	}
	c := mustConnect(t, addr)
	if ms := c.query(t, "MURMUR UNLOCK KEY 'old-secret'"); findCode(ms) != "28000" {
		t.Fatalf("old secret code = %q, want 28000", findCode(ms))
	}
	if ms := c.query(t, "MURMUR UNLOCK KEY 'new-secret'"); findCode(ms) != "" {
		t.Fatal(ms)
	}
}

// TestRotationWrongPrevious pins that rotation verifies the previous
// credential before adopting the new one.
func TestRotationWrongPrevious(t *testing.T) {
	f := &fixtureEngine{}
	_, addr, _ := startFixture(t, config.DefaultConfig(), f)
	w := mustConnect(t, addr)
	if ms := w.query(t, "MURMUR UNLOCK KEY 'old-secret'"); findCode(ms) != "" {
		t.Fatal(ms)
	}
	if ms := w.query(t, "MURMUR ROTATE KEY 'wrong' 'new-secret'"); findCode(ms) != "28000" {
		t.Fatalf("code = %q, want 28000", findCode(ms))
	}
	c := mustConnect(t, addr)
	if ms := c.query(t, "MURMUR UNLOCK KEY 'old-secret'"); findCode(ms) != "" {
		t.Fatalf("credential changed after failed rotation: %v", ms)
	}
}
