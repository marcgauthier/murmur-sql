package spool_test

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/marcgauthier/murmur-sql-go/murmur/spool"
)

func writeSeededStore(t *testing.T, dir string, n int) int64 {
	t.Helper()
	st, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < n; i++ {
		if err := st.Put([]byte(fmt.Sprintf("k%04d", i)), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return segSize(t, dir, 1)
}

func segSize(t *testing.T, dir string, id int) int64 {
	t.Helper()
	st, err := os.Stat(filepath.Join(dir, "segments", fmt.Sprintf("%012d.spool", id)))
	if err != nil {
		t.Fatalf("stat segment %d: %v", id, err)
	}
	return st.Size()
}

func appendTail(t *testing.T, dir string, id int, tail []byte) {
	t.Helper()
	p := filepath.Join(dir, "segments", fmt.Sprintf("%012d.spool", id))
	f, err := os.OpenFile(p, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("open segment: %v", err)
	}
	defer f.Close()
	if _, err := f.Write(tail); err != nil {
		t.Fatalf("append tail: %v", err)
	}
}

// TestOpenTornZeroTailRecovers replays the endurance crash: a SIGKILL
// mid-append left 363 zero bytes past the last valid frame. The open
// must truncate the torn tail and keep every committed record.
func TestOpenTornZeroTailRecovers(t *testing.T) {
	dir := t.TempDir()
	clean := writeSeededStore(t, dir, 200)
	appendTail(t, dir, 1, make([]byte, 363))

	st, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	stats := st.Stats()
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if stats.Keys != 200 {
		t.Fatalf("Keys = %d, want 200", stats.Keys)
	}
	if stats.TruncatedTails == 0 {
		t.Fatal("TruncatedTails = 0, want the torn tail counted")
	}
	if got := segSize(t, dir, 1); got != clean {
		t.Fatalf("segment size = %d, want truncated %d", got, clean)
	}
	got := loadAll(t, dir)
	if len(got) != 200 || string(got["k0199"]) != "v199" {
		t.Fatalf("reloaded %d keys, want 200 intact", len(got))
	}
}

// TestOpenTornGarbageTailRecovers covers non-zero damage: random bytes
// with no valid header must truncate exactly like a zero fill.
func TestOpenTornGarbageTailRecovers(t *testing.T) {
	dir := t.TempDir()
	clean := writeSeededStore(t, dir, 200)
	junk := make([]byte, 5000)
	rng := rand.New(rand.NewSource(7))
	if _, err := rng.Read(junk); err != nil {
		t.Fatal(err)
	}
	appendTail(t, dir, 1, junk)

	st, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	keys := st.Stats().Keys
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if keys != 200 {
		t.Fatalf("Keys = %d, want 200", keys)
	}
	if got := segSize(t, dir, 1); got != clean {
		t.Fatalf("segment size = %d, want truncated %d", got, clean)
	}
}

// TestOpenMidFileCorruptionFailsClosed proves the resync rule: damage
// followed by more valid frames is genuine corruption, not a torn
// tail, and must fail the open instead of silently skipping bytes.
func TestOpenMidFileCorruptionFailsClosed(t *testing.T) {
	dir := t.TempDir()
	writeSeededStore(t, dir, 200)
	p := filepath.Join(dir, "segments", "000000000001.spool")
	orig, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	junk := make([]byte, 500)
	rng := rand.New(rand.NewSource(13))
	if _, err := rng.Read(junk); err != nil {
		t.Fatal(err)
	}
	doctored := append(append(append([]byte(nil), orig...), junk...), orig...)
	if err := os.WriteFile(p, doctored, 0o600); err != nil {
		t.Fatal(err)
	}
	_, err = spool.Open(testOptions(t, dir))
	if err == nil || !strings.Contains(err.Error(), "next valid frame") {
		t.Fatalf("reopen = %v, want corruption naming the resync frame", err)
	}
}

// TestOpenSealedSegmentDamageFailsClosed proves truncation applies to
// the active segment only: sealed files never take appends, so any
// framing damage there fails closed even with nothing after it.
func TestOpenSealedSegmentDamageFailsClosed(t *testing.T) {
	dir := t.TempDir()
	o := testOptions(t, dir)
	o.TargetBlockBytes = 4 << 10
	o.MaxBlockBytes = 64 << 10
	o.MaxKeySize = 1 << 10
	o.MaxValueSize = 8 << 10
	o.MaxSegmentSize = 256 << 10
	st, err := spool.Open(o)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	rng := rand.New(rand.NewSource(21))
	val := make([]byte, 1024)
	for i := 0; i < 500; i++ {
		if _, err := rng.Read(val); err != nil {
			t.Fatal(err)
		}
		if err := st.Put([]byte(fmt.Sprintf("k%04d", i)), val); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	entries, err := os.ReadDir(filepath.Join(dir, "segments"))
	if err != nil || len(entries) < 2 {
		t.Fatalf("segments = %v, want a rotation", entries)
	}
	appendTail(t, dir, 1, make([]byte, 363))
	if _, err := spool.Open(testOptions(t, dir)); err == nil {
		t.Fatal("reopen with damaged sealed segment succeeded")
	}
}
