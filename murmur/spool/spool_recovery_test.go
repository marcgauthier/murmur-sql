package spool_test

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/marcgauthier/murmur-sql/murmur/spool"
)

// TestTornTailRecovery simulates a crash mid-append: garbage bytes
// past the last valid block must be truncated on open, keeping all
// committed data and unblocking new writes.
func TestTornTailRecovery(t *testing.T) {
	dir := t.TempDir()
	st, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < 100; i++ {
		if err := st.Put([]byte(fmt.Sprintf("k%d", i)), []byte("v")); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	seg := filepath.Join(dir, "segments", "000000000001.spool")
	before, err := os.Stat(seg)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	// Append a torn header plus junk.
	f, err := os.OpenFile(seg, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := f.Write(bytes.Repeat([]byte{0xAA}, 40)); err != nil {
		t.Fatalf("write: %v", err)
	}
	f.Close()

	st2, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("reopen with torn tail: %v", err)
	}
	defer st2.Close()
	if got := st2.Stats().TruncatedTails; got != 1 {
		t.Fatalf("TruncatedTails = %d, want 1", got)
	}
	after, err := os.Stat(seg)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if after.Size() != before.Size() {
		t.Fatalf("size after truncate = %d, want %d", after.Size(), before.Size())
	}
	// New writes append after the truncation point, not past garbage.
	if err := st2.Put([]byte("new"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st2.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := st2.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	got := loadAll(t, dir)
	if len(got) != 101 || string(got["new"]) != "v" {
		t.Fatalf("recovered %d keys, want 101 with new=v", len(got))
	}
}

// TestTornTailFullFrame corrupts committed bytes after a clean
// close: unlike appended junk, damaged committed content fails
// authentication loudly at validation instead of being truncated.
func TestTornTailFullFrame(t *testing.T) {
	dir := t.TempDir()
	st, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	seg := filepath.Join(dir, "segments", "000000000001.spool")
	raw, err := os.ReadFile(seg)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	// Corrupt the last payload byte (block extends exactly to EOF).
	raw[len(raw)-1] ^= 0xFF
	if err := os.WriteFile(seg, raw, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := spool.Open(testOptions(t, dir)); !errors.Is(err, spool.ErrAuthFailed) {
		t.Fatalf("reopen with corrupt committed tail err = %v, want ErrAuthFailed", err)
	}
}

// TestCleanShutdownDetectsFileLoss closes cleanly, removes a
// segment externally, and requires the next open to fail loudly
// instead of silently accepting the loss.
func TestCleanShutdownDetectsFileLoss(t *testing.T) {
	for _, tamper := range []struct {
		name string
		fn   func(t *testing.T, segDir string)
	}{
		{"remove file", func(t *testing.T, segDir string) {
			if err := os.Remove(filepath.Join(segDir, "000000000001.spool")); err != nil {
				t.Fatalf("remove: %v", err)
			}
		}},
		{"add file", func(t *testing.T, segDir string) {
			// Unlisted files are swept as unreferenced garbage;
			// assert the sweep instead of a corruption error.
			raw, err := os.ReadFile(filepath.Join(segDir, "000000000001.spool"))
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			if err := os.WriteFile(filepath.Join(segDir, "000000000002.spool"), raw, 0o600); err != nil {
				t.Fatalf("write: %v", err)
			}
		}},
		{"truncate bytes", func(t *testing.T, segDir string) {
			p := filepath.Join(segDir, "000000000001.spool")
			st, err := os.Stat(p)
			if err != nil {
				t.Fatalf("stat: %v", err)
			}
			if err := os.Truncate(p, st.Size()-100); err != nil {
				t.Fatalf("truncate: %v", err)
			}
		}},
	} {
		t.Run(tamper.name, func(t *testing.T) {
			dir := t.TempDir()
			st, err := spool.Open(testOptions(t, dir))
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			for i := 0; i < 10; i++ {
				if err := st.Put([]byte(fmt.Sprintf("k%d", i)), make([]byte, 100)); err != nil {
					t.Fatalf("Put: %v", err)
				}
				if err := st.Flush(); err != nil {
					t.Fatalf("Flush: %v", err)
				}
			}
			if err := st.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			tamper.fn(t, filepath.Join(dir, "segments"))
			if tamper.name == "add file" {
				st, err := spool.Open(testOptions(t, dir))
				if err != nil {
					t.Fatalf("open after sweep: %v", err)
				}
				st.Close()
				if _, err := os.Stat(filepath.Join(dir, "segments", "000000000002.spool")); !os.IsNotExist(err) {
					t.Fatalf("unlisted file not swept")
				}
				return
			}
			if _, err := spool.Open(testOptions(t, dir)); !errors.Is(err, spool.ErrCorrupt) {
				t.Fatalf("open err = %v, want ErrCorrupt", err)
			}
		})
	}
}

// TestMidFileCorruptionFails corrupts a complete block followed by
// more blocks: unlike a torn tail, this must fail loudly.
func TestMidFileCorruptionFails(t *testing.T) {
	dir := t.TempDir()
	st, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < 3; i++ {
		if err := st.Put([]byte(fmt.Sprintf("k%d", i)), []byte("v")); err != nil {
			t.Fatalf("Put: %v", err)
		}
		if err := st.Flush(); err != nil {
			t.Fatalf("Flush: %v", err)
		}
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	seg := filepath.Join(dir, "segments", "000000000001.spool")
	raw, err := os.ReadFile(seg)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	// Corrupt the first block's payload (more blocks follow it).
	raw[63+5] ^= 0xFF
	if err := os.WriteFile(seg, raw, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := spool.Open(testOptions(t, dir)); err == nil {
		t.Fatalf("open with mid-file corruption succeeded, want error")
	} else if !errors.Is(err, spool.ErrAuthFailed) && !errors.Is(err, spool.ErrCorrupt) {
		t.Fatalf("open err = %v, want auth/corruption failure", err)
	}
}
