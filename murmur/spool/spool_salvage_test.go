package spool_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/marcgauthier/murmur-sql/murmur/spool"
)

func TestSalvageCleanStore(t *testing.T) {
	dir := t.TempDir()
	opts := testOptions(t, dir)
	opts.TargetBlockBytes = 1024
	opts.MaxBlockBytes = 4096
	opts.MaxKeySize = 128
	opts.MaxValueSize = 1024
	opts.MaxSegmentSize = 8192 // force several segments
	st, err := spool.Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	const n = 300
	for i := 0; i < n; i++ {
		if err := st.Put([]byte(fmt.Sprintf("k%04d", i)), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	// Overwrite some keys and delete others: salvage must preserve the
	// latest version of each key, tombstones included.
	for i := 0; i < 50; i++ {
		if err := st.Put([]byte(fmt.Sprintf("k%04d", i)), []byte("v2")); err != nil {
			t.Fatalf("Put overwrite: %v", err)
		}
	}
	for i := 50; i < 100; i++ {
		if err := st.Delete([]byte(fmt.Sprintf("k%04d", i))); err != nil {
			t.Fatalf("Delete: %v", err)
		}
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	want := loadAll(t, dir)

	dest := filepath.Join(t.TempDir(), "salvaged")
	rep, err := spool.Salvage(spool.SalvageOptions{SrcDir: dir, DestDir: dest, MasterKey: testMasterKey})
	if err != nil {
		t.Fatalf("Salvage: %v", err)
	}
	if len(rep.Gaps) != 0 {
		t.Fatalf("clean store: expected no gaps, got %+v", rep.Gaps)
	}
	if rep.Records == 0 {
		t.Fatal("clean store: expected recovered records, got 0")
	}
	// Salvage copies raw records (including superseded versions), so the
	// recovered count is >= the live key count.
	if rep.Records < int64(len(want)) {
		t.Fatalf("clean store: recovered %d records, want >= %d", rep.Records, len(want))
	}
	got := loadAll(t, dest)
	if len(got) != len(want) {
		t.Fatalf("dest has %d live keys, want %d", len(got), len(want))
	}
	for k, v := range want {
		if gv, ok := got[k]; !ok || string(gv) != string(v) {
			t.Fatalf("key %q: got %q,%v want %q", k, gv, ok, v)
		}
	}
	// Salvage must not modify the source directory.
	if got2 := loadAll(t, dir); len(got2) != len(want) {
		t.Fatal("source directory changed by Salvage")
	}
	// Sequence continuity: the destination reopens, and a new
	// write sorts after every salvaged version.
	db, err := spool.Open(testOptions(t, dest))
	if err != nil {
		t.Fatalf("reopen salvaged store: %v", err)
	}
	if err := db.Put([]byte("k0000"), []byte("v3")); err != nil {
		t.Fatalf("Put after salvage: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close after salvage: %v", err)
	}
	got3 := loadAll(t, dest)
	if string(got3["k0000"]) != "v3" {
		t.Fatalf("post-salvage write lost: k0000=%q want v3", got3["k0000"])
	}
}

func TestSalvageSurvivesCorruptMiddle(t *testing.T) {
	dir := t.TempDir()
	// Four open/write/close cycles: four commit groups (one per
	// generation) and four sequence epochs, so central damage
	// cannot destroy every group. Multi-epoch sources also
	// exercise the destination epoch bump.
	const n = 200
	for batch := 0; batch < 4; batch++ {
		st, err := spool.Open(testOptions(t, dir))
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		for i := batch * 50; i < batch*50+50; i++ {
			if err := st.Put([]byte(fmt.Sprintf("k%04d", i)), []byte(fmt.Sprintf("v%d", i))); err != nil {
				t.Fatalf("Put: %v", err)
			}
		}
		if err := st.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}

	// Corrupt the middle of the single segment file.
	p := filepath.Join(dir, "segments", "000000000001.spool")
	raw, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	mid := len(raw) / 2
	for i := 0; i < 128 && mid+i < len(raw); i++ {
		raw[mid+i] ^= 0xff
	}
	if err := os.WriteFile(p, raw, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	// Anchor: the corruption must actually break normal loading.
	loadErr := spool.Load(dir, testMasterKey, func([]spool.Record) error { return nil })
	if loadErr == nil {
		t.Fatal("Load succeeded on corrupted store; fixture is not corrupt")
	}

	dest := filepath.Join(t.TempDir(), "salvaged")
	rep, err := spool.Salvage(spool.SalvageOptions{SrcDir: dir, DestDir: dest, MasterKey: testMasterKey})
	if err != nil {
		t.Fatalf("Salvage: %v", err)
	}
	if len(rep.Gaps) == 0 {
		t.Fatal("expected gaps to be recorded for corrupted region")
	}
	if rep.Records == 0 {
		t.Fatal("expected some records to be recovered")
	}
	got := loadAll(t, dest)
	// The first and last groups sit at the file ends, far from the
	// central damage; both must survive.
	if gv, ok := got["k0000"]; !ok || string(gv) != "v0" {
		t.Fatalf("k0000: got %q,%v want v0,true", gv, ok)
	}
	if gv, ok := got["k0199"]; !ok || string(gv) != "v199" {
		t.Fatalf("k0199: got %q,%v want v199,true", gv, ok)
	}
	// At most the two central groups can be lost to 128 central
	// bytes: at least half the keys survive.
	if len(got) < n/2 {
		t.Fatalf("only %d of %d keys survived", len(got), n)
	}
}

func TestSalvageTornTail(t *testing.T) {
	dir := t.TempDir()
	st, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	const n = 50
	for i := 0; i < n; i++ {
		if err := st.Put([]byte(fmt.Sprintf("k%04d", i)), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Simulate a torn tail: garbage bytes the writer never finished.
	p := filepath.Join(dir, "segments", "000000000001.spool")
	f, err := os.OpenFile(p, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if _, err := f.Write([]byte("PARTIAL-WRITE-GARBAGE-TAIL-BY")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	dest := filepath.Join(t.TempDir(), "salvaged")
	rep, err := spool.Salvage(spool.SalvageOptions{SrcDir: dir, DestDir: dest, MasterKey: testMasterKey})
	if err != nil {
		t.Fatalf("Salvage: %v", err)
	}
	if len(rep.Gaps) == 0 {
		t.Fatal("expected a gap for the torn tail")
	}
	got := loadAll(t, dest)
	if len(got) != n {
		t.Fatalf("dest has %d keys, want all %d", len(got), n)
	}
}

func TestSalvageWrongKey(t *testing.T) {
	dir := t.TempDir()
	st, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	bad := make([]byte, 32)
	for i := range bad {
		bad[i] = byte(i + 1)
	}
	dest := filepath.Join(t.TempDir(), "salvaged")
	if _, err := spool.Salvage(spool.SalvageOptions{SrcDir: dir, DestDir: dest, MasterKey: bad}); err == nil {
		t.Fatal("expected error with wrong key")
	}
}

func TestSalvageRefusesOccupiedDest(t *testing.T) {
	dir := t.TempDir()
	st, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	dest := t.TempDir()
	if err := os.WriteFile(filepath.Join(dest, "junk"), []byte("x"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if _, err := spool.Salvage(spool.SalvageOptions{SrcDir: dir, DestDir: dest, MasterKey: testMasterKey}); err == nil {
		t.Fatal("expected error for non-empty dest dir")
	}
}

func TestSalvageRefusesLiveStore(t *testing.T) {
	dir := t.TempDir()
	st, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()
	if err := st.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	dest := filepath.Join(t.TempDir(), "salvaged")
	_, err = spool.Salvage(spool.SalvageOptions{SrcDir: dir, DestDir: dest, MasterKey: testMasterKey})
	if err == nil {
		t.Fatal("expected error salvaging a live (locked) store")
	}
	if !errors.Is(err, spool.ErrLocked) && !strings.Contains(err.Error(), "lock") {
		t.Fatalf("expected lock error, got: %v", err)
	}
}

func TestReclaimStepReportsWork(t *testing.T) {
	dir := t.TempDir()
	opts := testOptions(t, dir)
	opts.TargetBlockBytes = 1024
	opts.MaxBlockBytes = 4096
	opts.MaxKeySize = 128
	opts.MaxValueSize = 1024
	opts.MaxSegmentSize = 8192
	st, err := spool.Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	const n = 300
	for i := 0; i < n; i++ {
		if err := st.Put([]byte(fmt.Sprintf("k%04d", i)), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	// Create garbage: overwrite half, delete a quarter.
	for i := 0; i < n/2; i++ {
		if err := st.Put([]byte(fmt.Sprintf("k%04d", i)), []byte("v2")); err != nil {
			t.Fatalf("Put overwrite: %v", err)
		}
	}
	for i := n / 2; i < 3*n/4; i++ {
		if err := st.Delete([]byte(fmt.Sprintf("k%04d", i))); err != nil {
			t.Fatalf("Delete: %v", err)
		}
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopts := testOptions(t, dir)
	reopts.CompactionThreshold = 0.95 // any file with 5% garbage qualifies
	st, err = spool.Open(reopts)
	if err != nil {
		t.Fatalf("re-Open: %v", err)
	}
	sawWork := false
	steps := 0
	for i := 0; i < 50; i++ {
		worked, err := st.ReclaimStep()
		if err != nil {
			t.Fatalf("ReclaimStep: %v", err)
		}
		steps++
		if !worked {
			break
		}
		sawWork = true
	}
	if !sawWork {
		t.Fatal("expected ReclaimStep to report work on a store with garbage")
	}
	// Quiescence: once it reports no work, it keeps reporting no work.
	if worked, err := st.ReclaimStep(); err != nil || worked {
		t.Fatalf("expected quiescent ReclaimStep, got worked=%v err=%v", worked, err)
	}
	t.Logf("quiesced after %d steps", steps)
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	got := loadAll(t, dir)
	if len(got) != n-n/4 {
		t.Fatalf("live keys: got %d want %d", len(got), n-n/4)
	}
	if gv := got["k0000"]; string(gv) != "v2" {
		t.Fatalf("k0000: got %q want v2", gv)
	}
}
