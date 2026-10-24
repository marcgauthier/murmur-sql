package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/marcgauthier/murmur-sql/murmur"
	"github.com/marcgauthier/murmur-sql/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql/murmur/spool"
)

var adminBin string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "murmur-admin-test")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	adminBin = filepath.Join(dir, "murmur-admin")
	build := exec.Command("go", "build", "-o", adminBin, ".")
	if out, err := build.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "build murmur-admin: %v\n%s", err, out)
		os.RemoveAll(dir)
		os.Exit(1)
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

func runAdmin(t *testing.T, args ...string) (stdout, stderr string, exit int) {
	t.Helper()
	cmd := exec.Command(adminBin, args...)
	var so, se bytes.Buffer
	cmd.Stdout = &so
	cmd.Stderr = &se
	if err := cmd.Run(); err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			return so.String(), se.String(), ee.ExitCode()
		}
		t.Fatalf("run murmur-admin: %v", err)
	}
	return so.String(), se.String(), 0
}

// makeDB creates a database under path with the CLI default key and
// writes batches of rows in separate transactions (separate commit
// groups), churning each batch once to leave reclaimable garbage.
func makeDB(t *testing.T, path string, batches, perBatch int) {
	t.Helper()
	options := murmur.DefaultOptions()
	options.Mode = murmur.OpenCreateNew
	db, err := murmur.Open(path, defaultKey[:], options)
	if err != nil {
		t.Fatalf("create db: %v", err)
	}
	if err := db.Migrate(murmur.NewMigration("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")); err != nil {
		t.Fatalf("create table: %v", err)
	}
	for b := 0; b < batches; b++ {
		base := b * perBatch
		_, err = db.Transaction(func(s *murmur.Sql) (any, error) {
			for i := 0; i < perBatch; i++ {
				if _, err := s.Execute("INSERT INTO t (v) VALUES (?)", fmt.Sprintf("v%d", base+i)); err != nil {
					return nil, err
				}
			}
			return nil, nil
		})
		if err != nil {
			t.Fatalf("insert batch %d: %v", b, err)
		}
		_, err = db.Transaction(func(s *murmur.Sql) (any, error) {
			if _, err := s.Execute("UPDATE t SET v = v || '-churned' WHERE id > ? AND id <= ?", base, base+perBatch); err != nil {
				return nil, err
			}
			return nil, nil
		})
		if err != nil {
			t.Fatalf("churn batch %d: %v", b, err)
		}
		// Seal each batch into its own commit group(s): checkpoint
		// captures the batch delta and the barrier seals it, so
		// batches land in order in distinct groups.
		if err := db.Flush(); err != nil {
			t.Fatalf("flush batch %d: %v", b, err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// rowCount reopens path and counts rows in t.
func rowCount(t *testing.T, path string) int64 {
	t.Helper()
	options := murmur.DefaultOptions()
	options.Mode = murmur.OpenExisting
	db, err := murmur.Open(path, defaultKey[:], options)
	if err != nil {
		t.Fatalf("reopen %s: %v", path, err)
	}
	defer db.Close()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.QueryRow("SELECT COUNT(*) FROM t")
	})
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	return v.([]sqlite.Value)[0].Int
}

func TestCompactQuiesces(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")
	makeDB(t, path, 4, 50)

	stdout, stderr, exit := runAdmin(t, "compact", path)
	if exit != 0 {
		t.Fatalf("compact: exit %d: %s", exit, stderr)
	}
	if !strings.Contains(stdout, "PASS") || !strings.Contains(stdout, "quiesced") {
		t.Fatalf("compact output missing PASS/quiesced:\n%s", stdout)
	}

	stdout, stderr, exit = runAdmin(t, "--json", "compact", path)
	if exit != 0 {
		t.Fatalf("compact --json: exit %d: %s", exit, stderr)
	}
	var rep compactReport
	if err := json.Unmarshal([]byte(stdout), &rep); err != nil {
		t.Fatalf("compact --json: %v:\n%s", err, stdout)
	}
	if !rep.Quiesced {
		t.Fatalf("second compact should quiesce immediately: %+v", rep)
	}
	// The second run must be a single no-work pass.
	if rep.Passes != 1 {
		t.Fatalf("second compact passes = %d, want 1: %+v", rep.Passes, rep)
	}

	// The compacted store still verifies and keeps every row.
	if _, stderr, exit := runAdmin(t, "verify", path); exit != 0 {
		t.Fatalf("verify after compact: exit %d: %s", exit, stderr)
	}
	if n := rowCount(t, path); n != 200 {
		t.Fatalf("compacted store has %d rows, want 200", n)
	}
}

func TestCompactRefusesLiveStore(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")
	makeDB(t, path, 1, 10)

	options := murmur.DefaultOptions()
	options.Mode = murmur.OpenExisting
	db, err := murmur.Open(path, defaultKey[:], options)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db.Close()

	_, stderr, exit := runAdmin(t, "compact", path)
	if exit == 0 {
		t.Fatal("compact on a live store should fail")
	}
	if !strings.Contains(stderr, "locked") {
		t.Fatalf("expected lock error, got: %s", stderr)
	}
}

// makeSpoolFixture builds a raw spool store (no murmur schema)
// with reclaimable garbage across sealed segments: small segments
// force rotation, and five full overwrites leave ~83% of every
// file dead, past the default 0.20 compaction threshold that
// compact runs with. compact operates on any spool directory, so
// no murmur layer is needed to exercise real work.
func makeSpoolFixture(t *testing.T, path string) {
	t.Helper()
	opts := spool.DefaultOptions(path)
	opts.MasterKey = append([]byte(nil), defaultKey[:]...)
	opts.TargetBlockBytes = 1024
	opts.MaxBlockBytes = 4096
	opts.MaxSegmentSize = 8192
	opts.MaxKeySize = 128
	opts.MaxValueSize = 1024
	st, err := spool.Open(opts)
	if err != nil {
		t.Fatalf("open spool: %v", err)
	}
	for i := 0; i < 300; i++ {
		if err := st.Put([]byte(fmt.Sprintf("k%04d", i)), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	for round := 0; round < 5; round++ {
		for i := 0; i < 300; i++ {
			if err := st.Put([]byte(fmt.Sprintf("k%04d", i)), []byte(fmt.Sprintf("v%d", round))); err != nil {
				t.Fatalf("overwrite: %v", err)
			}
		}
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close spool: %v", err)
	}
}

func TestCompactReclaimsGarbage(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "spool")
	makeSpoolFixture(t, path)

	stdout, stderr, exit := runAdmin(t, "--json", "compact", path)
	if exit != 0 {
		t.Fatalf("compact: exit %d: %s", exit, stderr)
	}
	var rep compactReport
	if err := json.Unmarshal([]byte(stdout), &rep); err != nil {
		t.Fatalf("compact --json: %v:\n%s", err, stdout)
	}
	if !rep.Quiesced {
		t.Fatalf("expected quiescence: %+v", rep)
	}
	if rep.Passes < 2 {
		t.Fatalf("expected multiple passes, got %d: %+v", rep.Passes, rep)
	}
	if rep.Compactions < 1 {
		t.Fatalf("expected rewrites, got %+v", rep)
	}
	if rep.DeadRecordsAfter >= rep.DeadRecordsBefore {
		t.Fatalf("dead records not reduced: %+v", rep)
	}
}

func TestCompactMaxPasses(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "spool")
	makeSpoolFixture(t, path)

	stdout, stderr, exit := runAdmin(t, "--json", "compact", "--max-passes=1", path)
	if exit != 0 {
		t.Fatalf("compact --max-passes=1: exit %d: %s", exit, stderr)
	}
	var rep compactReport
	if err := json.Unmarshal([]byte(stdout), &rep); err != nil {
		t.Fatalf("compact --json: %v:\n%s", err, stdout)
	}
	if rep.Quiesced {
		t.Fatalf("single pass over garbage should cap, not quiesce: %+v", rep)
	}
	if rep.Passes != 1 {
		t.Fatalf("passes = %d, want 1: %+v", rep.Passes, rep)
	}
}

func TestRepairCleanStore(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")
	makeDB(t, path, 2, 25)
	dest := filepath.Join(dir, "fixed")

	stdout, stderr, exit := runAdmin(t, "repair", path, "--out", dest)
	if exit != 0 {
		t.Fatalf("repair: exit %d: %s", exit, stderr)
	}
	if !strings.Contains(stdout, "no gaps") {
		t.Fatalf("clean repair should report no gaps:\n%s", stdout)
	}
	if _, stderr, exit := runAdmin(t, "verify", dest); exit != 0 {
		t.Fatalf("verify repaired: exit %d: %s", exit, stderr)
	}
	if n := rowCount(t, dest); n != 50 {
		t.Fatalf("repaired store has %d rows, want 50", n)
	}
}

func TestRepairCorruptStore(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")
	makeDB(t, path, 4, 50)

	// Corrupt late in the single segment file. Batches land in
	// order in distinct groups, and framing recovery only skips
	// forward, so the first two batches (before ~50% of bytes)
	// provably survive damage at 85%.
	p := filepath.Join(path, "segments", "000000000001.spool")
	raw, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("read segment: %v", err)
	}
	off := len(raw) * 85 / 100
	for i := 0; i < 128 && off+i < len(raw); i++ {
		raw[off+i] ^= 0xff
	}
	if err := os.WriteFile(p, raw, 0o600); err != nil {
		t.Fatalf("write segment: %v", err)
	}

	// Anchor: the corruption must actually break normal opens.
	if _, _, exit := runAdmin(t, "verify", path); exit == 0 {
		t.Fatal("verify succeeded on corrupted store; fixture is not corrupt")
	}

	dest := filepath.Join(dir, "fixed")
	stdout, stderr, exit := runAdmin(t, "--json", "repair", path, "--out", dest)
	if exit != 0 {
		t.Fatalf("repair: exit %d: %s", exit, stderr)
	}
	var rep repairReport
	if err := json.Unmarshal([]byte(stdout), &rep); err != nil {
		t.Fatalf("repair --json: %v:\n%s", err, stdout)
	}
	if rep.GapCount == 0 {
		t.Fatalf("expected gaps, got: %+v", rep)
	}
	if rep.Records == 0 {
		t.Fatal("expected some records to be recovered")
	}
	if !rep.Verified || rep.DatabaseID == nil {
		t.Fatalf("expected verified repair: %+v", rep)
	}
	// The first two batches sit before ~50% of bytes, far from
	// the damage at 85%; both survive with all their rows.
	if n := rowCount(t, dest); n < 100 {
		t.Fatalf("expected >= 100 surviving rows, got %d", n)
	}
}

func TestRepairRefusesExistingDest(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")
	makeDB(t, path, 1, 10)
	dest := filepath.Join(dir, "fixed")

	if _, stderr, exit := runAdmin(t, "repair", path, "--out", dest); exit != 0 {
		t.Fatalf("first repair: exit %d: %s", exit, stderr)
	}
	_, stderr, exit := runAdmin(t, "repair", path, "--out", dest)
	if exit == 0 {
		t.Fatal("second repair into existing dest should fail")
	}
	if !strings.Contains(stderr, "already exists") {
		t.Fatalf("expected exists error, got: %s", stderr)
	}
}
