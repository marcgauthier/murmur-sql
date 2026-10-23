package spool_test

import (
	"testing"

	"github.com/marcgauthier/murmur-sql/murmur/spool"
)

// TestDiagnostics exercises the info, file, and latency surfaces:
// identity and positions, per-file ratios, and counters that move
// with operations.
func TestDiagnostics(t *testing.T) {
	dir := t.TempDir()
	o := testOptions(t, dir)
	o.WrappingKeyID = "diag-key"
	st, err := spool.Open(o)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()
	if got := st.Stats().IndexBytesEstimate; got != 0 {
		t.Fatalf("empty index estimate = %d, want 0", got)
	}
	for i := 0; i < 10; i++ {
		if err := st.Put([]byte{byte('a' + i)}, []byte("v")); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := st.Delete([]byte("a")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := st.Commit([]spool.Mutation{{Key: []byte("c"), Value: []byte("v")}}, spool.DurabilitySync); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := st.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := st.Reclaim(); err != nil {
		t.Fatalf("Reclaim: %v", err)
	}
	stats := st.Stats()
	if stats.Commits < 1 || stats.Syncs < 1 || stats.Flushes < 1 || stats.Reclaims < 1 {
		t.Fatalf("counters = %+v, want commits+syncs+flushes+reclaims", stats)
	}
	if stats.IndexBytesEstimate == 0 {
		t.Fatal("index estimate = 0 with keys loaded")
	}
	info := st.Info()
	if !info.Encrypted || info.Encryption != spool.EncryptionAES256GCM {
		t.Fatalf("info = %+v, want encrypted AES-GCM", info)
	}
	if info.Compression != o.Compression {
		t.Fatalf("compression = %v, want %v", info.Compression, o.Compression)
	}
	if info.WrappingKeyID != "diag-key" {
		t.Fatalf("wrapping id = %q", info.WrappingKeyID)
	}
	var zero [16]byte
	if info.StoreID == zero {
		t.Fatal("zero store id")
	}
	if info.Generation == 0 || info.Epoch == 0 || info.LastGroupID == 0 {
		t.Fatalf("info = %+v, want generation+epoch+group", info)
	}
	if info.NextFileID <= info.ActiveFileID || info.ActiveFileID == 0 {
		t.Fatalf("info = %+v, want next > active > 0", info)
	}
	files := st.FileStats()
	if len(files) == 0 {
		t.Fatal("no file stats")
	}
	for i := 1; i < len(files); i++ {
		if files[i].FileID <= files[i-1].FileID {
			t.Fatalf("file stats not ascending: %+v", files)
		}
	}
	var total, live uint64
	for _, f := range files {
		total += f.TotalRecords
		live += f.LiveRecords
		if f.LiveRecords+f.DeadRecords > f.TotalRecords {
			t.Fatalf("file %+v overcounts", f)
		}
	}
	if total == 0 || live == 0 || live > total {
		t.Fatalf("total=%d live=%d, want 0 < live <= total", total, live)
	}
	if live != stats.LiveRecords || total < stats.Keys {
		t.Fatalf("files live=%d total=%d vs stats %+v", live, total, stats)
	}
}
