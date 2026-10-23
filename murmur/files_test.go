package murmur

import (
	"bytes"
	"net/netip"
	"strings"
	"testing"

	"github.com/marcgauthier/murmur-sql/murmur/replication"
)

func TestFileAvailabilityRequiresStore(t *testing.T) {
	db := openTestDB(t, t.TempDir()+"/db")
	defer db.Close()
	// No replication configured: every file API reports it.
	if _, err := db.FileAvailability("f"); err == nil ||
		!strings.Contains(err.Error(), "not configured") {
		t.Fatalf("availability without store = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.FileAvailability("f"); err != ErrClosed {
		t.Fatalf("availability after close = %v", err)
	}
}

func TestFileAPIsRequireStore(t *testing.T) {
	db := openTestDB(t, t.TempDir()+"/db")
	defer db.Close()
	var sink bytes.Buffer
	checks := map[string]func() error{
		"upload":   func() error { _, err := db.UploadFile("f", bytes.NewReader(nil)); return err },
		"list":     func() error { _, err := db.ListFiles(); return err },
		"search":   func() error { _, err := db.SearchFiles("f"); return err },
		"metadata": func() error { _, err := db.FileMetadata("f"); return err },
		"get":      func() error { _, err := db.GetFile("f", &sink); return err },
		"status":   func() error { _, err := db.FileKeyRotationStatus(); return err },
		"delete":   func() error { return db.DeleteFile("f") },
		"avail": func() error {
			_, err := db.FileAvailable("f")
			return err
		},
	}
	for name, check := range checks {
		if err := check(); err == nil || !strings.Contains(err.Error(), "not configured") {
			t.Fatalf("%s without store = %v", name, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	for name, check := range checks {
		if err := check(); err != ErrClosed {
			t.Fatalf("%s after close = %v", name, err)
		}
	}
}

func TestFileAPIsDelegateToStore(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping live replication test in short mode")
	}
	dir := t.TempDir()
	idA := compatNodeID(0xA1)
	dbID := compatNodeID(0xD4)
	cfg := compatConfigs(t, [][16]byte{idA})[0]
	var fileKey [32]byte
	for i := range fileKey {
		fileKey[i] = 0x46
	}
	cfg.Files = replication.NewFilesConfig(dir+"/objects", fileKey, netip.MustParseAddrPort("127.0.0.1:0"))
	options := DefaultOptions()
	options.Mode = OpenCreateNew
	options.DatabaseID = &dbID
	options.Replication = cfg
	db, err := Open(dir+"/db", testKey(), options)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	data := []byte("delegated-bytes")
	meta, err := db.UploadFile("doc", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	if meta.Bytes != uint64(len(data)) {
		t.Fatalf("bytes = %d", meta.Bytes)
	}
	if _, err := db.UploadFile("", bytes.NewReader(data)); err == nil {
		t.Fatal("empty name accepted")
	}
	listed, err := db.ListFiles()
	if err != nil || len(listed) != 1 || listed[0].Name != "doc" {
		t.Fatalf("list = %+v, %v", listed, err)
	}
	found, err := db.SearchFiles("do")
	if err != nil || len(found) != 1 {
		t.Fatalf("search hit = %+v, %v", found, err)
	}
	missed, err := db.SearchFiles("zzz")
	if err != nil || len(missed) != 0 {
		t.Fatalf("search miss = %+v, %v", missed, err)
	}
	got, err := db.FileMetadata("doc")
	if err != nil || got == nil || got.Digest != meta.Digest {
		t.Fatalf("metadata = %+v, %v", got, err)
	}
	absent, err := db.FileMetadata("missing")
	if err != nil || absent != nil {
		t.Fatalf("missing metadata = %+v, %v", absent, err)
	}
	var out bytes.Buffer
	if _, err := db.GetFile("doc", &out); err != nil || !bytes.Equal(out.Bytes(), data) {
		t.Fatalf("get = %d bytes, %v", out.Len(), err)
	}
	if _, err := db.GetFile("missing", &out); err == nil {
		t.Fatal("missing download accepted")
	}
	available, err := db.FileAvailable("doc")
	if err != nil || !available {
		t.Fatalf("available = %v, %v", available, err)
	}
	availability, err := db.FileAvailability("doc")
	if err != nil || availability == nil || availability.Kind != replication.FileLocal {
		t.Fatalf("availability = %+v, %v", availability, err)
	}
	status, err := db.FileKeyRotationStatus()
	if err != nil || status == nil || status.CurrentGeneration != 0 {
		t.Fatalf("rotation status = %+v, %v", status, err)
	}
	if err := db.DeleteFile("doc"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if meta, err := db.FileMetadata("doc"); err != nil || meta != nil {
		t.Fatalf("metadata after delete = %+v, %v", meta, err)
	}
}
