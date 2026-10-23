package murmur

import (
	"strings"
	"testing"
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
