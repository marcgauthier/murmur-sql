package murmur

import (
	"testing"
)

func TestCaptureTables(t *testing.T) {
	c := openItems(t)
	cap, err := NewCapture(c, []string{"items"})
	if err != nil {
		t.Fatal(err)
	}
	tables := cap.Tables()
	if len(tables) != 1 || tables[0] != "items" {
		t.Fatalf("tables = %v", tables)
	}
	cap.Close()
	// Closed sessions report empty state instead of panicking.
	if got := cap.Size(); got != 0 {
		t.Fatalf("size after close = %d", got)
	}
	if got := cap.Memory(); got != 0 {
		t.Fatalf("memory after close = %d", got)
	}
	if buf, err := cap.Bytes(); err != nil || buf != nil {
		t.Fatalf("bytes after close = %d, %v", len(buf), err)
	}
	cap.Close()
}
