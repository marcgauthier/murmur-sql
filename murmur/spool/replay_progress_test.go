package spool_test

import (
	"fmt"
	"testing"

	"github.com/marcgauthier/murmur-sql-go/murmur/spool"
)

// TestOpenReplayProgress proves reopening a store reports advancing
// replay snapshots: files/bytes/records move while the scan works, so
// watchers can tell a slow open from a wedged one.
func TestOpenReplayProgress(t *testing.T) {
	dir := t.TempDir()
	st, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < 200; i++ {
		if err := st.Put([]byte(fmt.Sprintf("k%04d", i)), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	o := testOptions(t, dir)
	var events []spool.ReplayProgress
	o.OnReplayProgress = func(p spool.ReplayProgress) { events = append(events, p) }
	st2, err := spool.Open(o)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer st2.Close()
	if len(events) == 0 {
		t.Fatal("no replay events")
	}
	last := events[len(events)-1]
	if last.FilesTotal == 0 || last.FilesDone != last.FilesTotal {
		t.Fatalf("files = %d/%d", last.FilesDone, last.FilesTotal)
	}
	if last.BytesTotal == 0 || last.BytesDone == 0 || last.BytesDone > last.BytesTotal {
		t.Fatalf("bytes = %d/%d", last.BytesDone, last.BytesTotal)
	}
	if last.Records < 200 {
		t.Fatalf("records = %d, want >= 200", last.Records)
	}
	for i := 1; i < len(events); i++ {
		if events[i].Records < events[i-1].Records || events[i].BytesDone < events[i-1].BytesDone {
			t.Fatalf("replay regressed: %+v -> %+v", events[i-1], events[i])
		}
	}
}

// TestOpenReplayProgressDisabled proves a nil hook changes nothing: the
// scan reports nowhere and the store opens identically.
func TestOpenReplayProgressDisabled(t *testing.T) {
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
	st2, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer st2.Close()
	if got := st2.Stats().Keys; got != 1 {
		t.Fatalf("Keys = %d, want 1", got)
	}
}
