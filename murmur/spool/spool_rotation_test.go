package spool_test

import (
	"testing"

	"github.com/marcgauthier/murmur-sql-go/murmur/spool"
)

// TestKeyRotation writes data, rotates twice, and requires everything
// old and new to stay readable across a reopen.
func TestKeyRotation(t *testing.T) {
	dir := t.TempDir()
	st, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.Put([]byte("gen1"), []byte("v1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := st.RotateKey(); err != nil {
		t.Fatalf("RotateKey: %v", err)
	}
	if err := st.Put([]byte("gen2"), []byte("v2")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := st.RotateKey(); err != nil {
		t.Fatalf("RotateKey: %v", err)
	}
	if err := st.Put([]byte("gen3"), []byte("v3")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	got := loadAll(t, dir)
	for k, v := range map[string]string{"gen1": "v1", "gen2": "v2", "gen3": "v3"} {
		if string(got[k]) != v {
			t.Fatalf("%s = %q, want %q", k, got[k], v)
		}
	}
	// Reopen once more: the ring with three keys persists.
	st2, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer st2.Close()
	if err := st2.Put([]byte("gen4"), []byte("v4")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st2.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := st2.RotateKey(); err != nil {
		t.Fatalf("RotateKey after reopen: %v", err)
	}
}

func TestRotateKeyPlainStoreFails(t *testing.T) {
	dir := t.TempDir()
	o := testOptions(t, dir)
	o.Encryption = spool.EncryptionNone
	o.MasterKey = nil
	st, err := spool.Open(o)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()
	if err := st.RotateKey(); err == nil {
		t.Fatalf("RotateKey on plain store succeeded")
	}
}
