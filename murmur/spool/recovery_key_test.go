package spool_test

import (
	"testing"

	"github.com/marcgauthier/murmur-sql/murmur/spool"
)

func TestRecoveryKeyRoundTrip(t *testing.T) {
	dir := t.TempDir()
	st, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()
	id, key, err := st.CurrentRecoveryKey()
	if err != nil {
		t.Fatalf("CurrentRecoveryKey: %v", err)
	}
	again, err := st.RecoveryKey(id)
	if err != nil {
		t.Fatalf("RecoveryKey: %v", err)
	}
	if again != key {
		t.Fatal("recovery key not deterministic per id")
	}
	if _, err := st.RecoveryKey(id + 1000); err == nil {
		t.Fatal("unknown key id accepted")
	}
	// Rotation retires the id but keeps it resolvable.
	if err := st.RotateKey(); err != nil {
		t.Fatalf("RotateKey: %v", err)
	}
	id2, key2, err := st.CurrentRecoveryKey()
	if err != nil {
		t.Fatalf("CurrentRecoveryKey: %v", err)
	}
	if id2 == id {
		t.Fatal("rotation kept current key id")
	}
	if key2 == key {
		t.Fatal("rotation kept recovery key")
	}
	if old, err := st.RecoveryKey(id); err != nil || old != key {
		t.Fatalf("retired id: %v", err)
	}
}

func TestRecoveryKeyUnencrypted(t *testing.T) {
	dir := t.TempDir()
	o := testOptions(t, dir)
	o.Encryption = spool.EncryptionNone
	o.MasterKey = nil
	st, err := spool.Open(o)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()
	if _, _, err := st.CurrentRecoveryKey(); err == nil {
		t.Fatal("unencrypted store yielded recovery key")
	}
	if _, err := st.RecoveryKey(0); err == nil {
		t.Fatal("unencrypted store resolved key id")
	}
}

func TestRecoveryKeyClosed(t *testing.T) {
	dir := t.TempDir()
	st, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, _, err := st.CurrentRecoveryKey(); err != spool.ErrClosed {
		t.Fatalf("err = %v, want ErrClosed", err)
	}
	if _, err := st.RecoveryKey(0); err != spool.ErrClosed {
		t.Fatalf("err = %v, want ErrClosed", err)
	}
}
