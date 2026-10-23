package spool_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/marcgauthier/murmur-sql/murmur/spool"
)

// TestPutBatchRoundTrip writes, overwrites and deletes through the
// batch APIs and requires the reloaded state to match.
func TestPutBatchRoundTrip(t *testing.T) {
	dir := t.TempDir()
	st, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	pairs := make([]spool.KV, 200)
	for i := range pairs {
		pairs[i] = spool.KV{
			Key:   []byte(fmt.Sprintf("k%03d", i)),
			Value: []byte(fmt.Sprintf("v%d", i)),
		}
	}
	if err := st.PutBatch(pairs); err != nil {
		t.Fatalf("PutBatch: %v", err)
	}
	// Overwrite half, delete a quarter.
	over := make([]spool.KV, 100)
	for i := range over {
		over[i] = spool.KV{Key: []byte(fmt.Sprintf("k%03d", i)), Value: []byte("new")}
	}
	if err := st.PutBatch(over); err != nil {
		t.Fatalf("PutBatch overwrite: %v", err)
	}
	del := make([][]byte, 50)
	for i := range del {
		del[i] = []byte(fmt.Sprintf("k%03d", 150+i))
	}
	if err := st.DeleteBatch(del); err != nil {
		t.Fatalf("DeleteBatch: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	got := loadAll(t, dir)
	if len(got) != 150 {
		t.Fatalf("loaded %d keys, want 150", len(got))
	}
	for i := 0; i < 100; i++ {
		if string(got[fmt.Sprintf("k%03d", i)]) != "new" {
			t.Fatalf("k%03d not overwritten", i)
		}
	}
	for i := 100; i < 150; i++ {
		if string(got[fmt.Sprintf("k%03d", i)]) != fmt.Sprintf("v%d", i) {
			t.Fatalf("k%03d damaged", i)
		}
	}
}

// TestPutBatchAdmission rejects oversize batches atomically: nothing
// is buffered when any entry violates a limit.
func TestPutBatchAdmission(t *testing.T) {
	dir := t.TempDir()
	o := testOptions(t, dir)
	o.MaxValueSize = 64
	st, err := spool.Open(o)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()
	bad := []spool.KV{
		{Key: []byte("good"), Value: []byte("x")},
		{Key: []byte("bad"), Value: make([]byte, 65)},
	}
	if err := st.PutBatch(bad); !errors.Is(err, spool.ErrValueTooLarge) {
		t.Fatalf("PutBatch err = %v, want ErrValueTooLarge", err)
	}
	if n := st.Stats().PendingRecords; n != 0 {
		t.Fatalf("PendingRecords = %d after rejected batch", n)
	}
	if err := st.DeleteBatch([][]byte{make([]byte, o.MaxKeySize+1)}); !errors.Is(err, spool.ErrKeyTooLarge) {
		t.Fatalf("DeleteBatch err = %v, want ErrKeyTooLarge", err)
	}
	if err := st.PutBatch(nil); err != nil {
		t.Fatalf("empty PutBatch: %v", err)
	}
	if err := st.DeleteBatch(nil); err != nil {
		t.Fatalf("empty DeleteBatch: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := st.PutBatch(bad[:1]); !errors.Is(err, spool.ErrClosed) {
		t.Fatalf("PutBatch after close = %v, want ErrClosed", err)
	}
}

// TestRotateMasterKey re-protects the keyring: the new material
// opens the store with all data intact, the old material fails.
func TestRotateMasterKey(t *testing.T) {
	dir := t.TempDir()
	st, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.RotateKey(); err != nil {
		t.Fatalf("RotateKey: %v", err)
	}
	if err := st.Put([]byte("k2"), []byte("v2")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	newMaster := make([]byte, 32)
	for i := range newMaster {
		newMaster[i] = byte(255 - i)
	}
	if err := st.RotateMasterKey(newMaster, ""); err != nil {
		t.Fatalf("RotateMasterKey: %v", err)
	}
	// Rotation must not disturb the data-key line: rotate again and
	// keep writing under the new protector.
	if err := st.RotateKey(); err != nil {
		t.Fatalf("RotateKey after master rotation: %v", err)
	}
	if err := st.Put([]byte("k3"), []byte("v3")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Old material fails.
	o := testOptions(t, dir)
	if _, err := spool.Open(o); !errors.Is(err, spool.ErrWrongKey) {
		t.Fatalf("open with old key err = %v, want ErrWrongKey", err)
	}
	// New material opens with everything intact.
	o.MasterKey = newMaster
	st2, err := spool.Open(o)
	if err != nil {
		t.Fatalf("open with new key: %v", err)
	}
	st2.Close()
	got := map[string][]byte{}
	seqs := map[string]uint64{}
	if err := spool.LoadWithOptions(spool.LoadOptions{Path: dir, MasterKey: newMaster}, func(recs []spool.Record) error {
		for _, r := range recs {
			if r.Sequence < seqs[string(r.Key)] {
				continue
			}
			seqs[string(r.Key)] = r.Sequence
			got[string(r.Key)] = append([]byte(nil), r.Value...)
		}
		return nil
	}); err != nil {
		t.Fatalf("Load: %v", err)
	}
	for k, v := range map[string]string{"k": "v", "k2": "v2", "k3": "v3"} {
		if string(got[k]) != v {
			t.Fatalf("%s = %q, want %q", k, got[k], v)
		}
	}
}

// TestRotateMasterKeyValidation covers protector switching and the
// rejection cases.
func TestRotateMasterKeyValidation(t *testing.T) {
	dir := t.TempDir()
	st, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()
	if err := st.RotateMasterKey(nil, ""); err == nil {
		t.Fatalf("empty rotation accepted")
	}
	if err := st.RotateMasterKey(testMasterKey, "x"); err == nil {
		t.Fatalf("dual-material rotation accepted")
	}
	if err := st.RotateMasterKey([]byte("short"), ""); err == nil {
		t.Fatalf("short master rotation accepted")
	}
	// Switch master key -> passphrase and back.
	if err := st.RotateMasterKey(nil, "correct horse battery staple"); err != nil {
		t.Fatalf("switch to passphrase: %v", err)
	}
	if err := st.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	o := testOptions(t, dir)
	o.MasterKey = nil
	o.Passphrase = "correct horse battery staple"
	st2, err := spool.Open(o)
	if err != nil {
		t.Fatalf("open with passphrase: %v", err)
	}
	defer st2.Close()
	if err := st2.RotateMasterKey(testMasterKey, ""); err != nil {
		t.Fatalf("switch back to master key: %v", err)
	}

	plainDir := t.TempDir()
	po := testOptions(t, plainDir)
	po.Encryption = spool.EncryptionNone
	po.MasterKey = nil
	plain, err := spool.Open(po)
	if err != nil {
		t.Fatalf("open plain: %v", err)
	}
	defer plain.Close()
	if err := plain.RotateMasterKey(testMasterKey, ""); err == nil {
		t.Fatalf("rotation on plain store accepted")
	}
}
