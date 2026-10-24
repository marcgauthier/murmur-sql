package spool_test

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"bytes"

	"github.com/marcgauthier/murmur-sql/murmur/spool"
)

func TestValidateWrappingKeyAuthenticatesEnvelope(t *testing.T) {
	dir := t.TempDir()
	options := testOptions(t, dir)
	options.MasterKey = bytes.Repeat([]byte{0x4a}, 32)
	options.WrappingKeyID = "current"
	store, err := spool.Open(options)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	hint, err := spool.ReadKeyHint(dir)
	if err != nil {
		t.Fatalf("ReadKeyHint: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := spool.ValidateWrappingKey(dir, options.MasterKey, hint.ContextID[:], "current"); err != nil {
		t.Fatalf("ValidateWrappingKey: %v", err)
	}
	if err := spool.ValidateWrappingKey(dir, bytes.Repeat([]byte{0x4b}, 32), nil, "current"); !errors.Is(err, spool.ErrWrongKey) {
		t.Fatalf("wrong key returned %v, want ErrWrongKey", err)
	}
	wrongContext := bytes.Repeat([]byte{0x4c}, 16)
	if err := spool.ValidateWrappingKey(dir, options.MasterKey, wrongContext, "current"); !errors.Is(err, spool.ErrContextMismatch) {
		t.Fatalf("wrong context returned %v, want ErrContextMismatch", err)
	}
}

func TestValidateCredentialPassphrase(t *testing.T) {
	dir := t.TempDir()
	options := testOptions(t, dir)
	options.MasterKey = nil
	options.Passphrase = "correct horse battery staple"
	store, err := spool.Open(options)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	hint, err := spool.ReadKeyHint(dir)
	if err != nil {
		t.Fatalf("ReadKeyHint: %v", err)
	}
	if hint.KDF != spool.KDFArgon2id {
		t.Fatalf("hint KDF = %d, want KDFArgon2id", hint.KDF)
	}
	if err := spool.ValidateCredential(dir, nil, options.Passphrase, nil, ""); err != nil {
		t.Fatalf("ValidateCredential: %v", err)
	}
	if err := spool.ValidateCredential(dir, nil, "wrong passphrase", nil, ""); !errors.Is(err, spool.ErrWrongKey) {
		t.Fatalf("wrong passphrase returned %v, want ErrWrongKey", err)
	}
	if err := spool.ValidateCredential(dir, bytes.Repeat([]byte{0x4b}, 32), "", nil, ""); !errors.Is(err, spool.ErrWrongKey) {
		t.Fatalf("master key against passphrase store returned %v, want ErrWrongKey", err)
	}
	if err := spool.ValidateCredential(dir, nil, "", nil, ""); err == nil {
		t.Fatal("empty credential accepted, want error")
	}
	if err := spool.ValidateCredential(dir, bytes.Repeat([]byte{0x4b}, 32), "both", nil, ""); err == nil {
		t.Fatal("dual credential accepted, want error")
	}
}

// TestPlaintextAudit writes high-entropy canary keys and values
// through every mutation path, rotates, rewrites, and checkpoints,
// then requires the canaries to appear in no store file.
func TestPlaintextAudit(t *testing.T) {
	dir := t.TempDir()
	o := testOptions(t, dir)
	o.WrappingKeyID = "audit-key"
	st, err := spool.Open(o)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	var needles [][]byte
	canary := func(prefix string) []byte {
		out := []byte(prefix)
		rnd := make([]byte, 32)
		if _, err := rand.Read(rnd); err != nil {
			t.Fatalf("rand: %v", err)
		}
		out = append(out, rnd...)
		needles = append(needles, out)
		return out
	}
	if err := st.Put(canary("AUDIT-KEY-"), canary("AUDIT-VALUE-")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Commit([]spool.Mutation{{Key: canary("AUDIT-CK-"), Value: canary("AUDIT-CV-")}}, spool.DurabilitySync); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := st.PutBatch([]spool.KV{{Key: canary("AUDIT-BK-"), Value: canary("AUDIT-BV-")}}); err != nil {
		t.Fatalf("PutBatch: %v", err)
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := st.RotateKey(); err != nil {
		t.Fatalf("RotateKey: %v", err)
	}
	if err := st.Put(canary("AUDIT-K2-"), canary("AUDIT-V2-")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := st.RewriteDataKeys(); err != nil {
		t.Fatalf("RewriteDataKeys: %v", err)
	}
	if _, err := st.Checkpoint(context.Background(), filepath.Join(dir, "cp")); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	var files int
	err = filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		files++
		for _, needle := range needles {
			for i := 0; i+len(needle) <= len(raw); i++ {
				if string(raw[i:i+len(needle)]) == string(needle) {
					t.Errorf("canary %.12s leaks in %s", needle, path)
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if files == 0 {
		t.Fatal("no files audited")
	}
	t.Logf("audited %d files, no canary leaks", files)
}

// TestContextDefaultIsStoreID verifies standalone stores default
// their database context to the generated store identity.
func TestContextDefaultIsStoreID(t *testing.T) {
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
	hint, err := spool.ReadKeyHint(dir)
	if err != nil {
		t.Fatalf("ReadKeyHint: %v", err)
	}
	if !hint.Encrypted {
		t.Fatal("hint.Encrypted = false, want true")
	}
	if hint.ContextID != hint.StoreID {
		t.Fatal("default context != store id")
	}
	// Reopen without a context accepts the persisted one.
	st, err = spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	st.Close()
}

// TestContextRoundTripAndMismatch verifies provided contexts persist
// and mismatches fail closed on open and load.
func TestContextRoundTripAndMismatch(t *testing.T) {
	dir := t.TempDir()
	ctx := []byte("0123456789abcdef")
	o := testOptions(t, dir)
	o.ContextID = ctx
	st, err := spool.Open(o)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	hint, err := spool.ReadKeyHint(dir)
	if err != nil {
		t.Fatalf("ReadKeyHint: %v", err)
	}
	if string(hint.ContextID[:]) != string(ctx) {
		t.Fatalf("hint context = %x, want %x", hint.ContextID, ctx)
	}
	// Matching context opens.
	o2 := testOptions(t, dir)
	o2.ContextID = append([]byte(nil), ctx...)
	st, err = spool.Open(o2)
	if err != nil {
		t.Fatalf("open with context: %v", err)
	}
	st.Close()
	// Wrong context fails before any writer initializes.
	o3 := testOptions(t, dir)
	o3.ContextID = []byte("fedcba9876543210")
	if _, err := spool.Open(o3); !errors.Is(err, spool.ErrContextMismatch) {
		t.Fatalf("open err = %v, want ErrContextMismatch", err)
	}
	load := func() error {
		return spool.LoadWithOptions(spool.LoadOptions{
			Path: dir, MasterKey: testMasterKey,
			ContextID: []byte("fedcba9876543210"),
		}, func([]spool.Record) error { return nil })
	}
	if err := load(); !errors.Is(err, spool.ErrContextMismatch) {
		t.Fatalf("load err = %v, want ErrContextMismatch", err)
	}
	// Bad lengths are rejected by validation.
	o4 := testOptions(t, dir)
	o4.ContextID = []byte("short")
	if _, err := spool.Open(o4); err == nil {
		t.Fatal("short ContextID accepted, want rejection")
	}
}

// TestContextTamperFailsClosed flips persisted context bytes and
// requires open failures rather than silent cross-context reads.
func TestContextTamperFailsClosed(t *testing.T) {
	dir := t.TempDir()
	o := testOptions(t, dir)
	o.ContextID = []byte("0123456789abcdef")
	st, err := spool.Open(o)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Corrupt the manifest's context copy (body offset 16:32, file
	// offset 32:48): the CRC must catch it.
	mp := filepath.Join(dir, "manifest")
	raw, err := os.ReadFile(mp)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	raw[32] ^= 0xff
	if err := os.WriteFile(mp, raw, 0o600); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	if _, err := spool.Open(testOptions(t, dir)); !errors.Is(err, spool.ErrCorrupt) {
		t.Fatalf("tampered manifest err = %v, want ErrCorrupt", err)
	}
}

// TestBlockContextBindingSwappedStores verifies blocks sealed under
// one context do not open under another: copying a segment file
// across same-key stores fails authentication.
func TestBlockContextBindingSwappedStores(t *testing.T) {
	mk := func(ctx string) string {
		dir := t.TempDir()
		o := testOptions(t, dir)
		o.ContextID = []byte(ctx)
		st, err := spool.Open(o)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		if err := st.Put([]byte("k"), []byte("v")); err != nil {
			t.Fatalf("Put: %v", err)
		}
		if err := st.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		return dir
	}
	src := mk("aaaaaaaaaaaaaaaa")
	dst := mk("bbbbbbbbbbbbbbbb")
	seg := filepath.Join("segments", "000000000001.spool")
	raw, err := os.ReadFile(filepath.Join(src, seg))
	if err != nil {
		t.Fatalf("read segment: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dst, seg), raw, 0o600); err != nil {
		t.Fatalf("write segment: %v", err)
	}
	if err := spool.Load(dst, testMasterKey, func([]spool.Record) error { return nil }); err == nil {
		t.Fatal("cross-context segment accepted, want authentication failure")
	} else if !errors.Is(err, spool.ErrAuthFailed) {
		t.Fatalf("cross-context err = %v, want ErrAuthFailed", err)
	}
}

// TestWrappingKeyIDRoundTrip verifies the wrapping id persists in
// the authenticated envelope and the open verifies it.
func TestWrappingKeyIDRoundTrip(t *testing.T) {
	dir := t.TempDir()
	o := testOptions(t, dir)
	o.WrappingKeyID = "provider-key-7"
	st, err := spool.Open(o)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	hint, err := spool.ReadKeyHint(dir)
	if err != nil {
		t.Fatalf("ReadKeyHint: %v", err)
	}
	if hint.WrappingKeyID != "provider-key-7" {
		t.Fatalf("hint wrapping id = %q", hint.WrappingKeyID)
	}
	// Matching id opens; mismatched id fails closed.
	o2 := testOptions(t, dir)
	o2.WrappingKeyID = "provider-key-7"
	st, err = spool.Open(o2)
	if err != nil {
		t.Fatalf("open with id: %v", err)
	}
	st.Close()
	o3 := testOptions(t, dir)
	o3.WrappingKeyID = "provider-key-8"
	if _, err := spool.Open(o3); !errors.Is(err, spool.ErrWrongKey) {
		t.Fatalf("open err = %v, want ErrWrongKey", err)
	}
	// Empty id accepts any envelope.
	st, err = spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("open without id: %v", err)
	}
	st.Close()
}

// TestKeyHintPlainStore verifies hints on unencrypted stores carry
// manifest identity without key material.
func TestKeyHintPlainStore(t *testing.T) {
	dir := t.TempDir()
	o := testOptions(t, dir)
	o.Encryption = spool.EncryptionNone
	o.MasterKey = nil
	st, err := spool.Open(o)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	st.Close()
	hint, err := spool.ReadKeyHint(dir)
	if err != nil {
		t.Fatalf("ReadKeyHint: %v", err)
	}
	if hint.Encrypted {
		t.Fatal("hint.Encrypted = true, want false")
	}
	if hint.ContextID != hint.StoreID {
		t.Fatal("plain default context != store id")
	}
	if hint.WrappingKeyID != "" {
		t.Fatalf("plain wrap id = %q, want empty", hint.WrappingKeyID)
	}
}

// TestRejectsPreContextVersions writes stale version markers for the
// manifest, keys.enc and a block, and requires clean rejection.
func TestRejectsPreContextVersions(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "segments"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	// Stale manifest version fails before checksums matter.
	raw := make([]byte, 16)
	copy(raw[0:4], "SPLM")
	raw[4] = 2 // format version 2
	if err := os.WriteFile(filepath.Join(dir, "manifest"), raw, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := spool.Open(testOptions(t, dir)); !errors.Is(err, spool.ErrUnsupportedVersion) {
		t.Fatalf("v2 manifest err = %v, want ErrUnsupportedVersion", err)
	}
	if _, err := spool.ReadKeyHint(dir); !errors.Is(err, spool.ErrUnsupportedVersion) {
		t.Fatalf("hint err = %v, want ErrUnsupportedVersion", err)
	}
}

// TestRotateDuringConcurrentWrites hammers rotation while writers
// commit: every group must reference a durable key, so the full
// reload after close succeeds and every key survives.
func TestRotateDuringConcurrentWrites(t *testing.T) {
	dir := t.TempDir()
	st, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	const writers = 8
	const perWriter = 100
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				k := []byte(fmt.Sprintf("w%d-k%d", w, i))
				if err := st.Commit([]spool.Mutation{{Key: k, Value: []byte("v")}}, spool.DurabilityAsync); err != nil {
					t.Errorf("Commit: %v", err)
					return
				}
			}
		}(w)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			if err := st.RotateKey(); err != nil {
				t.Errorf("RotateKey: %v", err)
				return
			}
		}
	}()
	wg.Wait()
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	got := loadAll(t, dir)
	if len(got) != writers*perWriter {
		t.Fatalf("loaded %d keys, want %d", len(got), writers*perWriter)
	}
	// The envelope holds every generation; reopen keeps working.
	st2, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer st2.Close()
	inv := st2.KeyInventory()
	if len(inv.DataKeys) != 21 || inv.ActiveDataKeyID != 21 {
		t.Fatalf("inventory = %d keys active %d, want 21 keys active 21",
			len(inv.DataKeys), inv.ActiveDataKeyID)
	}
}

// TestDataKeyMaxAgeValidation rejects negative aging. The rotation
// mechanism itself is covered deterministically by white-box tests
// that backdate keys instead of sleeping past wall-clock margins.
func TestDataKeyMaxAgeValidation(t *testing.T) {
	o := testOptions(t, t.TempDir())
	o.DataKeyMaxAge = -time.Second
	if _, err := spool.Open(o); err == nil {
		t.Fatal("negative DataKeyMaxAge accepted, want rejection")
	}
}

// TestRotateWrappingKey exercises the named-key entrypoint: new
// material plus a new authenticated id, with the old material
// failing afterwards and all data intact.
func TestRotateWrappingKey(t *testing.T) {
	dir := t.TempDir()
	o := testOptions(t, dir)
	o.WrappingKeyID = "key-1"
	st, err := spool.Open(o)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	newKey := []byte("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
	if err := st.RotateWrappingKey(newKey, "key-2"); err != nil {
		t.Fatalf("RotateWrappingKey: %v", err)
	}
	if inv := st.KeyInventory(); inv.WrappingKeyID != "key-2" {
		t.Fatalf("wrapping id = %q, want key-2", inv.WrappingKeyID)
	}
	if err := st.Put([]byte("k2"), []byte("v2")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	hint, err := spool.ReadKeyHint(dir)
	if err != nil {
		t.Fatalf("ReadKeyHint: %v", err)
	}
	if hint.WrappingKeyID != "key-2" {
		t.Fatalf("hint wrapping id = %q, want key-2", hint.WrappingKeyID)
	}
	// Old material fails; new material opens with both keys' data.
	if _, err := spool.Open(testOptions(t, dir)); err == nil {
		t.Fatal("open with old wrapping key succeeded, want failure")
	}
	o2 := testOptions(t, dir)
	o2.MasterKey = newKey
	o2.WrappingKeyID = "key-2"
	st2, err := spool.Open(o2)
	if err != nil {
		t.Fatalf("open with new key: %v", err)
	}
	st2.Close()
	got := map[string][]byte{}
	if err := spool.LoadWithOptions(spool.LoadOptions{Path: dir, MasterKey: newKey},
		func(recs []spool.Record) error {
			for _, r := range recs {
				if !r.Deleted {
					got[string(r.Key)] = append([]byte(nil), r.Value...)
				}
			}
			return nil
		}); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if string(got["k"]) != "v" || string(got["k2"]) != "v2" {
		t.Fatalf("reloaded = %v, want k+k2", got)
	}
	// Standalone master rotation still works on top.
	st3, err := spool.Open(o2)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer st3.Close()
	third := []byte("BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB")
	if err := st3.RotateMasterKey(third, ""); err != nil {
		t.Fatalf("RotateMasterKey: %v", err)
	}
	if inv := st3.KeyInventory(); inv.WrappingKeyID != "key-2" {
		t.Fatalf("wrapping id = %q, want key-2 preserved", inv.WrappingKeyID)
	}
}

// TestKeyInventory checks the non-secret snapshot after rotations.
func TestKeyInventory(t *testing.T) {
	dir := t.TempDir()
	o := testOptions(t, dir)
	o.WrappingKeyID = "inv-key"
	st, err := spool.Open(o)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()
	if err := st.RotateKey(); err != nil {
		t.Fatalf("RotateKey: %v", err)
	}
	if err := st.RotateKey(); err != nil {
		t.Fatalf("RotateKey: %v", err)
	}
	inv := st.KeyInventory()
	if !inv.Encrypted {
		t.Fatal("Encrypted = false, want true")
	}
	if inv.WrappingKeyID != "inv-key" {
		t.Fatalf("WrappingKeyID = %q", inv.WrappingKeyID)
	}
	if inv.ActiveDataKeyID != 3 || len(inv.DataKeys) != 3 {
		t.Fatalf("keys = %d active %d, want 3 active 3", len(inv.DataKeys), inv.ActiveDataKeyID)
	}
	for i, k := range inv.DataKeys {
		if k.ID != uint32(i+1) {
			t.Fatalf("keys not sorted by id: %+v", inv.DataKeys)
		}
		if k.CreatedAt.IsZero() {
			t.Fatalf("key %d lacks creation time", k.ID)
		}
	}
	if inv.DataKeys[2].Status != spool.KeyStatusActive {
		t.Fatal("newest key not active")
	}
	if inv.DataKeys[0].Status != spool.KeyStatusRetired {
		t.Fatal("oldest key not retired")
	}
	if inv.KeyringSeq == 0 || inv.ManifestGeneration == 0 {
		t.Fatalf("seq=%d gen=%d, want both nonzero", inv.KeyringSeq, inv.ManifestGeneration)
	}
	if inv.MaintenancePhase != "idle" {
		t.Fatalf("phase = %q, want idle", inv.MaintenancePhase)
	}
}

// TestPruneDataKeys overwrites a whole oversized generation (so no
// file is shared), reclaims the dead files, and requires pruning to
// drop exactly the unreferenced retired key while data stays intact.
func TestPruneDataKeys(t *testing.T) {
	dir := t.TempDir()
	o := testOptions(t, dir)
	o.MaxSegmentSize = 16 << 10
	o.MaxBlockBytes = 8 << 10
	o.TargetBlockBytes = 4 << 10
	o.MaxRecordsPerBlock = 100
	o.MaxKeySize = 64
	o.MaxValueSize = 2048
	st, err := spool.Open(o)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	// Generation 1: one oversized group in its own sealed file.
	gen1 := make([]spool.Mutation, 20)
	for i := range gen1 {
		gen1[i] = spool.Mutation{
			Key:   []byte(fmt.Sprintf("k%02d", i)),
			Value: incompressible(1000, uint64(i)),
		}
	}
	if err := st.Commit(gen1, spool.DurabilitySync); err != nil {
		t.Fatalf("Commit gen1: %v", err)
	}
	if err := st.RotateKey(); err != nil {
		t.Fatalf("RotateKey: %v", err)
	}
	// Generation 2 overwrites every key; the committed group lands
	// in a fresh file because generation 1 sealed its own.
	gen2 := make([]spool.Mutation, 20)
	for i := range gen2 {
		gen2[i] = spool.Mutation{
			Key:   []byte(fmt.Sprintf("k%02d", i)),
			Value: incompressible(1000, uint64(100+i)),
		}
	}
	if err := st.Commit(gen2, spool.DurabilitySync); err != nil {
		t.Fatalf("Commit gen2: %v", err)
	}
	// Key 1 is retired but still referenced: nothing prunable.
	if pruned, err := st.PruneDataKeys(); err != nil {
		t.Fatalf("PruneDataKeys: %v", err)
	} else if len(pruned) != 0 {
		t.Fatalf("pruned %v before reclaim, want none", pruned)
	}
	refs, err := st.KeyReferences()
	if err != nil {
		t.Fatalf("KeyReferences: %v", err)
	}
	if len(refs) != 2 || refs[0].KeyID != 1 || refs[0].SegmentFiles != 1 {
		t.Fatalf("refs = %+v, want key 1 in 1 file", refs)
	}
	// Reclaim unlinks the fully dead generation-1 file; then key 1
	// is provably unreferenced and prunes away.
	for i := 0; i < 10 && segmentCount(t, dir) > 2; i++ {
		if err := st.Reclaim(); err != nil {
			t.Fatalf("Reclaim: %v", err)
		}
	}
	if n := segmentCount(t, dir); n != 2 {
		t.Fatalf("segments = %d, want 2 (live + active)", n)
	}
	pruned, err := st.PruneDataKeys()
	if err != nil {
		t.Fatalf("PruneDataKeys: %v", err)
	}
	if len(pruned) != 1 || pruned[0] != 1 {
		t.Fatalf("pruned = %v, want [1]", pruned)
	}
	if inv := st.KeyInventory(); len(inv.DataKeys) != 1 || inv.ActiveDataKeyID != 2 {
		t.Fatalf("inventory = %+v, want only key 2", inv.DataKeys)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	got := loadAll(t, dir)
	if len(got) != 20 {
		t.Fatalf("loaded %d keys, want 20", len(got))
	}
	for i := 0; i < 20; i++ {
		want := incompressible(1000, uint64(100+i))
		if string(got[fmt.Sprintf("k%02d", i)]) != string(want) {
			t.Fatalf("k%02d mismatch after prune", i)
		}
	}
}
