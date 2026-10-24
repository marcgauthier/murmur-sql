package murmur

import (
	"bytes"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/marcgauthier/murmur-sql/murmur/spool"
)

// TestDataKeyRotationProgress pins the operator-visible migration
// lifecycle: after a data-key rotation the inventory shows the retired
// key, references still point at it, pruning is a safe no-op, and rows
// stay readable throughout.
func TestDataKeyRotationProgress(t *testing.T) {
	path := t.TempDir() + "/db"
	options := DefaultOptions()
	options.Mode = OpenCreateNew
	db, err := Open(path, testKey(), options)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	writeCanary(t, db)
	before := db.KeyInventory()
	if len(before.DataKeys) != 1 {
		t.Fatalf("data keys = %d, want 1", len(before.DataKeys))
	}
	if err := db.RotateDataKey(); err != nil {
		t.Fatalf("rotate data key: %v", err)
	}
	after := db.KeyInventory()
	if len(after.DataKeys) != 2 {
		t.Fatalf("data keys = %d, want 2", len(after.DataKeys))
	}
	var states []spool.KeyStatus
	for _, k := range after.DataKeys {
		states = append(states, k.Status)
	}
	if !(containsStatus(states, spool.KeyStatusActive) && containsStatus(states, spool.KeyStatusRetired)) {
		t.Fatalf("states = %v, want active and retired", states)
	}
	refs, err := db.KeyReferences()
	if err != nil {
		t.Fatalf("references: %v", err)
	}
	if len(refs) == 0 {
		t.Fatal("no key references reported")
	}
	retired := before.ActiveDataKeyID
	found := false
	for _, r := range refs {
		if r.KeyID == retired {
			found = true
			if r.Blocks == 0 {
				t.Fatal("retired key reports no blocks")
			}
		}
	}
	if !found {
		t.Fatalf("retired key %d missing from references %+v", retired, refs)
	}
	pruned, err := db.PruneDataKeys()
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if len(pruned) != 0 {
		t.Fatalf("pruned %v, want nothing while referenced", pruned)
	}
	if got := readCanary(t, db); got != "1" {
		t.Fatalf("value = %q, want %q", got, "1")
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	options.Mode = OpenExisting
	db2, err := Open(path, testKey(), options)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()
	if got := readCanary(t, db2); got != "1" {
		t.Fatalf("value after reopen = %q, want %q", got, "1")
	}
	if len(db2.KeyInventory().DataKeys) != 2 {
		t.Fatal("retired key lost across reopen")
	}
}

func containsStatus(states []spool.KeyStatus, want spool.KeyStatus) bool {
	for _, s := range states {
		if s == want {
			return true
		}
	}
	return false
}

var errInjectedKeyring = errors.New("injected keyring persist failure")

var errInjectedManifest = errors.New("injected manifest persist failure")

// TestRotateCredentialFailureKeepsOldKey pins rollback on a failed
// envelope persist: rotation reports the error, the envelope is
// untouched, and after reopen the old credential opens while the new
// one is rejected.
func TestRotateCredentialFailureKeepsOldKey(t *testing.T) {
	path := t.TempDir() + "/db"
	options := DefaultOptions()
	options.Mode = OpenCreateNew
	db, err := Open(path, testKey(), options)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	writeCanary(t, db)
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	seqBefore := keyHint(t, path).KeyringSeq
	var fail atomic.Bool
	fail.Store(true)
	options.Mode = OpenExisting
	options.FaultHooks = &spool.FaultHooks{
		KeyringPersist: func() error {
			if fail.Swap(false) {
				return errInjectedKeyring
			}
			return nil
		},
	}
	db2, err := Open(path, testKey(), options)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	newKey := bytes.Repeat([]byte{0x9e}, 32)
	if err := db2.RotateCredential(KeyCredential(newKey)); err == nil || !strings.Contains(err.Error(), "injected keyring persist failure") {
		t.Fatalf("rotate returned %v, want injected failure", err)
	}
	// The failed rotation poisons the handle: Close reports the
	// terminal error. Only a fresh open recovers.
	if err := db2.Close(); err == nil {
		t.Fatal("close after failed rotation succeeded, want terminal error")
	}
	if seq := keyHint(t, path).KeyringSeq; seq != seqBefore {
		t.Fatalf("keyring seq = %d, want unchanged %d", seq, seqBefore)
	}
	options.FaultHooks = nil
	db3, err := Open(path, testKey(), options)
	if err != nil {
		t.Fatalf("reopen with old key: %v", err)
	}
	defer db3.Close()
	if got := readCanary(t, db3); got != "1" {
		t.Fatalf("value = %q, want %q", got, "1")
	}
	if _, err := Open(path, newKey, options); !errors.Is(err, ErrWrongKey) {
		t.Fatalf("new key returned %v, want ErrWrongKey", err)
	}
}

// TestDataKeyRotationManifestFailureReopens pins crash-consistency when
// the manifest publish fails mid-rotation: the envelope already holds
// both keys, so reopening works and rows stay readable.
func TestDataKeyRotationManifestFailureReopens(t *testing.T) {
	path := t.TempDir() + "/db"
	options := DefaultOptions()
	options.Mode = OpenCreateNew
	db, err := Open(path, testKey(), options)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	writeCanary(t, db)
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	var fail atomic.Bool
	options.Mode = OpenExisting
	options.FaultHooks = &spool.FaultHooks{
		ManifestPersist: func() error {
			if fail.Swap(false) {
				return errInjectedManifest
			}
			return nil
		},
	}
	db2, err := Open(path, testKey(), options)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	fail.Store(true)
	if err := db2.RotateDataKey(); err == nil || !strings.Contains(err.Error(), "injected manifest persist failure") {
		t.Fatalf("rotate returned %v, want injected failure", err)
	}
	_ = db2.Close()
	options.FaultHooks = nil
	db3, err := Open(path, testKey(), options)
	if err != nil {
		t.Fatalf("reopen after failed rotation: %v", err)
	}
	defer db3.Close()
	if got := readCanary(t, db3); got != "1" {
		t.Fatalf("value = %q, want %q", got, "1")
	}
	if len(db3.KeyInventory().DataKeys) != 2 {
		t.Fatal("successor key lost across reopen")
	}
}
