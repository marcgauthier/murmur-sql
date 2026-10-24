// Live scenario: passphrase migration and data-key rotation progress.
//
// A single node migrates its legacy envelope to an Argon2id passphrase
// without downtime: rows written before and after the rotation read back
// after reopening with the passphrase, the envelope reports KDF-1, and
// both the retired master key and the legacy passphrase mapping are
// rejected. The second leg pins data-key rotation progress: after
// rotating the data key the inventory shows the retired key, references
// still point at it, pruning is a safe no-op, and rows survive a
// restart.
package rekey

import (
	"errors"
	"testing"

	"github.com/marcgauthier/murmur-sql/murmur"
	"github.com/marcgauthier/murmur-sql/murmur/spool"
	"github.com/marcgauthier/murmur-sql/murmur/tests-live/harness"
)

func TestPassphraseRotationMigratesEnvelope(t *testing.T) {
	cluster, err := harness.NewCluster(harness.ClusterOptions{Name: "rekey-passphrase", NumNodes: 1, Key: oldKey})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("rekey-passphrase")
		}
		cluster.Finish(failed)
	}()

	if err := cluster.EnsureMigrated(0, ddl); err != nil {
		t.Fatalf("migrate typed table: %v", err)
	}
	if err := cluster.Node(0).SetReplication("records", true); err != nil {
		t.Fatalf("opt-in records: %v", err)
	}
	typedInsert(t, cluster.Node(0), "typed-before-rotate")

	// Rotate the raw master key to a passphrase: the envelope migrates
	// from the legacy direct KDF to Argon2id.
	if err := cluster.Node(0).RotateCredential(murmur.PassphraseCredential("live-passphrase")); err != nil {
		t.Fatalf("rotate to passphrase: %v", err)
	}
	if err := cluster.Node(0).Flush(); err != nil {
		t.Fatalf("flush after rotation: %v", err)
	}
	typedInsert(t, cluster.Node(0), "typed-after-rotate")

	dataDir := cluster.NodeInfo(0).DataDir
	hint, err := spool.ReadKeyHint(dataDir)
	if err != nil {
		t.Fatalf("read key hint: %v", err)
	}
	if hint.KDF != spool.KDFArgon2id {
		t.Fatalf("hint KDF = %d, want KDFArgon2id", hint.KDF)
	}
	cluster.StopNode(0)

	db, err := murmur.OpenWithCredential(dataDir, murmur.PassphraseCredential("live-passphrase"), murmur.DefaultOptions())
	if err != nil {
		t.Fatalf("reopen with passphrase: %v", err)
	}
	if got := typedCount(t, db, "typed-before-rotate"); got != 1 {
		_ = db.Close()
		t.Fatalf("pre-rotation row after restart = %d, want 1", got)
	}
	if got := typedCount(t, db, "typed-after-rotate"); got != 1 {
		_ = db.Close()
		t.Fatalf("post-rotation row after restart = %d, want 1", got)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close reopened store: %v", err)
	}

	// The retired master key no longer opens the store.
	assertKeyRejected(t, dataDir, oldKey)

	// Neither does the legacy passphrase mapping: KDF-0 derivation of
	// the new passphrase must fail closed against the Argon2id envelope.
	db, err = murmur.OpenWithCredential(dataDir, murmur.CredentialForSecret("live-passphrase", spool.KDFDirect), murmur.DefaultOptions())
	switch {
	case err == nil:
		_ = db.Close()
		t.Fatal("store unexpectedly accepted the legacy passphrase mapping")
	case errors.Is(err, murmur.ErrWrongKey):
	default:
		t.Fatalf("legacy mapping failed with %v, want WrongKey", err)
	}
	failed = false
}

func TestDataKeyRotationProgressLive(t *testing.T) {
	cluster, err := harness.NewCluster(harness.ClusterOptions{Name: "rekey-progress", NumNodes: 1, Key: oldKey})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("rekey-progress")
		}
		cluster.Finish(failed)
	}()

	if err := cluster.EnsureMigrated(0, ddl); err != nil {
		t.Fatalf("migrate typed table: %v", err)
	}
	if err := cluster.Node(0).SetReplication("records", true); err != nil {
		t.Fatalf("opt-in records: %v", err)
	}
	typedInsert(t, cluster.Node(0), "progress-row")

	node := cluster.Node(0)
	activeBefore := node.KeyInventory().ActiveDataKeyID
	if err := node.RotateDataKey(); err != nil {
		t.Fatalf("rotate data key: %v", err)
	}
	inv := node.KeyInventory()
	if len(inv.DataKeys) != 2 {
		t.Fatalf("data keys = %d, want 2", len(inv.DataKeys))
	}
	if inv.ActiveDataKeyID == activeBefore {
		t.Fatal("active data key unchanged after rotation")
	}
	refs, err := node.KeyReferences()
	if err != nil {
		t.Fatalf("key references: %v", err)
	}
	found := false
	for _, r := range refs {
		if r.KeyID == activeBefore {
			found = true
			if r.Blocks == 0 {
				t.Fatal("retired key reports no blocks")
			}
		}
	}
	if !found {
		t.Fatalf("retired key %d missing from references", activeBefore)
	}
	pruned, err := node.PruneDataKeys()
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if len(pruned) != 0 {
		t.Fatalf("pruned %v, want nothing while referenced", pruned)
	}

	dataDir := cluster.NodeInfo(0).DataDir
	cluster.StopNode(0)
	db, err := murmur.Open(dataDir, oldKey[:], murmur.DefaultOptions())
	if err != nil {
		t.Fatalf("reopen after data-key rotation: %v", err)
	}
	if got := typedCount(t, db, "progress-row"); got != 1 {
		_ = db.Close()
		t.Fatalf("row after restart = %d, want 1", got)
	}
	if len(db.KeyInventory().DataKeys) != 2 {
		_ = db.Close()
		t.Fatal("retired key lost across restart")
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close reopened store: %v", err)
	}
	failed = false
}
