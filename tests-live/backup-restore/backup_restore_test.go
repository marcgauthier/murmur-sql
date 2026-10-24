// Live scenario: backups use the streaming SQL API, with no offline
// engine opens.
//
// Go port of tests/backup_restore.rs using tests-live/gosqlharness.
// Identical behavior: a typed row set plus replication flags round-trips
// through SAVE/DOWNLOAD/UPLOAD/RESTORE and survives a restart, while a
// wrong-key restore and a foreign-identity restore are both rejected
// without changing live rows.
package backuprestore

import (
	"os"
	"strconv"
	"testing"

	goharness "github.com/marcgauthier/murmur-sql/tests-live/goharness"
	gosqlharness "github.com/marcgauthier/murmur-sql/tests-live/gosqlharness"
)

func TestSQLBackupStreamRoundTrip(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	cluster, err := gosqlharness.NewCluster(gosqlharness.ClusterOptions{
		Name:      "backup-restore",
		NumNodes:  1,
		LocalOnly: true,
	})
	if err != nil {
		t.Fatalf("start cluster: %v", err)
	}
	failed := true
	defer func() { cluster.Finish(failed) }()

	if _, err := cluster.Node(0).Simple("CREATE TABLE records(id INTEGER PRIMARY KEY, title TEXT NOT NULL, score REAL, payload BLOB, optional TEXT); INSERT INTO records VALUES(1,'quoted '' title',1.25,x'0001feff',NULL); INSERT INTO records VALUES(2,'日本語',-7.5,x'', 'present')"); err != nil {
		t.Fatalf("seed records: %v", err)
	}
	// Table flags are authoritative config state and must survive restore.
	if _, err := cluster.Node(0).Simple("MURMUR SET TABLE records REPLICATED true"); err != nil {
		t.Fatalf("replicate records: %v", err)
	}
	query := "SELECT id,title,score,hex(payload),optional,id_murmur_primary_key FROM records ORDER BY id"
	expected, err := cluster.Node(0).Simple(query)
	if err != nil {
		t.Fatalf("read expected: %v", err)
	}
	saved, err := cluster.Node(0).Simple("MURMUR BACKUP SAVE")
	if err != nil {
		t.Fatalf("backup save: %v", err)
	}
	size, err := strconv.ParseInt(saved[0][1], 10, 64)
	if err != nil || size <= 0 {
		t.Fatalf("backup size = %q, want > 0", saved[0][1])
	}
	var bytes []byte
	if err := gosqlharness.Download(cluster.Node(0),
		"MURMUR BACKUP DOWNLOAD "+gosqlharness.SQLLiteral(saved[0][0]), &bytes); err != nil {
		t.Fatalf("backup download: %v", err)
	}
	if int64(len(bytes)) != size {
		t.Fatalf("downloaded %d bytes, want %d", len(bytes), size)
	}
	if err := gosqlharness.Upload(cluster.Node(0), "roundtrip.murmur-backup", bytes); err != nil {
		t.Fatalf("backup upload: %v", err)
	}
	listed, err := cluster.Node(0).Simple("MURMUR BACKUP LIST")
	if err != nil {
		t.Fatalf("backup list: %v", err)
	}
	found := false
	for _, b := range listed {
		n, err := strconv.ParseInt(b[1], 10, 64)
		if err == nil && b[0] == "roundtrip.murmur-backup" && n == size {
			found = true
		}
	}
	if !found {
		t.Fatalf("roundtrip.murmur-backup missing from %v", listed)
	}
	if _, err := cluster.Node(0).Simple("DELETE FROM records; INSERT INTO records(id,title) VALUES(99,'after-backup')"); err != nil {
		t.Fatalf("overwrite records: %v", err)
	}
	if _, err := cluster.Node(0).Simple("MURMUR RESTORE FROM " +
		gosqlharness.SQLLiteral("roundtrip.murmur-backup") + " KEY " +
		gosqlharness.SQLLiteral(gosqlharness.Key)); err != nil {
		t.Fatalf("restore: %v", err)
	}
	fresh, err := cluster.Connect(0)
	if err != nil {
		t.Fatalf("reconnect: %v", err)
	}
	got, err := fresh.Simple(query)
	if err != nil {
		_ = fresh.Close()
		t.Fatalf("read restored: %v", err)
	}
	if !equalRows(got, expected) {
		_ = fresh.Close()
		t.Fatalf("restored rows = %v, want %v", got, expected)
	}
	flags, err := fresh.Simple("MURMUR SHOW TABLES REPLICATION")
	if err != nil {
		_ = fresh.Close()
		t.Fatalf("show table flags: %v", err)
	}
	replicated := false
	for _, f := range flags {
		if f[0] == "records" && f[1] == "true" {
			replicated = true
		}
	}
	if !replicated {
		_ = fresh.Close()
		t.Fatalf("records replication flag lost: %v", flags)
	}
	if _, err := fresh.Simple("MURMUR FLUSH"); err != nil {
		_ = fresh.Close()
		t.Fatalf("flush restored: %v", err)
	}
	_ = fresh.Close()
	if err := cluster.StopNode(0); err != nil {
		t.Fatalf("stop node: %v", err)
	}
	if err := cluster.StartNode(0); err != nil {
		t.Fatalf("restart node: %v", err)
	}
	got, err = cluster.Node(0).Simple(query)
	if err != nil {
		t.Fatalf("read after restart: %v", err)
	}
	if !equalRows(got, expected) {
		t.Fatalf("post-restart rows = %v, want %v", got, expected)
	}
	failed = false
}

func TestRestoreRejectsWrongKeyAndForeignIdentity(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	cluster, err := gosqlharness.NewCluster(gosqlharness.ClusterOptions{
		Name:      "backup-restore-rejections",
		NumNodes:  2,
		LocalOnly: true,
	})
	if err != nil {
		t.Fatalf("start cluster: %v", err)
	}
	failed := true
	defer func() { cluster.Finish(failed) }()

	for i := 0; i < 2; i++ {
		if _, err := cluster.Node(i).Simple("CREATE TABLE records(id INTEGER PRIMARY KEY, body TEXT)"); err != nil {
			t.Fatalf("node %d create records: %v", i, err)
		}
	}
	if _, err := cluster.Node(0).Simple("INSERT INTO records VALUES(1,'source')"); err != nil {
		t.Fatalf("node 0 insert: %v", err)
	}
	if _, err := cluster.Node(1).Simple("INSERT INTO records VALUES(2,'target-kept')"); err != nil {
		t.Fatalf("node 1 insert: %v", err)
	}
	saved, err := cluster.Node(0).Simple("MURMUR BACKUP SAVE")
	if err != nil {
		t.Fatalf("backup save: %v", err)
	}
	_, err = cluster.Node(0).Simple("MURMUR RESTORE FROM " +
		gosqlharness.SQLLiteral(saved[0][0]) + " KEY " +
		gosqlharness.SQLLiteral("wrong-restore-key"))
	if err == nil {
		t.Fatal("restore with wrong key accepted")
	}
	if code := goharness.ErrorCode(err); code != "28000" {
		t.Fatalf("wrong-key restore code = %q, want 28000 (%v)", code, err)
	}
	kept, err := cluster.Node(0).Simple("SELECT body FROM records")
	if err != nil {
		t.Fatalf("node 0 read: %v", err)
	}
	if !equalRows(kept, [][]string{{"source"}}) {
		t.Fatalf("node 0 rows = %v, want source", kept)
	}
	var bytes []byte
	if err := gosqlharness.Download(cluster.Node(0),
		"MURMUR BACKUP DOWNLOAD "+gosqlharness.SQLLiteral(saved[0][0]), &bytes); err != nil {
		t.Fatalf("backup download: %v", err)
	}
	if err := gosqlharness.Upload(cluster.Node(1), "foreign.murmur-backup", bytes); err != nil {
		t.Fatalf("backup upload: %v", err)
	}
	if _, err := cluster.Node(1).Simple("MURMUR RESTORE FROM " +
		gosqlharness.SQLLiteral("foreign.murmur-backup") + " KEY " +
		gosqlharness.SQLLiteral(gosqlharness.Key)); err == nil {
		t.Fatal("backup with foreign writer identity accepted")
	}
	kept, err = cluster.Node(1).Simple("SELECT body FROM records")
	if err != nil {
		t.Fatalf("node 1 read: %v", err)
	}
	if !equalRows(kept, [][]string{{"target-kept"}}) {
		t.Fatalf("node 1 rows = %v, want target-kept", kept)
	}
	failed = false
}

func equalRows(a, b [][]string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if len(a[i]) != len(b[i]) {
			return false
		}
		for j := range a[i] {
			if a[i][j] != b[i][j] {
				return false
			}
		}
	}
	return true
}
