// Live scenario: corrupt archives fail closed and leave the live database
// untouched.
//
// Go port of tests/tampered_backup.rs using tests-live/gosqlharness.
// Identical behavior: a single local-only real murmur-server child holds
// 30 rows, a backup is saved and downloaded, three corruptions (bad magic,
// flipped payload byte, truncation) are uploaded and must be rejected at
// upload or restore while the live rows stay intact on a fresh
// connection, and the pristine backup still restores after a DELETE.
package tamperedbackup

import (
	"fmt"
	"os"
	"testing"

	gosqlharness "github.com/marcgauthier/murmur-sql/tests-live/gosqlharness"
)

func TestTamperedBackupStreamsLeaveLiveDatabaseUntouched(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	cluster, err := gosqlharness.NewCluster(gosqlharness.ClusterOptions{
		Name:      "tampered-backup",
		NumNodes:  1,
		LocalOnly: true,
	})
	if err != nil {
		t.Fatalf("start cluster: %v", err)
	}
	failed := true
	defer func() { cluster.Finish(failed) }()

	if _, err := cluster.Node(0).Simple(
		"CREATE TABLE records(id INTEGER PRIMARY KEY, name TEXT NOT NULL)"); err != nil {
		t.Fatalf("create records: %v", err)
	}
	for id := int64(0); id < 30; id++ {
		name := fmt.Sprintf("kept-%03d", id)
		if _, err := cluster.Node(0).Execute(
			"INSERT INTO records(id,name) VALUES($1,$2)", id, name); err != nil {
			t.Fatalf("insert %d: %v", id, err)
		}
	}
	expected, err := cluster.Node(0).Simple("SELECT id,name FROM records ORDER BY id")
	if err != nil {
		t.Fatalf("read expected: %v", err)
	}
	saved, err := cluster.Node(0).Simple("MURMUR BACKUP SAVE")
	if err != nil {
		t.Fatalf("backup save: %v", err)
	}
	var original []byte
	if err := gosqlharness.Download(cluster.Node(0),
		"MURMUR BACKUP DOWNLOAD "+gosqlharness.SQLLiteral(saved[0][0]), &original); err != nil {
		t.Fatalf("backup download: %v", err)
	}
	if len(original) <= 100 {
		t.Fatalf("backup is %d bytes, want > 100", len(original))
	}
	badMagic := append([]byte(nil), original...)
	badMagic[0] ^= 0xff
	payloadFlip := append([]byte(nil), original...)
	payloadFlip[len(payloadFlip)/2] ^= 0x80
	truncated := append([]byte(nil), original[:len(original)/2]...)
	for _, tc := range []struct {
		name string
		data []byte
	}{
		{"bad-magic.murmur-backup", badMagic},
		{"bad-payload.murmur-backup", payloadFlip},
		{"truncated.murmur-backup", truncated},
	} {
		// Upload may reject a malformed envelope; otherwise restore must reject it.
		if err := gosqlharness.Upload(cluster.Node(0), tc.name, tc.data); err == nil {
			if _, err := cluster.Node(0).Simple("MURMUR RESTORE FROM " +
				gosqlharness.SQLLiteral(tc.name) + " KEY " + gosqlharness.SQLLiteral(gosqlharness.Key)); err == nil {
				t.Fatalf("accepted corrupt archive %s", tc.name)
			}
		}
		fresh, err := cluster.Connect(0)
		if err != nil {
			t.Fatalf("fresh connect after %s: %v", tc.name, err)
		}
		rows, err := fresh.Simple("SELECT id,name FROM records ORDER BY id")
		_ = fresh.Close()
		if err != nil {
			t.Fatalf("reread after %s: %v", tc.name, err)
		}
		if !equalRows(rows, expected) {
			t.Fatalf("corrupt restore modified live rows: %s", tc.name)
		}
	}
	if _, err := cluster.Node(0).Simple("DELETE FROM records"); err != nil {
		t.Fatalf("delete records: %v", err)
	}
	if _, err := cluster.Node(0).Simple("MURMUR RESTORE FROM " +
		gosqlharness.SQLLiteral(saved[0][0]) + " KEY " + gosqlharness.SQLLiteral(gosqlharness.Key)); err != nil {
		t.Fatalf("restore pristine backup: %v", err)
	}
	fresh, err := cluster.Connect(0)
	if err != nil {
		t.Fatalf("fresh connect after restore: %v", err)
	}
	rows, err := fresh.Simple("SELECT id,name FROM records ORDER BY id")
	_ = fresh.Close()
	if err != nil {
		t.Fatalf("reread after restore: %v", err)
	}
	if !equalRows(rows, expected) {
		t.Fatalf("restored rows = %v, want %v", rows, expected)
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
