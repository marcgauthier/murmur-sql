// Live scenario: take streaming online backups while a separate SQL
// connection commits transactions.
//
// Go port of tests/backup_under_fire.rs using tests-live/gosqlharness.
// Identical behavior: a writer commits 40 atomic five-row batches while
// four online backups are taken; every restored backup holds a
// transaction-aligned row prefix. The Rust tokio::join! over the writer
// and the backup loop becomes a sync.WaitGroup with the backup loop on
// the test goroutine.
package backupunderfire

import (
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	gosqlharness "github.com/marcgauthier/murmur-sql/tests-live/gosqlharness"
)

func TestOnlineBackupsCaptureAtomicCommits(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	cluster, err := gosqlharness.NewCluster(gosqlharness.ClusterOptions{
		Name:      "backup-under-fire",
		NumNodes:  1,
		LocalOnly: true,
	})
	if err != nil {
		t.Fatalf("start cluster: %v", err)
	}
	failed := true
	defer func() { cluster.Finish(failed) }()

	if _, err := cluster.Node(0).Simple("CREATE TABLE records(id INTEGER PRIMARY KEY, batch INTEGER NOT NULL)"); err != nil {
		t.Fatalf("create records: %v", err)
	}
	writer, err := cluster.Connect(0)
	if err != nil {
		t.Fatalf("connect writer: %v", err)
	}
	defer writer.Close()

	var wg sync.WaitGroup
	var writerErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		for batch := int64(0); batch < 40; batch++ {
			// One multi-row INSERT is the actual atomic unit:
			// transaction control words are documented no-ops on the
			// SQL server.
			ids := [5]int64{batch * 5, batch*5 + 1, batch*5 + 2, batch*5 + 3, batch*5 + 4}
			if _, err := writer.Execute(
				"INSERT INTO records(id,batch) VALUES($1,$6),($2,$6),($3,$6),($4,$6),($5,$6)",
				ids[0], ids[1], ids[2], ids[3], ids[4], batch); err != nil {
				writerErr = err
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
		if _, err := writer.Simple("MURMUR FLUSH"); err != nil {
			writerErr = err
		}
	}()

	var archives []string
	var backupErr error
	for i := 0; i < 4 && backupErr == nil; i++ {
		saved, err := cluster.Node(0).Simple("MURMUR BACKUP SAVE")
		if err != nil {
			backupErr = err
			break
		}
		var bytes []byte
		if err := gosqlharness.Download(cluster.Node(0),
			"MURMUR BACKUP DOWNLOAD "+gosqlharness.SQLLiteral(saved[0][0]), &bytes); err != nil {
			backupErr = err
			break
		}
		size, err := strconv.ParseInt(saved[0][1], 10, 64)
		if err != nil || int64(len(bytes)) != size {
			backupErr = fmt.Errorf("downloaded %d bytes, want %s", len(bytes), saved[0][1])
			break
		}
		name := fmt.Sprintf("under-fire-%d.murmur-backup", i)
		if err := gosqlharness.Upload(cluster.Node(0), name, bytes); err != nil {
			backupErr = err
			break
		}
		archives = append(archives, name)
		time.Sleep(30 * time.Millisecond)
	}
	wg.Wait()
	if writerErr != nil {
		t.Fatalf("writer: %v", writerErr)
	}
	if backupErr != nil {
		t.Fatalf("backup loop: %v", backupErr)
	}
	count, err := cluster.Node(0).Simple("SELECT count(*) FROM records")
	if err != nil {
		t.Fatalf("count records: %v", err)
	}
	if !equalRows(count, [][]string{{"200"}}) {
		t.Fatalf("record count = %v, want 200", count)
	}
	previous := 0
	for _, name := range archives {
		admin, err := cluster.Connect(0)
		if err != nil {
			t.Fatalf("connect admin: %v", err)
		}
		_, err = admin.Simple("MURMUR RESTORE FROM " + gosqlharness.SQLLiteral(name) +
			" KEY " + gosqlharness.SQLLiteral(gosqlharness.Key))
		_ = admin.Close()
		if err != nil {
			t.Fatalf("restore %s: %v", name, err)
		}
		fresh, err := cluster.Connect(0)
		if err != nil {
			t.Fatalf("reconnect: %v", err)
		}
		rows, err := fresh.Simple("SELECT id,batch FROM records ORDER BY id")
		_ = fresh.Close()
		if err != nil {
			t.Fatalf("read restored %s: %v", name, err)
		}
		if len(rows) < previous || len(rows) > 200 {
			t.Fatalf("backup prefix regressed: %d rows after %d", len(rows), previous)
		}
		previous = len(rows)
		if len(rows)%5 != 0 {
			t.Fatalf("snapshot split an atomic five-row transaction: %d rows", len(rows))
		}
		for id, row := range rows {
			want := []string{fmt.Sprintf("%d", id), fmt.Sprintf("%d", id/5)}
			if !equalRows([][]string{row}, [][]string{want}) {
				t.Fatalf("row %d = %v, want %v", id, row, want)
			}
		}
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
