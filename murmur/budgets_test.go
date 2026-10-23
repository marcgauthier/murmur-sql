package murmur

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/marcgauthier/murmur-sql/murmur/internal/sqlite"
)

// TestCommitBudgetRejectsOversizeWrite pins the transaction_bytes
// commit budget on one node: a single write past the budget fails with
// the KindLimit budget error, leaves no partial row, and does not
// poison the writer.
func TestCommitBudgetRejectsOversizeWrite(t *testing.T) {
	options := DefaultOptions()
	options.Mode = OpenCreateNew
	options.TransactionBytes = 64 << 10
	options.QueueBytes = 8 << 20
	options.CaptureBytes = 1 << 20
	db, err := Open(t.TempDir()+"/db", testKey(), options)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ddl := "CREATE TABLE records(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL, phone TEXT NOT NULL, score INTEGER NOT NULL)"
	if err := db.Migrate(NewMigration(ddl)); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	big := strings.Repeat("V", (64<<10)+1)
	_, err = db.Transaction(func(s *Sql) (any, error) {
		_, err := s.Execute(
			"INSERT INTO records(id, name, phone, score) VALUES(?, ?, ?, ?)",
			fmt.Sprintf("%032x", 2), big, "", int64(0),
		)
		return nil, err
	})
	if err == nil {
		t.Fatal("oversize value accepted, want budget rejection")
	}
	var mErr *Error
	if !errors.As(err, &mErr) || mErr.Kind != KindLimit {
		t.Fatalf("rejection error %v is not the budget limit", err)
	}
	if !strings.Contains(err.Error(), "budget exceeded") {
		t.Fatalf("rejection error %v does not name the budget", err)
	}
	count := func() int64 {
		v, err := db.Read(func(s *Sql) (any, error) {
			return s.QueryRow("SELECT COUNT(*) FROM records")
		})
		if err != nil {
			t.Fatalf("count: %v", err)
		}
		return v.([]sqlite.Value)[0].Int
	}
	if n := count(); n != 0 {
		t.Fatalf("%d rows after rejection, want 0 (partial apply?)", n)
	}
	// Writer not poisoned: a small write lands.
	_, err = db.Transaction(func(s *Sql) (any, error) {
		_, err := s.Execute(
			"INSERT INTO records(id, name, phone, score) VALUES(?, ?, ?, ?)",
			fmt.Sprintf("%032x", 3), "ok", "", int64(0),
		)
		return nil, err
	})
	if err != nil {
		t.Fatalf("post-rejection write: %v", err)
	}
	if n := count(); n != 1 {
		t.Fatalf("%d rows after small write, want 1", n)
	}
}
