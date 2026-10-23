package murmur

import (
	"testing"

	"github.com/marcgauthier/murmur-sql/murmur/internal/sqlite"
)

// hostileStrings mirrors the sqli-api live payloads: SQL text that must
// only ever travel as inert data.
var hostileStrings = []string{
	`{"query":"INSERT INTO records VALUES (1); DROP TABLE records"}`,
	`{"query":"' UNION SELECT secret FROM secrets --"}`,
	"DROP TABLE live_typed_records",
	"'; DELETE FROM live_typed_records; --",
	"' OR '1'='1",
	"1; DROP TABLE users",
	"Robert'); DROP TABLE users;--",
}

// TestHostileStringsStayInert pins the SQL-injection defense at the
// unit level: hostile text in bound parameters stores byte-exact and
// matches literally, and statement smuggling through Execute text
// never executes.
func TestHostileStringsStayInert(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, dir+"/db")
	defer func() { _ = db.Close() }()
	if err := db.Migrate(NewMigration(coreTestDDL)); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	execMust(t, db, "INSERT INTO users (id, name, score) VALUES (1, 'sentinel', 10)")
	count := func() int64 {
		v, err := db.Read(func(s *Sql) (any, error) {
			return s.QueryRow("SELECT COUNT(*) FROM users")
		})
		if err != nil {
			t.Fatalf("count: %v", err)
		}
		return v.([]sqlite.Value)[0].Int
	}
	// Bound parameters carry hostile text as data, byte-exact.
	for i, hostile := range hostileStrings {
		id := int64(100 + i)
		_, err := db.Transaction(func(s *Sql) (any, error) {
			return s.Execute("INSERT INTO users (id, name, score) VALUES (?, ?, ?)", id, hostile, int64(i))
		})
		if err != nil {
			t.Fatalf("insert hostile %d: %v", i, err)
		}
		v, err := db.Read(func(s *Sql) (any, error) {
			return s.QueryRow("SELECT name FROM users WHERE id = ?", id)
		})
		if err != nil {
			t.Fatalf("read hostile %d: %v", i, err)
		}
		if got := v.([]sqlite.Value)[0].Str; got != hostile {
			t.Fatalf("hostile %d round-trip = %q", i, got)
		}
	}
	if n := count(); n != int64(1+len(hostileStrings)) {
		t.Fatalf("rows = %d, want sentinel plus %d hostile", n, len(hostileStrings))
	}
	// Hostile text in a WHERE parameter matches literally: the
	// always-true smuggle finds no row named like itself... except the
	// row that literally holds that text.
	v, err := db.Read(func(s *Sql) (any, error) {
		return s.Query("SELECT id FROM users WHERE name = ?", "' OR '1'='1")
	})
	if err != nil {
		t.Fatalf("where hostile: %v", err)
	}
	rows := v.([][]sqlite.Value)
	if len(rows) != 1 || rows[0][0].Int != 104 {
		t.Fatalf("literal match = %v, want exactly the hostile row", rows)
	}
	// Statement smuggling through Execute text never executes: the
	// table survives with its rows whatever the driver does with the
	// trailing text.
	smuggled := "INSERT INTO users (id, name, score) VALUES (999, 'x', 0); DROP TABLE users"
	_, _ = db.Transaction(func(s *Sql) (any, error) {
		return s.Execute(smuggled)
	})
	if n := count(); n != int64(1+len(hostileStrings)) && n != int64(2+len(hostileStrings)) {
		t.Fatalf("rows after smuggle = %d (table dropped or phantom rows?)", n)
	}
	name, err := db.Read(func(s *Sql) (any, error) {
		return s.QueryRow("SELECT name FROM users WHERE id = 1")
	})
	if err != nil {
		t.Fatalf("sentinel unreadable after smuggle: %v", err)
	}
	if got := name.([]sqlite.Value)[0].Str; got != "sentinel" {
		t.Fatalf("sentinel = %q after smuggle", got)
	}
	// Batch splitting is quote-aware: a semicolon inside a string
	// value does not split the batch into a smuggled statement.
	before := count()
	_, err = db.Transaction(func(s *Sql) (any, error) {
		return nil, s.ExecuteBatch(
			`INSERT INTO users (id, name, score) VALUES (1000, 'a;b''c', 1)`)
	})
	if err != nil {
		t.Fatalf("quoted batch: %v", err)
	}
	if after := count(); after != before+1 {
		t.Fatalf("rows %d -> %d across quoted batch", before, after)
	}
	v, err = db.Read(func(s *Sql) (any, error) {
		return s.QueryRow("SELECT name FROM users WHERE id = 1000")
	})
	if err != nil {
		t.Fatalf("read quoted: %v", err)
	}
	if got := v.([]sqlite.Value)[0].Str; got != "a;b'c" {
		t.Fatalf("quoted value = %q", got)
	}
}
