package murmur_test

import (
	"encoding/binary"
	"fmt"
	"math"
	"net/netip"
	"os"

	"github.com/marcgauthier/murmur-sql/murmur"
	"github.com/marcgauthier/murmur-sql/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql/murmur/replication"
)

// exampleKey returns a fixed 32-byte data key. Production code loads the
// key from a KMS, a hardware module, or an encrypted key file instead.
func exampleKey() []byte {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 1)
	}
	return key
}

// exampleDir returns a scratch directory removed when the example ends.
func exampleDir() string {
	dir, err := os.MkdirTemp("", "murmur-example-")
	if err != nil {
		panic(err)
	}
	return dir
}

func Example_open() {
	dir := exampleDir()
	defer os.RemoveAll(dir)

	options := murmur.DefaultOptions()
	options.Mode = murmur.OpenCreateNew
	db, err := murmur.Open(dir+"/store", exampleKey(), options)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	if err := db.Migrate(murmur.NewMigration(
		`CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT NOT NULL)`)); err != nil {
		panic(err)
	}
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute("INSERT INTO users(id, name) VALUES(?, ?)", 1, "ada")
		return nil, err
	}); err != nil {
		panic(err)
	}
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT name FROM users WHERE id = ?", 1)
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(v.([][]sqlite.Value)[0][0].Str)
	// Output: ada
}

func Example_reopen() {
	dir := exampleDir()
	defer os.RemoveAll(dir)

	options := murmur.DefaultOptions()
	options.Mode = murmur.OpenCreateNew
	db, err := murmur.Open(dir+"/store", exampleKey(), options)
	if err != nil {
		panic(err)
	}
	if err := db.Migrate(murmur.NewMigration(
		`CREATE TABLE kv(k TEXT NOT NULL PRIMARY KEY, v TEXT NOT NULL)`)); err != nil {
		panic(err)
	}
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute("INSERT INTO kv(k, v) VALUES(?, ?)", "greeting", "hello")
		return nil, err
	}); err != nil {
		panic(err)
	}
	if err := db.Close(); err != nil {
		panic(err)
	}

	// Reopening with the same key unlocks the encrypted store.
	options.Mode = murmur.OpenExisting
	db, err = murmur.Open(dir+"/store", exampleKey(), options)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT v FROM kv WHERE k = ?", "greeting")
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(v.([][]sqlite.Value)[0][0].Str)
	// Output: hello
}

func Example_searchFTS() {
	dir := exampleDir()
	defer os.RemoveAll(dir)

	options := murmur.DefaultOptions()
	options.Mode = murmur.OpenCreateNew
	db, err := murmur.Open(dir+"/store", exampleKey(), options)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	if err := db.Migrate(murmur.NewMigration(
		`CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)`)); err != nil {
		panic(err)
	}
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		if _, err := s.Execute("INSERT INTO docs(id, body) VALUES(?, ?)", 1, "the quick fox"); err != nil {
			return nil, err
		}
		_, err := s.Execute("INSERT INTO docs(id, body) VALUES(?, ?)", 2, "lazy dog")
		return nil, err
	}); err != nil {
		panic(err)
	}
	if err := db.SetColumnFts("docs", "body", true); err != nil {
		panic(err)
	}
	if err := db.RefreshIndexes(); err != nil {
		panic(err)
	}
	_, statuses, err := db.SchemaWithSearchStatus()
	if err != nil || len(statuses) != 1 || !statuses[0].Ready {
		panic("fts index not ready")
	}
	index := `"` + statuses[0].Name + `"`
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT d.id FROM docs d JOIN temp."+index+" f ON f.rowid = d.id"+
			" WHERE "+index+" MATCH ? ORDER BY d.id", "quick")
	})
	if err != nil {
		panic(err)
	}
	for _, row := range v.([][]sqlite.Value) {
		fmt.Println(row[0].Int)
	}
	// Output: 1
}

func Example_searchVector() {
	dir := exampleDir()
	defer os.RemoveAll(dir)

	options := murmur.DefaultOptions()
	options.Mode = murmur.OpenCreateNew
	db, err := murmur.Open(dir+"/store", exampleKey(), options)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	if err := db.Migrate(murmur.NewMigration(
		`CREATE TABLE docs(id INTEGER PRIMARY KEY, embedding BLOB)`)); err != nil {
		panic(err)
	}
	blob := func(x, y float32) []byte {
		out := make([]byte, 8)
		binary.LittleEndian.PutUint32(out[0:], math.Float32bits(x))
		binary.LittleEndian.PutUint32(out[4:], math.Float32bits(y))
		return out
	}
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		for id, pt := range map[int64][2]float32{1: {0, 0}, 2: {3, 0}, 3: {0, 4}} {
			if _, err := s.Execute("INSERT INTO docs(id, embedding) VALUES(?, ?)",
				id, blob(pt[0], pt[1])); err != nil {
				return nil, err
			}
		}
		return nil, nil
	}); err != nil {
		panic(err)
	}
	if err := db.SetColumnVector("docs", "embedding",
		&murmur.VectorColumnConfig{Dimensions: 2, Distance: murmur.DistL2}); err != nil {
		panic(err)
	}
	if err := db.RefreshIndexes(); err != nil {
		panic(err)
	}
	_, statuses, err := db.SchemaWithSearchStatus()
	if err != nil || len(statuses) != 1 || !statuses[0].Ready {
		panic("vector index not ready")
	}
	index := `"` + statuses[0].Name + `"`
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT d.id FROM temp."+index+" v JOIN docs d ON d.id = v.rowid"+
			" WHERE v.embedding MATCH ? AND v.k = ? ORDER BY v.distance", "[0,0]", 2)
	})
	if err != nil {
		panic(err)
	}
	for _, row := range v.([][]sqlite.Value) {
		fmt.Println(row[0].Int)
	}
	// Output:
	// 1
	// 2
}

func Example_scan() {
	dir := exampleDir()
	defer os.RemoveAll(dir)

	options := murmur.DefaultOptions()
	options.Mode = murmur.OpenCreateNew
	db, err := murmur.Open(dir+"/store", exampleKey(), options)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	if err := db.Migrate(murmur.NewMigration(
		`CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT NOT NULL)`)); err != nil {
		panic(err)
	}
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		return s.ExecuteMany("INSERT INTO users(id, name) VALUES(?, ?)",
			[]any{1, "ada"},
			[]any{2, "grace"})
	}); err != nil {
		panic(err)
	}
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		row, err := s.Row("SELECT name FROM users WHERE id = ?", 1)
		if err != nil {
			return nil, err
		}
		var name string
		if err := row.Scan(&name); err != nil {
			return nil, err
		}
		rows, err := s.Rows("SELECT id, name FROM users ORDER BY id")
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		out := []string{name}
		for rows.Next() {
			var id int64
			var n string
			if err := rows.Scan(&id, &n); err != nil {
				return nil, err
			}
			out = append(out, fmt.Sprintf("%d=%s", id, n))
		}
		return out, rows.Err()
	})
	if err != nil {
		panic(err)
	}
	for _, line := range v.([]string) {
		fmt.Println(line)
	}
	// Output:
	// ada
	// 1=ada
	// 2=grace
}

func Example_stdlib() {
	dir := exampleDir()
	defer os.RemoveAll(dir)

	options := murmur.DefaultOptions()
	options.Mode = murmur.OpenCreateNew
	db, err := murmur.Open(dir+"/store", exampleKey(), options)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	sqldb := db.OpenDB()
	defer sqldb.Close()

	if _, err := sqldb.Exec(
		`CREATE TABLE accounts(id INTEGER PRIMARY KEY, balance INTEGER NOT NULL)`); err != nil {
		panic(err)
	}
	if _, err := sqldb.Exec("INSERT INTO accounts(id, balance) VALUES(1, 100), (2, 100)"); err != nil {
		panic(err)
	}
	tx, err := sqldb.Begin()
	if err != nil {
		panic(err)
	}
	if _, err := tx.Exec("UPDATE accounts SET balance = balance - 30 WHERE id = 1"); err != nil {
		panic(err)
	}
	if _, err := tx.Exec("UPDATE accounts SET balance = balance + 30 WHERE id = 2"); err != nil {
		panic(err)
	}
	if err := tx.Commit(); err != nil {
		panic(err)
	}
	var a, b int64
	if err := sqldb.QueryRow("SELECT balance FROM accounts WHERE id = 1").Scan(&a); err != nil {
		panic(err)
	}
	if err := sqldb.QueryRow("SELECT balance FROM accounts WHERE id = 2").Scan(&b); err != nil {
		panic(err)
	}
	fmt.Println(a, b)
	// Output: 70 130
}

func Example_backup() {
	dir := exampleDir()
	defer os.RemoveAll(dir)

	options := murmur.DefaultOptions()
	options.Mode = murmur.OpenCreateNew
	db, err := murmur.Open(dir+"/store", exampleKey(), options)
	if err != nil {
		panic(err)
	}
	if err := db.Migrate(murmur.NewMigration(
		`CREATE TABLE kv(k TEXT NOT NULL PRIMARY KEY, v TEXT NOT NULL)`)); err != nil {
		panic(err)
	}
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute("INSERT INTO kv(k, v) VALUES(?, ?)", "greeting", "hello")
		return nil, err
	}); err != nil {
		panic(err)
	}
	// A backup is a consistent, still-encrypted copy of the store.
	if err := db.Backup(dir + "/backup"); err != nil {
		panic(err)
	}
	if err := db.Close(); err != nil {
		panic(err)
	}

	restore := murmur.DefaultRestoreOptions()
	if err := murmur.RestoreBackup(dir+"/backup", dir+"/restored", exampleKey(), &restore); err != nil {
		panic(err)
	}
	options.Mode = murmur.OpenExisting
	db, err = murmur.Open(dir+"/restored", exampleKey(), options)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT v FROM kv WHERE k = ?", "greeting")
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(v.([][]sqlite.Value)[0][0].Str)
	// Output: hello
}

// Example_replication shows the wiring for one replicating node: identity,
// TLS credentials, table opt-in, and peering. It is compile-verified only;
// running it needs CA-issued certificates and a reachable peer (the server
// packages this wiring behind config files; see docs/USAGE.md).
func Example_replication() {
	dir := exampleDir()
	defer os.RemoveAll(dir)

	var nodeID, dbID [16]byte
	nodeID[0], dbID[0] = 1, 2
	credentials := replication.Credentials{
		Chain:      [][]byte{[]byte("ca-issued leaf chain (DER)")},
		PrivateKey: []byte("leaf private key (PKCS8 DER)"),
		Roots:      [][]byte{[]byte("cluster CA (DER)")},
	}
	var signingKey [32]byte
	config := replication.NewConfig(
		netip.MustParseAddrPort("127.0.0.1:0"), credentials, signingKey)
	options := murmur.DefaultOptions()
	options.Mode = murmur.OpenCreateNew
	options.Replication = config
	options.NodeID = &nodeID
	options.DatabaseID = &dbID
	db, err := murmur.Open(dir+"/store", exampleKey(), options)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	if err := db.Migrate(murmur.NewMigration(
		`CREATE TABLE records(id INTEGER PRIMARY KEY, name TEXT NOT NULL)`)); err != nil {
		panic(err)
	}
	// Tables replicate only after an explicit opt-in.
	if err := db.SetReplication("records", true); err != nil {
		panic(err)
	}
	// Dial one peer; every ordered pair on a small cluster, gossip and
	// anti-entropy beyond that.
	_ = db.Replication().AddPeer(netip.MustParseAddrPort("127.0.0.1:9001"))
}
