// Example replication manages peers, retention, and CRDT policy operations
// across two meshed scratch servers.
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	murmur "github.com/marcgauthier/murmur-sql/clients/go"
	"github.com/marcgauthier/murmur-sql/clients/go/examples/internal/run"
	"github.com/marcgauthier/murmur-sql/clients/go/internal/servertest"
)

func open(t servertest.TB, port int) *sql.DB {
	db, err := murmur.Open(context.Background(), murmur.Config{
		Host: "127.0.0.1", Port: port,
		User: "admin", Password: "test-password", Key: "example-key",
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return db
}

func main() {
	run.Main(func(t servertest.TB, dir string) {
		ctx := context.Background()
		dirs := []string{filepath.Join(dir, "n0"), filepath.Join(dir, "n1")}
		for _, d := range dirs {
			if err := os.MkdirAll(d, 0o755); err != nil {
				t.Fatalf("mkdir: %v", err)
			}
		}
		policies := "  merge_policies:\n" +
			"    \"stats.count\": \"counter\"\n" +
			"    \"stats.highest\": \"max\"\n" +
			"    \"stats.lowest\": \"min\"\n" +
			"    \"stats.tags\": \"set\"\n"
		opts := servertest.MeshOptions(t, dirs, policies)
		node0, node1 := opts[0].NodeID, opts[1].NodeID
		servers := []*servertest.Server{
			servertest.Start(t, dirs[0], opts[0]),
			servertest.Start(t, dirs[1], opts[1]),
		}
		db0 := open(t, servers[0].Port)
		defer db0.Close()
		db1 := open(t, servers[1].Port)
		defer db1.Close()

		var running bool
		var address, lastError *string
		var peers, members, staged, applied, committed int64
		if err := db1.QueryRowContext(ctx, "MURMUR SHOW REPLICATION").Scan(&running, &address, &peers, &members, &staged, &applied, &lastError, &committed); err != nil || !running || address == nil {
			t.Fatalf("status: %v", err)
		}
		// The address is produced by our scratch server. Escape SQL literals.
		if _, err := db0.ExecContext(ctx, "MURMUR PEER ADD '"+strings.ReplaceAll(*address, "'", "''")+"'"); err != nil {
			t.Fatalf("add: %v", err)
		}
		deadline := time.Now().Add(30 * time.Second)
		for {
			rows, err := db0.QueryContext(ctx, "MURMUR SHOW PEERS")
			if err != nil {
				t.Fatalf("peers: %v", err)
			}
			connected := false
			for rows.Next() {
				var id string
				var addr *string
				var active, schema, push bool
				var seen *int64
				var watermark int64
				if err := rows.Scan(&id, &addr, &active, &seen, &watermark, &schema, &push); err != nil {
					t.Fatalf("scan: %v", err)
				}
				if id == node1 && active {
					connected = true
				}
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("rows: %v", err)
			}
			rows.Close()
			if connected {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("peer never connected")
			}
			time.Sleep(200 * time.Millisecond)
		}
		if _, err := db0.ExecContext(ctx, "MURMUR PEER SYNC"); err != nil {
			t.Fatalf("sync: %v", err)
		}
		var collected int64
		if err := db0.QueryRowContext(ctx, "MURMUR RETENTION COLLECT").Scan(&collected); err != nil || collected < 0 {
			t.Fatalf("collect: %v", err)
		}
		if _, err := db0.ExecContext(ctx, `CREATE TABLE stats(key TEXT NOT NULL PRIMARY KEY,
			count INTEGER NOT NULL, highest INTEGER NOT NULL, lowest REAL NOT NULL, tags BLOB)`); err != nil {
			t.Fatalf("create: %v", err)
		}
		if _, err := db0.ExecContext(ctx, "INSERT INTO stats VALUES ($1, $2, $3, 2.5, NULL)",
			"row-1", 10, 5); err != nil {
			t.Fatalf("insert: %v", err)
		}
		if _, err := db0.ExecContext(ctx, "MURMUR POLICY COUNTER_ADD 'stats' 'count' 'row-1' 5"); err != nil {
			t.Fatalf("counter: %v", err)
		}
		if _, err := db0.ExecContext(ctx, "MURMUR POLICY MAX 'stats' 'highest' 'row-1' 42"); err != nil {
			t.Fatalf("max: %v", err)
		}
		if _, err := db0.ExecContext(ctx, "MURMUR POLICY MIN 'stats' 'lowest' 'row-1' -1.5"); err != nil {
			t.Fatalf("min: %v", err)
		}
		if _, err := db0.ExecContext(ctx, "MURMUR POLICY SET_ADD 'stats' 'tags' 'row-1' 'blue'"); err != nil {
			t.Fatalf("set add: %v", err)
		}
		var added []byte
		if err := db0.QueryRowContext(ctx, "SELECT tags FROM stats WHERE key=$1", "row-1").Scan(&added); err != nil || len(added) == 0 {
			t.Fatalf("set after add: %x, %v", added, err)
		}
		if _, err := db0.ExecContext(ctx, "MURMUR POLICY SET_REMOVE 'stats' 'tags' 'row-1' 'blue'"); err != nil {
			t.Fatalf("set remove: %v", err)
		}
		var removed []byte
		if err := db0.QueryRowContext(ctx, "SELECT tags FROM stats WHERE key=$1", "row-1").Scan(&removed); err != nil || string(added) == string(removed) {
			t.Fatalf("set after remove: %x, %v", removed, err)
		}
		var lowest float64
		if err := db0.QueryRowContext(ctx, "SELECT lowest FROM stats WHERE key=$1", "row-1").Scan(&lowest); err != nil || lowest != -1.5 {
			t.Fatalf("min result: %v, %v", lowest, err)
		}
		var count, highest int64
		if err := db0.QueryRowContext(ctx,
			"SELECT count, highest FROM stats WHERE key = $1", "row-1").Scan(&count, &highest); err != nil {
			t.Fatalf("select: %v", err)
		}
		if count != 15 || highest != 42 {
			t.Fatalf("got %d/%d, want 15/42", count, highest)
		}

		if _, err := db0.ExecContext(ctx, "MURMUR PEER REMOVE "+"'"+strings.ReplaceAll(node1, "'", "''")+"'"); err != nil {
			t.Fatalf("remove: %v", err)
		}
		if _, err := db0.ExecContext(ctx, "MURMUR PEER RETIRE "+"'"+strings.ReplaceAll(node1, "'", "''")+"'"); err != nil {
			t.Fatalf("retire: %v", err)
		}
		rows, err := db0.QueryContext(ctx, "MURMUR SHOW RETENTION")
		if err != nil {
			t.Fatalf("retention: %v", err)
		}
		retiredPeer := false
		for rows.Next() {
			var id string
			var seen, collected, last int64
			var retired bool
			var origin *string
			var ack *int64
			if err := rows.Scan(&id, &seen, &retired, &origin, &ack, &collected, &last); err != nil {
				t.Fatalf("scan: %v", err)
			}
			if id == node1 && retired {
				retiredPeer = true
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("rows: %v", err)
		}
		rows.Close()
		if !retiredPeer {
			t.Fatalf("retired peer missing")
		}
		_, err = db0.ExecContext(ctx, "MURMUR PEER RETIRE '"+node0+"'")
		var serverErr *murmur.Error
		if !errors.As(err, &serverErr) || serverErr.Code != "22023" {
			t.Fatalf("self-retire: %v", err)
		}
		fmt.Println("replication: verified peers, retention, policy ops, and retire")
	})
}
