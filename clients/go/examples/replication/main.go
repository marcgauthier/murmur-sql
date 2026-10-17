// Example replication manages peers, retention, and CRDT policy operations
// across two meshed scratch servers.
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	murmur "github.com/marcgauthier/murmur/clients/go"
	"github.com/marcgauthier/murmur/clients/go/examples/internal/run"
	"github.com/marcgauthier/murmur/clients/go/internal/servertest"
)

func open(t servertest.TB, port int) *murmur.DB {
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
		policies := "merge_policies = { \"stats.count\" = \"counter\", \"stats.highest\" = \"max\", \"stats.lowest\" = \"min\", \"stats.tags\" = \"set\" }\n"
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

		status, err := db0.ShowReplication(ctx)
		if err != nil || !status.Running || status.Peers != 0 {
			t.Fatalf("status: %+v, %v", status, err)
		}
		peerStatus, err := db1.ShowReplication(ctx)
		if err != nil || peerStatus.Address == nil {
			t.Fatalf("peer status: %+v, %v", peerStatus, err)
		}
		fmt.Printf("node0 repl=%s node1 repl=%s\n", *status.Address, *peerStatus.Address)

		if err := db0.PeerAdd(ctx, *peerStatus.Address); err != nil {
			t.Fatalf("add: %v", err)
		}
		deadline := time.Now().Add(30 * time.Second)
		for {
			peers, err := db0.ShowPeers(ctx)
			if err != nil {
				t.Fatalf("peers: %v", err)
			}
			connected := false
			for _, p := range peers {
				if p.NodeID == node1 && p.Connected {
					connected = true
					fmt.Printf("peer %s connected sent_watermark=%d\n", p.NodeID, p.SentWatermark)
				}
			}
			if connected {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("peer never connected")
			}
			time.Sleep(200 * time.Millisecond)
		}
		if err := db0.PeerSync(ctx); err != nil {
			t.Fatalf("sync: %v", err)
		}
		if collected, err := db0.RetentionCollect(ctx); err != nil || collected < 0 {
			t.Fatalf("collect: %d, %v", collected, err)
		}
		entries, err := db0.ShowRetention(ctx)
		if err != nil {
			t.Fatalf("retention: %v", err)
		}
		fmt.Printf("retention rows: %d\n", len(entries))

		if _, err := db0.ExecContext(ctx, `CREATE TABLE stats(key TEXT NOT NULL PRIMARY KEY,
			count INTEGER NOT NULL, highest INTEGER NOT NULL, lowest REAL NOT NULL, tags BLOB)`); err != nil {
			t.Fatalf("create: %v", err)
		}
		if _, err := db0.ExecContext(ctx, "INSERT INTO stats VALUES ($1, $2, $3, 2.5, NULL)",
			"row-1", 10, 5); err != nil {
			t.Fatalf("insert: %v", err)
		}
		if err := db0.PolicyCounterAdd(ctx, "stats", "count", "row-1", 5); err != nil {
			t.Fatalf("counter: %v", err)
		}
		if err := db0.PolicyMax(ctx, "stats", "highest", "row-1", 42); err != nil {
			t.Fatalf("max: %v", err)
		}
		if err := db0.PolicyMin(ctx, "stats", "lowest", "row-1", -1.5); err != nil {
			t.Fatalf("min: %v", err)
		}
		if err := db0.PolicySetAdd(ctx, "stats", "tags", "row-1", "blue"); err != nil {
			t.Fatalf("set add: %v", err)
		}
		var added []byte
		if err := db0.QueryRowContext(ctx, "SELECT tags FROM stats WHERE key=$1", "row-1").Scan(&added); err != nil || len(added) == 0 {
			t.Fatalf("set after add: %x, %v", added, err)
		}
		if err := db0.PolicySetRemove(ctx, "stats", "tags", "row-1", "blue"); err != nil {
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

		if err := db0.PeerRemove(ctx, node1); err != nil {
			t.Fatalf("remove: %v", err)
		}
		if err := db0.PeerRetire(ctx, node1); err != nil {
			t.Fatalf("retire: %v", err)
		}
		entries, err = db0.ShowRetention(ctx)
		if err != nil {
			t.Fatalf("retention: %v", err)
		}
		retired := false
		for _, e := range entries {
			if e.NodeID == node1 && e.Retired {
				retired = true
			}
		}
		if !retired {
			t.Fatalf("retired peer missing from retention")
		}
		if err := db0.PeerRetire(ctx, node0); murmur.CodeOf(err) != "22023" {
			t.Fatalf("self-retire: code = %q, want 22023", murmur.CodeOf(err))
		}
		fmt.Println("replication: verified peers, retention, policy ops, and retire")
	})
}
