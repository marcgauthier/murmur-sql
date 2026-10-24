package murmur_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/marcgauthier/murmur-sql/clients/go/internal/servertest"
)

// TestHostnameBootstrapPeersNodes pins DNS seeds end to end: two nodes
// with hostname bootstrap entries (and a hostname advertise on node 0)
// peer and converge with no runtime PEER ADD.
func TestHostnameBootstrapPeersNodes(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	dirs := []string{filepath.Join(base, "n0"), filepath.Join(base, "n1")}
	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	opts := servertest.MeshOptions(t, dirs, "")
	ports := []int{servertest.FreePort(t), servertest.FreePort(t)}
	for i := range opts {
		peer := ports[1-i]
		patched := strings.Replace(opts[i].ExtraYAML, `bind: "127.0.0.1:0"`,
			fmt.Sprintf(`bind: "127.0.0.1:%d"`, ports[i]), 1)
		opts[i].ExtraYAML = patched + fmt.Sprintf("  bootstrap:\n    - \"localhost:%d\"\n", peer)
	}
	opts[0].ExtraYAML += fmt.Sprintf("  advertise: \"localhost:%d\"\n", ports[0])

	var dbs []*sql.DB
	for i, dir := range dirs {
		server := servertest.Start(t, dir, opts[i])
		cfg := testConfig(server)
		cfg.Key = "mesh-key"
		dbs = append(dbs, open(t, cfg))
	}
	for _, db := range dbs {
		if _, err := db.ExecContext(ctx, `CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT NOT NULL)`); err != nil {
			t.Fatalf("create: %v", err)
		}
		if _, err := db.ExecContext(ctx, "MURMUR SET TABLE docs REPLICATED true"); err != nil {
			t.Fatalf("replicate: %v", err)
		}
	}
	if _, err := dbs[0].ExecContext(ctx, "INSERT INTO docs(id, body) VALUES(1, 'hello')"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if _, err := dbs[0].ExecContext(ctx, "MURMUR FLUSH"); err != nil {
		t.Fatalf("flush: %v", err)
	}
	heads, err := sqlRows(ctx, dbs[0], "MURMUR SHOW HEAD")
	if err != nil {
		t.Fatalf("head: %v", err)
	}
	var seq int64 = -1
	for _, h := range heads {
		if h["origin"].(string) == opts[0].NodeID {
			seq = h["applied"].(int64)
		}
	}
	if seq <= 0 {
		t.Fatalf("no writer head in %+v", heads)
	}
	if _, err := dbs[1].ExecContext(ctx,
		fmt.Sprintf("MURMUR WAIT FOR %s %d TIMEOUT 90", quoteArg(opts[0].NodeID), seq)); err != nil {
		t.Fatalf("wait: %v", err)
	}
	var body string
	if err := dbs[1].QueryRowContext(ctx, "SELECT body FROM docs WHERE id = 1").Scan(&body); err != nil {
		t.Fatalf("read: %v", err)
	}
	if body != "hello" {
		t.Fatalf("body = %q, want hello", body)
	}
	peers, err := sqlRows(ctx, dbs[1], "MURMUR SHOW PEERS")
	if err != nil {
		t.Fatalf("peers: %v", err)
	}
	if len(peers) == 0 {
		t.Fatal("no peers after hostname bootstrap")
	}
}
