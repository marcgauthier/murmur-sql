// Post-failure diagnostics capture.
//
// Status summaries go to the test log (truncated); full states go to
// forensics-<reason>-* files under each node directory (preserved with
// failure artifacts). Every fetch is infallible: forensics must survive
// dead and wedged nodes alike, so failures are logged, never fatal.
package harness

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/marcgauthier/murmur-sql/murmur"
)

// DumpNodeForensics dumps diagnostics for one node. It never panics and
// never fails the test.
func DumpNodeForensics(label, nodeDir string, db *murmur.Database, reason string) {
	defer func() {
		_ = recover()
	}()
	dumpInner(label, nodeDir, db, reason)
}

func dumpInner(label, nodeDir string, db *murmur.Database, reason string) {
	safe := make([]rune, 0, len(reason))
	for _, c := range reason {
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' {
			safe = append(safe, c)
		} else {
			safe = append(safe, '_')
		}
	}
	safeReason := string(safe)

	status := fetch(func() string {
		if db == nil {
			return "<node stopped>"
		}
		return fmt.Sprintf("%#v", db.Status())
	})
	fmt.Fprintf(os.Stderr, "forensics(%s): %s status: %s\n", reason, label, truncate(status, 2048))

	peers := fetch(func() string {
		if db == nil {
			return "<node stopped>"
		}
		if h := db.Replication(); h != nil {
			return fmt.Sprintf("%#v", h.Status())
		}
		return "<no replication handle>"
	})
	fmt.Fprintf(os.Stderr, "forensics(%s): %s replication: %s\n", reason, label, truncate(peers, 8192))

	metrics := fetch(func() string {
		if db == nil {
			return "<node stopped>"
		}
		sqlite, err := db.SqliteMetrics()
		sqliteText := "<sqlite metrics unavailable>"
		if err == nil {
			sqliteText = fmt.Sprintf("%#v", sqlite)
		} else {
			sqliteText = fmt.Sprintf("<sqlite metrics: %v>", err)
		}
		return fmt.Sprintf("sqlite: %s\nspool: %#v", sqliteText, db.SpoolMetrics())
	})

	writeForensicsFile(nodeDir, "forensics-"+safeReason+"-status.txt", status)
	writeForensicsFile(nodeDir, "forensics-"+safeReason+"-peers.txt", peers)
	writeForensicsFile(nodeDir, "forensics-"+safeReason+"-metrics.txt", metrics)
}

// fetch runs a fetch; panics and empty results degrade to a marker string.
func fetch(f func() string) (out string) {
	defer func() {
		if recover() != nil {
			out = "<fetch panicked>"
		}
	}()
	if out = f(); out == "" {
		return "<empty>"
	}
	return out
}

func writeForensicsFile(nodeDir, name, body string) {
	if body == "" {
		return
	}
	if err := os.WriteFile(filepath.Join(nodeDir, name), []byte(body), 0o600); err != nil {
		fmt.Fprintf(os.Stderr, "forensics: %s write failed: %v\n", name, err)
	}
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n] + "...<truncated>"
	}
	return s
}
