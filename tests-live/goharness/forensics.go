// Post-failure diagnostics capture.
//
// Status summaries go to the test log (truncated); full states go to
// forensics-<reason>-* files under each node directory (preserved with
// failure artifacts). Every fetch is infallible: timeouts and errors
// degrade to marker strings, so forensics survives dead and wedged nodes
// alike.
package goharness

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode"
)

// DumpNodeForensics dumps diagnostics for one node over a fresh
// short-lived wire connection (or records that it is stopped). It never
// fails the test.
//
// Deviation: the Rust function borrows the node's shared client and
// relies on per-query async cancellation for its 10 s fetch timeouts. A
// Go deadline on the shared connection would poison concurrent queries,
// so this port dials isolated connections through dial instead (nil dial
// means the node is stopped). Observable behavior is identical: the same
// queries, timeouts, truncations, log lines, and files.
func DumpNodeForensics(label, nodeDir string, dial func() (*NodeClient, error), reason string) {
	safeReason := strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' {
			return r
		}
		return '_'
	}, reason)

	replication := fetch(dial, "MURMUR SHOW REPLICATION")
	fmt.Fprintf(os.Stderr, "forensics(%s): %s replication: %s\n", reason, label, truncate(replication, 2048))
	peers := fetch(dial, "MURMUR SHOW PEERS")
	fmt.Fprintf(os.Stderr, "forensics(%s): %s peers: %s\n", reason, label, truncate(peers, 8192))
	metrics := fetch(dial, "MURMUR SHOW METRICS")

	writeFile(nodeDir, "forensics-"+safeReason+"-replication.txt", replication)
	writeFile(nodeDir, "forensics-"+safeReason+"-peers.txt", peers)
	writeFile(nodeDir, "forensics-"+safeReason+"-metrics.txt", metrics)
}

func fetch(dial func() (*NodeClient, error), sql string) string {
	if dial == nil {
		return "<node stopped>"
	}
	client, err := dial()
	if err != nil {
		if isTimeout(err) {
			return "<fetch timed out>"
		}
		return fmt.Sprintf("<fetch failed: %s>", oneLine(err.Error()))
	}
	defer client.Close()
	rows, err := client.client.SimpleTimeout(sql, 10*time.Second)
	if err != nil {
		if isTimeout(err) {
			return "<fetch timed out>"
		}
		return fmt.Sprintf("<fetch failed: %s>", oneLine(err.Error()))
	}
	if len(rows) == 0 {
		return "<empty>"
	}
	lines := make([]string, len(rows))
	for i, row := range rows {
		lines[i] = strings.Join(row, " | ")
	}
	return strings.Join(lines, "\n")
}

func isTimeout(err error) bool {
	type timeout interface{ Timeout() bool }
	if t, ok := err.(timeout); ok && t.Timeout() {
		return true
	}
	return os.IsTimeout(err)
}

func writeFile(nodeDir, name, body string) {
	if body == "" {
		return
	}
	if err := os.WriteFile(filepath.Join(nodeDir, name), []byte(body), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "forensics: %s write failed: %v\n", name, err)
	}
}

func oneLine(s string) string {
	// Mirror Rust one_line exactly: drop control runes, keep the
	// first 300 RUNES (not bytes).
	var b strings.Builder
	kept := 0
	for _, r := range s {
		if unicode.IsControl(r) {
			continue
		}
		b.WriteRune(r)
		kept++
		if kept >= 300 {
			break
		}
	}
	return b.String()
}

func truncate(s string, n int) string {
	if len(s) > n {
		for n > 0 && s[n] >= 0x80 && s[n] < 0xc0 {
			n--
		}
		return s[:n] + "...<truncated>"
	}
	return s
}
