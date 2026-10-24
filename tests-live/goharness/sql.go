// SQL assertions shared by live workloads. No embedded database types or calls.
//
// Deviation: the Rust helpers panic (expect/assert) on failure; these
// return errors so scenarios decide how to fail (usually t.Fatalf with
// forensics). Messages match the Rust assertion texts.
package goharness

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// Fixture creates the shared records fixture on every node and enables
// its replication.
func Fixture(cluster *Cluster) error {
	for i := 0; i < cluster.Len(); i++ {
		if _, err := cluster.Node(i).Simple("CREATE TABLE IF NOT EXISTS records(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL)"); err != nil {
			return fmt.Errorf("create SQL fixture: %w", err)
		}
		if _, err := cluster.Node(i).Simple("MURMUR SET TABLE records REPLICATED true"); err != nil {
			return fmt.Errorf("enable fixture replication: %w", err)
		}
		if _, err := cluster.Node(i).Simple("MURMUR FLUSH"); err != nil {
			return fmt.Errorf("flush fixture schema: %w", err)
		}
	}
	return nil
}

// Rows reads the fixture rows.
func Rows(client *NodeClient) ([][]string, error) {
	rows, err := client.Simple("SELECT id, name FROM records ORDER BY id")
	if err != nil {
		return nil, fmt.Errorf("read SQL rows: %w", err)
	}
	return rows, nil
}

// Status queries the engine diagnostic snapshot over SQL (one JSON row).
func Status(client *NodeClient) (any, error) {
	rows, err := client.Simple("MURMUR SHOW STATUS")
	if err != nil {
		return nil, fmt.Errorf("query engine diagnostic snapshot over SQL: %w", err)
	}
	if len(rows) != 1 {
		return nil, fmt.Errorf("query engine diagnostic snapshot over SQL: expected 1 row, got %d", len(rows))
	}
	var value any
	if err := json.Unmarshal([]byte(rows[0][0]), &value); err != nil {
		return nil, fmt.Errorf("decode diagnostic JSON: %w", err)
	}
	return value, nil
}

// Peers queries SQL peers.
func Peers(client *NodeClient) ([][]string, error) {
	rows, err := client.Simple("MURMUR SHOW PEERS")
	if err != nil {
		return nil, fmt.Errorf("query SQL peers: %w", err)
	}
	return rows, nil
}

// Converge waits until every listed node reads expected fixture rows.
func Converge(cluster *Cluster, nodes []int, expected [][]string, timeout time.Duration) error {
	end := time.Now().Add(timeout)
	for {
		type snapshot struct {
			node int
			rows [][]string
		}
		// Like Rust converge: snapshot EVERY node each round and
		// report them all on timeout, not just the mismatches.
		var last []snapshot
		for _, i := range nodes {
			rows, err := Rows(cluster.Node(i))
			if err != nil {
				return err
			}
			last = append(last, snapshot{node: i, rows: rows})
		}
		all := true
		for _, s := range last {
			if !rowsEqual(s.rows, expected) {
				all = false
				break
			}
		}
		if all {
			return nil
		}
		if !time.Now().Before(end) {
			actual := make([]string, 0, len(last))
			for _, s := range last {
				actual = append(actual, fmt.Sprintf("(%d, %q)", s.node, s.rows))
			}
			return fmt.Errorf("SQL convergence timeout: expected=%q, actual=[%s]",
				expected, strings.Join(actual, ", "))
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// AssertRows waits until every listed node reads expected rows for an
// arbitrary query. It mirrors the Rust assert_rows helper exactly: a 60 s
// deadline, 50 ms polls, and tolerance for 42703/42P01 while fixtures
// converge (any other query error fails immediately).
//
// Deviation: the Rust helper panics on failure; this returns an error so
// scenarios decide how to fail (usually t.Fatalf with forensics).
// Messages match the Rust assertion texts.
func AssertRows(cluster *Cluster, nodes []int, query string, expected [][]string) error {
	deadline := time.Now().Add(60 * time.Second)
	for {
		all := true
		type mismatch struct {
			node int
			rows [][]string
		}
		var last []mismatch
		for _, i := range nodes {
			rows, err := cluster.Node(i).Simple(query)
			if err != nil {
				if code := ErrorCode(err); code == "42703" || code == "42P01" {
					all = false
					continue
				}
				return fmt.Errorf("poll SQL state: %w", err)
			}
			if !rowsEqual(rows, expected) {
				all = false
				last = append(last, mismatch{node: i, rows: rows})
			}
		}
		if all {
			return nil
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("SQL convergence timed out: %s; expected=%v, got=%v", query, expected, last)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// PeerCounts waits until the listed nodes report the expected connected
// peer counts (SHOW PEERS rows with connected "true").
func PeerCounts(cluster *Cluster, nodes []int, counts []int, timeout time.Duration) error {
	if len(nodes) != len(counts) {
		return fmt.Errorf("peer counts: %d nodes but %d counts", len(nodes), len(counts))
	}
	end := time.Now().Add(timeout)
	for {
		actual := make([]int, 0, len(nodes))
		for _, i := range nodes {
			peers, err := Peers(cluster.Node(i))
			if err != nil {
				return err
			}
			n := 0
			for _, p := range peers {
				if len(p) > 2 && p[2] == "true" {
					n++
				}
			}
			actual = append(actual, n)
		}
		if intsEqual(actual, counts) {
			return nil
		}
		if !time.Now().Before(end) {
			return fmt.Errorf("SQL peer counts: actual=%v, expected=%v", actual, counts)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// rowsEqual compares row sets by value. Unlike reflect.DeepEqual it treats
// a nil set as equal to an empty one (both mean "no rows").
func rowsEqual(a, b [][]string) bool {
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

func intsEqual(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
