// Live scenario: a real encrypted store holding 6000 rows x 2 tables is
// populated, closed and reopened under the unlock watchdog, and every
// reopen must recover identical contents.
//
// Go port of tests/open_progress.rs using tests-live/goharness. Identical
// behavior: migrate and populate two tables, verify count + ordinal-sum
// contents, reload under the watchdog twice with identical contents,
// reject wrong/short unlock keys without a handle, fail a can-never-
// succeed unlock closed under a tightened stall knob, and open a fresh
// empty database. Phase sequences, heartbeats, per-table tracking,
// per-phase timing and status-snapshot isolation have no wire counterpart
// and are dropped (see the scenario README.md).
package openprogress

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	goharness "github.com/marcgauthier/murmur-sql/tests-live/goharness"
)

// Rows per table (Go: `rowsPerTable = 6000`).
const rowsPerTable = 6000

// Short key for the open-failure leg (Go: `[]byte{1, 2, 3}`).
const shortKeyHex = "010203"

// PG credentials the harness provisions (`admin` / `test-password`;
// see `goharness/cluster.go`).
const (
	pgUser     = "admin"
	pgPassword = "test-password"
)

// repeat32 returns 32 copies of b: the application key is 0x63 x32 (Go:
// `bytes.Repeat([]byte{0x63}, 32)`), the wrong key 0x44 x32.
func repeat32(b byte) [32]byte {
	var out [32]byte
	for i := range out {
		out[i] = b
	}
	return out
}

var (
	appKey   = repeat32(0x63)
	wrongKey = repeat32(0x44)
)

// migrate creates both tables on the single node (Go: the `populate`
// role schema).
func migrate(cluster *goharness.Cluster) error {
	for _, table := range []string{"requests", "events"} {
		if err := cluster.EnsureMigrated(0,
			fmt.Sprintf("CREATE TABLE %s(id INTEGER PRIMARY KEY, message TEXT NOT NULL, ordinal INTEGER NOT NULL)", table)); err != nil {
			return fmt.Errorf("migrate %s: %w", table, err)
		}
	}
	return nil
}

// populate fills both tables (Go: the `populate` role). One durability
// barrier at the end; rows are visible locally as they commit.
func populate(cluster *goharness.Cluster) error {
	for _, table := range []string{"requests", "events"} {
		for i := int64(0); i < rowsPerTable; i++ {
			message := fmt.Sprintf("realistic diagnostic %d %s", i, strings.Repeat("trace ", 512))
			if _, err := cluster.Node(0).Execute(
				fmt.Sprintf("INSERT INTO %s(id, message, ordinal) VALUES($1, $2, $3)", table),
				i, message, i); err != nil {
				return fmt.Errorf("populate: insert %s %d: %w", table, i, err)
			}
		}
	}
	if _, err := cluster.Node(0).Simple("MURMUR FLUSH"); err != nil {
		return fmt.Errorf("populate: flush: %w", err)
	}
	fmt.Fprintf(os.Stderr, "populate: %d rows x2 tables\n", rowsPerTable)
	return nil
}

// verifyContents checks count + ordinal-sum per table (Go:
// `verifyProgressTable`).
func verifyContents(cluster *goharness.Cluster, name string) error {
	for _, table := range []string{"requests", "events"} {
		rows, err := cluster.Node(0).Simple(
			fmt.Sprintf("SELECT COUNT(*), COALESCE(SUM(ordinal), 0) FROM %s", table))
		if err != nil {
			return fmt.Errorf("%s: verify %s: %w", name, table, err)
		}
		if len(rows) == 0 {
			return fmt.Errorf("%s: verify %s: no rows", name, table)
		}
		if len(rows[0]) < 2 {
			return fmt.Errorf("%s: verify %s: bad row %v", name, table, rows[0])
		}
		count, err := strconv.ParseInt(rows[0][0], 10, 64)
		if err != nil {
			return fmt.Errorf("%s: verify %s: bad count %v", name, table, rows[0])
		}
		sum, err := strconv.ParseInt(rows[0][1], 10, 64)
		if err != nil {
			return fmt.Errorf("%s: verify %s: bad sum %v", name, table, rows[0])
		}
		if count != rowsPerTable {
			return fmt.Errorf("%s: wrong count %s: %d", name, table, count)
		}
		if sum != rowsPerTable*(rowsPerTable-1)/2 {
			return fmt.Errorf("%s: wrong contents %s: sum=%d", name, table, sum)
		}
	}
	return nil
}

// connectKey connects through the wire client's SQL unlock path.
func connectKey(cluster *goharness.Cluster, key string) (*goharness.Client, error) {
	addr := cluster.NodeInfo(0).SQLAddr
	config := goharness.NewConfig(pgUser, pgPassword, key)
	config.Host = addr.Addr().String()
	config.Port = addr.Port()
	config.ConnectTimeout = 5 * time.Second
	return goharness.Connect(config)
}

// reloadAndVerify is the reload leg (Go: the `reload` role): stop +
// restart under the unlock watchdog, then contents must verify exactly.
func reloadAndVerify(cluster *goharness.Cluster, name string) error {
	start := time.Now()
	cluster.StopNode(0)
	if err := cluster.StartNode(0); err != nil {
		return fmt.Errorf("%s: restart under watchdog: %w", name, err)
	}
	fmt.Fprintf(os.Stderr, "%s: reopened under watchdog in %v\n", name, time.Since(start))
	if err := verifyContents(cluster, name); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "PASS %s: contents identical after reopen\n", name)
	return nil
}

// openFailureLegs is the open-failure leg (Go: the `open-failure` role):
// the unlock must fail, no usable handle is returned, and the correct
// key still works after.
func openFailureLegs(cluster *goharness.Cluster) error {
	for _, key := range []string{goharness.Hex(wrongKey[:]), shortKeyHex} {
		client, err := connectKey(cluster, key)
		if err == nil {
			_ = client.Close()
			return fmt.Errorf("wrong unlock key unexpectedly accepted")
		}
		if message := goharness.DBMessage(err); goharness.ErrorCode(err) != "28000" || !strings.Contains(message, "incorrect key") {
			return fmt.Errorf("unlock failure did not identify wrong key: %s", message)
		}
	}
	client, err := connectKey(cluster, goharness.Hex(appKey[:]))
	if err != nil {
		return fmt.Errorf("correct key after failures: %w", err)
	}
	defer client.Close()
	if _, err := client.Simple("SELECT 1"); err != nil {
		return err
	}
	fmt.Fprintln(os.Stderr, "PASS open-failure: wrong/short keys rejected, correct key accepted")
	return nil
}

// watchdogLeg: an unlock that can never succeed must fail closed after
// the stall window instead of hanging. The stall knob is tightened for
// this leg only and restored afterwards.
func watchdogLeg(cluster *goharness.Cluster) error {
	const stallEnv = "MURMUR_LIVE_UNLOCK_STALL_SECONDS"
	saved, hadSaved := os.LookupEnv(stallEnv)
	_ = os.Setenv(stallEnv, "2")
	defer func() {
		if hadSaved {
			_ = os.Setenv(stallEnv, saved)
		} else {
			_ = os.Unsetenv(stallEnv)
		}
	}()
	addr := cluster.NodeInfo(0).SQLAddr
	start := time.Now()
	client, err := goharness.ConnectWithWatchdog(addr, pgUser, pgPassword, goharness.Hex(wrongKey[:]))
	if err == nil {
		_ = client.Close()
		return fmt.Errorf("watchdog: unlock with wrong key unexpectedly succeeded")
	}
	fmt.Fprintf(os.Stderr, "PASS watchdog: unopenable unlock failed closed in %v: %v\n", time.Since(start), err)
	return nil
}

// emptyDBLeg is the empty-database leg (Go: the `empty-db` role): a fresh
// database opens under the watchdog with zero rows and answers queries.
func emptyDBLeg() error {
	opts := goharness.DefaultClusterOptions("open-progress-empty")
	opts.NumNodes = 1
	opts.Key = appKey
	cluster, err := goharness.NewCluster(opts)
	if err != nil {
		return fmt.Errorf("empty-db cluster setup: %w", err)
	}
	failed := true
	defer func() { cluster.Finish(failed) }()
	if err := cluster.EnsureMigrated(0, "CREATE TABLE probe(id INTEGER PRIMARY KEY)"); err != nil {
		fmt.Fprintf(os.Stderr, "scenario failed: empty-db migrate: %v\n", err)
		cluster.DumpForensics("open-progress-empty")
		return fmt.Errorf("empty-db migrate: %w", err)
	}
	rows, err := cluster.Node(0).Simple("SELECT COUNT(*) FROM probe")
	if err != nil {
		fmt.Fprintf(os.Stderr, "scenario failed: empty-db count: %v\n", err)
		cluster.DumpForensics("open-progress-empty")
		return fmt.Errorf("empty-db count: %w", err)
	}
	count := ""
	if len(rows) > 0 && len(rows[0]) > 0 {
		count = rows[0][0]
	}
	if count != "0" {
		err := fmt.Errorf("empty-db: fresh table holds %s rows, want 0", count)
		fmt.Fprintf(os.Stderr, "scenario failed: %v\n", err)
		cluster.DumpForensics("open-progress-empty")
		return err
	}
	fmt.Fprintln(os.Stderr, "PASS empty-db: fresh database opened under watchdog with zero rows")
	failed = false
	return nil
}

func runBody(cluster *goharness.Cluster) error {
	// Main flow (Go: populate, reload, open-failure, reload). The cancel
	// role has no counterpart: opens over the wire cannot be cancelled.
	if err := migrate(cluster); err != nil {
		return err
	}
	if err := populate(cluster); err != nil {
		return err
	}
	if err := verifyContents(cluster, "populate"); err != nil {
		return err
	}
	if err := reloadAndVerify(cluster, "reload-1"); err != nil {
		return err
	}
	if err := openFailureLegs(cluster); err != nil {
		return err
	}
	if err := watchdogLeg(cluster); err != nil {
		return err
	}
	if err := reloadAndVerify(cluster, "reload-2-recovery"); err != nil {
		return err
	}

	// Fresh empty database opens under the watchdog.
	if err := emptyDBLeg(); err != nil {
		return err
	}

	fmt.Fprintln(os.Stderr, "PASS open-progress: all legs green")
	return nil
}

func TestOpenProgress(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	opts := goharness.DefaultClusterOptions("open-progress")
	opts.NumNodes = 1
	opts.Key = appKey
	cluster, err := goharness.NewCluster(opts)
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	if cluster.Len() != 1 {
		t.Fatalf("this scenario is written for 1 node, got %d", cluster.Len())
	}
	failed := true
	defer func() { cluster.Finish(failed) }()
	if err := runBody(cluster); err != nil {
		fmt.Fprintf(os.Stderr, "scenario failed: %v\n", err)
		cluster.DumpForensics("open-progress-failed")
		t.Fatalf("scenario failed: %v", err)
	}
	failed = false
}
