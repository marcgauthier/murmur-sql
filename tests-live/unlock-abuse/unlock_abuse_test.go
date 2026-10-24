// Live scenario: bad credentials and unlock keys neither disclose secrets
// nor damage SQL.
//
// Go port of tests/unlock_abuse.rs using tests-live/gosqlharness plus the
// goharness wire client. Identical behavior: a single real murmur-server
// child holds one row, twelve bad-credential connects (rotating password /
// user / unlock key) are rejected with 28P01 / 28P01 / 28000 without
// echoing the secret, the row stays readable after every attempt, and it
// survives a restart.
package unlockabuse

import (
	"os"
	"strconv"
	"strings"
	"testing"

	goharness "github.com/marcgauthier/murmur-sql/tests-live/goharness"
	gosqlharness "github.com/marcgauthier/murmur-sql/tests-live/gosqlharness"
)

func TestBadCredentialsAndUnlockKeysDoNotDiscloseSecretsOrDamageSQL(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	cluster, err := gosqlharness.NewCluster(gosqlharness.ClusterOptions{
		Name:     "unlock-abuse",
		NumNodes: 1,
	})
	if err != nil {
		t.Fatalf("start cluster: %v", err)
	}
	failed := true
	defer func() { cluster.Finish(failed) }()

	if _, err := cluster.Node(0).Simple(
		"CREATE TABLE records(id INTEGER PRIMARY KEY, body TEXT); INSERT INTO records VALUES(1,'kept')"); err != nil {
		t.Fatalf("seed records: %v", err)
	}
	if _, err := cluster.Node(0).Simple("MURMUR FLUSH"); err != nil {
		t.Fatalf("flush seed: %v", err)
	}
	for attempt := 0; attempt < 12; attempt++ {
		config := cluster.Config(0)
		secret := "unlock-attack-secret-" + strconv.Itoa(attempt)
		switch attempt % 3 {
		case 0:
			config.Password = secret
		case 1:
			config.User = secret
		default:
			config.Key = secret
		}
		conn, err := goharness.Connect(config)
		if err == nil {
			_ = conn.Close()
			t.Fatal("bad credentials accepted")
		}
		want := "28P01"
		if attempt%3 == 2 {
			want = "28000"
		}
		if code := goharness.ErrorCode(err); code != want {
			t.Fatalf("attempt %d code = %q, want %s (%v)", attempt, code, want, err)
		}
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("credential leaked: %v", err)
		}
		rows, err := cluster.Node(0).Simple("SELECT body FROM records")
		if err != nil {
			t.Fatalf("attempt %d reread: %v", attempt, err)
		}
		if !equalRows(rows, [][]string{{"kept"}}) {
			t.Fatalf("attempt %d rows = %v, want [[kept]]", attempt, rows)
		}
	}
	if err := cluster.StopNode(0); err != nil {
		t.Fatalf("stop node: %v", err)
	}
	if err := cluster.StartNode(0); err != nil {
		t.Fatalf("restart node: %v", err)
	}
	rows, err := cluster.Node(0).Simple("SELECT body FROM records")
	if err != nil {
		t.Fatalf("post-restart reread: %v", err)
	}
	if !equalRows(rows, [][]string{{"kept"}}) {
		t.Fatalf("post-restart rows = %v, want [[kept]]", rows)
	}
	failed = false
}

func equalRows(a, b [][]string) bool {
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
