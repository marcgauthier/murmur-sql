package native

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"fmt"
	"math/big"
	"net/netip"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/engine"
	"github.com/marcgauthier/murmur-sql/murmur"
	"github.com/marcgauthier/murmur-sql/murmur/replication"
)

// policyTestDB opens a scratch store with replication enabled (no peers)
// and the given merge policies, for dispatch tests.
func policyTestDB(t *testing.T, dir string, policies map[replication.PolicyColumn]replication.Policy) *murmur.Database {
	t.Helper()
	var id [16]byte
	id[0], id[15] = 0xA1, 0x5E
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "murmur-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, caKey.Public(), caKey)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.CreateCertificate(rand.Reader, &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "murmur-test-node"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		URIs:         []*url.URL{{Scheme: "replicateddb", Host: "node", Path: "/" + replication.Hex(id[:])}},
	}, ca, key.Public(), caKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	var seed [32]byte
	for i := range seed {
		seed[i] = byte(0xA0 + i)
	}
	cfg := replication.NewConfig(netip.MustParseAddrPort("127.0.0.1:0"),
		replication.Credentials{Chain: [][]byte{der}, PrivateKey: keyDER, Roots: [][]byte{caDER}}, seed)
	cfg.TrustedOrigins = map[[16]byte][32]byte{id: replication.PublicKey(&seed)}
	cfg.MergePolicies = policies
	options := murmur.DefaultOptions()
	options.Mode = murmur.OpenCreateNew
	options.DatabaseID = &id
	options.Replication = cfg
	var raw [32]byte
	for i := range raw {
		raw[i] = byte(i + 1)
	}
	db, err := murmur.Open(dir, raw[:], options)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return db
}

// TestPolicyCompositeDispatch pins composite-key dispatch: the engine
// resolves key arity from the table schema and coerces each key
// component by its column type.
func TestPolicyCompositeDispatch(t *testing.T) {
	ctx := context.Background()
	db := policyTestDB(t, t.TempDir()+"/db", map[replication.PolicyColumn]replication.Policy{
		{Table: "pairs", Column: "n"}: replication.PolicyCounter,
	})
	defer db.Close()
	d := &database{db: db}
	if err := db.Migrate(murmur.NewMigration("CREATE TABLE pairs(a INTEGER NOT NULL, b TEXT NOT NULL, n INTEGER NOT NULL, PRIMARY KEY (a, b))")); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if _, err := db.Transaction(func(sql *murmur.Sql) (any, error) {
		_, err := sql.Execute("INSERT INTO pairs VALUES (1, 'x', 10)")
		return nil, err
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	res, err := d.Command(ctx, 1, engine.Command{Operation: "POLICY COUNTER_ADD",
		Arguments: []engine.Value{"pairs", "n", "1", "x", int64(5)}})
	if err != nil {
		t.Fatalf("composite dispatch: %v", err)
	}
	if res.Tag != "POLICY" {
		t.Fatalf("tag = %q, want POLICY", res.Tag)
	}
	got, err := d.Execute(ctx, "SELECT n FROM pairs WHERE a = 1 AND b = 'x'", nil)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got.Rows) != 1 || fmt.Sprintf("%v", got.Rows[0][0]) != "15" {
		t.Fatalf("rows = %+v, want [[15]]", got.Rows)
	}
}

// TestPolicyDispatchArityMismatch pins that a key prefix that does not
// cover the table key fails instead of addressing the wrong row.
func TestPolicyDispatchArityMismatch(t *testing.T) {
	ctx := context.Background()
	db := policyTestDB(t, t.TempDir()+"/db", map[replication.PolicyColumn]replication.Policy{
		{Table: "pairs", Column: "n"}: replication.PolicyCounter,
	})
	defer db.Close()
	d := &database{db: db}
	if err := db.Migrate(murmur.NewMigration("CREATE TABLE pairs(a INTEGER NOT NULL, b TEXT NOT NULL, n INTEGER NOT NULL, PRIMARY KEY (a, b))")); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	_, err := d.Command(ctx, 1, engine.Command{Operation: "POLICY COUNTER_ADD",
		Arguments: []engine.Value{"pairs", "n", "1", int64(5)}})
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "key") {
		t.Fatalf("short key returned %v, want key arity error", err)
	}
}

// TestPolicyJSONDispatch pins JSON_SET/JSON_REMOVE dispatch through the
// engine: path writes render canonical object text, removals drop
// paths, and quoted numerics stay textual.
func TestPolicyJSONDispatch(t *testing.T) {
	ctx := context.Background()
	db := policyTestDB(t, t.TempDir()+"/db", map[replication.PolicyColumn]replication.Policy{
		{Table: "docs", Column: "doc"}: replication.PolicyJson,
	})
	defer db.Close()
	d := &database{db: db}
	if err := db.Migrate(murmur.NewMigration("CREATE TABLE docs(k TEXT NOT NULL PRIMARY KEY, doc TEXT)")); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if _, err := db.Transaction(func(sql *murmur.Sql) (any, error) {
		_, err := sql.Execute("INSERT INTO docs VALUES ('d1', NULL)")
		return nil, err
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	set := func(path string, v engine.Value) {
		t.Helper()
		if _, err := d.Command(ctx, 1, engine.Command{Operation: "POLICY JSON_SET",
			Arguments: []engine.Value{"docs", "doc", "d1", path, v}}); err != nil {
			t.Fatalf("json set %s: %v", path, err)
		}
	}
	set("b", "x")
	set("a", int64(1))
	set("n", "123")
	got, err := d.Execute(ctx, "SELECT doc FROM docs WHERE k = 'd1'", nil)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got.Rows) != 1 || fmt.Sprintf("%v", got.Rows[0][0]) != `{"a":1,"b":"x","n":"123"}` {
		t.Fatalf("rows = %+v", got.Rows)
	}
	if _, err := d.Command(ctx, 1, engine.Command{Operation: "POLICY JSON_REMOVE",
		Arguments: []engine.Value{"docs", "doc", "d1", "b"}}); err != nil {
		t.Fatalf("json remove: %v", err)
	}
	got, err = d.Execute(ctx, "SELECT doc FROM docs WHERE k = 'd1'", nil)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got.Rows) != 1 || fmt.Sprintf("%v", got.Rows[0][0]) != `{"a":1,"n":"123"}` {
		t.Fatalf("rows = %+v", got.Rows)
	}
}

// TestPolicyListDispatch pins LIST_APPEND/LIST_REMOVE dispatch through
// the engine: appends union, removes drop every matching element.
func TestPolicyListDispatch(t *testing.T) {
	ctx := context.Background()
	db := policyTestDB(t, t.TempDir()+"/db", map[replication.PolicyColumn]replication.Policy{
		{Table: "nums", Column: "list"}: replication.PolicyList,
	})
	defer db.Close()
	d := &database{db: db}
	if err := db.Migrate(murmur.NewMigration("CREATE TABLE nums(k TEXT NOT NULL PRIMARY KEY, list TEXT)")); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if _, err := db.Transaction(func(sql *murmur.Sql) (any, error) {
		_, err := sql.Execute("INSERT INTO nums VALUES ('n1', NULL)")
		return nil, err
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	appendOp := func(v engine.Value) {
		t.Helper()
		if _, err := d.Command(ctx, 1, engine.Command{Operation: "POLICY LIST_APPEND",
			Arguments: []engine.Value{"nums", "list", "n1", v}}); err != nil {
			t.Fatalf("list append: %v", err)
		}
	}
	appendOp(int64(2))
	appendOp("x")
	appendOp(int64(2))
	got, err := d.Execute(ctx, "SELECT list FROM nums WHERE k = 'n1'", nil)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got.Rows) != 1 || !listTextHas(t, fmt.Sprintf("%v", got.Rows[0][0]), `"x"`, 3) {
		t.Fatalf("rows = %+v", got.Rows)
	}
	if _, err := d.Command(ctx, 1, engine.Command{Operation: "POLICY LIST_REMOVE",
		Arguments: []engine.Value{"nums", "list", "n1", int64(2)}}); err != nil {
		t.Fatalf("list remove: %v", err)
	}
	got, err = d.Execute(ctx, "SELECT list FROM nums WHERE k = 'n1'", nil)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got.Rows) != 1 || fmt.Sprintf("%v", got.Rows[0][0]) != `["x"]` {
		t.Fatalf("rows = %+v", got.Rows)
	}
}

// listTextHas checks a rendered JSON array holds want among size elements.
func listTextHas(t *testing.T, raw, want string, size int) bool {
	t.Helper()
	var list []any
	if err := json.Unmarshal([]byte(raw), &list); err != nil || len(list) != size {
		return false
	}
	for _, e := range list {
		b, err := json.Marshal(e)
		if err != nil {
			t.Fatal(err)
		}
		if string(b) == want {
			return true
		}
	}
	return false
}

// TestPolicySingleKeyLegacy pins that the four-argument single-key form
// keeps working after composite support lands.
func TestPolicySingleKeyLegacy(t *testing.T) {
	ctx := context.Background()
	db := policyTestDB(t, t.TempDir()+"/db", map[replication.PolicyColumn]replication.Policy{
		{Table: "counters", Column: "n"}: replication.PolicyCounter,
	})
	defer db.Close()
	d := &database{db: db}
	if err := db.Migrate(murmur.NewMigration("CREATE TABLE counters(k TEXT NOT NULL PRIMARY KEY, n INTEGER NOT NULL)")); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if _, err := db.Transaction(func(sql *murmur.Sql) (any, error) {
		_, err := sql.Execute("INSERT INTO counters VALUES ('k1', 0)")
		return nil, err
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := d.Command(ctx, 1, engine.Command{Operation: "POLICY COUNTER_ADD",
		Arguments: []engine.Value{"counters", "n", "k1", int64(5)}}); err != nil {
		t.Fatalf("legacy dispatch: %v", err)
	}
	got, err := d.Execute(ctx, "SELECT n FROM counters WHERE k = 'k1'", nil)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got.Rows) != 1 || fmt.Sprintf("%v", got.Rows[0][0]) != "5" {
		t.Fatalf("rows = %+v, want [[5]]", got.Rows)
	}
}
