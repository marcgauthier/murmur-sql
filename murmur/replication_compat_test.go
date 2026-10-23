package murmur

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net/netip"
	"net/url"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql-go/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql-go/murmur/replication"
)

func compatNodeID(b byte) (id [16]byte) {
	id[0] = b
	id[15] = b ^ 0xff
	return id
}

func compatCredentials(t *testing.T, ids [][16]byte) ([][]byte, []replication.Credentials) {
	t.Helper()
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
	var creds []replication.Credentials
	for i, id := range ids {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		template := &x509.Certificate{
			SerialNumber: big.NewInt(int64(i + 2)),
			Subject:      pkix.Name{CommonName: "murmur-test-node"},
			NotBefore:    time.Now().Add(-time.Hour),
			NotAfter:     time.Now().Add(time.Hour),
			KeyUsage:     x509.KeyUsageDigitalSignature,
			ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
			URIs:         []*url.URL{{Scheme: "replicateddb", Host: "node", Path: "/" + replication.Hex(id[:])}},
		}
		der, err := x509.CreateCertificate(rand.Reader, template, ca, key.Public(), caKey)
		if err != nil {
			t.Fatal(err)
		}
		keyDER, err := x509.MarshalPKCS8PrivateKey(key)
		if err != nil {
			t.Fatal(err)
		}
		creds = append(creds, replication.Credentials{Chain: [][]byte{der}, PrivateKey: keyDER, Roots: [][]byte{caDER}})
	}
	return [][]byte{caDER}, creds
}

func compatSeeds() (a, b [32]byte) {
	for i := range a {
		a[i] = byte(0xA0 + i)
		b[i] = byte(0xB0 + i)
	}
	return a, b
}

func compatConfigs(t *testing.T, ids [][16]byte) []*replication.Config {
	t.Helper()
	_, creds := compatCredentials(t, ids)
	seedA, seedB := compatSeeds()
	seeds := [][32]byte{seedA, seedB}
	trust := make(map[[16]byte][32]byte)
	for i, id := range ids {
		seed := seeds[i]
		trust[id] = replication.PublicKey(&seed)
	}
	var out []*replication.Config
	for i := range ids {
		c := replication.NewConfig(netip.MustParseAddrPort("127.0.0.1:0"), creds[i], seeds[i])
		c.TrustedOrigins = trust
		c.AntiEntropyInterval = 300 * time.Millisecond
		c.IncomingInterval = 150 * time.Millisecond
		out = append(out, c)
	}
	return out
}

func compatRowCount(t *testing.T, db *Database, id int64) int {
	t.Helper()
	v, err := db.Read(func(sql *Sql) (any, error) {
		return sql.Query("SELECT id FROM users WHERE id = ?", id)
	})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return len(v.([][]sqlite.Value))
}

func TestTwoNodeReplicationSync(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping live replication test in short mode")
	}
	dir := t.TempDir()
	idA, idB := compatNodeID(0xA1), compatNodeID(0xB2)
	dbID := compatNodeID(0xD4)
	configs := compatConfigs(t, [][16]byte{idA, idB})

	openNode := func(t *testing.T, path string, cfg *replication.Config) *Database {
		t.Helper()
		options := DefaultOptions()
		options.Mode = OpenCreateNew
		options.DatabaseID = &dbID
		options.Replication = cfg
		db, err := Open(path, testKey(), options)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		return db
	}
	dbA := openNode(t, dir+"/a", configs[0])
	defer dbA.Close()
	dbB := openNode(t, dir+"/b", configs[1])
	defer dbB.Close()

	if got := dbA.NodeID(); got != idA {
		t.Fatalf("node A id = %x, want %x", got, idA)
	}
	if got := dbB.NodeID(); got != idB {
		t.Fatalf("node B id = %x, want %x", got, idB)
	}

	for _, db := range []*Database{dbA, dbB} {
		if err := db.Migrate(NewMigration(coreTestDDL)); err != nil {
			t.Fatalf("migrate: %v", err)
		}
		if err := db.SetReplication("users", true); err != nil {
			t.Fatalf("enable replication: %v", err)
		}
	}
	execMust(t, dbA, "INSERT INTO users (id, name, score) VALUES (1, 'ada', 10)")
	if err := dbA.Flush(); err != nil {
		t.Fatalf("flush A: %v", err)
	}

	// Connect the engines both ways.
	dbA.inner.replicationMu.Lock()
	handleA := dbA.inner.replication
	dbA.inner.replicationMu.Unlock()
	dbB.inner.replicationMu.Lock()
	handleB := dbB.inner.replication
	dbB.inner.replicationMu.Unlock()
	if handleA == nil || handleB == nil {
		t.Fatal("replication handles missing")
	}
	addrA, addrB := handleA.Status().Address, handleB.Status().Address
	if addrA == nil || addrB == nil {
		t.Fatal("missing listen addresses")
	}
	if err := handleA.AddPeer(*addrB); err != nil {
		t.Fatalf("add peer B: %v", err)
	}
	if err := handleB.AddPeer(*addrA); err != nil {
		t.Fatalf("add peer A: %v", err)
	}

	deadline := time.Now().Add(60 * time.Second)
	for compatRowCount(t, dbB, 1) == 0 {
		if time.Now().After(deadline) {
			stA, stB := handleA.Status(), handleB.Status()
			t.Fatalf("row never arrived on B (peers=%d/%d applied=%d/%d)",
				stA.Peers, stB.Peers, stA.AppliedTransactions, stB.AppliedTransactions)
		}
		time.Sleep(200 * time.Millisecond)
	}

	// Reverse direction.
	execMust(t, dbB, "INSERT INTO users (id, name, score) VALUES (2, 'grace', 20)")
	if err := dbB.Flush(); err != nil {
		t.Fatalf("flush B: %v", err)
	}
	deadline = time.Now().Add(60 * time.Second)
	for compatRowCount(t, dbA, 2) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("row never arrived on A")
		}
		time.Sleep(200 * time.Millisecond)
	}
}
