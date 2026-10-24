package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestExampleAndDefaults(t *testing.T) {
	c, e := Load("../config.example.yaml")
	if e != nil {
		t.Fatal(e)
	}
	if !reflect.DeepEqual(c, DefaultConfig()) {
		t.Fatalf("example differs from defaults:\n%+v", c)
	}
	c, e = Parse([]byte("engine:\n  capture_interval: 2\n  capture_bytes: '2MiB'\n"))
	if e != nil {
		t.Fatal(e)
	}
	if c.Engine.CaptureInterval != Duration(2*time.Second) || c.Engine.CaptureBytes != 2<<20 {
		t.Fatal(c.Engine)
	}
}

func TestStrictConfig(t *testing.T) {
	for _, s := range []string{
		"server: [invalid\n",
		"server:\n  nope: 1\n",
		"server:\n  BIND: '127.0.0.1:1'\n",
		"SERVER:\n  bind: '127.0.0.1:1'\n",
		"server:\n  max_connections: 0\n",
		"server:\n  production: true\n",
		"server:\n  tls:\n    ca_file: 'ca.pem'\n",
		"engine:\n  queue_bytes: '129MiB'\n",
		"engine:\n  capture_bytes: '65MiB'\n",
		"engine:\n  transaction_bytes: '65MiB'\n",
		"engine:\n  segment_bytes: '64MiB'\n",
		"engine:\n  scheduler:\n    local_share: 91\n",
		"auth:\n  users:\n    - name: 'a'\n      password_hash: 'plaintext'\n",
		"auth:\n  users:\n    - name: 'a'\n      password_hash: '$argon2id$x'\n    - name: 'a'\n      password_hash: '$argon2id$x'\n",
		"replication:\n  allowed_networks:\n    - '1.2.3.4/99'\n",
		"replication:\n  bootstrap:\n    - 'db0.example'\n",
		"replication:\n  bootstrap:\n    - 'db0.example:0'\n",
		"replication:\n  bootstrap:\n    - 'db0.example:99999'\n",
		"replication:\n  bootstrap:\n    - 'db0.example:http'\n",
		"replication:\n  bootstrap:\n    - 'not a host:9001'\n",
		"replication:\n  bootstrap:\n    - ':9001'\n",
		"replication:\n  advertise: 'db0.example'\n",
		"replication:\n  bind: 'db0.example:9001'\n",
		"replication:\n  enabled: true\n",
		"replication:\n  dissemination: 'unknown'\n",
		"backups:\n  dir: ' '\n",
		"logging:\n  file_prefix: '../bad'\n",
	} {
		t.Run(s, func(t *testing.T) {
			if _, e := Parse([]byte(s)); e == nil {
				t.Fatal("accepted invalid configuration")
			}
		})
	}
}

// TestHostnameSeeds pins that bootstrap and advertise accept DNS names
// with explicit ports (validated offline, resolved at startup).
func TestHostnameSeeds(t *testing.T) {
	c, e := Parse([]byte("replication:\n  advertise: 'db0.example:9001'\n  bootstrap:\n    - 'db0.example:9001'\n    - 'db1.example:9002'\n    - '203.0.113.11:9001'\n"))
	if e != nil {
		t.Fatal(e)
	}
	if c.Replication.Advertise == nil || *c.Replication.Advertise != "db0.example:9001" {
		t.Fatalf("advertise = %+v", c.Replication.Advertise)
	}
	if len(c.Replication.Bootstrap) != 3 || c.Replication.Bootstrap[1] != "db1.example:9002" {
		t.Fatalf("bootstrap = %v", c.Replication.Bootstrap)
	}
}

func TestUnits(t *testing.T) {
	for s, want := range map[string]Bytes{"64KiB": 65536, "64KB": 64000, "3 GiB": 3 << 30, "0": 0} {
		got, e := ParseBytes(s)
		if e != nil || got != want {
			t.Fatalf("%s: %d %v", s, got, e)
		}
	}
	for _, s := range []string{"-1", "1.5MiB", "18446744073709551615GiB", "1TiB"} {
		if _, e := ParseBytes(s); e == nil {
			t.Fatal(s)
		}
	}
	for s, want := range map[string]Duration{"7d": Duration(7 * 24 * time.Hour), "500ms": Duration(500 * time.Millisecond), "2 seconds": Duration(2 * time.Second)} {
		got, e := ParseDuration(s)
		if e != nil || got != want {
			t.Fatalf("%s: %d %v", s, got, e)
		}
	}
	for _, s := range []string{"-1s", "1.2s", "99999999999999999d"} {
		if _, e := ParseDuration(s); e == nil {
			t.Fatal(s)
		}
	}
}

func TestNestedDefaultsAndSnapshot(t *testing.T) {
	s := "replication:\n  files:\n    key_hex: '" + strings.Repeat("aa", 32) + "'\n"
	c, e := Parse([]byte(s))
	if e != nil {
		t.Fatal(e)
	}
	if c.Replication.Files.MaxFileBytes != 4<<30 || c.Replication.Files.FetchInterval != Duration(30*time.Second) {
		t.Fatal(c.Replication.Files)
	}
	c.Replication.Tables = map[string]bool{"one": true}
	copy := c.Snapshot()
	copy.Replication.Tables["one"] = false
	copy.Replication.Files.KeyHex = "changed"
	if !c.Replication.Tables["one"] || c.Replication.Files.KeyHex == "changed" {
		t.Fatal("snapshot aliases running config")
	}
}

func TestTableFlagAtomicUpdates(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "config.yaml")
	source := []byte("# keep this\nserver:\n  bind: '127.0.0.1:5432'\nreplication:\n  tables:\n    # table comment\n    old: true\nlogging:\n  console: false\n")
	if e := os.WriteFile(p, source, 0600); e != nil {
		t.Fatal(e)
	}
	var wg sync.WaitGroup
	for _, key := range []string{"one", "two", "table.with.dot", "quote\"key"} {
		wg.Add(1)
		go func(key string) {
			defer wg.Done()
			if e := SaveTableFlag(p, key, true); e != nil {
				t.Error(e)
			}
		}(key)
	}
	wg.Wait()
	c, e := Load(p)
	if e != nil {
		t.Fatal(e)
	}
	if len(c.Replication.Tables) != 5 {
		t.Fatal(c.Replication.Tables)
	}
	b, _ := os.ReadFile(p)
	if !strings.Contains(string(b), "# keep this") || !strings.Contains(string(b), "# table comment") || !strings.Contains(string(b), "bind: '127.0.0.1:5432'") {
		t.Fatal("lost unrelated formatting")
	}
	if e = SaveTableFlags(p, map[string]bool{"replacement": false}); e != nil {
		t.Fatal(e)
	}
	c, e = Load(p)
	if e != nil || len(c.Replication.Tables) != 1 {
		t.Fatal(c, e)
	}
	before, _ := os.ReadFile(p)
	if e = SaveTableFlags(p, map[string]bool{"": true}); e == nil {
		t.Fatal("accepted empty table ID")
	}
	after, _ := os.ReadFile(p)
	if string(before) != string(after) {
		t.Fatal("failed save mutated config")
	}
	matches, _ := filepath.Glob(filepath.Join(dir, ".murmur-config-*"))
	if len(matches) != 0 {
		t.Fatal(matches)
	}
}

func TestInlineTableFlagUpdate(t *testing.T) {
	p := filepath.Join(t.TempDir(), "config.yaml")
	if e := os.WriteFile(p, []byte("replication: { tables: { old: true } }\n"), 0600); e != nil {
		t.Fatal(e)
	}
	if e := SaveTableFlag(p, "new", false); e != nil {
		t.Fatal(e)
	}
	c, e := Load(p)
	if e != nil || !c.Replication.Tables["old"] || len(c.Replication.Tables) != 2 {
		t.Fatal(c.Replication.Tables, e)
	}
}

func TestConfigJSONRoundTrip(t *testing.T) {
	c := DefaultConfig()
	raw, e := json.Marshal(c)
	if e != nil {
		t.Fatal(e)
	}
	var decoded Config
	if e = json.Unmarshal(raw, &decoded); e != nil {
		t.Fatal(e)
	}
	if !reflect.DeepEqual(c, decoded) {
		t.Fatal("configuration JSON changed durations, units or defaults")
	}
}
