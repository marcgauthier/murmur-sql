package keyprovider

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

func TestParseKeyMaterial(t *testing.T) {
	raw := bytes.Repeat([]byte{0xab}, 32)
	if got, err := ParseKeyMaterial(raw); err != nil || got != [32]byte(bytes.Repeat([]byte{0xab}, 32)) {
		t.Fatalf("raw = %x %v", got, err)
	}
	hexed := strings.Repeat("ab", 32)
	if got, err := ParseKeyMaterial([]byte("  " + hexed + "\n")); err != nil || got[0] != 0xab || got[31] != 0xab {
		t.Fatalf("hex = %x %v", got, err)
	}
	for _, bad := range [][]byte{{1, 2, 3}, bytes.Repeat([]byte{0xab}, 33), []byte(strings.Repeat("zz", 32)), []byte(strings.Repeat("ab", 31))} {
		if _, err := ParseKeyMaterial(bad); err == nil {
			t.Fatalf("accepted %d-byte payload", len(bad))
		}
	}
}

func TestParseURI(t *testing.T) {
	valid := map[string][2]string{
		"file:/etc/murmur/key":           {"file", "/etc/murmur/key"},
		"file:relative/path":             {"file", "relative/path"},
		"env:MURMUR_SIGNING_KEY":         {"env", "MURMUR_SIGNING_KEY"},
		"exec:/bin/helper":               {"exec", "/bin/helper"},
		"exec:/bin/helper?timeout=5s":    {"exec", "/bin/helper"},
		"exec:/bin/helper?timeout=250ms": {"exec", "/bin/helper"},
	}
	for uri, want := range valid {
		s, b, err := ParseURI(uri)
		if err != nil || s != want[0] || b != want[1] {
			t.Fatalf("ParseURI(%q) = %q %q %v", uri, s, b, err)
		}
	}
	for _, uri := range []string{
		"helper", "file:", ":body", "unknown:x", "file:/x?timeout=1s",
		"env:bad-name", "env:9bad", "env:",
		"exec:relative/helper", "exec:/h?timeout=0s", "exec:/h?timeout=-1s",
		"exec:/h?timeout=6m", "exec:/h?timeout=soon", "exec:/h?bogus=1",
	} {
		if _, _, err := ParseURI(uri); err == nil {
			t.Fatalf("ParseURI(%q) accepted", uri)
		}
	}
}

func TestFileProvider(t *testing.T) {
	dir := t.TempDir()
	rawPath := dir + "/raw.key"
	if err := os.WriteFile(rawPath, bytes.Repeat([]byte{0xab}, 32), 0o600); err != nil {
		t.Fatal(err)
	}
	hexPath := dir + "/hex.key"
	if err := os.WriteFile(hexPath, []byte(strings.Repeat("cd", 32)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	got, err := ResolveURI(ctx, "file:"+rawPath)
	if err != nil || got[0] != 0xab {
		t.Fatalf("raw file = %x %v", got, err)
	}
	got, err = ResolveURI(ctx, "file:"+hexPath)
	if err != nil || got[0] != 0xcd {
		t.Fatalf("hex file = %x %v", got, err)
	}
	if _, err := ResolveURI(ctx, "file:"+dir+"/missing"); err == nil {
		t.Fatal("missing file accepted")
	}
	bigPath := dir + "/big.key"
	if err := os.WriteFile(bigPath, bytes.Repeat([]byte{0xab}, maxPayload+1), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ResolveURI(ctx, "file:"+bigPath); err == nil {
		t.Fatal("oversized file accepted")
	}
}

func TestEnvProvider(t *testing.T) {
	t.Setenv("MURMUR_TEST_KEY_HEX", strings.Repeat("ef", 32))
	ctx := context.Background()
	got, err := ResolveURI(ctx, "env:MURMUR_TEST_KEY_HEX")
	if err != nil || got[0] != 0xef {
		t.Fatalf("env = %x %v", got, err)
	}
	if _, err := ResolveURI(ctx, "env:MURMUR_TEST_KEY_MISSING"); err == nil {
		t.Fatal("missing env accepted")
	}
	t.Setenv("MURMUR_TEST_KEY_BAD", "not-hex")
	if _, err := ResolveURI(ctx, "env:MURMUR_TEST_KEY_BAD"); err == nil {
		t.Fatal("bad env accepted")
	}
}

// TestHelperProcess re-executes the test binary as a fake exec helper.
// It runs only under GO_WANT_HELPER_PROCESS; behavior comes from
// HELPER_MODE (print, raw, fail, sleep) and HELPER_HEX.
func TestHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_HELPER_PROCESS") != "1" {
		return
	}
	switch os.Getenv("HELPER_MODE") {
	case "print":
		fmt.Print(os.Getenv("HELPER_HEX"))
	case "raw":
		_, _ = os.Stdout.Write(bytes.Repeat([]byte{0xab}, 32))
	case "fail":
		fmt.Fprintln(os.Stderr, "boom-helper")
		os.Exit(3)
	case "sleep":
		time.Sleep(5 * time.Second)
	default:
		fmt.Fprintln(os.Stderr, "unknown helper mode")
		os.Exit(4)
	}
	os.Exit(0)
}

func helperURI(t *testing.T, query string) string {
	t.Helper()
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	uri := "exec:" + self
	if query != "" {
		uri += "?" + query
	}
	return uri
}

func TestExecProvider(t *testing.T) {
	ctx := context.Background()
	t.Setenv("GO_WANT_HELPER_PROCESS", "1")
	t.Setenv("HELPER_HEX", strings.Repeat("ab", 32))
	t.Setenv("HELPER_MODE", "print")
	got, err := ResolveURI(ctx, helperURI(t, ""))
	if err != nil || got[0] != 0xab {
		t.Fatalf("print helper = %x %v", got, err)
	}
	t.Setenv("HELPER_MODE", "raw")
	got, err = ResolveURI(ctx, helperURI(t, ""))
	if err != nil || got[0] != 0xab {
		t.Fatalf("raw helper = %x %v", got, err)
	}
	t.Setenv("HELPER_MODE", "fail")
	if _, err := ResolveURI(ctx, helperURI(t, "")); err == nil || !strings.Contains(err.Error(), "boom-helper") {
		t.Fatalf("failing helper = %v, want boom-helper", err)
	}
	t.Setenv("HELPER_MODE", "sleep")
	start := time.Now()
	if _, err := ResolveURI(ctx, helperURI(t, "timeout=50ms")); err == nil {
		t.Fatal("sleeping helper accepted")
	}
	if elapsed := time.Since(start); elapsed > 4*time.Second {
		t.Fatalf("timeout took %v", elapsed)
	}
}

func TestExecProviderMissingHelper(t *testing.T) {
	ctx := context.Background()
	if _, err := ResolveURI(ctx, "exec:/nonexistent-helper-xyz"); err == nil {
		t.Fatal("missing helper accepted")
	}
}
