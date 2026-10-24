package cli

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/marcgauthier/murmur-sql/auth"
	"github.com/marcgauthier/murmur-sql/engine"
)

type captureAdmin struct {
	got engine.AdminRequest
}

func (c *captureAdmin) Open(context.Context, engine.OpenRequest) (engine.Database, engine.OpenOutcome, error) {
	return nil, 0, engine.ErrUnavailable
}

func (c *captureAdmin) Admin(_ context.Context, r engine.AdminRequest) (any, error) {
	c.got = r
	return map[string]any{"ok": true}, nil
}

func TestAdminPasswordAndUnavailable(t *testing.T) {
	var out, errs bytes.Buffer
	code := Admin(context.Background(), []string{"hash-password"}, strings.NewReader("test-password\n"), &out, &errs, nil)
	if code != 0 || !auth.VerifyPassword(strings.TrimSpace(out.String()), []byte("test-password")) {
		t.Fatal(code, out.String(), errs.String())
	}
	out.Reset()
	errs.Reset()
	code = Admin(context.Background(), []string{"status", "/unused", "--passphrase", "do-not-log-me"}, strings.NewReader(""), &out, &errs, nil)
	if code != 1 || !strings.Contains(errs.String(), "engine is intentionally disabled") || strings.Contains(errs.String(), "do-not-log-me") {
		t.Fatal(code, errs.String())
	}
}
func TestCLIValidation(t *testing.T) {
	for _, args := range [][]string{{"--config"}, {"--unknown"}, {"--config", "/missing", "--validate-config"}} {
		var out, errs bytes.Buffer
		if code := Server(context.Background(), args, &out, &errs, nil); code != 1 {
			t.Fatal(args, code)
		}
	}
	var out, errs bytes.Buffer
	if code := Server(context.Background(), []string{"--config", "../../config.example.yaml", "--validate-config"}, &out, &errs, nil); code != 0 || !strings.Contains(out.String(), "config OK") {
		t.Fatal(code, out.String(), errs.String())
	}
	for _, args := range [][]string{{"hash-password", "--password"}, {"restore", "only-one-path"}, {"status", "/unused", "--key-hex", "bad"}, {"status", "/unused", "--fresh-db-id=bad"}} {
		out.Reset()
		errs.Reset()
		if code := Admin(context.Background(), args, strings.NewReader(""), &out, &errs, nil); code != 1 {
			t.Fatal(args, code)
		}
	}
}

func TestAdminCredentialFlags(t *testing.T) {
	raw := strings.Repeat("ab", 32)
	admin := func(args ...string) (int, *captureAdmin, string) {
		var out, errs bytes.Buffer
		c := &captureAdmin{}
		code := Admin(context.Background(), args, strings.NewReader(""), &out, &errs, c)
		return code, c, errs.String()
	}
	if code, c, errs := admin("status", "/unused", "--passphrase", "s3cret"); code != 0 {
		t.Fatal(code, errs)
	} else if c.got.Passphrase != "s3cret" || c.got.Key != [32]byte{} {
		t.Fatalf("passphrase routed to %+v", c.got)
	}
	if code, c, errs := admin("status", "/unused", "--passphrase", raw); code != 0 {
		t.Fatal(code, errs)
	} else if c.got.Passphrase != "" || c.got.Key != auth.ResolveKey(raw) {
		t.Fatalf("64-hex passphrase routed to %+v", c.got)
	}
	if code, _, errs := admin("status", "/unused", "--key-hex", raw, "--passphrase", "x"); code != 1 {
		t.Fatal("dual credential accepted", code, errs)
	}
	if code, c, errs := admin("status", "/unused", "--key-provider-uri", "env:MURMUR_KEY"); code != 0 {
		t.Fatal(code, errs)
	} else if c.got.KeyProviderURI != "env:MURMUR_KEY" || c.got.Passphrase != "" || c.got.Key != [32]byte{} {
		t.Fatalf("provider uri routed to %+v", c.got)
	}
	if code, _, errs := admin("status", "/unused", "--key-provider-uri", "env:X", "--passphrase", "x"); code != 1 {
		t.Fatal("provider plus passphrase accepted", code, errs)
	}
	if code, c, errs := admin("rekey", "/unused", "--passphrase", "x", "--new-passphrase", "y"); code != 0 {
		t.Fatal(code, errs)
	} else if c.got.NewPassphrase != "y" || c.got.NewKey != [32]byte{} {
		t.Fatalf("new passphrase routed to %+v", c.got)
	}
	if code, c, errs := admin("rekey", "/unused", "--key-hex", raw, "--new-key-hex", raw); code != 0 {
		t.Fatal(code, errs)
	} else if c.got.NewPassphrase != "" || c.got.NewKey != auth.ResolveKey(raw) {
		t.Fatalf("new key routed to %+v", c.got)
	}
	for _, args := range [][]string{
		{"rekey", "/unused", "--passphrase", "x"},
		{"rekey", "/unused", "--passphrase", "x", "--new-passphrase", "y", "--new-key-hex", raw},
		{"rekey", "/unused", "--passphrase", "x", "--new-key-hex", "bad"},
		{"status", "/unused", "--new-passphrase", "y"},
	} {
		if code, _, errs := admin(args...); code != 1 {
			t.Fatal(args, code, errs)
		}
	}
}
