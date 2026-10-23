package cli

import (
	"bytes"
	"context"
	"github.com/marcgauthier/murmur-sql/auth"
	"strings"
	"testing"
)

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
