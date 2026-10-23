package logging

import (
	"encoding/json"
	"github.com/marcgauthier/murmur-sql-go/config"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestFileAndVictoriaLogs(t *testing.T) {
	received := make(chan map[string]any, 1)
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/loki/api/v1/push" {
			t.Error(r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer fixture-token" {
			t.Error("missing auth")
		}
		raw, e := io.ReadAll(r.Body)
		if e != nil {
			t.Error(e)
		}
		var body map[string]any
		if e = json.Unmarshal(raw, &body); e != nil {
			t.Error(e)
		}
		received <- body
		w.WriteHeader(204)
	}))
	defer remote.Close()
	cfg := config.DefaultLoggingConfig()
	cfg.Dir = t.TempDir()
	cfg.Victorialogs.Enabled = true
	cfg.Victorialogs.URL = remote.URL
	token := "fixture-token"
	cfg.Victorialogs.BearerToken = &token
	cfg.Victorialogs.Labels = map[string]string{"node": "test"}
	logger, sink, e := New(cfg)
	if e != nil {
		t.Fatal(e)
	}
	logger.Info("fixture event", "count", 42)
	if e = sink.Close(); e != nil {
		t.Fatal(e)
	}
	select {
	case body := <-received:
		stream := body["streams"].([]any)[0].(map[string]any)
		if stream["stream"].(map[string]any)["node"] != "test" {
			t.Fatal(body)
		}
	case <-time.After(time.Second):
		t.Fatal("remote batch was not flushed")
	}
	b, e := os.ReadFile(filepath.Join(cfg.Dir, cfg.FilePrefix+"."+time.Now().UTC().Format("2006-01-02")))
	if e != nil || !strings.Contains(string(b), "fixture event") {
		t.Fatal(string(b), e)
	}
	if _, e = sink.Write([]byte("late")); e == nil {
		t.Fatal("write after close succeeded")
	}
}
func TestRetentionPreservesUnrelatedFiles(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 10, 22, 0, 0, 0, 0, time.UTC)
	for _, name := range []string{"murmur.2026-10-01", "murmur.2026-10-21", "other.2026-10-01", "murmur.not-a-date", "murmur.2026-02-31"} {
		os.WriteFile(filepath.Join(dir, name), nil, 0600)
	}
	n, e := EnforceRetention(dir, "murmur", 14, now)
	if e != nil || n != 1 {
		t.Fatal(n, e)
	}
	if _, e = os.Stat(filepath.Join(dir, "murmur.2026-10-21")); e != nil {
		t.Fatal(e)
	}
	n, e = EnforceRetention(dir, "murmur", 0, now)
	if e != nil || n != 0 {
		t.Fatal(n, e)
	}
}
