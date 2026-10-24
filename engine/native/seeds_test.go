package native

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestResolveSocket pins hostname seed resolution: literals pass
// through, names resolve, and failures name the entry.
func TestResolveSocket(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	v, err := resolveSocket(ctx, "203.0.113.11:9001")
	if err != nil {
		t.Fatalf("literal: %v", err)
	}
	if v.String() != "203.0.113.11:9001" {
		t.Fatalf("literal = %v", v)
	}
	v, err = resolveSocket(ctx, "localhost:9001")
	if err != nil {
		t.Fatalf("localhost: %v", err)
	}
	if !v.Addr().IsLoopback() || v.Port() != 9001 {
		t.Fatalf("localhost = %v, want loopback:9001", v)
	}
	if _, err := resolveSocket(ctx, "localhost"); err == nil {
		t.Fatal("bare name accepted, want explicit port")
	} else if !strings.Contains(err.Error(), "localhost") {
		t.Fatalf("err = %v, want entry named", err)
	}
	if _, err := resolveSocket(ctx, "not a host:9001"); err == nil {
		t.Fatal("bad name accepted, want error")
	}
	if _, err := resolveSocket(ctx, "nonexistent.invalid:9001"); err == nil {
		t.Fatal("unresolvable name accepted, want error")
	} else if !strings.Contains(err.Error(), "nonexistent.invalid:9001") {
		t.Fatalf("err = %v, want entry named", err)
	}
}
