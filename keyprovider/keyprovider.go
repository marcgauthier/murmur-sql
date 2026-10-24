// Package keyprovider resolves 32-byte secrets from URIs so key
// material can live outside config files. It is standard library
// only: cloud KMS and HSM integrations plug in through the exec
// scheme, which shells out to an operator-supplied helper.
//
// Supported URIs:
//
//	file:/path/to/key      raw 32 bytes, or whitespace-trimmed hex text
//	env:VAR_NAME           hex text (whitespace-trimmed) from the environment
//	exec:/helper?timeout=5s
//	                       helper invoked as `helper <uri>`; stdout carries
//	                       raw 32 bytes or hex text
//
// File and helper output share one format: exactly 32 raw bytes, or
// hex text decoding to 32 bytes after trimming whitespace. Helpers
// inherit the process environment (for cloud credentials), must exit
// zero, and must not print secrets to stderr: a bounded stderr tail
// is included in resolution errors for diagnostics.
package keyprovider

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
	"unicode"
)

// Provider resolves 32 bytes of key material.
type Provider interface {
	Resolve(ctx context.Context) ([32]byte, error)
}

// DefaultExecTimeout bounds helper execution when the URI carries no
// timeout parameter.
const DefaultExecTimeout = 30 * time.Second

// maxExecTimeout caps the timeout parameter. maxPayload bounds file,
// environment, and helper output before parsing.
const (
	maxExecTimeout = 5 * time.Minute
	maxPayload     = 1 << 20
)

// ParseKeyMaterial decodes helper, file, or environment output: exactly
// 32 raw bytes, or hex text decoding to 32 bytes after trimming
// whitespace.
func ParseKeyMaterial(raw []byte) ([32]byte, error) {
	var out [32]byte
	if len(raw) == 32 {
		copy(out[:], raw)
		return out, nil
	}
	text := strings.Map(func(c rune) rune {
		if unicode.IsSpace(c) {
			return -1
		}
		return c
	}, string(raw))
	if len(text) != 64 {
		return out, fmt.Errorf("keyprovider: want 32 bytes or 64 hex characters, got %d bytes", len(raw))
	}
	b, err := hex.DecodeString(text)
	if err != nil {
		return out, fmt.Errorf("keyprovider: invalid hex key: %v", err)
	}
	copy(out[:], b)
	return out, nil
}

// parsedURI is a validated key URI: scheme plus scheme-specific body.
type parsedURI struct {
	scheme string
	body   string
	query  url.Values
}

// ParseURI validates a key URI without resolving it. It reports the
// scheme so callers can reject unknown providers before use.
func ParseURI(uri string) (scheme, body string, err error) {
	p, err := parseURI(uri)
	if err != nil {
		return "", "", err
	}
	return p.scheme, p.body, nil
}

func parseURI(uri string) (parsedURI, error) {
	var out parsedURI
	scheme, rest, ok := strings.Cut(uri, ":")
	if !ok || scheme == "" || rest == "" {
		return out, fmt.Errorf("keyprovider: malformed key URI %q", uri)
	}
	body, query, _ := strings.Cut(rest, "?")
	out = parsedURI{scheme: scheme, body: body}
	switch scheme {
	case "file":
		if strings.Contains(rest, "?") {
			return out, fmt.Errorf("keyprovider: file URI takes no parameters: %q", uri)
		}
		if body == "" {
			return out, fmt.Errorf("keyprovider: file URI needs a path: %q", uri)
		}
	case "env":
		if strings.Contains(rest, "?") {
			return out, fmt.Errorf("keyprovider: env URI takes no parameters: %q", uri)
		}
		if !validEnvName(body) {
			return out, fmt.Errorf("keyprovider: invalid environment name %q", body)
		}
	case "exec":
		if !filepath.IsAbs(body) {
			return out, fmt.Errorf("keyprovider: exec helper must be an absolute path: %q", uri)
		}
		q, err := url.ParseQuery(query)
		if err != nil {
			return out, fmt.Errorf("keyprovider: bad exec parameters: %v", err)
		}
		for name := range q {
			if name != "timeout" {
				return out, fmt.Errorf("keyprovider: unknown exec parameter %q", name)
			}
		}
		if t := q.Get("timeout"); t != "" {
			d, err := time.ParseDuration(t)
			if err != nil || d <= 0 || d > maxExecTimeout {
				return out, fmt.Errorf("keyprovider: timeout must be within (0, %s]: %q", maxExecTimeout, t)
			}
		}
		out.query = q
	default:
		return out, fmt.Errorf("keyprovider: unknown scheme %q", scheme)
	}
	return out, nil
}

func validEnvName(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c != '_' && (c < 'A' || c > 'Z') && (c < 'a' || c > 'z') && (i == 0 || c < '0' || c > '9') {
			return false
		}
	}
	return len(s) > 0
}

// ForURI builds the provider for uri. Use ResolveURI for one-shot
// resolution.
func ForURI(uri string) (Provider, error) {
	p, err := parseURI(uri)
	if err != nil {
		return nil, err
	}
	switch p.scheme {
	case "file":
		return fileProvider{path: p.body}, nil
	case "env":
		return envProvider{name: p.body}, nil
	case "exec":
		timeout := DefaultExecTimeout
		if t := p.query.Get("timeout"); t != "" {
			timeout, _ = time.ParseDuration(t)
		}
		return execProvider{helper: p.body, uri: uri, timeout: timeout}, nil
	}
	return nil, fmt.Errorf("keyprovider: unknown scheme %q", p.scheme)
}

// ResolveURI resolves uri in one call.
func ResolveURI(ctx context.Context, uri string) ([32]byte, error) {
	p, err := ForURI(uri)
	if err != nil {
		return [32]byte{}, err
	}
	return p.Resolve(ctx)
}

type fileProvider struct{ path string }

func (p fileProvider) Resolve(context.Context) ([32]byte, error) {
	raw, err := readBounded(p.path)
	if err != nil {
		return [32]byte{}, err
	}
	defer clear(raw)
	key, err := ParseKeyMaterial(raw)
	if err != nil {
		return [32]byte{}, fmt.Errorf("keyprovider: file %s: %v", p.path, err)
	}
	return key, nil
}

func readBounded(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("keyprovider: open %s: %v", path, err)
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, maxPayload+1))
	if err != nil {
		return nil, fmt.Errorf("keyprovider: read %s: %v", path, err)
	}
	if len(raw) > maxPayload {
		return nil, fmt.Errorf("keyprovider: file %s exceeds %d bytes", path, maxPayload)
	}
	return raw, nil
}

type envProvider struct{ name string }

func (p envProvider) Resolve(context.Context) ([32]byte, error) {
	raw, ok := os.LookupEnv(p.name)
	if !ok || raw == "" {
		return [32]byte{}, fmt.Errorf("keyprovider: environment %s is not set", p.name)
	}
	if len(raw) > maxPayload {
		return [32]byte{}, fmt.Errorf("keyprovider: environment %s exceeds %d bytes", p.name, maxPayload)
	}
	key, err := ParseKeyMaterial([]byte(raw))
	if err != nil {
		return [32]byte{}, fmt.Errorf("keyprovider: env %s: %v", p.name, err)
	}
	return key, nil
}

type execProvider struct {
	helper  string
	uri     string
	timeout time.Duration
}

func (p execProvider) Resolve(ctx context.Context) ([32]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, p.helper, p.uri)
	var stdout, stderr boundedBuffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg == "" {
			msg = err.Error()
		} else if len(msg) > 1024 {
			msg = msg[:1024]
		}
		return [32]byte{}, fmt.Errorf("keyprovider: helper %s: %s", p.helper, msg)
	}
	out := stdout.Bytes()
	defer clear(out)
	if len(out) > maxPayload {
		return [32]byte{}, fmt.Errorf("keyprovider: helper %s output exceeds %d bytes", p.helper, maxPayload)
	}
	key, err := ParseKeyMaterial(out)
	if err != nil {
		return [32]byte{}, fmt.Errorf("keyprovider: helper %s: %v", p.helper, err)
	}
	return key, nil
}

// boundedBuffer caps captured helper output at maxPayload+1 so a
// runaway helper cannot grow memory without bound.
type boundedBuffer struct {
	buf bytes.Buffer
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	room := maxPayload + 1 - b.buf.Len()
	if room <= 0 {
		return len(p), nil
	}
	if len(p) > room {
		p = p[:room]
	}
	return b.buf.Write(p)
}

func (b *boundedBuffer) String() string { return b.buf.String() }
func (b *boundedBuffer) Bytes() []byte  { return b.buf.Bytes() }
