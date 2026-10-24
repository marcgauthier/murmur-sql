package config

import (
	"strings"
	"testing"
)

func uriTestIDs() (domain, recipient, key string) {
	return strings.Repeat("ab", 16), strings.Repeat("cd", 32), strings.Repeat("ef", 32)
}

func TestSigningKeyURISource(t *testing.T) {
	base := "replication:\n  enabled: true\n  cert_chain_file: 'c'\n  private_key_file: 'k'\n  ca_file: 'ca'\n"
	if _, e := Parse([]byte(base + "  signing_key_uri: 'env:MURMUR_SIGNING_KEY'\n")); e != nil {
		t.Fatalf("signing_key_uri rejected: %v", e)
	}
	for _, s := range []string{
		base + "  signing_key_hex: '" + strings.Repeat("ab", 32) + "'\n  signing_key_uri: 'env:X'\n",
		base + "  signing_key_file: 'k'\n  signing_key_uri: 'env:X'\n",
		base + "  signing_key_uri: 'bogus:x'\n",
		base + "  signing_key_uri: ''\n",
	} {
		if _, e := Parse([]byte(s)); e == nil {
			t.Fatalf("accepted %q", s)
		}
	}
}

func TestFilesKeyURISource(t *testing.T) {
	_, _, key := uriTestIDs()
	base := "replication:\n  files:\n    directory: './files'\n    fetch_bind: '127.0.0.1:0'\n    max_file_bytes: '1MiB'\n    concurrent_fetches: 1\n    fetch_interval: '1s'\n"
	if _, e := Parse([]byte(base + "    key_uri: 'env:MURMUR_FILES_KEY'\n")); e != nil {
		t.Fatalf("key_uri rejected: %v", e)
	}
	if _, e := Parse([]byte(base + "    key_hex: '" + key + "'\n")); e != nil {
		t.Fatalf("key_hex rejected: %v", e)
	}
	for _, s := range []string{
		base,
		base + "    key_hex: '" + key + "'\n    key_uri: 'env:X'\n",
		base + "    key_uri: 'bogus:x'\n",
		base + "    key_hex: '" + key + "'\n    previous_key_hex: '" + key + "'\n    previous_key_uri: 'env:X'\n",
	} {
		if _, e := Parse([]byte(s)); e == nil {
			t.Fatalf("accepted %q", s)
		}
	}
	if _, e := Parse([]byte(base + "    key_hex: '" + key + "'\n    previous_key_uri: 'env:X'\n")); e != nil {
		t.Fatalf("previous_key_uri rejected: %v", e)
	}
}

func TestBridgeKeyURISource(t *testing.T) {
	domain, recipient, key := uriTestIDs()
	base := "replication:\n  bridge:\n    role: 'low-exporter'\n    domain: '" + domain + "'\n    directory: './bridge'\n    stream: 'main'\n    recipient: '" + recipient + "'\n"
	if _, e := Parse([]byte(base + "    signer_uri: 'env:MURMUR_SIGNER'\n")); e != nil {
		t.Fatalf("signer_uri rejected: %v", e)
	}
	high := "replication:\n  bridge:\n    role: 'high-receiver'\n    domain: '" + domain + "'\n    trusted_domains: ['" + domain + "']\n    directory: './bridge'\n    stream: 'main'\n    recipient: '" + recipient + "'\n"
	if _, e := Parse([]byte(high + "    receiver_secret_uri: 'env:MURMUR_RECEIVER'\n")); e != nil {
		t.Fatalf("receiver_secret_uri rejected: %v", e)
	}
	for _, s := range []string{
		base + "    signer_hex: '" + key + "'\n    signer_uri: 'env:X'\n",
		base,
		high,
		high + "    receiver_secret_hex: '" + key + "'\n    receiver_secret_uri: 'env:X'\n",
	} {
		if _, e := Parse([]byte(s)); e == nil {
			t.Fatalf("accepted %q", s)
		}
	}
}
