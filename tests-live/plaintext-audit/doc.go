// Package plaintextaudit is the Go port of the Rust plaintext-audit live
// scenario (tests/plaintext_audit.rs), driven by
// tests-live/gosqlharness.
//
// The scenario itself lives in plaintext_audit_test.go; this file only
// gives `go build ./...` a non-test file (Go reports test-only
// directories as build failures).
package plaintextaudit
