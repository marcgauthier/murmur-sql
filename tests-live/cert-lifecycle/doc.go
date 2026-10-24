// Package certlifecycle is the Go port of the Rust cert-lifecycle live
// scenario (tests/cert_lifecycle.rs), driven by tests-live/goharness.
//
// The scenario itself lives in cert_lifecycle_test.go; this file only
// gives `go build ./...` a non-test file (Go reports test-only
// directories as build failures).
package certlifecycle
