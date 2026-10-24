// Package downgradeguard is the Go port of the Rust downgrade-guard live
// scenario (tests/downgrade_guard.rs), driven by
// tests-live/gosqlharness.
//
// The scenario itself lives in downgrade_guard_test.go; this file only
// gives `go build ./...` a non-test file (Go reports test-only directories
// as build failures).
package downgradeguard
