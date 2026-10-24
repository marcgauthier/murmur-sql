// Package gracefulshutdown is the Go port of the Rust graceful-shutdown
// live scenario (tests/graceful_shutdown.rs), driven by
// tests-live/gosqlharness.
//
// The scenario itself lives in graceful_shutdown_test.go; this file only
// gives `go build ./...` a non-test file (Go reports test-only
// directories as build failures).
package gracefulshutdown
