// Package crashrecovery is the Go port of the Rust crash-recovery live
// scenario (tests/crash_recovery.rs), driven by tests-live/goharness.
//
// The scenario itself lives in crash_recovery_test.go; this file only
// gives `go build ./...` a non-test file (Go reports test-only directories
// as build failures).
package crashrecovery
