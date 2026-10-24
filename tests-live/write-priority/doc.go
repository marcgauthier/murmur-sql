// Package writepriority is the Go port of the Rust write-priority live
// scenario (tests/write_priority.rs), driven by tests-live/goharness.
//
// The scenario itself lives in write_priority_test.go; this file only
// gives `go build ./...` a non-test file (Go reports test-only
// directories as build failures).
package writepriority
