// Package crashloop is the Go port of the Rust crash-loop live scenario
// (tests/crash_loop.rs), driven by tests-live/goharness.
//
// The scenario itself lives in crash_loop_test.go; this file only gives
// `go build ./...` a non-test file (Go reports test-only directories as
// build failures).
package crashloop
