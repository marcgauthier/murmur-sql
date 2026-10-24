// Package highlow is the Go port of the Rust highlow live scenarios
// (tests/highlow.rs), driven by tests-live/goharness.
//
// The scenarios themselves live in highlow_test.go; this file only
// gives `go build ./...` a non-test file (Go reports test-only directories
// as build failures).
package highlow
