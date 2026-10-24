// Package logboundedness is the Go port of the Rust log-boundedness live
// scenario (tests/log_boundedness.rs), driven by tests-live/goharness.
//
// The scenario itself lives in log_boundedness_test.go; this file only
// gives `go build ./...` a non-test file (Go reports test-only directories
// as build failures).
package logboundedness
