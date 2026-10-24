// Package diskfulllive is the Go port of the Rust diskfull-live live
// scenario (tests/diskfull_live.rs), driven by tests-live/goharness.
//
// The scenario itself lives in diskfull_live_test.go; this file only
// gives `go build ./...` a non-test file (Go reports test-only directories
// as build failures).
package diskfulllive
