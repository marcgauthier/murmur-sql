// Package rollingrestart is the Go port of the Rust rolling-restart
// live scenario (tests/rolling_restart.rs), driven by
// tests-live/goharness.
//
// The scenario itself lives in rolling_restart_test.go; this file only
// gives `go build ./...` a non-test file (Go reports test-only
// directories as build failures).
package rollingrestart
