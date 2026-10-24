// Package chaosload is the Go port of the Rust chaos-load live scenario
// (tests/chaos_load.rs), driven by tests-live/goharness.
//
// The scenario itself lives in chaos_load_test.go; this file only gives
// `go build ./...` a non-test file (Go reports test-only directories
// as build failures).
package chaosload
