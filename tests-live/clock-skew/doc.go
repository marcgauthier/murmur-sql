// Package clockskew is the Go port of the Rust clock-skew live scenario
// (tests/clock_skew.rs), driven by tests-live/gosqlharness.
//
// The scenario itself lives in clock_skew_test.go; this file only gives
// `go build ./...` a non-test file (Go reports test-only directories
// as build failures).
package clockskew
