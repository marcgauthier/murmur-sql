// Package gcbalance is the Go port of the Rust gc-balance live scenario
// (tests/gc_balance.rs), driven by tests-live/goharness.
//
// The scenario itself lives in gc_balance_test.go; this file only gives
// `go build ./...` a non-test file (Go reports test-only directories as
// build failures).
package gcbalance
