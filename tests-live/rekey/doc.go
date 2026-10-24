// Package rekey is the Go port of the Rust rekey live scenario
// (tests/rekey.rs), driven by tests-live/gosqlharness.
//
// The scenario itself lives in rekey_test.go; this file only gives
// `go build ./...` a non-test file (Go reports test-only directories as
// build failures).
package rekey
