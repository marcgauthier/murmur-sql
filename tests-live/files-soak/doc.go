// Package filessoak is the Go port of the Rust files-soak live scenario
// (tests/files_soak.rs), driven by tests-live/goharness.
//
// The scenario itself lives in files_soak_test.go; this file only gives
// `go build ./...` a non-test file (Go reports test-only directories as
// build failures).
package filessoak
