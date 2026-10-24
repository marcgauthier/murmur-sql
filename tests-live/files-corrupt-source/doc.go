// Package filescorruptsource is the Go port of the Rust
// files-corrupt-source live scenario
// (tests/files_corrupt_source.rs), driven by tests-live/goharness.
//
// The scenario itself lives in files_corrupt_source_test.go; this file
// only gives `go build ./...` a non-test file (Go reports test-only
// directories as build failures).
package filescorruptsource
