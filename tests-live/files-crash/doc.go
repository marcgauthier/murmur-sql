// Package filescrash is the Go port of the Rust files-crash live
// scenario (tests/files_crash.rs), driven by tests-live/goharness.
//
// The scenario itself lives in files_crash_test.go; this file only gives
// `go build ./...` a non-test file (Go reports test-only directories as
// build failures).
package filescrash
