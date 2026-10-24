// Package compression is the Go port of the Rust compression live
// scenario (tests/compression.rs), driven by tests-live/goharness.
//
// The scenario itself lives in compression_test.go; this file only gives
// `go build ./...` a non-test file (Go reports test-only directories as
// build failures).
package compression
