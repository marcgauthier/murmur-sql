// Package largepayload is the Go port of the Rust large-payload live
// scenario (tests/large_payload.rs), driven by tests-live/goharness.
//
// The scenario itself lives in large_payload_test.go; this file only
// gives `go build ./...` a non-test file (Go reports test-only directories
// as build failures).
package largepayload
