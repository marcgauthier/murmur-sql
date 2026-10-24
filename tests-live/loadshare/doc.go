// Package loadshare is the Go port of the Rust loadshare live scenario
// (tests/loadshare.rs), driven by tests-live/goharness.
//
// The scenario itself lives in loadshare_test.go; this file only
// gives `go build ./...` a non-test file (Go reports test-only directories
// as build failures).
package loadshare
