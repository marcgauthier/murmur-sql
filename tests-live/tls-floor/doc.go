// Package tlsfloor is the Go port of the Rust tls-floor live scenario
// (tests/tls_floor.rs), driven by tests-live/gosqlharness.
//
// The scenario itself lives in tls_floor_test.go; this file only gives
// `go build ./...` a non-test file (Go reports test-only directories as
// build failures).
package tlsfloor
