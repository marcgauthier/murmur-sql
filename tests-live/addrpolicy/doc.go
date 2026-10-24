// Package addrpolicy is the Go port of the Rust addrpolicy live scenario
// (tests/addrpolicy.rs), driven by tests-live/gosqlharness.
//
// The scenario itself lives in addrpolicy_test.go; this file only gives
// `go build ./...` a non-test file (Go reports test-only directories
// as build failures).
package addrpolicy
