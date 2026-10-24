// Package impairednetwork is the Go port of the Rust impaired-network
// live scenario (tests/impaired_network.rs), driven by
// tests-live/goharness.
//
// The scenario itself lives in impaired_network_test.go; this file only
// gives `go build ./...` a non-test file (Go reports test-only directories
// as build failures).
package impairednetwork
