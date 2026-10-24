// Package bridgetwostreams is the Go port of the Rust bridge-two-streams
// live scenario (tests/bridge_two_streams.rs), driven by
// tests-live/goharness.
//
// The scenario itself lives in bridge_two_streams_test.go; this file
// only gives `go build ./...` a non-test file (Go reports test-only
// directories as build failures).
package bridgetwostreams
