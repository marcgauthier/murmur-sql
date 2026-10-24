// Package hostilepeer is the Go port of the Rust hostile-peer live
// scenario (tests/hostile_peer.rs), driven by tests-live/goharness.
//
// The scenario itself lives in hostile_peer_test.go; this file only
// gives `go build ./...` a non-test file (Go reports test-only directories
// as build failures).
package hostilepeer
