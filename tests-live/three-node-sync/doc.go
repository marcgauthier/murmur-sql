// Package threenodesync is the Go port of the Rust three-node-sync live
// scenario (tests/three_node_sync.rs), driven by tests-live/gosqlharness.
//
// The scenario itself lives in three_node_sync_test.go; this file only
// gives `go build ./...` a non-test file (Go reports test-only directories
// as build failures).
package threenodesync
