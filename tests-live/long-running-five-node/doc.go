// Package longrunningfivenode is the Go port of the Rust
// long-running-five-node live scenario (tests/long_running_five_node.rs),
// driven by tests-live/goharness.
//
// The scenario itself lives in long_running_five_node_test.go; this file
// only gives `go build ./...` a non-test file (Go reports test-only
// directories as build failures).
package longrunningfivenode
