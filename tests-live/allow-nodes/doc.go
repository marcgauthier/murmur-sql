// Package allownodes is the Go port of the Rust allow-nodes live scenario
// (tests/allow_nodes.rs), driven by tests-live/gosqlharness.
//
// The scenario itself lives in allow_nodes_test.go; this file only gives
// `go build ./...` a non-test file (Go reports test-only directories
// as build failures).
package allownodes
