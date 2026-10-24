// Package partialmesh is the Go port of the Rust partial-mesh live
// scenario (tests/partial_mesh.rs), driven by tests-live/goharness.
//
// The scenario itself lives in partial_mesh_test.go; this file only gives
// `go build ./...` a non-test file (Go reports test-only directories as
// build failures).
package partialmesh
