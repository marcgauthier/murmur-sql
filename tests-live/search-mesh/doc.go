// Package searchmesh is the Go port of the Rust search-mesh live
// scenario (tests/search_mesh.rs), driven by tests-live/gosqlharness.
//
// The scenario itself lives in search_mesh_test.go; this file only
// gives `go build ./...` a non-test file (Go reports test-only
// directories as build failures).
package searchmesh
