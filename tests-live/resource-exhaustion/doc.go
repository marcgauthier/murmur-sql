// Package resourceexhaustion is the Go port of the Rust
// resource-exhaustion live scenario
// (tests/resource_exhaustion.rs), driven by tests-live/goharness.
//
// The scenario itself lives in resource_exhaustion_test.go; this file
// only gives `go build ./...` a non-test file (Go reports test-only
// directories as build failures).
package resourceexhaustion
