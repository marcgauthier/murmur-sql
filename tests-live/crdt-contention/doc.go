// Package crdtcontention is the Go port of the Rust crdt-contention live
// scenario (tests/crdt_contention.rs), driven by tests-live/goharness.
//
// The scenario itself lives in crdt_contention_test.go; this file only
// gives `go build ./...` a non-test file (Go reports test-only directories
// as build failures).
package crdtcontention
