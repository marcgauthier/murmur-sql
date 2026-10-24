// Package originsignatures is the Go port of the Rust origin-signatures
// live scenario (tests/origin_signatures.rs), driven by
// tests-live/goharness plus the product murmur/replication wire types.
//
// The scenario itself lives in origin_signatures_test.go; this file only
// gives `go build ./...` a non-test file (Go reports test-only
// directories as build failures).
package originsignatures
