// Package hostileschema is the Go port of the Rust hostile-schema live
// scenario (tests/hostile_schema.rs), driven by tests-live/goharness.
//
// The scenario itself lives in hostile_schema_test.go; this file only
// gives `go build ./...` a non-test file (Go reports test-only directories
// as build failures).
package hostileschema
