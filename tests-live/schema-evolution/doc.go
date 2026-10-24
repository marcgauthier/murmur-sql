// Package schemaevolution is the Go port of the Rust schema-evolution
// live scenario (tests/schema_evolution.rs), driven by
// tests-live/gosqlharness.
//
// The scenario itself lives in schema_evolution_test.go; this file only
// gives `go build ./...` a non-test file (Go reports test-only
// directories as build failures).
package schemaevolution
