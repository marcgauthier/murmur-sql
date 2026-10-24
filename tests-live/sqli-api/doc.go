// Package sqliapi is the Go port of the Rust sqli-api live scenario
// (tests/sqli_api.rs), driven by tests-live/gosqlharness.
//
// The scenario itself lives in sqli_api_test.go; this file only gives
// `go build ./...` a non-test file (Go reports test-only directories as
// build failures).
package sqliapi
