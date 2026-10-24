// Package typedbridge is the Go port of the Rust typed-bridge live
// scenario (tests/typed_bridge.rs), driven by tests-live/goharness.
//
// The scenario itself lives in typed_bridge_test.go; this file only gives
// `go build ./...` a non-test file (Go reports test-only directories as
// build failures).
package typedbridge
