// Package txchunkresume is the Go port of the Rust txchunk-resume live
// scenario (tests/txchunk_resume.rs), driven by tests-live/goharness.
//
// The scenario itself lives in txchunk_resume_test.go; this file only
// gives `go build ./...` a non-test file (Go reports test-only
// directories as build failures).
package txchunkresume
