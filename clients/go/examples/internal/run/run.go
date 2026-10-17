// Package run adapts servertest.TB to main packages so examples share the
// same server-spawning helpers as the test suite.
package run

import (
	"log"
	"os"

	"github.com/marcgauthier/murmur/clients/go/internal/servertest"
)

// T implements servertest.TB for examples.
type T struct {
	cleanups []func()
}

func (t *T) Helper() {}

// Fatalf aborts the example.
func (t *T) Fatalf(format string, args ...any) {
	log.Fatalf("example: "+format, args...)
}

// Cleanup registers a deferred shutdown step.
func (t *T) Cleanup(f func()) {
	t.cleanups = append(t.cleanups, f)
}

// TempDir creates a scratch directory removed during Finish.
func (t *T) TempDir() string {
	dir, err := os.MkdirTemp("", "murmur-example-")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

// Finish runs cleanups in reverse order.
func (t *T) Finish() {
	for i := len(t.cleanups) - 1; i >= 0; i-- {
		t.cleanups[i]()
	}
	t.cleanups = nil
}

// Main runs fn with teardown handling and a servertest handle.
func Main(fn func(t servertest.TB, dir string)) {
	t := &T{}
	defer t.Finish()
	fn(t, t.TempDir())
}
