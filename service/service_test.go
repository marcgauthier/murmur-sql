package service

import (
	"context"
	"github.com/marcgauthier/murmur-sql/config"
	"path/filepath"
	"strings"
	"testing"
)

func TestPathBoundaries(t *testing.T) {
	for _, p := range []string{"relative", "/", "/tmp/../tmp/murmur"} {
		if e := safeDirectory(p); e == nil {
			t.Fatal(p)
		}
	}
	if within("/tmp/foo", "/tmp/foobar") {
		t.Fatal("prefix mistaken for child")
	}
	base, e := filepath.EvalSymlinks(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	i := installation{Install: filepath.Join(base, "bin"), Runtime: filepath.Join(base, "runtime")}
	c := config.DefaultConfig()
	if e := validateWritable(i, c); e == nil {
		t.Fatal("relative runtime paths accepted")
	}
	c.Storage.DataDir = filepath.Join(i.Runtime, "data")
	c.Logging.Dir = filepath.Join(i.Runtime, "logs")
	c.Backups.Dir = filepath.Join(i.Runtime, "backups")
	if e := validateWritable(i, c); e != nil {
		t.Fatal(e)
	}
}
func TestDispatchDoesNotConsumeNormalArguments(t *testing.T) {
	handled, e := Dispatch(context.Background(), []string{"--help"}, strings.NewReader(""), &strings.Builder{})
	if handled || e != nil {
		t.Fatal(handled, e)
	}
	handled, e = Dispatch(context.Background(), []string{"/install", "--config", "x"}, strings.NewReader(""), &strings.Builder{})
	if !handled || e == nil {
		t.Fatal(handled, e)
	}
}
