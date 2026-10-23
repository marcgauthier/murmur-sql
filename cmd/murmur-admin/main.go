package main

import (
	"context"
	"os"

	"github.com/marcgauthier/murmur-sql/engine/native"
	"github.com/marcgauthier/murmur-sql/internal/cli"
)

func main() {
	os.Exit(cli.Admin(context.Background(), os.Args[1:], os.Stdin, os.Stdout, os.Stderr, native.Factory{}))
}
