package main

import (
	"context"
	"os"

	"github.com/marcgauthier/murmur-sql-go/engine/native"
	"github.com/marcgauthier/murmur-sql-go/internal/cli"
)

func main() {
	os.Exit(cli.Admin(context.Background(), os.Args[1:], os.Stdin, os.Stdout, os.Stderr, native.Factory{}))
}
