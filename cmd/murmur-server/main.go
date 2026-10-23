package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/marcgauthier/murmur-sql-go/engine/native"
	"github.com/marcgauthier/murmur-sql-go/internal/cli"
	"github.com/marcgauthier/murmur-sql-go/service"
)

func main() { os.Exit(run()) }
func run() int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if handled, e := service.Dispatch(ctx, os.Args[1:], os.Stdin, os.Stdout); handled {
		if e != nil {
			fmt.Fprintln(os.Stderr, e)
			return 1
		}
		return 0
	}
	run := func(ctx context.Context) int {
		return cli.Server(ctx, os.Args[1:], os.Stdout, os.Stderr, native.Factory{})
	}
	if handled, code := service.Host(ctx, run); handled {
		return code
	}
	return run(ctx)
}
