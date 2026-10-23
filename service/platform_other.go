//go:build !linux && !windows

package service

import (
	"context"
	"errors"
	"github.com/marcgauthier/murmur-sql/config"
)

func unsupported() error {
	return errors.New("service installation supports Linux/systemd and Windows only")
}
func recordPath() string                                             { return "/unsupported/murmur-sql/install.json" }
func defaults() (string, string)                                     { return "", "" }
func protectRecord() error                                           { return unsupported() }
func preflight() error                                               { return unsupported() }
func registered(context.Context) (bool, error)                       { return false, unsupported() }
func running(context.Context) (bool, error)                          { return false, unsupported() }
func stop(context.Context) error                                     { return unsupported() }
func start(context.Context) error                                    { return unsupported() }
func identity(context.Context) error                                 { return unsupported() }
func register(context.Context, installation) error                   { return unsupported() }
func unregister(context.Context) error                               { return unsupported() }
func permissions(context.Context, installation, config.Config) error { return unsupported() }
func Host(context.Context, func(context.Context) int) (bool, int)    { return false, 0 }
func validateAncestors(string) error                                 { return unsupported() }
