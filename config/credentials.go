package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
	"unicode"
)

// ValidateCredentials checks configured replication credentials without opening
// storage or starting a replication transport. Node-specific semantic checks
// remain the engine's responsibility at Open.
func (c Config) ValidateCredentials() error {
	if e := c.Validate(); e != nil {
		return e
	}
	r := c.Replication
	if !r.Enabled {
		return nil
	}
	if _, e := tls.LoadX509KeyPair(*r.CertChainFile, *r.PrivateKeyFile); e != nil {
		return fmt.Errorf("replication TLS credentials: %w", e)
	}
	b, e := os.ReadFile(*r.CaFile)
	if e != nil {
		return e
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(b) {
		return fmt.Errorf("replication CA bundle contains no certificates")
	}
	if r.SigningKeyFile != nil {
		b, e = os.ReadFile(*r.SigningKeyFile)
		if e != nil {
			return e
		}
		defer clear(b)
		if len(b) != 32 {
			if _, e = ParseHex(strings.Map(func(r rune) rune {
				if unicode.IsSpace(r) {
					return -1
				}
				return r
			}, string(b)), 32); e != nil {
				return fmt.Errorf("replication signing key file: %w", e)
			}
		}
	}
	return nil
}
