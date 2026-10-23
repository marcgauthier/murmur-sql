package server

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"github.com/marcgauthier/murmur-sql-go/config"
)

func TLSConfig(c config.Config) (*tls.Config, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	t := c.Server.TLS
	if !t.Enabled {
		return nil, nil
	}
	pair, e := tls.LoadX509KeyPair(*t.CertFile, *t.KeyFile)
	if e != nil {
		return nil, e
	}
	out := &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{pair}}
	if c.Server.Production {
		b, e := os.ReadFile(*t.CaFile)
		if e != nil {
			return nil, e
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(b) {
			return nil, fmt.Errorf("no client CA certificates")
		}
		out.ClientCAs = pool
		out.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return out, nil
}
