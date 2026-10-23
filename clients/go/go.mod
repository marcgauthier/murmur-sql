module github.com/marcgauthier/murmur-sql/clients/go

go 1.24.0

toolchain go1.24.7

require github.com/lib/pq v1.10.9

replace github.com/lib/pq => github.com/marcgauthier/pq v0.0.0-20260820222753-451ac10e4e85

require github.com/marcgauthier/murmur-sql v0.0.0

require (
	golang.org/x/crypto v0.41.0 // indirect
	golang.org/x/sys v0.35.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/marcgauthier/murmur-sql => ../..

replace github.com/DataDog/zstd => ../../murmur/third_party/zstd
