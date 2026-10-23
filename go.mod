module github.com/marcgauthier/murmur-sql-go

go 1.24.0

require (
	github.com/DataDog/zstd v1.5.7
	github.com/quic-go/quic-go v0.57.1
	golang.org/x/crypto v0.41.0
	golang.org/x/sync v0.16.0
	golang.org/x/sys v0.35.0
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/kr/text v0.2.0 // indirect
	golang.org/x/net v0.43.0 // indirect
)

replace github.com/DataDog/zstd => ./murmur/third_party/zstd
