# Direct external dependencies

This is the direct-dependency inventory for **Murmur-SQL** and its language
clients. It includes explicitly declared runtime, optional, and production
build dependencies. Dependencies used only for tests, benchmarks, or test
tooling are excluded, as are transitive dependencies, standard libraries,
toolchains, and this project's own path dependencies (including the sibling `../MURMUR` engine). Dependencies
used only inside that engine belong in the engine's inventory.

Versions below are **manifest requirements**, not resolved versions. Rust
requirements come from the root and Rust-client `Cargo.toml` files, excluding
`dev-dependencies`; Go requirements come from `clients/go/go.mod` excluding `// indirect`; Python requirements
come from `clients/python/pyproject.toml`. A package appears once per ecosystem,
even when several repository packages declare it directly.

Author names and upstream links come from published package metadata. Project
names identify open-source maintainers, not necessarily incorporated companies.
No company is attributed solely because it employs or sponsors a contributor.
Countries are not inferred from names, email domains, or project names.
“Not established” means the available sources do not establish a country of
origin. Explicit author-location entries cite public profiles and indicate
current stated location, not nationality or the package's legal origin.
Reviewed 2026-10-20.

## Rust crates

Company attribution follows package metadata; where no company is declared,
the maintaining project or author is listed instead.

| Package | Required version(s) | Direct use | Company / project | Author(s) / maintainers | Country / origin evidence |
| --- | --- | --- | --- | --- | --- |
| [`argon2`](https://github.com/RustCrypto/password-hashes/tree/master/argon2) | `0.5` | server | RustCrypto project | RustCrypto Developers | Not established |
| [`async-trait`](https://github.com/dtolnay/async-trait) | `0.1` | server | No company declared | David Tolnay | [United States (author location)](https://github.com/dtolnay) |
| [`base64`](https://github.com/marshallpierce/rust-base64) | `0.22` | Rust client, server | No company declared | Marshall Pierce | Not established |
| [`blake3`](https://github.com/BLAKE3-team/BLAKE3) | `1` | server | BLAKE3 team | Jack O'Connor; Samuel Neves | Not established |
| [`futures`](https://github.com/rust-lang/futures-rs) | `0.3` | server | Rust futures-rs project | futures-rs contributors | Not established |
| [`password-hash`](https://github.com/RustCrypto/traits/tree/master/password-hash) | `0.5` | server | RustCrypto project | RustCrypto Developers | Not established |
| [`pgwire`](https://github.com/sunng87/pgwire) | `0.41` | server | No company declared | Ning Sun | [China (author location: Beijing)](https://github.com/sunng87) |
| [`rand`](https://github.com/rust-random/rand) | `0.8` | server | Rand project | The Rand Project Developers; The Rust Project Developers | Not established |
| [`reqwest`](https://github.com/seanmonstar/reqwest) | `0.12` | server | No company declared | Sean McArthur | Not established |
| [`rusqlite`](https://github.com/rusqlite/rusqlite) | `0.40.2` | server | rusqlite project | The rusqlite developers | Not established |
| [`rustls`](https://github.com/rustls/rustls) | `0.23` | Rust client, server | Rustls project | Rustls contributors | Not established |
| [`rustls-pemfile`](https://github.com/rustls/pemfile) | `2` | Rust client, server | Rustls project | Rustls contributors | Not established |
| [`serde`](https://github.com/serde-rs/serde) | `1` | server | Serde project | Erick Tryzelaar; David Tolnay | Not established |
| [`serde_json`](https://github.com/serde-rs/json) | `1` | server | Serde project | Serde JSON contributors | Not established |
| [`thiserror`](https://github.com/dtolnay/thiserror) | `2` | Rust client | No company declared | David Tolnay | [United States (author location)](https://github.com/dtolnay) |
| [`tokio`](https://github.com/tokio-rs/tokio) | `1` | Rust client, server | Tokio project | Tokio Contributors | Not established |
| [`tokio-postgres`](https://github.com/rust-postgres/rust-postgres) | `0.7` | Rust client | No company declared | Steven Fackler | Not established |
| [`tokio-postgres-rustls`](https://github.com/jbg/tokio-postgres-rustls) | `0.13` | Rust client | No company declared | Jasper Hugo | Not established |
| [`tokio-rustls`](https://github.com/rustls/tokio-rustls) | `0.26` | server | Rustls project | Rustls contributors | Not established |
| [`toml`](https://github.com/toml-rs/toml) | `1` | server | TOML-rs project | TOML-rs contributors | Not established |
| [`toml_edit`](https://github.com/toml-rs/toml) | `0.22` | server | TOML-rs project | TOML-rs contributors | Not established |
| [`tracing`](https://github.com/tokio-rs/tracing) | `0.1` | server | Tokio project | Eliza Weisman; Tokio Contributors | Not established |
| [`tracing-appender`](https://github.com/tokio-rs/tracing) | `0.2` | server | Tokio project | Zeki Sherif; Tokio Contributors | Not established |
| [`tracing-subscriber`](https://github.com/tokio-rs/tracing) | `0.3` | server | Tokio project | Eliza Weisman; David Barsky; Tokio Contributors | Not established |
| [`windows-service`](https://github.com/mullvad/windows-service-rs) | `0.8` | server, Windows only | Mullvad VPN AB | Mullvad VPN and contributors | [Sweden (company)](https://mullvad.net/en/about) |
| [`zeroize`](https://github.com/RustCrypto/utils/tree/master/zeroize) | `1` | server | RustCrypto project | The RustCrypto Project Developers | Not established |

## Go modules

The client declares one direct external Go module. The other entries in
`go.mod` are explicitly indirect and are excluded.

| Package | Required version | Direct use | Company / project | Author / maintainer | Country / origin evidence |
| --- | --- | --- | --- | --- | --- |
| [`github.com/jackc/pgx/v5`](https://github.com/jackc/pgx) | `v5.8.0` | Go client runtime | No company declared | Jack Christensen and contributors | [United States (author location)](https://github.com/jackc) |

## Python packages

These are direct runtime and build requirements only. Test extras and
pg8000's own runtime requirements are excluded. Metadata attribution links point to upstream
package records; no publishing company or single country is established there.

| Package | Required version | Direct use | Company / project | Author(s) / maintainers | Country / origin evidence |
| --- | --- | --- | --- | --- | --- |
| [`pg8000`](https://pypi.org/project/pg8000/) | `>=1.31,<2` | Python client runtime | pg8000 project | The Contributors (current package attribution) | Not established |
| [`setuptools`](https://pypi.org/project/setuptools/) | `>=68` | Python build backend | Python Packaging Authority | PyPA and contributors | Not established |
