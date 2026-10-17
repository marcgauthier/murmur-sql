# VENDORS

This inventory separates crates declared directly in Murmur's Cargo manifests from crates they bring in transitively. It covers the root `Cargo.lock` and the standalone `tests-benchmark/Cargo.lock` and `tests-benchmark-local/Cargo.lock`; locked versions are shown. Dev/test, build, optional, and platform-specific dependencies are included. Rust standard-library components and Murmur's own path dependencies are not external crates.

"Direct" means declared in a Murmur package manifest, including dev/test or build dependencies; it does not assert that source code directly imports the crate. A crate declared directly by one Murmur package can also be transitive for another. Crates not declared in any Murmur manifest are listed as transitive.

Crate authors below come from crate manifests when available. Cargo metadata does not consistently provide an organization or country of origin, so country is marked `Not stated` rather than inferred from a maintainer's personal location.

## Direct dependencies

| Crate | Locked version(s) | Declared by (scope) | Author(s) / organization stated by crate | Country of origin |
|---|---:|---|---|---|
| `aes-gcm` | `0.10.3` | murmur-replication (runtime), murmur-spool (runtime) | RustCrypto Developers | Not stated |
| `argon2` | `0.5.3` | murmur-spool (runtime), sqlite-session (runtime) | RustCrypto Developers | Not stated |
| `async-trait` | `0.1.92` | sqlite-session (runtime) | David Tolnay <dtolnay@gmail.com> | Not stated |
| `base64` | `0.22.1, 0.23.1` | sqlite-session (runtime) | Marshall Pierce <marshall@mpierce.org> | Not stated |
| `blake3` | `1.8.7` | sqlite-session (runtime), sqlite-session-benchmark (runtime) | Jack O'Connor <oconnor663@gmail.com>, Samuel Neves | Not stated |
| `chacha20poly1305` | `0.10.1` | murmur-replication (runtime) | RustCrypto Developers | Not stated |
| `crc32c` | `0.6.8` | murmur-spool (runtime) | Zack Owens | Not stated |
| `ed25519-dalek` | `2.2.0` | murmur-replication (runtime) | isis lovecruft <isis@patternsinthevoid.net>, Tony Arcieri <bascule@gmail.com>, Michael Rosenberg <michael@mrosenberg.pub> | Not stated |
| `flate2` | `1.1.10` | murmur-spool (runtime) | Alex Crichton <alex@alexcrichton.com>, Josh Triplett <josh@joshtriplett.org> | Not stated |
| `fs2` | `0.4.3` | murmur-spool (runtime) | Dan Burkert <dan@danburkert.com> | Not stated |
| `futures` | `0.3.34` | sqlite-session (runtime) | Not stated in crate metadata | Not stated |
| `hkdf` | `0.12.4` | murmur-replication (runtime), murmur-spool (runtime) | RustCrypto Developers | Not stated |
| `ipnet` | `2.12.2` | murmur-replication (runtime), sqlite-session (runtime) | Kris Price <kris@krisprice.nz> | Not stated |
| `libc` | `0.2.190` | murmur-spool (runtime) | Not stated in crate metadata | Not stated |
| `password-hash` | `0.5.0` | sqlite-session (runtime) | RustCrypto Developers | Not stated |
| `pgwire` | `0.41.1` | sqlite-session (runtime) | Ning Sun <sunng@pm.me> | Not stated |
| `postcard` | `1.1.3` | murmur-replication (runtime), sqlite-session (runtime) | James Munns <james@onevariable.com> | Not stated |
| `quinn` | `0.11.12` | murmur-replication (runtime) | Not stated in crate metadata | Not stated |
| `rand` | `0.8.8, 0.10.3` | murmur-replication (runtime), murmur-spool (runtime), sqlite-session (runtime) | The Rand Project Developers, The Rust Project Developers | Not stated |
| `rcgen` | `0.13.2` | murmur-replication (dev/test), sqlite-session (dev/test), sqlite-session-benchmark (runtime) | Not stated in crate metadata | Not stated |
| `reqwest` | `0.12.28` | murmur-replication (runtime), sqlite-session (runtime) | Sean McArthur <sean@seanmonstar.com> | Not stated |
| `rusqlite` | `0.40.2` | sqlite-session (runtime), sqlite-session-benchmark (runtime) | The rusqlite developers | Not stated |
| `rustls` | `0.23.45, 0.23.46` | murmur-replication (runtime), sqlite-session (runtime) | Not stated in crate metadata | Not stated |
| `rustls-pemfile` | `2.2.0` | murmur-replication (runtime), sqlite-session (runtime) | Not stated in crate metadata | Not stated |
| `serde` | `1.0.229` | murmur-replication (runtime), sqlite-session (runtime) | Erick Tryzelaar <erick.tryzelaar@gmail.com>, David Tolnay <dtolnay@gmail.com> | Not stated |
| `serde_json` | `1.0.154` | sqlite-session (runtime), sqlite-session-benchmark (runtime) | Not stated in crate metadata | Not stated |
| `sha2` | `0.10.9, 0.11.1` | murmur-replication (runtime), murmur-spool (runtime) | RustCrypto Developers | Not stated |
| `sqlite-vec` | `0.1.6` | sqlite-session (runtime) | Not stated in crate metadata | Not stated |
| `tempfile` | `3.27.0` | murmur-local-bench (runtime), murmur-replication (dev/test), murmur-spool (dev/test), sqlite-session (dev/test), sqlite-session-benchmark (runtime) | Steven Allen <steven@stebalien.com>, The Rust Project Developers, Ashley Mannix <ashleymannix@live.com.au>, Jason White <me@jasonwhite.io> | Not stated |
| `thiserror` | `2.0.21` | murmur-replication (runtime), sqlite-session (runtime) | David Tolnay <dtolnay@gmail.com> | Not stated |
| `tokio` | `1.53.2, 1.53.3` | murmur-local-bench (runtime), murmur-replication (runtime), sqlite-session (runtime) | Tokio Contributors <team@tokio.rs> | Not stated |
| `tokio-postgres` | `0.7.18` | sqlite-session (dev/test) | Steven Fackler <sfackler@gmail.com> | Not stated |
| `tokio-rustls` | `0.26.6` | sqlite-session (runtime) | Not stated in crate metadata | Not stated |
| `toml` | `1.1.8+spec-1.1.0` | sqlite-session (runtime) | Not stated in crate metadata | Not stated |
| `toml_edit` | `0.22.27` | sqlite-session (runtime) | Not stated in crate metadata | Not stated |
| `tracing` | `0.1.44` | sqlite-session (runtime) | Eliza Weisman <eliza@buoyant.io>, Tokio Contributors <team@tokio.rs> | Not stated |
| `tracing-appender` | `0.2.5` | sqlite-session (runtime) | Zeki Sherif <zekshi@amazon.com>, Tokio Contributors <team@tokio.rs> | Not stated |
| `tracing-subscriber` | `0.3.23` | sqlite-session (runtime) | Eliza Weisman <eliza@buoyant.io>, David Barsky <me@davidbarsky.com>, Tokio Contributors <team@tokio.rs> | Not stated |
| `uuid` | `1.28.0` | sqlite-session (runtime) | Ashley Mannix<ashleymannix@live.com.au>, Dylan DPC<dylan.dpc@gmail.com>, Hunar Roop Kahlon<hunar.roop@gmail.com> | Not stated |
| `x25519-dalek` | `2.0.1` | murmur-replication (runtime) | Isis Lovecruft <isis@patternsinthevoid.net>, DebugSteven <debugsteven@gmail.com>, Henry de Valence <hdevalence@hdevalence.ca> | Not stated |
| `x509-parser` | `0.18.1` | murmur-replication (runtime) | Pierre Chifflier <chifflier@wzdftpd.net> | Not stated |
| `zeroize` | `1.9.1` | murmur-replication (runtime), sqlite-session (runtime) | The RustCrypto Project Developers | Not stated |
| `zstd` | `0.13.3` | murmur-replication (runtime), sqlite-session (runtime) | Alexandre Bury <alexandre.bury@gmail.com> | Not stated |

Default builds enable the existing `sqlite-vec` dependency through the `vector`
feature; `--no-default-features` opts out. Rusqlite also enables column origin
metadata to describe vector search results consistently over the wire. These
feature changes add no new dependency packages or Go module requirements.

## Transitive dependencies

These crates are in the lockfile but are not declared directly by any Murmur package manifest.

| Crate | Locked version | Author(s) / organization stated by crate | Country of origin |
|---|---:|---|---|
| `adler2` | `2.0.1` | Jonas Schievink <jonasschievink@gmail.com>, oyvindln <oyvindln@users.noreply.github.com> | Not stated |
| `aead` | `0.5.2` | RustCrypto Developers | Not stated |
| `aes` | `0.8.4` | RustCrypto Developers | Not stated |
| `aho-corasick` | `1.1.5` | Andrew Gallant <jamslam@gmail.com> | Not stated |
| `android_system_properties` | `0.1.6` | Nicolas Silva <nical@fastmail.com> | Not stated |
| `array-init` | `2.1.0` | Manish Goregaokar <manishsmail@gmail.com>, Michal 'vorner' Vaner <vorner@vorner.cz> | Not stated |
| `arrayvec` | `0.7.8` | bluss | Not stated |
| `asn1-rs` | `0.7.2` | Pierre Chifflier <chifflier@wzdftpd.net> | Not stated |
| `asn1-rs-derive` | `0.6.0` | Pierre Chifflier <chifflier@wzdftpd.net> | Not stated |
| `asn1-rs-impl` | `0.2.0` | Pierre Chifflier <chifflier@wzdftpd.net> | Not stated |
| `atomic-polyfill` | `1.0.3` | Dario Nieuwenhuis <dirbaio@dirbaio.net> | Not stated |
| `atomic-waker` | `1.1.2` | Stjepan Glavina <stjepang@gmail.com>, Contributors to futures-rs | Not stated |
| `autocfg` | `1.5.1` | Josh Stone <cuviper@gmail.com> | Not stated |
| `base64ct` | `1.8.3` | RustCrypto Developers | Not stated |
| `bcder` | `0.7.7` | NLnet Labs <rust-team@nlnetlabs.nl> | Not stated |
| `bindgen` | `0.72.1` | Jyun-Yan You <jyyou.tw@gmail.com>, Emilio Cobos Álvarez <emilio@crisal.io>, Nick Fitzgerald <fitzgen@gmail.com>, The Servo project developers | Not stated |
| `bitflags` | `2.13.2` | The Rust Project Developers | Not stated |
| `blake2` | `0.10.6` | RustCrypto Developers | Not stated |
| `block-buffer` | `0.10.4, 0.12.1` | RustCrypto Developers | Not stated |
| `borsh` | `1.8.1` | Near Inc <hello@near.org> | Not stated |
| `bumpalo` | `3.20.3` | Nick Fitzgerald <fitzgen@gmail.com> | Not stated |
| `byteorder` | `1.5.0` | Andrew Gallant <jamslam@gmail.com> | Not stated |
| `bytes` | `1.12.1` | Carl Lerche <me@carllerche.com>, Sean McArthur <sean@seanmonstar.com> | Not stated |
| `cc` | `1.8.0` | Not stated in crate metadata | Not stated |
| `cexpr` | `0.6.0` | Jethro Beekman <jethro@jbeekman.nl> | Not stated |
| `cfg-if` | `1.0.5` | Alex Crichton <alex@alexcrichton.com> | Not stated |
| `cfg_aliases` | `0.2.2` | Zicklag <zicklag@katharostech.com> | Not stated |
| `chacha20` | `0.10.2, 0.9.1` | RustCrypto Developers | Not stated |
| `chrono` | `0.4.45` | Not stated in crate metadata | Not stated |
| `cipher` | `0.4.4` | RustCrypto Developers | Not stated |
| `clang-sys` | `1.9.1` | Kyle Mayes <kyle@mayeses.com> | Not stated |
| `cmov` | `0.5.4` | RustCrypto Developers | Not stated |
| `cobs` | `0.3.0` | Allen Welkie <>, James Munns <james@onevariable.com> | Not stated |
| `const-oid` | `0.10.2, 0.9.6` | RustCrypto Developers | Not stated |
| `constant_time_eq` | `0.4.2` | Cesar Eduardo Barros <cesarb@cesarb.eti.br> | Not stated |
| `core-foundation-sys` | `0.8.7` | The Servo Project Developers | Not stated |
| `cpufeatures` | `0.2.17, 0.3.1` | RustCrypto Developers | Not stated |
| `crc32fast` | `1.5.2` | Sam Rijs <srijs@airpost.net>, Alex Crichton <alex@alexcrichton.com> | Not stated |
| `critical-section` | `1.2.0` | Not stated in crate metadata | Not stated |
| `crossbeam-channel` | `0.5.17` | Not stated in crate metadata | Not stated |
| `crossbeam-utils` | `0.8.23` | Not stated in crate metadata | Not stated |
| `crypto-common` | `0.1.7, 0.2.2` | RustCrypto Developers | Not stated |
| `ctr` | `0.9.2` | RustCrypto Developers | Not stated |
| `ctutils` | `0.4.3` | RustCrypto Developers | Not stated |
| `curve25519-dalek` | `4.1.3` | Isis Lovecruft <isis@patternsinthevoid.net>, Henry de Valence <hdevalence@hdevalence.ca> | Not stated |
| `curve25519-dalek-derive` | `0.1.1` | Not stated in crate metadata | Not stated |
| `data-encoding` | `2.11.1` | Not stated in crate metadata | Not stated |
| `der` | `0.7.10` | RustCrypto Developers | Not stated |
| `der-parser` | `10.0.0` | Pierre Chifflier <chifflier@wzdftpd.net> | Not stated |
| `deranged` | `0.5.8, 0.5.9` | Jacob Pratt <jacob@jhpratt.dev> | Not stated |
| `derive-new` | `0.7.0` | Nick Cameron <nrc@ncameron.org> | Not stated |
| `digest` | `0.10.7, 0.11.3` | RustCrypto Developers | Not stated |
| `displaydoc` | `0.2.7` | Jane Lusby <jlusby@yaah.dev> | Not stated |
| `ed25519` | `2.2.3` | RustCrypto Developers | Not stated |
| `either` | `1.19.0` | Not stated in crate metadata | Not stated |
| `embedded-io` | `0.4.0, 0.6.1` | Not stated in crate metadata | Not stated |
| `equivalent` | `1.0.3` | Not stated in crate metadata | Not stated |
| `errno` | `0.3.14` | Chris Wong <lambda.fairy@gmail.com>, Dan Gohman <dev@sunfishcode.online> | Not stated |
| `fallible-iterator` | `0.2.0, 0.3.0` | Steven Fackler <sfackler@gmail.com> | Not stated |
| `fallible-streaming-iterator` | `0.1.9` | Steven Fackler <sfackler@gmail.com> | Not stated |
| `fastrand` | `2.5.0` | Stjepan Glavina <stjepang@gmail.com> | Not stated |
| `fiat-crypto` | `0.2.9` | Fiat Crypto library authors <jgross@mit.edu> | Not stated |
| `find-msvc-tools` | `0.1.14` | Not stated in crate metadata | Not stated |
| `foldhash` | `0.2.0` | Orson Peters <orsonpeters@gmail.com> | Not stated |
| `form_urlencoded` | `1.2.2` | The rust-url developers | Not stated |
| `futures-channel` | `0.3.34` | Not stated in crate metadata | Not stated |
| `futures-core` | `0.3.34` | Not stated in crate metadata | Not stated |
| `futures-executor` | `0.3.34` | Not stated in crate metadata | Not stated |
| `futures-io` | `0.3.34` | Not stated in crate metadata | Not stated |
| `futures-macro` | `0.3.34` | Not stated in crate metadata | Not stated |
| `futures-sink` | `0.3.34` | Not stated in crate metadata | Not stated |
| `futures-task` | `0.3.34` | Not stated in crate metadata | Not stated |
| `futures-util` | `0.3.34` | Not stated in crate metadata | Not stated |
| `generic-array` | `0.14.7` | Bartłomiej Kamiński <fizyk20@gmail.com>, Aaron Trent <novacrazy@gmail.com> | Not stated |
| `getrandom` | `0.2.17, 0.4.3` | The Rand Project Developers | Not stated |
| `ghash` | `0.5.1` | RustCrypto Developers | Not stated |
| `glob` | `0.3.4` | The Rust Project Developers | Not stated |
| `hash32` | `0.2.1` | Jorge Aparicio <jorge@japaric.io> | Not stated |
| `hashbrown` | `0.16.1, 0.17.1` | Not stated in crate metadata | Not stated |
| `hashlink` | `0.12.2` | Not stated in crate metadata | Not stated |
| `heapless` | `0.7.17` | Jorge Aparicio <jorge@japaric.io>, Per Lindgren <per.lindgren@ltu.se>, Emil Fresk <emil.fresk@gmail.com> | Not stated |
| `hex` | `0.4.3` | KokaKiwi <kokakiwi@kokakiwi.net> | Not stated |
| `hmac` | `0.12.1, 0.13.0` | RustCrypto Developers | Not stated |
| `http` | `1.5.0` | Alex Crichton <alex@alexcrichton.com>, Carl Lerche <me@carllerche.com>, Sean McArthur <sean@seanmonstar.com> | Not stated |
| `http-body` | `1.1.0` | Carl Lerche <me@carllerche.com>, Lucio Franco <luciofranco14@gmail.com>, Sean McArthur <sean@seanmonstar.com> | Not stated |
| `http-body-util` | `0.1.5` | Carl Lerche <me@carllerche.com>, Lucio Franco <luciofranco14@gmail.com>, Sean McArthur <sean@seanmonstar.com> | Not stated |
| `httparse` | `1.10.1` | Sean McArthur <sean@seanmonstar.com> | Not stated |
| `hybrid-array` | `0.4.15` | RustCrypto Developers | Not stated |
| `hyper` | `1.12.0` | Sean McArthur <sean@seanmonstar.com> | Not stated |
| `hyper-rustls` | `0.27.10` | Not stated in crate metadata | Not stated |
| `hyper-util` | `0.1.21` | Sean McArthur <sean@seanmonstar.com> | Not stated |
| `iana-time-zone` | `0.1.65` | Andrew Straw <strawman@astraw.com>, René Kijewski <rene.kijewski@fu-berlin.de>, Ryan Lopopolo <rjl@hyperbo.la> | Not stated |
| `iana-time-zone-haiku` | `0.1.2` | René Kijewski <crates.io@k6i.de> | Not stated |
| `icu_collections` | `2.3.0` | The ICU4X Project Developers | Not stated |
| `icu_locale_core` | `2.3.0` | The ICU4X Project Developers | Not stated |
| `icu_normalizer` | `2.3.0` | The ICU4X Project Developers | Not stated |
| `icu_normalizer_data` | `2.3.0` | The ICU4X Project Developers | Not stated |
| `icu_properties` | `2.3.0` | The ICU4X Project Developers | Not stated |
| `icu_properties_data` | `2.3.0` | The ICU4X Project Developers | Not stated |
| `icu_provider` | `2.3.1` | The ICU4X Project Developers | Not stated |
| `idna` | `1.1.0` | The rust-url developers | Not stated |
| `idna_adapter` | `1.2.2` | The rust-url developers | Not stated |
| `indexmap` | `2.14.2` | Not stated in crate metadata | Not stated |
| `inout` | `0.1.4` | RustCrypto Developers | Not stated |
| `itertools` | `0.13.0` | bluss | Not stated |
| `itoa` | `1.0.18` | David Tolnay <dtolnay@gmail.com> | Not stated |
| `jobserver` | `0.1.35` | Alex Crichton <alex@alexcrichton.com> | Not stated |
| `js-sys` | `0.3.106` | The wasm-bindgen Developers | Not stated |
| `lazy-regex` | `3.6.2` | Canop <cano.petrole@gmail.com> | Not stated |
| `lazy-regex-proc_macros` | `3.6.2` | Canop <cano.petrole@gmail.com> | Not stated |
| `lazy_static` | `1.5.1` | Marvin Löbel <loebel.marvin@gmail.com> | Not stated |
| `libloading` | `0.8.9` | Simonas Kazlauskas <libloading@kazlauskas.me> | Not stated |
| `libredox` | `0.1.25` | 4lDO2 <4lDO2@protonmail.com> | Not stated |
| `libsqlite3-sys` | `0.38.2` | The rusqlite developers | Not stated |
| `linux-raw-sys` | `0.12.1` | Dan Gohman <dev@sunfishcode.online> | Not stated |
| `litemap` | `0.8.3` | The ICU4X Project Developers | Not stated |
| `lock_api` | `0.4.14` | Amanieu d'Antras <amanieu@gmail.com> | Not stated |
| `log` | `0.4.34` | The Rust Project Developers | Not stated |
| `lru-slab` | `0.1.3` | Benjamin Saunders <ben.e.saunders@gmail.com> | Not stated |
| `md-5` | `0.11.0` | RustCrypto Developers | Not stated |
| `md5` | `0.8.1` | Daniel McKenna <danielmckenna93@gmail.com>, Ivan Ukhov <ivan.ukhov@gmail.com>, Kamal Ahmad <shibe@openmailbox.org>, Konstantin Stepanov <milezv@gmail.com>, Lukas Kalbertodt <lukas.kalbertodt@gmail.com>, Nathan Musoke <nathan.musoke@gmail.com>, Scott Mabin <scott@mabez.dev>, Tony Arcieri <bascule@gmail.com>, Wim de With <register@dewith.io>, Yosef Dinerstein <yosefdi@gmail.com> | Not stated |
| `memchr` | `2.8.3` | Andrew Gallant <jamslam@gmail.com>, bluss | Not stated |
| `minimal-lexical` | `0.2.1` | Alex Huszagh <ahuszagh@gmail.com> | Not stated |
| `miniz_oxide` | `0.9.1` | Frommi <daniil.liferenko@gmail.com>, oyvindln <oyvindln@users.noreply.github.com>, Rich Geldreich richgel99@gmail.com | Not stated |
| `mio` | `1.2.4` | Carl Lerche <me@carllerche.com>, Thomas de Zeeuw <thomasdezeeuw@gmail.com>, Tokio Contributors <team@tokio.rs> | Not stated |
| `nom` | `7.1.3` | contact@geoffroycouprie.com | Not stated |
| `nu-ansi-term` | `0.50.3` | ogham@bsago.me, Ryan Scheel (Havvy) <ryan.havvy@gmail.com>, Josh Triplett <josh@joshtriplett.org>, The Nushell Project Developers | Not stated |
| `num-bigint` | `0.4.8` | The Rust Project Developers | Not stated |
| `num-conv` | `0.2.2` | Jacob Pratt <jacob@jhpratt.dev> | Not stated |
| `num-integer` | `0.1.47` | The Rust Project Developers | Not stated |
| `num-traits` | `0.2.19` | The Rust Project Developers | Not stated |
| `objc2-core-foundation` | `0.3.2` | Not stated in crate metadata | Not stated |
| `objc2-system-configuration` | `0.3.2` | Not stated in crate metadata | Not stated |
| `oid-registry` | `0.8.1` | Pierre Chifflier <chifflier@wzdftpd.net> | Not stated |
| `once_cell` | `1.21.4` | Aleksey Kladov <aleksey.kladov@gmail.com> | Not stated |
| `opaque-debug` | `0.3.1` | RustCrypto Developers | Not stated |
| `parking_lot` | `0.12.5` | Amanieu d'Antras <amanieu@gmail.com> | Not stated |
| `parking_lot_core` | `0.9.12` | Amanieu d'Antras <amanieu@gmail.com> | Not stated |
| `pem` | `3.0.6` | Jonathan Creekmore <jonathan@thecreekmores.org> | Not stated |
| `percent-encoding` | `2.3.2` | The rust-url developers | Not stated |
| `pg_interval` | `0.6.0` | Ryan Piper <piper.ryan235@gmail.com>, Ning Sun <n@sunng.info> | Not stated |
| `phf` | `0.13.1` | Steven Fackler <sfackler@gmail.com> | Not stated |
| `phf_shared` | `0.13.1` | Steven Fackler <sfackler@gmail.com> | Not stated |
| `pin-project-lite` | `0.2.17` | Not stated in crate metadata | Not stated |
| `pkcs8` | `0.10.2` | RustCrypto Developers | Not stated |
| `pkg-config` | `0.3.34` | Alex Crichton <alex@alexcrichton.com> | Not stated |
| `poly1305` | `0.8.0` | RustCrypto Developers | Not stated |
| `polyval` | `0.6.2` | RustCrypto Developers | Not stated |
| `postgres-protocol` | `0.6.12` | Steven Fackler <sfackler@gmail.com> | Not stated |
| `postgres-types` | `0.2.14` | Steven Fackler <sfackler@gmail.com> | Not stated |
| `potential_utf` | `0.1.6` | The ICU4X Project Developers | Not stated |
| `powerfmt` | `0.2.1` | Jacob Pratt <jacob@jhpratt.dev> | Not stated |
| `ppv-lite86` | `0.2.21` | The CryptoCorrosion Contributors | Not stated |
| `proc-macro2` | `1.0.107` | David Tolnay <dtolnay@gmail.com>, Alex Crichton <alex@alexcrichton.com> | Not stated |
| `quinn-proto` | `0.11.19` | Not stated in crate metadata | Not stated |
| `quinn-udp` | `0.5.16` | Not stated in crate metadata | Not stated |
| `quote` | `1.0.47` | David Tolnay <dtolnay@gmail.com> | Not stated |
| `r-efi` | `6.0.0` | Not stated in crate metadata | Not stated |
| `rand_chacha` | `0.3.1` | The Rand Project Developers, The Rust Project Developers, The CryptoCorrosion Contributors | Not stated |
| `rand_core` | `0.10.1, 0.6.4` | The Rand Project Developers | Not stated |
| `rand_pcg` | `0.10.2` | The Rand Project Developers | Not stated |
| `redox_syscall` | `0.5.18` | Jeremy Soller <jackpot51@gmail.com> | Not stated |
| `regex` | `1.13.1` | The Rust Project Developers, Andrew Gallant <jamslam@gmail.com> | Not stated |
| `regex-automata` | `0.4.18` | The Rust Project Developers, Andrew Gallant <jamslam@gmail.com> | Not stated |
| `regex-lite` | `0.1.9` | The Rust Project Developers, Andrew Gallant <jamslam@gmail.com> | Not stated |
| `regex-syntax` | `0.8.11` | The Rust Project Developers, Andrew Gallant <jamslam@gmail.com> | Not stated |
| `ring` | `0.17.14` | Not stated in crate metadata | Not stated |
| `rsqlite-vfs` | `0.1.1` | Not stated in crate metadata | Not stated |
| `rustc-hash` | `2.1.3` | The Rust Project Developers | Not stated |
| `rustc_version` | `0.4.1` | Not stated in crate metadata | Not stated |
| `rusticata-macros` | `4.1.0` | Pierre Chifflier <chifflier@wzdftpd.net> | Not stated |
| `rustix` | `1.1.5` | Dan Gohman <dev@sunfishcode.online>, Jakub Konka <kubkon@jakubkonka.com> | Not stated |
| `rustls-pki-types` | `1.15.1` | Not stated in crate metadata | Not stated |
| `rustls-webpki` | `0.103.15` | Not stated in crate metadata | Not stated |
| `rustversion` | `1.0.23` | David Tolnay <dtolnay@gmail.com> | Not stated |
| `ryu` | `1.0.23` | David Tolnay <dtolnay@gmail.com> | Not stated |
| `scopeguard` | `1.2.0` | bluss | Not stated |
| `semver` | `1.0.28` | David Tolnay <dtolnay@gmail.com> | Not stated |
| `serde_core` | `1.0.229` | Erick Tryzelaar <erick.tryzelaar@gmail.com>, David Tolnay <dtolnay@gmail.com> | Not stated |
| `serde_derive` | `1.0.229` | Erick Tryzelaar <erick.tryzelaar@gmail.com>, David Tolnay <dtolnay@gmail.com> | Not stated |
| `serde_spanned` | `1.1.2` | Not stated in crate metadata | Not stated |
| `serde_urlencoded` | `0.7.1` | Anthony Ramine <n.oxyde@gmail.com> | Not stated |
| `sha1_smol` | `1.0.1` | Armin Ronacher <armin.ronacher@active-4.com> | Not stated |
| `sharded-slab` | `0.1.7` | Eliza Weisman <eliza@buoyant.io> | Not stated |
| `shlex` | `1.3.0, 2.0.1` | comex <comexk@gmail.com>, Fenhl <fenhl@fenhl.net>, Adrian Taylor <adetaylor@chromium.org>, Alex Touchet <alextouchet@outlook.com>, Daniel Parks <dp+git@oxidized.org>, Garrett Berg <googberg@gmail.com> | Not stated |
| `signal-hook-registry` | `1.4.8` | Michal 'vorner' Vaner <vorner@vorner.cz>, Masaki Hara <ackie.h.gmai@gmail.com> | Not stated |
| `signature` | `2.2.0` | RustCrypto Developers | Not stated |
| `simd-adler32` | `0.3.10` | Marvin Countryman <me@maar.vin> | Not stated |
| `siphasher` | `1.0.4` | Frank Denis <github@pureftpd.org> | Not stated |
| `slab` | `0.4.12` | Carl Lerche <me@carllerche.com> | Not stated |
| `smallvec` | `1.16.3` | The Servo Project Developers | Not stated |
| `smol_str` | `0.3.6` | Aleksey Kladov <aleksey.kladov@gmail.com>, Lukas Wirth <lukastw97@gmail.com> | Not stated |
| `socket2` | `0.6.5` | Alex Crichton <alex@alexcrichton.com>, Thomas de Zeeuw <thomasdezeeuw@gmail.com> | Not stated |
| `spin` | `0.9.9` | Mathijs van de Nes <git@mathijs.vd-nes.nl>, John Ericson <git@JohnEricson.me>, Joshua Barretto <joshua.s.barretto@gmail.com> | Not stated |
| `spki` | `0.7.3` | RustCrypto Developers | Not stated |
| `sqlite-wasm-rs` | `0.5.5` | Spxg <unsafe@outlook.es> | Not stated |
| `stable_deref_trait` | `1.2.1` | Robert Grosse <n210241048576@gmail.com> | Not stated |
| `stringprep` | `0.1.5` | Steven Fackler <sfackler@gmail.com> | Not stated |
| `subtle` | `2.6.1` | Isis Lovecruft <isis@patternsinthevoid.net>, Henry de Valence <hdevalence@hdevalence.ca> | Not stated |
| `symlink` | `0.1.0` | Chris Morgan <me@chrismorgan.info> | Not stated |
| `syn` | `2.0.119, 3.0.9` | David Tolnay <dtolnay@gmail.com> | Not stated |
| `sync_wrapper` | `1.0.2` | Actyx AG <developer@actyx.io> | Not stated |
| `synstructure` | `0.13.2, 0.14.0` | Nika Layzell <nika@thelayzells.com> | Not stated |
| `thiserror-impl` | `2.0.21` | David Tolnay <dtolnay@gmail.com> | Not stated |
| `thread_local` | `1.1.10` | Amanieu d'Antras <amanieu@gmail.com> | Not stated |
| `time` | `0.3.55` | Jacob Pratt <open-source@jhpratt.dev>, Time contributors | Not stated |
| `time-core` | `0.1.9` | Jacob Pratt <open-source@jhpratt.dev>, Time contributors | Not stated |
| `time-macros` | `0.2.32` | Jacob Pratt <open-source@jhpratt.dev>, Time contributors | Not stated |
| `tinystr` | `0.8.4` | The ICU4X Project Developers | Not stated |
| `tinyvec` | `1.13.3` | Lokathor <zefria@gmail.com> | Not stated |
| `tokio-macros` | `2.7.2` | Tokio Contributors <team@tokio.rs> | Not stated |
| `tokio-util` | `0.7.20` | Tokio Contributors <team@tokio.rs> | Not stated |
| `toml_datetime` | `0.6.11, 1.1.2+spec-1.1.0` | Not stated in crate metadata | Not stated |
| `toml_parser` | `1.1.5+spec-1.1.0` | Not stated in crate metadata | Not stated |
| `toml_write` | `0.1.2` | Not stated in crate metadata | Not stated |
| `toml_writer` | `1.1.3+spec-1.1.0` | Not stated in crate metadata | Not stated |
| `tower` | `0.5.3` | Tower Maintainers <team@tower-rs.com> | Not stated |
| `tower-http` | `0.6.11` | Tower Maintainers <team@tower-rs.com> | Not stated |
| `tower-layer` | `0.3.3` | Tower Maintainers <team@tower-rs.com> | Not stated |
| `tower-service` | `0.3.3` | Tower Maintainers <team@tower-rs.com> | Not stated |
| `tracing-attributes` | `0.1.31` | Tokio Contributors <team@tokio.rs>, Eliza Weisman <eliza@buoyant.io>, David Barsky <dbarsky@amazon.com> | Not stated |
| `tracing-core` | `0.1.36` | Tokio Contributors <team@tokio.rs> | Not stated |
| `tracing-log` | `0.2.0` | Tokio Contributors <team@tokio.rs> | Not stated |
| `try-lock` | `0.2.5` | Sean McArthur <sean@seanmonstar.com> | Not stated |
| `typenum` | `1.20.1` | Not stated in crate metadata | Not stated |
| `unicode-bidi` | `0.3.18` | The Servo Project Developers | Not stated |
| `unicode-ident` | `1.0.27` | David Tolnay <dtolnay@gmail.com> | Not stated |
| `unicode-normalization` | `0.1.25` | kwantam <kwantam@gmail.com>, Manish Goregaokar <manishsmail@gmail.com> | Not stated |
| `unicode-properties` | `0.1.4` | Charles Lew <crlf0710@gmail.com>, Manish Goregaokar <manishsmail@gmail.com> | Not stated |
| `universal-hash` | `0.5.1` | RustCrypto Developers | Not stated |
| `untrusted` | `0.9.0` | Brian Smith <brian@briansmith.org> | Not stated |
| `url` | `2.5.8` | The rust-url developers | Not stated |
| `utf8_iter` | `1.0.4` | Henri Sivonen <hsivonen@hsivonen.fi> | Not stated |
| `valuable` | `0.1.1` | Not stated in crate metadata | Not stated |
| `vcpkg` | `0.2.15` | Jim McGrath <jimmc2@gmail.com> | Not stated |
| `version_check` | `0.9.5` | Sergio Benitez <sb@sergio.bz> | Not stated |
| `want` | `0.3.2` | Sean McArthur <sean@seanmonstar.com> | Not stated |
| `wasi` | `0.11.1+wasi-snapshot-preview1, 0.14.7+wasi-0.2.4` | The Cranelift Project Developers | Not stated |
| `wasip2` | `1.0.4+wasi-0.2.12` | Not stated in crate metadata | Not stated |
| `wasite` | `1.0.2` | Not stated in crate metadata | Not stated |
| `wasm-bindgen` | `0.2.129` | The wasm-bindgen Developers | Not stated |
| `wasm-bindgen-futures` | `0.4.79` | The wasm-bindgen Developers | Not stated |
| `wasm-bindgen-macro` | `0.2.129` | The wasm-bindgen Developers | Not stated |
| `wasm-bindgen-macro-support` | `0.2.129` | The wasm-bindgen Developers | Not stated |
| `wasm-bindgen-shared` | `0.2.129` | The wasm-bindgen Developers | Not stated |
| `web-sys` | `0.3.106` | The wasm-bindgen Developers | Not stated |
| `web-time` | `1.1.0` | Not stated in crate metadata | Not stated |
| `webpki-roots` | `1.0.9` | Not stated in crate metadata | Not stated |
| `whoami` | `2.1.3` | Not stated in crate metadata | Not stated |
| `winapi` | `0.3.9` | Peter Atashian <retep998@gmail.com> | Not stated |
| `winapi-i686-pc-windows-gnu` | `0.4.0` | Peter Atashian <retep998@gmail.com> | Not stated |
| `winapi-x86_64-pc-windows-gnu` | `0.4.0` | Peter Atashian <retep998@gmail.com> | Not stated |
| `windows-core` | `0.62.2` | Not stated in crate metadata | Not stated |
| `windows-implement` | `0.60.2` | Not stated in crate metadata | Not stated |
| `windows-interface` | `0.59.3` | Not stated in crate metadata | Not stated |
| `windows-link` | `0.2.1` | Not stated in crate metadata | Not stated |
| `windows-result` | `0.4.1` | Not stated in crate metadata | Not stated |
| `windows-strings` | `0.5.1` | Not stated in crate metadata | Not stated |
| `windows-sys` | `0.52.0, 0.61.2` | Not stated in crate metadata | Not stated |
| `windows-targets` | `0.52.6` | Microsoft | Not stated |
| `windows_aarch64_gnullvm` | `0.52.6` | Microsoft | Not stated |
| `windows_aarch64_msvc` | `0.52.6` | Microsoft | Not stated |
| `windows_i686_gnu` | `0.52.6` | Microsoft | Not stated |
| `windows_i686_gnullvm` | `0.52.6` | Microsoft | Not stated |
| `windows_i686_msvc` | `0.52.6` | Microsoft | Not stated |
| `windows_x86_64_gnu` | `0.52.6` | Microsoft | Not stated |
| `windows_x86_64_gnullvm` | `0.52.6` | Microsoft | Not stated |
| `windows_x86_64_msvc` | `0.52.6` | Microsoft | Not stated |
| `winnow` | `0.7.15, 1.0.4` | Not stated in crate metadata | Not stated |
| `wit-bindgen` | `0.57.1` | Alex Crichton <alex@alexcrichton.com> | Not stated |
| `writeable` | `0.6.4` | The ICU4X Project Developers | Not stated |
| `x509-certificate` | `0.25.0` | Gregory Szorc <gregory.szorc@gmail.com> | Not stated |
| `yasna` | `0.5.2` | Masaki Hara <ackie.h.gmai@gmail.com> | Not stated |
| `yoke` | `0.8.3` | Manish Goregaokar <manishsmail@gmail.com> | Not stated |
| `yoke-derive` | `0.8.4` | Manish Goregaokar <manishsmail@gmail.com> | Not stated |
| `zerocopy` | `0.8.62, 0.8.63` | Not stated in crate metadata | Not stated |
| `zerocopy-derive` | `0.8.62, 0.8.63` | Not stated in crate metadata | Not stated |
| `zerofrom` | `0.1.8` | The ICU4X Project Developers | Not stated |
| `zerofrom-derive` | `0.1.8` | Manish Goregaokar <manishsmail@gmail.com> | Not stated |
| `zeroize_derive` | `1.5.0` | The RustCrypto Project Developers | Not stated |
| `zerotrie` | `0.2.5` | The ICU4X Project Developers | Not stated |
| `zerovec` | `0.11.8` | The ICU4X Project Developers | Not stated |
| `zerovec-derive` | `0.11.6` | Manish Goregaokar <manishsmail@gmail.com> | Not stated |
| `zlib-rs` | `0.6.8` | Not stated in crate metadata | Not stated |
| `zmij` | `1.0.23` | David Tolnay <dtolnay@gmail.com> | Not stated |
| `zstd-safe` | `7.3.0` | Alexandre Bury <alexandre.bury@gmail.com> | Not stated |
| `zstd-sys` | `2.1.1+zstd.1.5.7, 2.1.2+zstd.1.5.7` | Alexandre Bury <alexandre.bury@gmail.com> | Not stated |

## Go modules

The Go client (`clients/go`) is pure Go with one direct module requirement; Go module metadata does not declare authors, so origin is marked `Not stated`.

| Module | Version | Direct | Country of origin |
|---|---:|---|---:|
| `github.com/jackc/pgx/v5` | `v5.8.0` | yes | Not stated |
| `github.com/jackc/pgpassfile` | `v1.0.0` | no | Not stated |
| `github.com/jackc/pgservicefile` | `v0.0.0-20240606120523-5a60cdf6a761` | no | Not stated |
| `github.com/jackc/puddle/v2` | `v2.2.2` | no | Not stated |
| `golang.org/x/sync` | `v0.17.0` | no | Not stated |
| `golang.org/x/text` | `v0.29.0` | no | Not stated |

## Python packages

The Python client (`clients/python`) is pure Python with one runtime dependency; Python package metadata is not authoritative for company or country, so origin is marked `Not stated`. `pytest` and `cryptography` are test-only extras (`pip install -e 'clients/python[test]'`); only direct requirements are listed, plus pg8000's runtime requirements.

| Package | Version | Role | Author | Country of origin |
|---|---:|---|---|---:|
| `pg8000` | `>=1.31,<2` (tested 1.31.5) | runtime (direct) | Tony Locke | Not stated |
| `python-dateutil` | transitive of pg8000 | runtime | Gustavo Niemeyer, Paul Ganssle | Not stated |
| `scramp` | transitive of pg8000 | runtime | Tony Locke | Not stated |
| `asn1crypto` | transitive of scramp | runtime | wbond | Not stated |
| `pytest` | `>=7` | test only (direct) | Holger Krekel and contributors | Not stated |
| `cryptography` | `>=42` | test only (direct, test PKI) | Python Cryptographic Authority | Not stated |

## Rust client crates

The Rust client (`clients/rust`) is a standalone crate with its own `clients/rust/Cargo.lock` (152 packages including transitives; run `cargo metadata` there for the full list). Only direct dependencies are listed here; they are not in the root lockfile inventory above. Country is `Not stated` because crate metadata does not provide it.

| Crate | Version | Role | Authors | Country of origin |
|---|---:|---|---|---:|
| `tokio` | `1.53.3` | runtime | Tokio Contributors | Not stated |
| `tokio-postgres` | `0.7.18` | runtime | Steven Fackler | Not stated |
| `tokio-postgres-rustls` | `0.13.0` | runtime | Jasper Hugo | Not stated |
| `rustls` | `0.23.46` | runtime | Not stated in crate metadata | Not stated |
| `rustls-pemfile` | `2.2.0` | runtime | Not stated in crate metadata | Not stated |
| `base64` | `0.22.1` | runtime | Marshall Pierce | Not stated |
| `thiserror` | `2.0.21` | runtime | David Tolnay | Not stated |
| `rcgen` | `=0.13.2` | dev/test | Not stated in crate metadata | Not stated |
| `tempfile` | `3.27.0` | dev/test | Steven Allen, The Rust Project Developers, Ashley Mannix, Jason White | Not stated |
| `ed25519-dalek` | `2.2.0` | dev/test | isis lovecruft, Tony Arcieri, Michael Rosenberg | Not stated |
