# Vendored zstd: DataDog Go driver + Meta C library

This directory is a vendored copy of the
[DataDog zstd](https://github.com/DataDog/zstd) Go driver at v1.5.7,
which consists of:

- The DataDog Go driver (`*.go`, Simplified BSD License, see `LICENSE`).
  Only non-test driver files are vendored.
- The Meta zstandard C library 1.5.7 (`*.c`, `*.h`, `*.S`, BSD License,
  see `ZSTD_LICENSE`), compiled in by cgo.

The root module points at this copy with a `replace` directive, so all
builds use this exact C code with no system libzstd, pkg-config, or
build tags required. Refresh by copying a newer DataDog release's
driver plus its bundled C over this directory and updating the version
below.

Vendored from: github.com/DataDog/zstd v1.5.7 (C: facebook/zstd 1.5.7).
