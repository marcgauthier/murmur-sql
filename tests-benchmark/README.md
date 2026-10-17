# SQLite insert benchmark

Runs the same three single-row autocommit cases: plain SQLite, a table marked
with `crsql_as_crr`, and SQLite with a session attached to record changes.
Each timed insert uses a fresh SQL string instead of a reused prepared
statement. Session teardown is outside the timed section.

From this directory, run:

```sh
cargo run --release -- --duration 30
```

Build the local loadable extension once from this directory with
`sh build-crsqlite.sh`. The CRR case loads `./libcrsqlite.so`. Pass
`--extension PATH_WITHOUT_SUFFIX` to use a different extension file; SQLite
adds the platform suffix.

## Persistence comparison

Run `cargo run --release --bin persistence -- 8` in this directory for eight
seconds per case. This compares plain SQLite, session capture, managed session
capture with collection deferred, and background Spool persistence. The workload
uses repeated updates to 1,000 rows, with 100 updates per transaction. It reports
throughput, transaction p99, extraction pause, final flush, and recovery time.

The managed cases include callback/thread dispatch overhead. Background work uses
shared CPU and memory bandwidth, so this measures its effect rather than assuming
zero overhead. Conflict metadata lives in Spool and batch versions are assigned
at collection time. No second SQLite database is used.

## Database opening profile

`openbench` measures `Database::open` on an existing encrypted database, reports
each startup phase, and checks all recovered SQL values with a digest in primary
key order. Its JSON includes row/table/index counts and SQLite memory/page size.
The digest scan and close happen after the timed/profiled opening window.

Use a copy of a closed database or a consistent Murmur backup. Opening can update
Spool recovery files; do not point this benchmark at the application's live store.
The key file must contain the raw 32-byte application key, not a password or hex.
Supply the same options as the application when comparing its startup. This tool
uses default options without network replication or optional search definitions;
`MURMUR_OPEN_CACHE_BYTES` overrides its Spool cache budget for comparisons.

```sh
cargo build --release --bin openbench
target/release/openbench /tmp/database-copy /private/path/raw-key.bin
```

For CPU sampling, build `openbench` with the symbol/frame-pointer settings below,
preload the benchmark pthread interposer, and set `MURMUR_CPU_PROFILE_OUTPUT`:

```sh
LD_PRELOAD=/tmp/murmur-profile-threads.so CPUPROFILE_PER_THREAD_TIMERS=1 \
  MURMUR_CPU_PROFILE_OUTPUT=/tmp/open.cpu.prof \
  target/cpu-profile/release/openbench /tmp/database-copy /private/path/raw-key.bin
google-pprof --no_strip_temp --text target/cpu-profile/release/openbench /tmp/open.cpu.prof
```

The optional sampler is Linux-only. Release and debug builds must be compared
explicitly; rebuilding a Go executable does not rebuild the Rust archive it links.
See [the measured opening profile](OPENING_PROFILE.md) for the OVERWATCH fixture,
CPU attribution, build diagnosis, and validation limits.

## Replication workload

Run `cargo run --release --bin replbench -- 20 15 100 240` in this directory
for twenty seconds of paced medium load (100 insert+update+select
iterations/s), fifteen seconds of unpaced insert burst (20 rows per
transaction), then catch-up until the replica converges (240s budget). It
reports per-op latency for the medium phase, average inserts/s for the burst,
convergence time, and per-node RAM (SQLite page image) vs on-disk (Spool
segments) sizes with the disk/RAM ratio.

The burst profile explicitly uses 10 ms capture and a 128 MiB queue. The general
capture default is one second. Chunked replication handles oversized logical values
without increasing Spool's physical value limit. The benchmark closes both nodes
and fails with a nonzero exit if catch-up does not converge.

## Process-isolated mesh gate

`meshbench` launches one process and one encrypted database per node. It uses
localhost QUIC with balanced ring bootstrap candidates and writes 256-byte values.
It runs a persistence-only baseline followed by the same offered load with replication.

```sh
cargo run --release --bin meshbench -- 10 1800 1000
cargo run --release --bin meshbench -- 50 1800 1000
cargo run --release --bin meshbench -- 100 1800 1000
cargo run --release --bin meshbench -- 200 1800 1000
cargo run --release --bin meshbench -- 500 1800 1000 --chaos
```

The first argument is node count, the second seconds per phase, the third aggregate
application transactions/s. `--chaos` flushes and kills one receiver process halfway
through replication, then reopens its durable directory and restarts its writer.
`--smoke` skips latency percentile gates and allows 120 seconds for final digest
settling in a short functional run; it still checks
writes, convergence, final digests, acknowledged probes, and shutdown. Smoke output
is explicitly marked and is not a scalability acceptance result.

```sh
cargo run --release --bin meshbench -- 2 4 4 --chaos --smoke
```

Probe timestamps start at application commit, including the one-second capture
window. Every node is queried until each probe is visible while writes continue.
Convergence percentiles use one completion sample per probe: the last node to see
that write. Individual node deliveries are kept separately for diagnosis and do
not satisfy the full-cluster convergence gate.
The driver fails on a 15-second probe timeout, any write error, final digest mismatch,
missing acknowledged probes, or shutdown over five seconds. Non-smoke runs require
convergence p95 <5 s / p99 <15 s and transaction p95 <=120% / p99 <=150% of baseline.

`MURMUR_MESH_ARTIFACTS` selects the output directory; otherwise a unique temporary
directory is retained. `result.json` records configuration, hardware basics, latency,
and full-cluster convergence (tagged `all_nodes_per_probe`). Per-phase
`cluster-convergence.json` and `delivery-convergence.json` retain completion and
individual delivery samples. Older artifacts without this tag pooled individual
node deliveries and cannot establish the full-cluster percentile gate.
Per-phase `nodes.json` records offered/accepted counts, bounded latency
samples, RSS on Linux, Spool disk bytes, and replication diagnostics; failed probes
write `failure.json`. Directories include private test credentials: remove them after
review. Report CPU model, RAM, disk, network, and any shaping separately with results.

Limitations: localhost runs share CPU/disk/network, include no network shaping, and
cannot establish distributed 500-node capacity. The gate currently uses a disjoint
working set and one flushed restart; shared-key contention, long partitions, rejoin
storms, repeated crashes, explicit buffer peaks, CPU and bandwidth measurements,
and a >512 MiB snapshot scenario remain separate stress runs. A 30-minute duration
is required for sustained acceptance; short runs only validate the harness.

## Shared-host resource profiling (Linux)

Foreground calls and Spool diagnostic index accounting have separate ignored
component benchmarks, run from this directory:

```sh
cargo test --release --manifest-path ../Cargo.toml --features replication \
  --lib foreground_call_index_scaling_benchmark -- --ignored --nocapture
cargo test --release --manifest-path ../spool/Cargo.toml \
  --lib memory_accounting_benchmark -- --ignored --nocapture
cargo test --release --manifest-path ../spool/Cargo.toml \
  --lib prefix_search_benchmark -- --ignored --nocapture
```

The foreground benchmark seeds 1,000, 10,000 and 100,000 records in encrypted
Spool, then measures 1,000 `SELECT 1` database calls at each size. It reports wall
time and process CPU time on 64-bit Linux, excluding seeding and warmup. It does
not run network replication. The accounting benchmark compares the old diagnostic
scan with the maintained estimate; it does not measure application or convergence
latency. The prefix benchmark compares full-shard searches against namespace
hints over 100,000 other records, at 10 and 500 matching origins. It reports
component wall time and candidate shard count; 500 origins is not a 500-node
network benchmark. When comparing source copies, use separate Cargo target directories to
avoid cached artifacts from another copy, and retain binary fingerprints.

`meshbench` uses a 1-second capture interval by default. Set
`MURMUR_MESH_CAPTURE_MS=250` to benchmark a 250 ms interval; `result.json`
records the selected interval. The setting applies to both persistence-only and
replication phases and to restarted child nodes. Positive millisecond values are
required. This benchmark setting does not change the database's default.

For a capture-interval comparison with process CPU and disk sampling:

```sh
python3 tools/profile_mesh.py --binary target/release/meshbench \
  --nodes 10 --seconds 30 --rate 1000 --capture-ms 250 --smoke \
  --out /tmp/murmur-capture-250
```

Repeat with `--capture-ms 1000` in a separate directory. Run sequentially without
other heavy workloads. `--smoke` skips percentile acceptance gates and extends
final settling to 120 seconds; it still checks convergence probes, final data
hashes, and shutdown. Probe latency starts after the local transaction returns,
includes waiting for capture, and ends when every node observes the probe. Incoming
merge preparation can also capture local changes before the periodic deadline, so
the configured interval is not a guaranteed batching delay under active replication.

Build the release benchmark first, then run the profiler without concurrent builds:

```sh
python3 tools/profile_mesh.py --binary target/release/meshbench \
  --nodes 10 --seconds 30 --rate 1000 --device sda
```

Choose the actual physical disk containing the benchmark directory for `--device`.
`--out PATH` selects a directory; otherwise it retains a temporary directory.
The helper needs Python 3 and Linux `/proc`; cgroup v2 pressure/quota data is used
when available. If installed, `iostat` and `pidstat` add native diagnostic logs.
It does not change CPU affinity, scheduling priority, quotas, or kernel settings.

`summary.json` compares node CPU core-equivalents, host CPU busy/I/O wait, cgroup
pressure, sampled aggregate node RSS, and device busy/queue/read/write/flush waits.
`samples.jsonl` preserves one-second observations, selected using the benchmark's
`phase-timing.json` writing windows. Device and host statistics include other
applications; node process counters are sampled separately. Device wait means are
averages of interval means, not whole-run request-weighted latencies. Unsupported
counters are omitted or null. Native logs remain useful if a benchmark gate fails.

A shared-machine run measures resource contention on that machine. CPU saturation
across ten replicas on four cores cannot establish latency on ten separate hosts.
The retained benchmark directory includes private test credentials.

## Replication component CPU sampling (Linux)

Use gperftools `libprofiler` and `google-pprof` to sample native and Rust stacks
without kernel perf permissions. These are optional developer tools. Build a
separate optimized binary with symbols and frame pointers, and the benchmark-only
pthread interposer (set `PROFILER_DIR` to the directory containing libprofiler):

```sh
CARGO_TARGET_DIR=target/cpu-profile CARGO_PROFILE_RELEASE_DEBUG=1 \
  RUSTFLAGS='-C force-frame-pointers=yes' CFLAGS='-fno-omit-frame-pointer' \
  cargo build --release --bin meshbench
cc -shared -fPIC -Wall -Wextra -Werror tools/profile_threads.c \
  "$PROFILER_DIR/libprofiler.so.0" -ldl -Wl,-rpath,"$PROFILER_DIR" \
  -o /tmp/murmur-profile-threads.so
MURMUR_CPU_PROFILE_LIBRARY=/tmp/murmur-profile-threads.so \
  MURMUR_MESH_ARTIFACTS=/tmp/murmur-component-profile \
  target/cpu-profile/release/meshbench 10 15 1000 --smoke
google-pprof --no_strip_temp --text target/cpu-profile/release/meshbench \
  /tmp/murmur-component-profile/replication/node-0.cpu.prof
python3 tools/component_cpu.py --binary target/cpu-profile/release/meshbench \
  --pprof /usr/bin/google-pprof --phase /tmp/murmur-component-profile/replication
```

The driver preloads the interposer only in node processes. Each pthread registers
with the profiler; `CPUPROFILE_PER_THREAD_TIMERS=1` uses thread CPU clocks rather
than wall time or a process signal that can land on an idle thread. Recording starts
on the `run` command and stops after the writer joins, before final flush/settling.
Startup/recovery, final catch-up and snapshot transfers are excluded from this
steady-write recording window; any snapshot work triggered during active writes
is included. Per-node `.cpu.prof` files also exist for the persistence-only
baseline. No sampler or allocator replacement is linked into Murmur itself.

`component_cpu.py` symbolizes each process separately before summing exclusive
categories, writing `cpu-components.json` and retaining per-node `.cpu.stacks`.
Legacy Perl `google-pprof` can use incorrect ASLR mappings when raw profiles from
multiple processes are passed together. Keep `--no_strip_temp` so Rust type names
remain identifiable. Review the helper's classification rules and representative
stacks when changing code: its labels are an attribution aid, not exact counters.

Use mutually exclusive stack categories for a component percentage table; inclusive
caller percentages overlap and must not be added. Allocation/copy costs should be
charged to their closest identifiable caller, with unresolved/runtime stacks kept
visible. Crypto/compression stacks need their caller context to distinguish Spool
from transport and signatures. Inlining can limit that distinction. Sampling adds
overhead and short runs have statistical noise; compare a lower-load run and the
unsampled resource profile. A probe timeout or killed node can leave incomplete
profiles. This profile does not measure cold offline recovery or large transfers;
those need separate recording windows and workloads.

## Large transfer boundary matrix

Run this separately from other tests/builds to avoid CPU pressure affecting session
liveness and the transfer deadline:

```sh
cargo test --release --manifest-path ../Cargo.toml --features replication \
  --test large_transfer large_transfer_boundaries_and_oversized_row_recover -- --ignored
```

The matrix covers 32 MiB aggregate row data, 63 MiB aggregate data near the default
64 MiB signed-payload limit, and one 32 MiB row above Spool's 16 MiB physical limit.
It checks receiver reopen, row counts, lengths, and every byte against zero-filled
input. The local persistence queue is explicitly 128 MiB; wire payload size includes
CRDT/schema overhead and is not equal to application value size.

## Raw in-memory speed

Run `cargo run --release --bin rawbench -- 5` for five seconds per case with
persistence deferred (no replication, no checkpoint during the timed section):
raw rusqlite+session inserts, `Database` single-row inserts, `Database`
20-row batched inserts, point selects by PK, and full scans. Databases are
leaked instead of closed because `close` flushes the whole unflushed session
first, which is pathological after a deferred run.

## Batched reads and outgoing reuse comparison

Run these optimized component benchmarks from this directory:

```sh
cargo test --release --manifest-path ../Cargo.toml --features replication \
  --lib batched_spool_reads_benchmark -- --ignored --nocapture
cargo test --release --manifest-path ../replication/Cargo.toml \
  outgoing_reuse_benchmark -- --ignored --nocapture
```

The first uses the real serialized Spool worker, encrypted/compressed storage,
64 existing 256-byte values, a warmed decoded-block cache, and 500 read rounds.
It compares 32,000 individual worker requests with 500 batches, checking every
returned value. It excludes cold reads, initial persistence and network work.

The second uses a counting in-memory backend, 64 signed 256-byte transactions,
four recipients, and 200 push rounds. Its reference fetches/decodes separately
per recipient and performs queue-size serialization plus wire-frame preparation;
the optimized path reuses decoded transactions and prepared frames within each
round. It verifies identical total wire bytes and 75% fewer logical record reads.
It excludes Spool I/O, sockets, TLS, receiver merges and ACK processing.

Both print elapsed time and, on 64-bit Linux, process CPU nanoseconds measured
with `CLOCK_PROCESS_CPUTIME_ID` across all process threads. Run several repetitions
without concurrent builds and report medians. These component ratios do not predict
full-cluster speedups. The process-isolated mesh gate remains the end-to-end test.

For the measured results and full-cluster limits, see
[SCALABILITY.md](../docs/SCALABILITY.md#batched-spool-reads-and-outgoing-reuse-results).
The three-node smoke comparison showed mixed latency changes despite the component
CPU improvements; no overall cluster speedup or 500-node acceptance is claimed.
