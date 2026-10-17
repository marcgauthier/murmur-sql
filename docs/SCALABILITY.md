# Scalability implementation and acceptance status

The core changes are implemented, but the 500-node objective is **not validated**.
The scalability acceptance gates must remain open until sustained runs pass.

> [!NOTE]
> Murmur is now a PostgreSQL-wire server (`murmur-server`), not an embedded
> library. The measurements below were taken on the engine and replication crates
> through the benchmark harness, before the server wrapper was added. They remain
> valid for the engine, Spool and replication paths, but do not include wire
> protocol, TLS, authentication, or per-connection overhead. References to
> `Database::call`, `Database` and the Go native library describe the internal
> engine API and a retired cgo binding; the Go client is now a pure-Go
> `database/sql` driver. Server-level load testing is not yet done.

## Implemented

- Incoming row, tombstone, policy, and High/Low merges read affected primary keys
  from Spool. Durable deletion floors preserve stale-delete/reinsert behavior.
  A 10,000-row regression checks that one affected key requires two logical reads
  and produces the same patch as the full-state join oracle.
- A serialized apply worker drains count/byte-bounded batches continuously,
  selects origins round-robin, coalesces durable progress, and yields between batches.
  A regression drains sixteen one-transaction batches without sixteen timer ticks.
  A single transient keyed join is reused across incoming transactions; only touched
  rows are revisited after the initial baseline pass. Prefix-oracle tests preserve
  sequential joins through deletes/reinserts, duplicate rows and all merge policies.
  Incoming gossip envelopes use one atomic staging commit and coalesced clocks/
  watermarks; budget rejection publishes none of the envelope. Snapshot cleanup
  runs once per maintenance tick rather than per applied batch. An origin with
  a sequence gap stops after its first blocked key while other origins continue.
- Large logical values span 64 KiB physical records without increasing Spool's
  physical value limit. Tests cover a 21 MiB replicated capture, reopen, and a
  standalone 17 MiB logical value. Transfer deduplication includes the recipient;
  control queues remain available under data pressure; session reads and writes
  drain independently. Partial chunk manifests survive restart.
- Cancellation bypasses the command queue and interrupts pending SQLite owner
  requests and scheduler admission. A test holds a SQL callback while closing
  replication and requires completion within five seconds. Durable commits finish.
- Capture defaults to one second. Payload decoding and durability waits happen
  outside writer admission; contention triggers a bounded merge reservation so
  continuous local writes cannot starve publication. Incoming collection and final
  publication are both charged to remote writer admission; optimistic preparation
  releases admission between them. Row-only publication proves
  pending SQL keys are disjoint before preserving those writes through FIFO capture;
  overlapping keys, earlier captures, auxiliary updates and policy intents retry.
  Successful publication resets the contention fallback. SQLite publication reuses
  statements across adjacent table rows and borrows cell values for binding rather
  than cloning payloads into another Rust buffer.
- Peer discovery randomizes candidates, reserves outbound capacity, considers
  queue pressure for push selection, and replaces stale application sessions.
- Managed snapshots stream encoded records from an immutable Spool checkpoint.
  Verified receiver batches expose incremental visibility and commit installation
  cursors with their effects. Completion receipts and origin progress wait for all
  batches. Reopen/resume and subprocess snapshot crash tests pass.
- Prefix traversal uses the existing ordered Spool index with bounded pages.
  Retention rotates origin cursors, bounds history reads, and retains serialized
  head/pin rechecks. Spool's existing byte-bounded block cache remains disposable.
- Authentication results use a disposable 1,024-entry FIFO digest/trust-key cache.
  Byte changes, trust revocation/key changes, database and size checks remain enforced.
  Staging/apply reuse canonical transaction bytes and digests rather than serializing
  them again for logs and receipts; durable progress still waits for application.
- Optional adaptive capture/batch counts and incoming service metrics are exposed
  in the engine and are not yet exposed over the wire. Protocol 13 rejects older
  wire versions. Database format 8 upgrades format-6/7 stores on open; earlier
  database formats and downgrades remain unsupported.

## Verification performed

Rust coverage includes replication/convergence, persistence, policies, scheduling,
search, subscriptions, oversized transfers, Spool atomic groups and checkpoints,
plus the ignored subprocess retention/snapshot crash matrices. The retired Go native
binding was exercised at the time with API audit, race and pointer checks.
The latest throughput changes passed 48 replication-crate tests, 28 root library
tests, 20 persistence tests, 19 managed replication tests, four scheduling tests,
two large-transfer tests, and both subprocess crash matrices. New regressions
check atomic envelope staging, conflict rejection, complete budget rejection,
bounded reads across an origin gap, disjoint pending SQL surviving reopen, and
retrying overlapping updates/deletes or already-captured writes. Those Go checks no longer apply; the current Go client is tested against a
scratch `murmur-server` (see `TESTING.md`).
The no-feature build passes with unused-code warnings.

A stale separately-built Rust fixture initially failed direct manual Go runs;
build-script runs select the matching fixture. A persistence fault test had one
transient store-lock failure in an overlapping test run and passed in isolation.
The new Rust usage example compiles with the replication feature.

## Benchmark evidence and remaining gates

`tests-benchmark/src/bin/meshbench.rs` launches isolated node processes, compares
persistence-only and replication workloads, checks sustained probes and final row
hashes, and can flush/kill/reopen a node. Non-smoke runs fail on missed targets.
See `tests-benchmark/README.md` for commands and restrictions.

The earlier convergence numbers below pool individual node deliveries. They do
not establish full-cluster convergence percentiles. The harness now gates on the
last node to observe each probe, records `all_nodes_per_probe` in `result.json`,
and keeps individual delivery samples separately. Every run also requires every
probe to reach all nodes within 15 seconds.

Local functional runs used four available CPUs, localhost networking, one-second
capture, and only seconds of sustained work. Compilation and tests also shared
this machine during these runs; these numbers are not acceptance measurements.

| Functional run | Outcome (individual delivery percentiles) |
|---|---|
| 2 nodes, 4 tx/s, receiver restart | Resumed and converged; final hashes and shutdown passed |
| 10 nodes, 1,000 tx/s, 5 seconds | One run: convergence p95 2.926 s / p99 4.880 s; local latency exceeded the gate |
| 10 nodes, 1,000 tx/s, later 5-second smoke | Final hashes and shutdown passed with extended smoke settling; convergence p95 8.883 s / p99 10.089 s; local p95 7.058 ms versus 0.359 ms baseline |

Two additional 15-second, 10-node runs at 1,000 tx/s ran without concurrent builds
or tests on the same four-CPU host. Both passed final hashes and shutdown and met
individual delivery percentiles; neither passed local latency acceptance:

| Short measurement | Individual delivery p95 / p99 | Local p95 / p99 | Baseline p95 / p99 |
|---|---|---|---|
| Atomic envelope staging, before disjoint publication | 4.566 s / 10.866 s | 6.607 ms / 23.538 ms | 0.247 ms / 0.337 ms |
| FIFO capture preparation and disjoint publication | 3.155 s / 10.738 s | 3.927 ms / 6.186 ms | 0.242 ms / 0.302 ms |

These are short measurements, not 30-minute acceptance runs. Their artifacts are
`/tmp/murmur-mesh-10-1791936452805` and `/tmp/murmur-mesh-10-1791936863058` on
this workspace host. The latter records frequent capture/apply activity (one node
applied 2,360 signed transactions in 138 batches). Reducing forced capture frequency
needs care: remote durable cursors must never acknowledge uncaptured local SQL.
The current implementation retains that durability ordering.

The bounded authentication-cache run used the same isolated 10-node, 15-second,
1,000 tx/s profile. It passed final hashes and shutdown, with convergence p95
2.132 s / p99 13.348 s and local p95 3.592 ms / p99 5.472 ms versus baseline
0.260 ms / 0.450 ms. Local latency acceptance still failed. Its artifacts are
`/tmp/murmur-mesh-10-1791937575708`. Variability in these short runs prevents
attributing all changes in cluster percentiles to the cache.

A release microbenchmark of 10,000 identical 256-byte transactions measured
583.963 ms for repeated signature verification and 31.150 ms for cache lookups,
including canonical encoding and whole-transaction hashing. This measures one
component, not cluster scalability; rerun it with:

```sh
cargo test --release --manifest-path replication/Cargo.toml \
  verification::tests::repeated_authentication_cost -- --ignored --nocapture
```

The release transfer boundary matrix passed 32 MiB aggregate data, 63 MiB data
near the 64 MiB wire limit, and a single 32 MiB row, including byte checks after
receiver reopen. It took 96 seconds for all three cases. A debug 21 MiB transfer
first timed out under overlapping release builds, then passed in isolation.
Authentication tests cover tampering, changed trust/limits, eviction and restart,
including changed durable staging bytes after a cache hit. A startup race in the
maintenance fairness test was fixed by queuing both writers before the first grant.

A transient keyed batch join replaced repeated reconstruction of affected rows.
A release comparison of 20 batches (64 deltas, 512 affected rows with 1 KiB values)
measured 1.012 s for sequential joins and 23.122 ms for indexed batch joins. These
are component costs, not cluster acceptance. The SQLite publisher now borrows
values for binding and reuses statements per adjacent table group. Incoming
collection also consumes remote writer admission, with optimistic preparation
outside that admission.

The first run using full-cluster completion samples had nine probes over a short
10-node, 1,000 tx/s profile. Completion p95/p99 was 5.558 s; local p95/p99 was
4.146/6.340 ms against baseline 0.249/0.342 ms. Both gates failed. Artifacts:
`/tmp/murmur-mesh-10-1791939520273`.

Resource profiling confirms a major shared-host limitation. A 30-second-per-phase
10-node run at 1,000 aggregate tx/s sampled the four-core host without concurrent
builds. Mean readings over the active writing windows were:

| Resource | Persistence-only | Replication |
|---|---:|---:|
| CPU used by all node processes | 0.187 cores | 3.382 cores |
| Whole-host CPU busy | 19.4% | 95.1% |
| CPU pressure (some runnable work waiting) | 1.3% | 82.5% |
| Host I/O wait | 1.0% | 1.7% |
| Physical SSD busy | 1.8% | 36.1% |
| SSD write wait | 0.60 ms | 0.34 ms |
| SSD flush wait | 0.78 ms | 0.35 ms |

The primary observed resource bottleneck is CPU, rather than sustained disk
saturation. Device waits still contribute latency. All ten replicas share four
physical CPUs and one SSD; these results cannot establish separate-host capacity.
The run exceeded the 15-second all-node probe deadline, so it is resource evidence,
not a performance pass. Valid process/device samples are at
`/tmp/murmur-resource-profile-uiwvzw07/summary.json`, with native logs and raw samples.
An initial profiler missed blank `/proc/status` fields; its native logs were valid
but its empty process summary must not be used. The parser was corrected and
checked against live `/proc` data before the reported run.

Component CPU sampling now separates transport, storage, codecs and coordination.
An optimized build with debug symbols/frame pointers preloads optional gperftools
only in benchmark children. A benchmark-only pthread interposer registers every
worker for thread CPU timers. Recording covers active writing, excluding startup
and final flush/settling. Each ASLR process is symbolized separately before samples
are combined; the initial combined legacy-pprof reports were invalid and renamed
`invalid-combined-aslr-*`. Repeat instructions and attribution rules are in
`tests-benchmark/README.md` and `tools/component_cpu.py`.

Ten localhost nodes produced 3,828 samples at 1,000 aggregate tx/s (10 seconds
requested) and 5,752 samples at 300 tx/s (20 seconds requested). Probe completion
can extend the active window. Exclusive estimated CPU shares are:

| Component | 1,000 tx/s | 300 tx/s |
|---|---:|---:|
| Channels, scheduling, unattributed runtime syscalls and thread creation | 23.2% | 30.1% |
| Spool index/cache lookup | 11.7% | 7.8% |
| Spool block/record reads, excluding attributed codecs/crypto | 4.9% | 6.5% |
| Serialization/hashing shared across paths | 15.9% | 10.4% |
| QUIC transport | 4.6% | 5.1% |
| TLS/QUIC packet crypto | 3.5% | 3.6% |
| Wire compression/decompression | 6.7% | 8.2% |
| Spool compression | 1.8% | 2.7% |
| Spool decompression | 1.6% | 1.8% |
| Spool encryption/decryption | 0.3% | 0.6% |
| Transaction authentication/signatures | 3.5% | 3.2% |
| Merge preparation/metadata/publication, excluding separately attributed work | 3.7% | 3.8% |
| Full-index memory metrics | 4.3% | 1.5% |
| SQLite/session capture | 4.0% | 2.2% |
| Spool writes/other persistence | 4.1% | 3.7% |
| Replication orchestration/framing/data gossip | 5.5% | 8.5% |
| Directly attributed membership/SWIM | 0.03% | 0.03% |
| Other/unresolved | 0.6% | 0.3% |

The denominator is all sampled node CPU during replication, including local SQL
and persistence. Categories charge allocation/copy/syscall helpers to the closest
identified component. Shared serialization/hashing and transport costs also serve
merge and membership work; these are not complete end-to-end phase costs. Very
small categories are below reliable short-run resolution. Per-thread timers can
miss short-lived worker activity, and optimized inline frames limit attribution.
CPU timers exclude blocked wall time; channel samples indicate coordination CPU,
not total time waiting for replies. This is a prioritization profile, not precise
accounting or a distributed scalability result.

The 23.2% coordination grouping is not a measurement of message copying alone.
Reinspection of the 1,000 tx/s stacks partitions its 890 samples into park/wait
syscall stacks (318; 8.3% of all samples), wake/notify stacks (257; 6.7%), other
channel stacks (185; 4.8%), other synchronization/runtime (72; 1.9%), and thread
creation (58; 1.5%). Native syscall PCs do not provide kernel instruction-level
attribution. A stack at a wait call must not be interpreted as CPU spent sleeping
or a precisely removable coordination cost. `Adapter::get` creates a reply channel
for each key, queues a closure on the Spool worker, and waits for its reply; this
is a worker RPC with allocation/notification costs, not just a channel buffer write.

A separate sampler sanity check alternated computation and sleeping, measuring
each part with `CLOCK_THREAD_CPUTIME_ID`. Computation was 95.49% of measured thread
CPU and 95.0% of 201 samples; sleeping-call/clock overhead was the remainder. This
checks that the sampler is not simply counting blocked sleep time in that test,
but does not validate every Murmur attribution or quantify an optimization saving.
Source and outputs are retained at `/tmp/murmur-cpu-tools/check_sampling.c`,
`check_sampling.cpu-time.txt` and `check_sampling.samples.txt`. Measure RPC counts,
CPU clocks and wakeups, then compare an implementation change, before asserting
that a particular percentage can be recovered.

Both runs passed final hashes/shutdown but failed latency gates. Node CPU means
were 3.15 and 3.06 cores; host busy was 93.7% and 90.6%, respectively. The lower-rate
run still had 75.2% CPU pressure and does not establish behavior with CPU headroom.
Artifacts: `/tmp/murmur-component-profile-1000/mesh/replication/cpu-components.json`
and `/tmp/murmur-component-profile-300/mesh/replication/cpu-components.json`, with
raw per-node profiles/stacks and resource summaries retained in each parent.

The next measured optimization candidates are batched Spool reads to reduce
single-key worker round trips, fewer repeated prefix/progress enumerations, and
reuse of serialized signed transactions/hashes. Ordinary `Database::call` invokes
`status()` to check `closed`; this computes Spool memory metrics by scanning every
index key. Replace that status collection with a cheap closed-state check while
preserving explicit status reporting. These changes remain outstanding. QUIC/TLS
and SWIM are lower priorities in this workload. Cold offline recovery, reconnect
storms and large transaction/snapshot transfers require separate sampling windows;
warm-cache small-update results must not be extrapolated to those cases.

Earlier scale runs exposed stale-session reconnect suppression and publication
starvation; both have regression fixes. Some runs exceeded the 15-second final
settling deadline. Smoke mode permits 120 seconds for final settling and is explicitly
excluded from performance acceptance; normal runs retain the 15-second deadline.

Still required:

- Pass local latency degradation limits and the healthy sustained convergence gates.
- Run the full 10/50/100/200/500-node matrix for at least 30 minutes per profile on
  appropriately provisioned hardware, with actual network impairment and partitions.
- Add/run shared-key contention, rejoin storms, repeated restarts, exact signed-payload
  limits (the near-limit matrix passes), and snapshots above 512 MiB.
- Measure internal replication buffer peaks, network bandwidth, and connection-degree
  distribution on distributed hosts. Linux shared-host profiling now records process
  CPU, disk activity/pressure, and sampled RSS peaks; it does not establish distributed
  resource consumption or internal buffer peaks.
- Replace snapshot checkpoint index copies with a structurally shared Spool read
  view if profiling demonstrates index-copy pressure. Payload streaming is bounded,
  but checkpoint opening builds a disposable location index proportional to data.
- Add a decoded metadata cache only when profiling justifies it; no separate metadata
  authority or unbounded decoded cache has been introduced.


## Batched Spool reads and outgoing reuse results

Incoming envelope floors/observed progress, apply progress, retained heads,
ACK/progress watermarks and recovery staging reads now use grouped reads on the serialized Spool worker. Prefetch is byte-bounded and
checks sequence gaps before fetching a blocked origin's backlog. The default
backend hooks remain compatible, with explicit ordered/prefix length validation.
Outgoing push rounds read/decode retained transactions once per bounded working
set and share serialized/compressed frames across recipients of identical batches.
Caches are disposable, capped at 4 MiB each/1,024 entries, and discarded after the
round. Lookahead reads have a 1 MiB budget with one oversized first value allowed;
large transfer sends reuse the already loaded data instead of fetching it again.
QUIC encryption and durable ACK processing are unchanged. Older narrow per-peer
lookahead batching was already present in the captured before tree.

Five optimized component repetitions on this host produced these medians:

| Component workload | Before wall | After wall | Speedup | Before process CPU | After process CPU |
|---|---:|---:|---:|---:|---:|
| Encrypted Spool, warmed cache, 500 rounds × 64 keys × 256 bytes | 346.297 ms | 19.675 ms | 17.60× | 341.270 ms | 18.200 ms |
| Send preparation, 200 rounds × 64 signed 256-byte transactions × 4 peers | 231.424 ms | 67.291 ms | 3.44× | 213.363 ms | 66.087 ms |

Worker requests fell from 32,000 to 500 in the Spool case. Logical record reads
fell from 51,200 to 12,800 in the send case. Process CPU, measured across all
threads with `CLOCK_PROCESS_CPUTIME_ID`, fell 94.7% and 69.0%, respectively.
These are component comparisons: the Spool case excludes cold I/O, and the send
case uses a counting in-memory backend and excludes sockets/TLS/receiver merging.
The old and new loops check equal values/wire byte counts. They do not imply
17.6× or 3.44× faster full-cluster replication. Repeat commands and limits are in
`tests-benchmark/README.md`; raw runs and medians are retained at
`/tmp/murmur-batched-results/components.json`.

The initial 10-node/1,000 tx/s trial did not establish a performance improvement:
the before run exceeded a probe deadline, and the after run exceeded the final
digest settling window. One trial overlapped validation compilation, so the pair
is unsuitable for a numerical gain claim. Subsequent matched three-node smoke
runs avoid concurrent agent builds and validate final hashes/shutdown, but remain
short localhost evidence and skip performance acceptance gates. Other host work
can still affect wall time. Sustained multi-host validation remains outstanding.

The optional Rust bulk-read hooks have missing/empty/bounded result checks and
Rust replication/convergence/scheduling coverage. (A Go `BatchBackend` binding
existed when this was written and has been removed with the cgo client.)

Three preliminary matched three-node runs at 1,000 aggregate application tx/s, ten seconds per
phase, completed final digest/shutdown checks before and after. Medians of the
per-run percentiles were:

| Full-cluster smoke measurement | Before | After |
|---|---:|---:|
| Convergence p95 | 163 ms | 191 ms |
| Convergence p99 | 271 ms | 196 ms |
| Local transaction p95 | 1.296 ms | 1.509 ms |
| Local transaction p99 | 2.546 ms | 3.009 ms |

These preliminary runs preceded the final ACK/recovery grouping and the guard
against rewriting observed heads for duplicate-only origins. The outcome is mixed:
component CPU costs improved considerably, but an overall
replication latency improvement is not established. Convergence p95 ranges overlap
(before 154–211 ms, after 161–213 ms), while p99 varied substantially. Baseline local
latencies also varied between runs. All six runs were marked `smoke_only`; these
are short disjoint-key localhost tests, not acceptance of the latency degradation
or 500-node gates. Artifacts and individual runs are at
`/tmp/murmur-batched-results/mesh3-comparison.json`. The expensive full-index metrics
check on ordinary SQL calls remains unchanged by this two-item implementation.

The final build (including ACK/recovery batches and the duplicate-origin guard)
was then compared against the captured before binary at three nodes, 1,000
aggregate tx/s, 30 seconds per phase, with no concurrent agent build/test:

| Final-build full-cluster smoke result | Before | After |
|---|---:|---:|
| Convergence p95 | 150 ms | 150 ms |
| Convergence p99 | 158 ms | 166 ms |
| Local transaction p95 | 0.803 ms | 0.837 ms |
| Local transaction p99 | 1.311 ms | 1.482 ms |
| Completed full-cluster probes | 227 | 227 |

Both runs passed final data hashes and shutdown. Persistence-only p95 baselines
were 0.279/0.271 ms, respectively. This cleaner pair also does not establish an
end-to-end latency improvement: p95 synchronization was unchanged, p99 and local
latencies rose slightly. Component CPU savings are proven for their workloads;
whole-replication CPU/throughput gains and distributed scalability remain unproven.
The exact final pair and binary fingerprint are retained at
`/tmp/murmur-batched-results/final-cluster-comparison.json` and `final-binary.json`.

## Capture interval comparison: 250 ms versus 1 second

Two release runs per interval used 3 localhost node processes, 1,000 aggregate
application transactions/s, and 30 seconds per persistence-only/replication phase.
Order was 1000/250/250/1000 ms. Medians of run percentiles/CPU means were:

| Metric | 1 second | 250 ms |
|---|---:|---:|
| All-node convergence p95 | 160 ms | 149 ms |
| All-node convergence p99 | 171 ms | 151.5 ms |
| Local transaction p95 | 0.754 ms | 0.757 ms |
| Local transaction p99 | 1.173 ms | 1.166 ms |
| Aggregate replication-phase node CPU | 0.976 cores | 0.972 cores |
| Persistence-only node CPU | 0.170 cores | 0.183 cores |

All four short smoke runs passed final data equality and clean shutdown; percentile
acceptance gates were skipped. Convergence starts after the local probe transaction
returns and ends when every node observes it. Consecutive probes and incoming merge
captures can favor shorter capture waits; these are not general application latency
guarantees. Incoming merges may checkpoint local changes before the periodic timer.
The small differences do not establish a throughput or latency gain beyond run
variation. Persistence-only CPU was modestly higher with the shorter interval.
Both intervals failed the 15-second sustained-load convergence probe deadline in
a separate 10-node/1,000 aggregate tx/s comparison near shared-host CPU saturation.
Reducing capture did not resolve that overload. The product default remains 1 second.

Artifacts: `/tmp/murmur-capture-ab/REPORT.md` and `comparison.json`, with raw
process/device samples and individual run results. Repeat instructions and the
benchmark-only `MURMUR_MESH_CAPTURE_MS` setting are documented in
`tests-benchmark/README.md`; this setting does not alter product defaults.

## Immediate bounded outgoing follow-up experiment

A temporary release build tested up to four outgoing rounds per 100 ms timer tick,
yielding between rounds. Follow-up requires an advancing sent watermark and an
unsent retained range within the peer ACK window. Production scheduling is unchanged.

At 10 localhost nodes, 300 aggregate application tx/s, two runs per scheduler
(30 seconds requested per phase, order before/after/after/before), current scheduling
passed once at 10.231 seconds convergence p95 and timed out once (>15 seconds).
Follow-up passed twice at 7.585 and 7.696 seconds p95, with final equality/shutdown
checks passing. The first successful pair was about 26% faster; only 15–18 probes
completed per successful run, so p95/p99 both selected the maximum. This is an
encouraging experiment, not a robust tail-latency improvement claim. Both follow-up
results exceed the 5-second p95 target. Node CPU remained roughly 3.3 cores.

At 1,000 aggregate tx/s, both schedulers exceeded the 15-second probe deadline.
The experiment did not resolve shared-host CPU saturation. Smoke mode skipped
acceptance gates; no distributed scale or offline recovery claim follows.
Artifacts and the temporary source/binaries: `/tmp/murmur-push-followup/REPORT.md`
and `comparison.json`. No public API/default/configuration change was made.

## Incremental index-memory accounting and lightweight closed checks

Ordinary Database calls and close now inspect the existing closed flag directly,
avoiding full status collection. Spool maintains its existing key-length-plus-128
byte diagnostic estimate incrementally, including tombstones, so metrics no longer
scan every index entry. These are internal changes with unchanged APIs, defaults,
formats and SQL semantics; USAGE/Go inventory/VENDORS need no behavioral update.

Three release foreground benchmark repetitions measured 1,000 real `SELECT 1`
Database calls with increasing encrypted-Spool index sizes (seeding excluded):

| Seeded keys | Before process CPU | After process CPU | Reduction |
|---|---:|---:|---:|
| 1,000 | 19.122 ms | 13.234 ms | 30.8% |
| 10,000 | 51.770 ms | 13.383 ms | 74.1% |
| 100,000 | 1,633.615 ms | 13.314 ms | 99.2% |

Distinct before/after artifacts were verified; initially reused cached output was
discarded. Use isolated Cargo target directories for source-copy comparisons.
These are call-path component savings, not whole-node or replication speed gains.
A fixed-build 10-node/1,000 aggregate tx/s short run still exceeded the 15-second
convergence deadline, at ~3.40 node CPU cores. Both requested five-minute tests
at 10 nodes/300 aggregate tx/s (fixed current versus fixed follow-up scheduling)
also failed the probe deadline early; neither completed five minutes. Follow-up
scheduling remains experimental.

A controlled 3-node, 1,000 aggregate tx/s, 60-second CPU sampling run passed final
equality/shutdown: convergence p95 145 ms/p99 147 ms, ~0.818 node cores. Of 4,327
samples, ~18.4% attributed to Spool index prefix queries, 13.6% to serialization/
hashing, and 11.7% to SQLite/session capture. Index/cache total was 21.9%; actual
decoded-cache get calls were a small part of it. No full-index memory scan samples
remained. This workload differs from earlier 10-node profiles; percentages are not
a direct before/after comparison. Prefix-query caller/frequency measurement is
the next justified CPU investigation.

Full Spool, root replication library, persistence, convergence and writer scheduling
tests passed. Accounting oracle tests cover overwrite/delete/reinsertion, stale
delivery, compaction rewrite/drop and concurrent updates. Benchmark repeat commands
are in tests-benchmark/README.md. Logs and detailed results:
`/tmp/murmur-index-accounting/REPORT.md` and `micro-comparison.json`.

## Prefix lookup and staging admission follow-up

Spool now uses fixed, conservative namespace hints to skip index shards that
cannot contain a requested three-segment namespace. Binary/short prefixes retain
full searches; collisions and stale deletion hints only cause extra searches.
At 100,000 unrelated keys, the isolated lookup benchmark improved 31.25× for
10 origin heads, but only 1.13× for 500 heads where most shards were candidates.
The latter is a component dataset, not a 500-node replication test.

Two before/after three-node runs at 1,000 aggregate transactions/s measured
median aggregate node CPU of 0.753 versus 0.652 cores (13.35% less), with the
same 145 ms convergence p95. Ten-node runs at the same shared-host load still
hit the 15-second probe deadline in repeated runs. Details and limitations are
in `/tmp/murmur-prefix-hints/REPORT.md`.

Murmur also rotates outgoing origin order independently for each peer to prevent
continuously backlogged origins from monopolizing bounded push rounds. This
improves scheduling fairness; a ten-node/300 aggregate tx/s comparison did not
show a latency gain (10.967 versus 14.703 seconds with few probes). The 100 ms
sender timer remains unchanged.

Incoming admission now maintains a checked staged-byte total alongside the
existing disposable staged-key size map, rebuilding it from Spool on restart.
Admission avoids cloning and summing every pending key. Durable commits update
the accounting; applied or purged records subtract their stored sizes. Spool
remains authoritative, and staging limits/APIs/formats are unchanged. Application
SQL is unaffected; USAGE was reviewed and needs no additional behavioral change.
A release component benchmark at 4,096 pending records measured 10,000 total
checks at 5.688 seconds before versus 8 microseconds after; this excludes storage,
authentication and merging and is not an end-to-end speedup. Tests cover totals,
replacement/removal, overflow rejection, restart reconstruction and apply drain.

A subsequent isolated three-node/1,000 aggregate tx/s before/after smoke pair
passed final equality and shutdown: p95 166→165 ms, p99 186→165 ms, aggregate
node CPU 0.616→0.645 cores. This single pair establishes no overall CPU or latency
gain; low staging backlogs may not benefit materially. Builds/tests did not
interrupt the pair. An earlier overlapping verification run was excluded.
Results: `/tmp/murmur-staged-accounting/REPORT.md` and `comparison.json`.
Replication crate: 54 passed, three ignored component benchmarks. Managed
replication: 25 passed; scheduling: four passed; large transfer: two passed,
one ignored boundary matrix. No public exports changed.
