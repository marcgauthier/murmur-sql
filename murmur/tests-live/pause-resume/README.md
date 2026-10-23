# pause-resume (Go port)

Rust source: `/home/marc/MURMUR/tests-live/pause-resume/`
(`pause_resume.rs`, `pause_resume_loses_no_writes`).

## What it proves

Three nodes mesh while all take writes; node 2 is frozen for 15-30 s
while the survivors keep writing, then resumes with zero data loss and
converging digests. The freeze must be *detected* (alive-view dip or
probe failures), the frozen member must not be evicted, and the catch-up
path must be proven (gap-pull needs).

## Pass criteria (same as Rust)

- Full mesh (2 sessions + 2 members excl. self) within 60 s; 5-row
  baseline converges within 30 s.
- Freeze window honors `MURMUR_PAUSE_RESUME_STOP_SECONDS` (default 20,
  15 s floor) with writers flowing (25 ms tick) on the survivors.
- Detection proof: survivors' alive view dips below 2 or probe
  failures advance during the freeze (else the freeze was undetected
  and the test fails); at least one write acks during the freeze.
- After resume: every acknowledged write present everywhere within
  180 s; sessions + membership re-admitted within 90 s; no eviction
  (2 connected peers + full member view on every node).
- Catch-up proven via gap-pull needs delta on the frozen node.
- A `post-resume` write converges within 60 s.
- Env-gated: SKIPs when iptables rules cannot be installed (no
  privilege); teardown never leaks a rule (leak = loud failure).

## Deviations from the original

- SIGSTOP of one in-process node would stop the test driver too, so the
  freeze is a scoped iptables packet blackhole (DROP every UDP packet
  whose source or destination port is the frozen node's replication
  port, inserted at the head of OUTPUT so shadowing ACCEPT rules cannot
  neuter it). From the mesh's view the node goes dark exactly like a
  stopped process; locally it takes no writes, which a frozen node
  cannot do either.
- Plain SQL replaces the typed-records HTTP API
  (`live_typed_records(name)`).
- Peer/member/probe assertions use `Status()` (`Peers`, `Members` excl.
  self, `ProbesFailed`) instead of `/metrics` gauges; there is no
  separate alive gauge (evictions still fail the member view).
- The frozen node is never stopped, so writers hold pre-freeze handles
  (no refresh needed) and resume needs only rule deletion plus an
  explicit re-mesh (no auto-redial).
- With default retention the log still holds everything, so catch-up
  must arrive as gap-pull (`ChunkNeedSent` delta); the snapshot
  counter has no equivalent.
