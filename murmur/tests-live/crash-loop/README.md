# crash-loop (Go port)

Rust source: `/home/marc/MURMUR/tests-live/crash-loop/`
(`tests/crash_loop.rs`, `crash_loop_node_converges_without_loss`),
itself a port of the original Go `crash-loop` scenario
(`TestCrashLoopNodeConvergesWithoutLoss`).

## What it proves

Node 3 is restarted N times (`MURMUR_CRASH_LOOP_ROUNDS`, default 5)
under continuous writes from every node, with short uptimes between
restarts (`MURMUR_CRASH_LOOP_UPTIME_MS`, default 1000). Every write
acknowledged before each restart must survive; the looped node must
converge to identical contents; it must reach ready within 60 s after
the final restart (no wedge); and a post-loop write on it must
replicate everywhere.

## Pass criteria (same as Rust)

- Positive control: baseline + full mesh + convergence within 30 s.
- Every non-final restart lands with a write provably in flight on the
  victim (10 s gate); the final round bursts 30 rows on a survivor and
  restarts within 500 ms (mid-replication by construction).
- Final restart reaches ready in under 60 s; mesh reforms (`[2, 2, 2]`
  peers) within 90 s; convergence within 90 s; every acknowledged write
  present everywhere.
- A `post-loop-from-victim` write then converges within 60 s.

## Deviations

None from the Rust port: in-process multi-`Database` cluster, restart
is close + reopen (clean close, not SIGKILL), plain SQL table
`live_typed_records(name)`, writers tick every 25 ms, peer counts from
the replication status, ordered-name equality for convergence.
