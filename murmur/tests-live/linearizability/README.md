# linearizability (new Go scenario, no Rust original)

## What it proves

A Jepsen-style history checker over one shared cell, asserting exactly
what last-writer-wins replication guarantees (and no more):

- Sequential leg, no faults: single-threaded writes on node 0 are each
  followed by a same-node read that must observe the write
  (read-your-writes; for a sequential history that is linearity), then
  all nodes converge on the final write.
- Concurrent leg, node 2 repeatedly isolated: every recorded read must
  return the initial value or a value from a write that started before
  the read completed — never a phantom (never-written or from-the-future)
  value. After heal all nodes converge on one value from the acknowledged
  write set.

Strict linearizability under partition is NOT a product guarantee (LWW
resolves concurrent writes) and is deliberately not asserted; the
crdt-contention suite already pins LWW winner agreement.

## Pass criteria

- 3-node cluster opens meshed under `tests-live/runtime/linearizability/`.
- 20 sequential writes each read back immediately on the writing node.
- The full concurrent history (2 writers, 2 readers, 6 partition flaps)
  contains zero phantom reads.
- Post-heal convergence on one acknowledged value on all nodes.

## Env knobs

- `MURMUR_LINEARIZABILITY_WRITES` (default 60): concurrent writes per
  writer; each reader performs twice that many reads.
- Harness knobs apply: `MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
  `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
  `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`, `MURMUR_TEST_DEADLINE_SCALE`.

## Deviations from the Rust original

None: new scenario.
