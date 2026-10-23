# merge-policies (Go port)

Rust source: `/home/marc/MURMUR/tests-live/merge-policies/`
(`merge_policies.rs`: `disconnected_policies_forward_and_restart`).

## What it proves

Three in-process nodes share one typed row (`live_merge_policies`, TEXT
primary key, PN_COUNTER / OR_SET / MAX / MIN policy columns). While
disconnected, node 0 writes counter +10 / set {shared, red} / extrema
(10, 30), node 1 writes counter +7 / unobserved remove of "shared" / set
{blue} / extrema (20, 15), node 2 writes counter -2 / set {green} /
extrema (5, -7). After nodes 0+1 connect both converge counter 17; node 0
stops and node 1 forwards to node 2, converging counter 15, set {blue,
green, red, shared} (the unobserved remove erased nothing), peak 20,
floor -7 on nodes 1+2; node 2 restarts and recovers counter 15 and the
full projection; a later observed remove of "shared" from node 1
propagates to node 2 ({blue, green, red}, extrema unchanged).

## Env knobs

Standard harness knobs only (`MURMUR_LIVE_RUNTIME`,
`MURMUR_LIVE_FAILURES`, `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
`MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`). Convergence waits are 30 s with
50 ms polls, matching the original timeouts.

## Deviations (inherited from Rust)

- Plain SQL plus policy ops (`CounterAdd`, `SetAdd`/`SetRemove`, `Min`,
  `Max`) instead of the managed typed HTTP API.
- Merge policies bind at open through the harness
  `ReplicationOptions.MergePolicies` overlay.
- The node 2 restart rebinds a fresh ephemeral port, so the 1 <-> 2 edges
  are re-added explicitly after `StartNode`.
