# Three-way healing through live SQL

Four live Murmur-SQL processes split into isolated pairs. Each pair performs
its own SQL inserts and conflicting updates to shared baseline rows. Exact row
counts and differing row values prove the partition held. Links heal one at a
time in two different orders, with SQL peer/row checks after each link; all
nodes must converge to the same exact rows and the same later conflict winner.

`MURMUR_THREE_WAY_HEAL_ORDER` defaults to `0,1,2,3` and
`MURMUR_THREE_WAY_HEAL_ORDER_B` to `3,2,1,0`. Both must be permutations of the
four cross-pair links. Database work and diagnostics use SQL through
the `goharness` SQL client; process and peer fault controls belong to the live harness.
No embedded database is linked or opened by this scenario.

```sh
tests-live/run.sh three-way-heal
```

Use prebuilt replication-enabled server/admin binaries through
`MURMUR_SERVER_BIN` / `MURMUR_ADMIN_BIN`, or let the runner build them.
Failures preserve runtime artifacts. Passing a compile check does not replace
executing both healing orders against live servers.
