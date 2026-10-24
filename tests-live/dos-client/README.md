# SQL and QUIC connection pressure

Holds authenticated QUIC connections without sending the replication Hello, plus idle/partial PostgreSQL startup connections, while spraying invalid UDP datagrams. Honest Rust clients must continue writing within ten seconds, remote application must progress during the attack, incomplete connections must not become replication peers, and all exact rows must converge after teardown and a post-attack write.

Application operations use the [`goharness` SQL client](../goharness/)
over PostgreSQL wire connections to separate `murmur-server` processes.
No embedded database is opened by these tests.

Run from the repository root:

```sh
MURMUR_ENGINE_INTEGRATION=1 ./tests-live/run.sh dos-client
```

Defaults: MURMUR_DOS_CLIENT_ATTACK_SECONDS=60 and MURMUR_DOS_CLIENT_QUIC_HALFOPEN=24. Both accept positive integers. Expired QUIC half-opens are replaced throughout the window. This bounded loopback pressure test checks protocol admission and continued service; it does not claim internet-scale denial-of-service resistance.

The process harness builds `murmur-server` and `murmur-admin` with replication,
or reuses `MURMUR_SERVER_BIN` and its sibling admin executable
(`MURMUR_ADMIN_BIN` overrides the latter). `MURMUR_LIVE_RUNTIME` selects
the artifact parent. Each test receives a unique runtime directory; passing
tests clean up, while panics retain configuration and server logs.
