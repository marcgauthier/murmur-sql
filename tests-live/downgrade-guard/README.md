# Downgrade guard through live SQL

The test writes rows with SQL through the `goharness` SQL client, flushes them, and stops
the Murmur-SQL server. It copies the durable directory, then launches an
engine-owned artifact injector to mark only that copy with the next unsupported
format. A live server must reject the copied store during SQL unlock without
changing any file; the pristine directory must still reopen with identical rows.

The live test and its harness do not link the engine or Spool and do not copy
identity schemas, codecs, compression, or persistence implementations. The
injector is the explicitly ignored engine unit test
`storage::live_sql_format_fixture::write_future_format_to_copied_store` in the
sibling `../MURMUR` checkout. It reuses that engine's actual identity and codec,
and refuses to alter a directory whose final component is not `future-data`.
The test supplies an explicit disposable master key established through SQL key
rotation; it does not derive keys or implement crypto in the harness.

Run from the repository root:

```sh
tests-live/run.sh downgrade-guard
```

The runner builds the engine test utility before running the live scenario,
then exports `MURMUR_FORMAT_FIXTURE_BIN`. Set that variable to reuse a compatible
prebuilt engine unit-test executable. The injector opens only a stopped,
disposable artifact copy; all application writes and recovery assertions run
through SQL against live Murmur-SQL processes. A compile check alone cannot
validate the fail-closed recovery behavior.
