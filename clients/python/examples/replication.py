"""Two-node peer management, retention and every CRDT policy operation."""
import time
import murmur
import servertest


def main():
    with servertest.Scratch() as left, servertest.Scratch() as right:
        policies = ('  merge_policies:\n    "stats.count": "counter"\n'
                    '    "stats.tags": "set"\n    "stats.highest": "max"\n'
                    '    "stats.lowest": "min"\n')
        options = servertest.mesh_options([left.dir, right.dir], policies)
        s0, s1 = left.start(**options[0]), right.start(**options[1])
        def config(server):
            return murmur.Config(port=server.port, user="admin", password=servertest.PASSWORD, key="mesh-key")
        with murmur.connect(config(s0)) as db, murmur.connect(config(s1)) as peer:
            assert db.show_replication().running and not db.show_peers()
            db.peer_add(peer.show_replication().address)
            deadline = time.monotonic() + 30
            while not any(p.node_id == options[1]["node_id"] and p.connected for p in db.show_peers()):
                assert time.monotonic() < deadline, "peer never connected"
                time.sleep(0.1)
            db.peer_sync()
            assert db.show_replication().peers >= 1
            assert db.retention_collect() >= 0
            assert all(len(r.node_id) == 32 for r in db.show_retention())
            db.execute("CREATE TABLE stats(key TEXT NOT NULL PRIMARY KEY, count INTEGER NOT NULL, tags BLOB, highest INTEGER NOT NULL, lowest REAL NOT NULL)")
            db.execute("INSERT INTO stats VALUES(%s,%s,NULL,%s,%s)", ("row'1", 10, 5, 2.5))
            db.policy_counter_add("stats", "count", "row'1", 5)
            db.policy_set_add("stats", "tags", "row'1", "blue'green")
            added = bytes(db.execute("SELECT tags FROM stats")[0][0])
            assert added
            db.policy_set_remove("stats", "tags", "row'1", "blue'green")
            assert bytes(db.execute("SELECT tags FROM stats")[0][0]) != added
            db.policy_max("stats", "highest", "row'1", 42)
            db.policy_max("stats", "highest", "row'1", 1)
            db.policy_min("stats", "lowest", "row'1", -1.5)
            assert db.execute("SELECT count,highest,lowest FROM stats") == [[15, 42, -1.5]]
            try:
                db.policy_counter_add("stats", "count", "missing", 1)
            except Exception as err:
                assert murmur.code_of(err) == "02000"
            else:
                raise AssertionError("missing row accepted")
            db.peer_remove(options[1]["node_id"])
            db.peer_retire(options[1]["node_id"])
            assert any(r.node_id == options[1]["node_id"] and r.retired for r in db.show_retention())
            try:
                db.peer_retire(options[0]["node_id"])
            except Exception as err:
                assert murmur.code_of(err) == "22023"
            else:
                raise AssertionError("self retirement accepted")


if __name__ == "__main__":
    main()
