"""Replication flags, UUIDv5 keys, populated conversions, and flush."""
import servertest
import murmur


def main():
    with servertest.Scratch() as scratch:
        server = scratch.start()
        with murmur.connect(
            murmur.Config(port=server.port, user="admin", password=servertest.PASSWORD, key="example-key")
        ) as db:
            db.execute("CREATE TABLE orders(id INTEGER PRIMARY KEY, v TEXT NOT NULL)")
            assert not {f.table: f.replicated for f in db.table_replication()}["orders"]
            db.set_table_replicated("orders", True)
            first = db.execute(
                "INSERT INTO orders(id, v) VALUES(%s, %s) RETURNING id_murmur_primary_key", (1, "replicated")
            )[0][0]
            assert len(first) == 36 and first[14] == "5", first
            db.set_table_replicated("orders", False)
            db.set_table_replicated("orders", True)
            again = db.execute("SELECT id_murmur_primary_key FROM orders WHERE id = %s", (1,))[0][0]
            assert first == again
            db.flush()
            print("tables: verified flags, UUIDv5 keys, populated conversions and flush")


if __name__ == "__main__":
    main()
