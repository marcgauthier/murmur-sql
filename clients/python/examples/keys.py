"""Rotate the master key and verify a new session with preserved data."""
import murmur
import servertest


def main():
    with servertest.Scratch() as scratch:
        server = scratch.start()
        cfg = murmur.Config(port=server.port, user="admin", password=servertest.PASSWORD, key="old-key")
        with murmur.connect(cfg) as db:
            db.execute("CREATE TABLE records(id INTEGER PRIMARY KEY, value TEXT)")
            db.execute("INSERT INTO records VALUES(%s,%s)", (1, "kept"))
            db.rotate_key(cfg.key, "new-key")
        try:
            stale = murmur.connect(cfg)
        except Exception as err:
            assert murmur.code_of(err) == "28000"
        else:
            stale.close()
            raise AssertionError("old key still works")
        cfg.key = "new-key"
        with murmur.connect(cfg) as db:
            assert db.execute("SELECT value FROM records") == [["kept"]]


if __name__ == "__main__":
    main()
