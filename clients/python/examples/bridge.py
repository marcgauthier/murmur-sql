"""High-receiver ownership release without exchanging bridge bundles."""
import murmur
import servertest
from pathlib import Path


def main():
    with servertest.Scratch() as scratch:
        options = servertest.mesh_options([scratch.dir])[0]
        directory = str(Path(scratch.dir) / "staging").replace("\\", "/")
        options["extra_toml"] += (
            f'[replication.bridge]\nrole = "high-receiver"\ndomain = "{options["database_id"]}"\n'
            f'trusted_domains = ["{"11" * 16}"]\ndirectory = "{directory}"\n'
            f'stream = "test-stream"\nrecipient = "{"ee" * 32}"\nreceiver_secret_hex = "{"dd" * 32}"\n')
        server = scratch.start(**options)
        with murmur.connect(murmur.Config(
                port=server.port, user="admin", password=servertest.PASSWORD, key="bridge-key")) as db:
            db.execute("CREATE TABLE notes(name TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL)")
            db.execute("INSERT INTO notes VALUES(%s,%s)", ("r1", "v1"))
            db.flush()
            assert db.bridge_release("notes", "r1", "title") is True
            for table, column in [("notes", "title"), ("notes", None), ("missing", None)]:
                try:
                    db.bridge_release(table, "r1", column)
                except Exception as err:
                    assert murmur.code_of(err) == "22023"
                else:
                    raise AssertionError("invalid release accepted")


if __name__ == "__main__":
    main()
