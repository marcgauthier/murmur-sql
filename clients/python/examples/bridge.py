"""High-receiver ownership release without exchanging bridge bundles."""
import murmur
import servertest
from pathlib import Path


def main():
    with servertest.Scratch() as scratch:
        options = servertest.mesh_options([scratch.dir])[0]
        directory = str(Path(scratch.dir) / "staging").replace("\\", "/")
        options["extra_yaml"] += (
            '  bridge:\n    role: "high-receiver"\n    domain: "{}"\n'
            '    trusted_domains:\n      - "{}"\n    directory: "{}"\n'
            '    stream: "test-stream"\n    recipient: "{}"\n    receiver_secret_hex: "{}"\n'
        ).format(options["database_id"], "11" * 16, directory, "ee" * 32, "dd" * 32)
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
