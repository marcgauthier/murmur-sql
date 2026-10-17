"""Managed FTS/vector declarations, bound queries, refresh and disabling."""
import struct
import murmur
import servertest


def main():
    with servertest.Scratch() as scratch:
        server = scratch.start()
        with murmur.connect(murmur.Config(
                port=server.port, user="admin", password=servertest.PASSWORD, key="search-key")) as db:
            db.execute("CREATE TABLE documents(id INTEGER PRIMARY KEY, body TEXT, embedding BLOB)")
            for key, body, vector in [(1, "hello SQLite", (1, 0)), (2, "other", (0, 1))]:
                db.execute("INSERT INTO documents VALUES(%s,%s,%s)", (key, body, struct.pack("<2f", *vector)))
            db.set_column_fts("documents", "body", True)
            db.set_column_vector("documents", "embedding", murmur.VectorOptions(2))
            db.refresh_indexes()
            fts = next(c for c in db.column_fts() if c.enabled)
            assert fts.status.ready and fts.status.error is None
            ident = '"' + fts.status.index_name.replace('"', '""') + '"'
            assert db.execute(f"SELECT rowid FROM temp.{ident} WHERE {ident} MATCH %s", ("SQLite",)) == [[1]]
            db.execute("UPDATE documents SET body=%s WHERE id=%s", ("updated", 1))
            db.refresh_indexes()
            assert db.execute(f"SELECT rowid FROM temp.{ident} WHERE {ident} MATCH %s", ("updated",)) == [[1]]
            vector = next(c for c in db.column_vectors() if c.options is not None)
            assert vector.options == murmur.VectorOptions(2)
            assert vector.status.ready and vector.status.error is None
            ident = '"' + vector.status.index_name.replace('"', '""') + '"'
            rows = db.execute(
                f"SELECT rowid,distance FROM temp.{ident} WHERE embedding MATCH %s AND k=%s ORDER BY distance",
                ("[1,0]", 2))
            assert rows[0] == [1, 0.0] and rows[1][0] == 2
            assert abs(rows[1][1] - 2**0.5) < 1e-5
            db.set_column_fts("documents", "body", False)
            db.set_column_vector("documents", "embedding", None)
            assert all(c.status is None for c in db.column_fts())
            assert all(c.options is None and c.status is None for c in db.column_vectors())
            db.flush()


if __name__ == "__main__":
    main()
