import io
import os

import pytest

import murmur
import servertest


class PartialWriter:
    def __init__(self):
        self.output = bytearray()

    def write(self, value):
        part = value[:3]
        self.output.extend(part)
        return len(part)



@pytest.fixture(scope="module")
def scratch():
    with servertest.Scratch() as s:
        yield s


def cfg(server, **kw):
    return murmur.Config(
        port=server.port, user="admin", password=servertest.PASSWORD, key=kw.pop("key", "test-key"), **kw
    )


@pytest.fixture(scope="module")
def server(scratch):
    return scratch.start()


@pytest.fixture()
def db(server):
    with murmur.connect(cfg(server)) as conn:
        yield conn


def test_validate():
    with pytest.raises(ValueError):
        murmur.connect(murmur.Config(user="", password="p", key="k"))
    with pytest.raises(ValueError):
        murmur.connect(murmur.Config(user="u", password="p", key="k", tls=murmur.TLSConfig(mode="verify-full")))
    with pytest.raises(ValueError):
        murmur.connect(murmur.Config(user="u", password="p", key="k", port=0))


def test_crud_and_transaction_noops(db):
    db.execute("CREATE TABLE greetings(id INTEGER PRIMARY KEY, lang TEXT NOT NULL, text TEXT NOT NULL)")
    for i, (lang, text) in enumerate([("en", "hello"), ("fr", "bonjour"), ("es", "hola")], 1):
        db.execute("INSERT INTO greetings VALUES (%s, %s, %s)", (i, lang, text))
    rows = db.execute("SELECT lang, text FROM greetings WHERE id >= %s ORDER BY id", (1,))
    assert rows == [["en", "hello"], ["fr", "bonjour"], ["es", "hola"]]
    with pytest.raises(Exception) as err:
        db.execute("INSERT INTO greetings VALUES (%s, %s, %s)", (1, "de", "hallo"))
    assert murmur.code_of(err.value) == "23505"
    # BEGIN/COMMIT/ROLLBACK are accepted no-ops: each statement autocommits.
    db.execute("BEGIN")
    db.execute("DELETE FROM greetings WHERE id = %s", (3,))
    db.execute("ROLLBACK")
    assert db.execute("SELECT count(*) FROM greetings") == [[2]]
    db.flush()


def test_wrong_key_and_locked(server, db):
    other = murmur.Config(port=server.port, user="admin", password=servertest.PASSWORD, key="wrong")
    with pytest.raises(Exception) as err:
        murmur.connect(other)
    assert murmur.code_of(err.value) == "28000"


def test_table_replication_and_metrics(db):
    db.execute("CREATE TABLE orders(id INTEGER PRIMARY KEY, v TEXT NOT NULL)")
    flags = {f.table: f.replicated for f in db.table_replication()}
    assert flags["orders"] is False
    db.set_table_replicated("orders", True)
    first = db.execute("INSERT INTO orders(id, v) VALUES(%s, %s) RETURNING id_murmur_primary_key", (1, "x"))[0][0]
    assert len(first) == 36 and first[14] == "5"
    db.set_table_replicated("orders", False)
    db.set_table_replicated("orders", True)
    assert db.execute("SELECT id_murmur_primary_key FROM orders WHERE id = %s", (1,))[0][0] == first
    assert db.metrics() and all(m.subsystem for m in db.metrics("sqlite"))


def test_backup_roundtrip(server, db):
    db.execute("CREATE TABLE b(id INTEGER PRIMARY KEY, v TEXT)")
    db.execute("INSERT INTO b VALUES (1, 'kept')")
    saved = db.backup_save()
    assert saved.size > 0 and saved.name in [f.name for f in db.backup_list()]
    buf = io.BytesIO()
    db.backup_download(saved.name, buf)
    assert len(buf.getvalue()) == saved.size
    partial = PartialWriter()
    db.backup_download(saved.name, partial)
    assert bytes(partial.output) == buf.getvalue()
    class Stalled:
        def write(self, data):
            return 0
    with pytest.raises(BlockingIOError):
        db.backup_download(saved.name, Stalled())
    db.backup_upload("copy.murmur-backup", io.BytesIO(buf.getvalue()))
    assert "copy.murmur-backup" in [f.name for f in db.backup_list()]
    db.execute("DELETE FROM b")
    db.restore(saved.name, "test-key")
    with murmur.connect(cfg(server)) as fresh:
        assert fresh.execute("SELECT v FROM b") == [["kept"]]
    with pytest.raises(ValueError):
        db.backup_download("../evil", io.BytesIO())


def test_files():
    pytest.importorskip("cryptography")
    with servertest.Scratch() as s:
        server = s.start(**servertest.file_options(s.dir))
        with murmur.connect(cfg(server, key="files-key")) as db:
            payload = bytes([0, 1, 255, 128]) * 40000
            info = db.upload_file("reports/sample.bin", io.BytesIO(payload))
            assert info.size == len(payload) and info.local
            found = list(db.search_files("reports/%.bin"))
            assert [f.name for f in found] == ["reports/sample.bin"]
            assert [f.name for f in db.list_files()] == ["reports/sample.bin"]
            out = io.BytesIO()
            db.download_file(info.name, out)
            assert out.getvalue() == payload
            partial = PartialWriter()
            db.download_file(info.name, partial)
            assert bytes(partial.output) == payload
            class Broken:
                def write(self, data):
                    raise OSError("destination failed")
            with pytest.raises(OSError, match="destination failed"):
                db.download_file(info.name, Broken())
            # Cleanup after a failed transfer leaves the connection usable.
            assert db.execute("SELECT 1") == [[1]]


def test_tls():
    pytest.importorskip("cryptography")
    with servertest.Scratch() as s:
        pki = servertest.new_pki()
        server = s.start(tls=pki.write_server_files(s.dir))
        cert, key = pki.write_client_files(s.dir)
        ca = os.path.join(s.dir, "ca.crt")
        for mode in ("require", "verify-ca", "verify-full"):
            c = cfg(server, tls=murmur.TLSConfig(mode=mode, ca_file=ca))
            with murmur.connect(c) as db:
                assert db.execute("SELECT 1") == [[1]]
