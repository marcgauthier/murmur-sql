"""Save, list, download, upload, and restore a backup."""
import io

import servertest
import murmur


def main():
    with servertest.Scratch() as scratch:
        server = scratch.start()
        cfg = murmur.Config(port=server.port, user="admin", password=servertest.PASSWORD, key="example-key")
        with murmur.connect(cfg) as db:
            db.execute("CREATE TABLE notes(id INTEGER PRIMARY KEY, body TEXT)")
            db.execute("INSERT INTO notes VALUES (%s, %s)", (1, "kept"))
            saved = db.backup_save()
            buf = io.BytesIO()
            db.backup_download(saved.name, buf)
            assert len(buf.getvalue()) == saved.size
            db.backup_upload("copy.murmur-backup", io.BytesIO(buf.getvalue()))
            assert "copy.murmur-backup" in [f.name for f in db.backup_list()]
            db.execute("DELETE FROM notes")
            db.restore(saved.name, cfg.key)
        with murmur.connect(cfg) as db:
            assert db.execute("SELECT body FROM notes") == [["kept"]]
        print("backup: verified save, transfer, and restore")


if __name__ == "__main__":
    main()
