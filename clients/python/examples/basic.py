"""Connect, unlock, and run CRUD with parameters."""
import servertest
import murmur


def main():
    with servertest.Scratch() as scratch:
        server = scratch.start()
        with murmur.connect(
            murmur.Config(port=server.port, user="admin", password=servertest.PASSWORD, key="example-key")
        ) as db:
            db.execute("CREATE TABLE greetings(id INTEGER PRIMARY KEY, lang TEXT NOT NULL, text TEXT NOT NULL)")
            for i, (lang, text) in enumerate([("en", "hello"), ("fr", "bonjour"), ("es", "hola")], 1):
                db.execute("INSERT INTO greetings VALUES (%s, %s, %s)", (i, lang, text))
            rows = db.execute("SELECT lang, text FROM greetings WHERE id >= %s ORDER BY id", (1,))
            for lang, text in rows:
                print(f"{lang}: {text}")
            assert len(rows) == 3, rows
            print("basic: verified 3 rows")


if __name__ == "__main__":
    main()
