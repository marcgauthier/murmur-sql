"""JSON text, bound input, updates, arrays, validation and JSONB."""
import json
import murmur
import servertest


def main():
    with servertest.Scratch() as scratch:
        server = scratch.start()
        with murmur.connect(murmur.Config(
                port=server.port, user="admin", password=servertest.PASSWORD, key="json-key")) as db:
            db.execute("CREATE TABLE documents(id INTEGER PRIMARY KEY, body TEXT)")
            payload = json.dumps({"name": "Ada", "tags": ["rust", "sql"]})
            db.execute("INSERT INTO documents VALUES(%s,%s)", (1, payload))
            assert db.execute("SELECT json_extract(body,'$.name') FROM documents") == [["Ada"]]
            db.execute("UPDATE documents SET body=json_set(body,'$.name',%s)", ("Grace",))
            assert db.execute("SELECT body ->> '$.name' FROM documents") == [["Grace"]]
            assert db.execute("SELECT value FROM documents,json_each(body,'$.tags') ORDER BY value") == [["rust"], ["sql"]]
            assert db.execute("SELECT json_valid(body),json_extract(jsonb(body),'$.name') FROM documents") == [[1, "Grace"]]


if __name__ == "__main__":
    main()
