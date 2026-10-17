#[path = "../support/mod.rs"]
mod support;
use murmur_client::Client;

#[tokio::main]
async fn main() {
    let server = support::start_default();
    let db = Client::connect(&support::config(&server, "json-key"))
        .await
        .unwrap();
    db.simple("CREATE TABLE documents(id INTEGER PRIMARY KEY, body TEXT)")
        .await
        .unwrap();
    db.execute(
        "INSERT INTO documents VALUES($1,$2)",
        &[&1i64, &r#"{"name":"Ada","tags":["rust","sql"]}"#],
    )
    .await
    .unwrap();
    let rows = db
        .query(
            "SELECT json_extract(body,'$.name') FROM documents WHERE id=$1",
            &[&1i64],
        )
        .await
        .unwrap();
    assert_eq!(rows[0].get::<_, String>(0), "Ada");
    db.execute(
        "UPDATE documents SET body=json_set(body,'$.name',$1)",
        &[&"Grace"],
    )
    .await
    .unwrap();
    assert_eq!(
        db.simple("SELECT json_extract(body,'$.name') FROM documents")
            .await
            .unwrap()[0][0],
        "Grace"
    );
    assert_eq!(
        db.simple("SELECT value FROM documents,json_each(body,'$.tags') ORDER BY value")
            .await
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        db.simple("SELECT json_valid(body),json_extract(jsonb(body),'$.name') FROM documents")
            .await
            .unwrap()[0],
        ["1", "Grace"]
    );
}
