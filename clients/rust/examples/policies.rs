#[path = "../support/mod.rs"]
mod support;
use murmur_client::Client;

#[tokio::main]
async fn main() {
    let dir = tempfile::tempdir().unwrap();
    let policies = "merge_policies = { \"stats.count\" = \"counter\", \"stats.tags\" = \"set\", \
        \"stats.highest\" = \"max\", \"stats.lowest\" = \"min\" }\n";
    let opts = support::replication_options(dir.path(), policies);
    let server = support::start(dir, opts);
    let db = Client::connect(&support::config(&server, "policy-key"))
        .await
        .unwrap();
    db.execute(
        "CREATE TABLE stats(key TEXT NOT NULL PRIMARY KEY, count INTEGER NOT NULL, tags BLOB, highest INTEGER NOT NULL, lowest REAL NOT NULL)",
        &[],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO stats VALUES ($1, $2, NULL, $3, $4)",
        &[&"row-1", &10i64, &5i64, &2.5f64],
    )
    .await
    .unwrap();
    db.policy_counter_add("stats", "count", "row-1", 5)
        .await
        .unwrap();
    db.policy_set_add("stats", "tags", "row-1", "blue")
        .await
        .unwrap();
    db.policy_set_remove("stats", "tags", "row-1", "blue")
        .await
        .unwrap();
    db.policy_max("stats", "highest", "row-1", 42i64.into())
        .await
        .unwrap();
    db.policy_max("stats", "highest", "row-1", 1i64.into())
        .await
        .unwrap();
    db.policy_min(
        "stats",
        "lowest",
        "row-1",
        murmur_client::PolicyValue::Real(-1.5),
    )
    .await
    .unwrap();
    let row = db
        .query(
            "SELECT count, highest, lowest FROM stats WHERE key = $1",
            &[&"row-1"],
        )
        .await
        .unwrap();
    assert_eq!(row[0].get::<_, i64>(0), 15);
    assert_eq!(row[0].get::<_, i64>(1), 42);
    assert!((row[0].get::<_, f64>(2) - -1.5).abs() < f64::EPSILON);
    // Counter op on a max column is a policy mismatch; unknown row is 02000.
    assert_eq!(
        db.policy_counter_add("stats", "highest", "row-1", 1)
            .await
            .unwrap_err()
            .code(),
        Some("22023")
    );
    assert_eq!(
        db.policy_counter_add("stats", "count", "missing", 1)
            .await
            .unwrap_err()
            .code(),
        Some("02000")
    );
}
