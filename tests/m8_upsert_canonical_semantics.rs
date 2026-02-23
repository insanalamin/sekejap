use sekejap::SekejapDB;
use serde_json::json;
use tempfile::tempdir;

fn make_db(capacity: usize) -> (SekejapDB, tempfile::TempDir) {
    let dir = tempdir().expect("tempdir");
    let db = SekejapDB::new(dir.path(), capacity).expect("db init");
    (db, dir)
}

#[test]
fn upsert_keeps_single_logical_row_in_collection() {
    let (db, _dir) = make_db(128);

    db.nodes()
        .put_json(
            &json!({
                "_id":"blog/post-1",
                "title":"Hello",
                "status":"draft"
            })
            .to_string(),
        )
        .expect("insert v1");

    db.nodes()
        .put_json(
            &json!({
                "_id":"blog/post-1",
                "title":"Hello Updated",
                "status":"published"
            })
            .to_string(),
        )
        .expect("upsert v2");

    let rows = db
        .nodes()
        .collection("blog")
        .collect()
        .expect("collection collect")
        .data;
    assert_eq!(rows.len(), 1, "collection must expose one logical row");

    let payload = rows[0].payload.as_ref().expect("payload");
    let payload_json: serde_json::Value =
        serde_json::from_str(payload).expect("payload json decode");
    assert_eq!(payload_json["title"], "Hello Updated");
    assert_eq!(payload_json["status"], "published");

    let count = db
        .nodes()
        .collection("blog")
        .count()
        .expect("collection count")
        .data;
    assert_eq!(count, 1, "count must also be canonical");
}

#[test]
fn upsert_rewrites_hash_index_membership() {
    let (db, _dir) = make_db(128);

    db.schema()
        .define(
            "jobs",
            r#"{
        "hot_fields": {
            "hash_index": ["status"],
            "range_index": [],
            "vector": [],
            "spatial": [],
            "fulltext": []
        }
    }"#,
        )
        .expect("define schema");

    db.nodes()
        .put_json(
            &json!({
                "_id":"jobs/job-1",
                "status":"pending"
            })
            .to_string(),
        )
        .expect("insert pending");

    db.nodes()
        .put_json(
            &json!({
                "_id":"jobs/job-1",
                "status":"done"
            })
            .to_string(),
        )
        .expect("upsert done");

    let pending = db
        .nodes()
        .all()
        .where_eq("status", json!("pending"))
        .count()
        .expect("pending query")
        .data;
    assert_eq!(pending, 0, "old hash-index membership must be removed");

    let done = db
        .nodes()
        .all()
        .where_eq("status", json!("done"))
        .count()
        .expect("done query")
        .data;
    assert_eq!(done, 1, "new hash-index membership must be present");
}
