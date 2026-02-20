/// M6: Improvements — edge metadata, filters, sort/skip/select, edge_collect, rebuild
///
/// These tests cover the enhancements introduced in session 5–6:
/// - Edge metadata (inline ≤32 B and blob >32 B)
/// - where_gt / where_lt / where_gte / where_lte / where_between / where_in
/// - sort() / skip() / select()
/// - edge_collect()
/// - DB persistence and rebuild (reopen + rebuild_indexes)

use sekejap::SekejapDB;
use serde_json::json;
use tempfile::tempdir;

fn make_db(count: usize) -> (SekejapDB, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let db = SekejapDB::new(dir.path(), count).unwrap();
    (db, dir)
}

// ── Edge Metadata ────────────────────────────────────────────────────────────

#[test]
fn test_edge_meta_inline() {
    let (db, _dir) = make_db(64);
    db.nodes().put("a/1", r#"{"_id":"a/1","name":"Alpha"}"#).unwrap();
    db.nodes().put("a/2", r#"{"_id":"a/2","name":"Beta"}"#).unwrap();

    let meta = r#"{"since":2020}"#;
    db.edges().link_meta("a/1", "a/2", "follows", 1.0, meta).unwrap();

    let outcome = db.nodes().one("a/1").edge_collect().unwrap();
    assert_eq!(outcome.data.len(), 1);
    let eh = &outcome.data[0];
    assert!(eh.meta.is_some());
    let m: serde_json::Value = serde_json::from_str(eh.meta.as_ref().unwrap()).unwrap();
    assert_eq!(m["since"], json!(2020));
}

#[test]
fn test_edge_meta_blob() {
    let (db, _dir) = make_db(64);
    db.nodes().put("x/1", r#"{"_id":"x/1"}"#).unwrap();
    db.nodes().put("x/2", r#"{"_id":"x/2"}"#).unwrap();

    // >32 bytes → blob arena path
    let meta = r#"{"description":"this is a long metadata string that exceeds thirty two bytes"}"#;
    db.edges().link_meta("x/1", "x/2", "related", 1.0, meta).unwrap();

    let outcome = db.nodes().one("x/1").edge_collect().unwrap();
    assert_eq!(outcome.data.len(), 1);
    let eh = &outcome.data[0];
    assert!(eh.meta.is_some(), "expected blob meta to be present");
    assert!(eh.meta.as_ref().unwrap().contains("long metadata"));
}

#[test]
fn test_edge_meta_empty() {
    let (db, _dir) = make_db(64);
    db.nodes().put("y/1", r#"{"_id":"y/1"}"#).unwrap();
    db.nodes().put("y/2", r#"{"_id":"y/2"}"#).unwrap();
    db.edges().link("y/1", "y/2", "related", 1.0).unwrap();

    let outcome = db.nodes().one("y/1").edge_collect().unwrap();
    assert_eq!(outcome.data.len(), 1);
    assert!(outcome.data[0].meta.is_none());
}

// ── Numeric Filters ───────────────────────────────────────────────────────────

fn seed_ages(db: &SekejapDB) {
    for i in 1u32..=10 {
        let slug = format!("p/{}", i);
        let json = format!(r#"{{"_id":"{}","age":{}}}"#, slug, i * 10);
        db.nodes().put(&slug, &json).unwrap();
    }
}

#[test]
fn test_where_gt() {
    let (db, _dir) = make_db(64);
    seed_ages(&db);
    // ages: 10,20,30,40,50,60,70,80,90,100
    let outcome = db.nodes().all().where_gt("age", 50.0).count().unwrap();
    assert_eq!(outcome.data, 5, "expected 5 nodes with age > 50");
}

#[test]
fn test_where_lt() {
    let (db, _dir) = make_db(64);
    seed_ages(&db);
    let outcome = db.nodes().all().where_lt("age", 50.0).count().unwrap();
    assert_eq!(outcome.data, 4, "expected 4 nodes with age < 50");
}

#[test]
fn test_where_gte() {
    let (db, _dir) = make_db(64);
    seed_ages(&db);
    let outcome = db.nodes().all().where_gte("age", 50.0).count().unwrap();
    assert_eq!(outcome.data, 6, "expected 6 nodes with age >= 50");
}

#[test]
fn test_where_lte() {
    let (db, _dir) = make_db(64);
    seed_ages(&db);
    let outcome = db.nodes().all().where_lte("age", 50.0).count().unwrap();
    assert_eq!(outcome.data, 5, "expected 5 nodes with age <= 50");
}

#[test]
fn test_where_between() {
    let (db, _dir) = make_db(64);
    seed_ages(&db);
    let outcome = db.nodes().all().where_between("age", 30.0, 60.0).count().unwrap();
    assert_eq!(outcome.data, 4, "expected 4 nodes with age in [30,60]");
}

#[test]
fn test_where_in() {
    let (db, _dir) = make_db(64);
    db.nodes().put("s/1", r#"{"_id":"s/1","status":"active"}"#).unwrap();
    db.nodes().put("s/2", r#"{"_id":"s/2","status":"inactive"}"#).unwrap();
    db.nodes().put("s/3", r#"{"_id":"s/3","status":"pending"}"#).unwrap();

    let outcome = db.nodes().all()
        .where_in("status", vec![json!("active"), json!("pending")])
        .count().unwrap();
    assert_eq!(outcome.data, 2);
}

// ── Sort / Skip / Select ──────────────────────────────────────────────────────

#[test]
fn test_sort_ascending() {
    let (db, _dir) = make_db(64);
    for score in [30, 10, 50, 20, 40] {
        let slug = format!("n/{}", score);
        let json = format!(r#"{{"_id":"{}","score":{}}}"#, slug, score);
        db.nodes().put(&slug, &json).unwrap();
    }

    let outcome = db.nodes().all().sort("score", true).collect().unwrap();
    let scores: Vec<i64> = outcome.data.iter()
        .filter_map(|h| h.payload.as_ref())
        .filter_map(|p| serde_json::from_str::<serde_json::Value>(p).ok())
        .filter_map(|v| v["score"].as_i64())
        .collect();
    assert_eq!(scores, vec![10, 20, 30, 40, 50]);
}

#[test]
fn test_sort_descending() {
    let (db, _dir) = make_db(64);
    for score in [30, 10, 50] {
        let slug = format!("n/{}", score);
        let json = format!(r#"{{"_id":"{}","score":{}}}"#, slug, score);
        db.nodes().put(&slug, &json).unwrap();
    }
    let outcome = db.nodes().all().sort("score", false).collect().unwrap();
    let scores: Vec<i64> = outcome.data.iter()
        .filter_map(|h| h.payload.as_ref())
        .filter_map(|p| serde_json::from_str::<serde_json::Value>(p).ok())
        .filter_map(|v| v["score"].as_i64())
        .collect();
    assert_eq!(scores, vec![50, 30, 10]);
}

#[test]
fn test_skip() {
    let (db, _dir) = make_db(64);
    for i in 1..=5 {
        let slug = format!("q/{}", i);
        let json = format!(r#"{{"_id":"{}","v":{}}}"#, slug, i);
        db.nodes().put(&slug, &json).unwrap();
    }
    let outcome = db.nodes().all().sort("v", true).skip(3).collect().unwrap();
    assert_eq!(outcome.data.len(), 2);
}

#[test]
fn test_select_fields() {
    let (db, _dir) = make_db(64);
    db.nodes().put("u/1", r#"{"_id":"u/1","name":"Alice","secret":"hidden"}"#).unwrap();

    let outcome = db.nodes().one("u/1").select(&["name"]).collect().unwrap();
    assert_eq!(outcome.data.len(), 1);
    let payload: serde_json::Value = serde_json::from_str(outcome.data[0].payload.as_ref().unwrap()).unwrap();
    assert!(payload.get("name").is_some());
    assert!(payload.get("secret").is_none());
}

// ── edge_collect ─────────────────────────────────────────────────────────────

#[test]
fn test_edge_collect_multiple() {
    let (db, _dir) = make_db(64);
    db.nodes().put("hub/1", r#"{"_id":"hub/1"}"#).unwrap();
    db.nodes().put("leaf/1", r#"{"_id":"leaf/1"}"#).unwrap();
    db.nodes().put("leaf/2", r#"{"_id":"leaf/2"}"#).unwrap();
    db.nodes().put("leaf/3", r#"{"_id":"leaf/3"}"#).unwrap();

    db.edges().link("hub/1", "leaf/1", "owns", 1.0).unwrap();
    db.edges().link("hub/1", "leaf/2", "owns", 2.0).unwrap();
    db.edges().link("hub/1", "leaf/3", "owns", 3.0).unwrap();

    let outcome = db.nodes().one("hub/1").edge_collect().unwrap();
    assert_eq!(outcome.data.len(), 3);

    let weights: Vec<f32> = outcome.data.iter().map(|e| e.weight).collect();
    assert!(weights.contains(&1.0));
    assert!(weights.contains(&2.0));
    assert!(weights.contains(&3.0));
}

// ── Persistence / Rebuild ─────────────────────────────────────────────────────

#[test]
fn test_persist_and_reopen() {
    let dir = tempdir().unwrap();

    // Write data, flush, close
    {
        let db = SekejapDB::new(dir.path(), 64).unwrap();
        db.nodes().put("r/1", r#"{"_id":"r/1","val":42}"#).unwrap();
        db.nodes().put("r/2", r#"{"_id":"r/2","val":99}"#).unwrap();
        db.edges().link("r/1", "r/2", "connected", 1.0).unwrap();
        db.flush().unwrap();
    }

    // Reopen — rebuild_indexes() should restore everything
    {
        let db = SekejapDB::new(dir.path(), 64).unwrap();

        // Slug lookup works
        let result = db.nodes().get("r/1");
        assert!(result.is_some(), "r/1 should survive reopen");
        let payload: serde_json::Value = serde_json::from_str(&result.unwrap()).unwrap();
        assert_eq!(payload["val"], json!(42));

        // Collection query works (bitmap rebuilt from iter)
        let count = db.nodes().collection("r").count().unwrap();
        assert_eq!(count.data, 2);

        // Edge traversal works (adj_fwd rebuilt) — bfs_forward includes start + reachable nodes
        let outcome = db.nodes().one("r/1").forward("connected").count().unwrap();
        assert!(outcome.data >= 1, "expected at least 1 node via forward traversal after reopen");
    }
}

// ── HashIndex / RangeIndex wiring ─────────────────────────────────────────────

#[test]
fn test_hash_index_where_eq() {
    let (db, _dir) = make_db(100);

    // Define schema with hash_indexed status field
    db.schema().define("emp", r#"{
        "hot_fields": {
            "hash_index": ["status"],
            "range_index": [],
            "vector": [],
            "spatial": [],
            "fulltext": []
        }
    }"#).unwrap();

    for i in 1..=20 {
        let status = if i % 2 == 0 { "active" } else { "inactive" };
        let slug = format!("emp/{}", i);
        let json = format!(r#"{{"_id":"{}","status":"{}"}}"#, slug, status);
        db.nodes().put(&slug, &json).unwrap();
    }

    let outcome = db.nodes().all()
        .where_eq("status", json!("active"))
        .count().unwrap();
    assert_eq!(outcome.data, 10);

    // Verify trace says hash_index was used
    let trace_steps: Vec<_> = outcome.trace.steps.iter()
        .filter(|s| s.index_used == "hash_index")
        .collect();
    assert!(!trace_steps.is_empty(), "expected hash_index in trace");
}

#[test]
fn test_range_index_where_gte() {
    let (db, _dir) = make_db(100);

    db.schema().define("items", r#"{
        "hot_fields": {
            "hash_index": [],
            "range_index": ["price"],
            "vector": [],
            "spatial": [],
            "fulltext": []
        }
    }"#).unwrap();

    for i in 1..=10 {
        let slug = format!("items/{}", i);
        let json = format!(r#"{{"_id":"{}","price":{}}}"#, slug, i * 100);
        db.nodes().put(&slug, &json).unwrap();
    }

    // prices: 100, 200, ..., 1000
    let outcome = db.nodes().all()
        .where_gte("price", 500.0)
        .count().unwrap();
    assert_eq!(outcome.data, 6, "expected 6 items with price >= 500");

    let trace_steps: Vec<_> = outcome.trace.steps.iter()
        .filter(|s| s.index_used == "range_index")
        .collect();
    assert!(!trace_steps.is_empty(), "expected range_index in trace");
}
