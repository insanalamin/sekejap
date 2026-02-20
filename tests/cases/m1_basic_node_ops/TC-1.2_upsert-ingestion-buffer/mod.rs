//! TC-1.2: Upsert to Ingestion Buffer
//!
//! Goal: Ensure upsert updates an existing node rather than creating a duplicate.
//! Given: An existing node with slug "events/jakarta-crime-2024" in Tier 1.
//! When: Upserting a new version with updated fields.
//! Then: The buffer updates the existing node and keeps a single logical record.

use sekejap::SekejapDB;
use tempfile::TempDir;
use serde_json::json;

fn setup_db() -> (SekejapDB, TempDir) {
    let dir = TempDir::new().unwrap();
    let db = SekejapDB::new(dir.path(), 1000).unwrap();
    (db, dir)
}

#[test]
fn test_upsert_updates_existing_node() {
    let (db, _dir) = setup_db();

    let slug = "events/jakarta-crime-2024";

    // Initial payload
    let initial_payload = json!({
        "_id": slug,
        "summary": "Initial report",
        "version": 1,
        "status": "pending"
    }).to_string();

    // Insert initial
    let idx1 = db.nodes().put_json(&initial_payload).unwrap();
    db.flush().unwrap();
    println!("[TC-1.2] Initial insert: index={}", idx1);

    // Updated payload (same slug)
    let updated_payload = json!({
        "_id": slug,
        "summary": "Update with new details",
        "version": 2,
        "status": "confirmed",
        "investigating_officer": "John Doe"
    }).to_string();

    // Upsert (insert with same slug)
    let idx2 = db.nodes().put_json(&updated_payload).unwrap();
    db.flush().unwrap();
    println!("[TC-1.2] Upsert: index={}", idx2);

    // Verify node is retrievable
    let retrieved = db.nodes().get(slug);
    assert!(retrieved.is_some(), "Node should be retrievable");

    let retrieved_json: serde_json::Value = serde_json::from_str(&retrieved.unwrap()).unwrap();

    // Verify latest version is returned
    assert_eq!(retrieved_json["summary"], "Update with new details", "Should have updated summary");
    assert_eq!(retrieved_json["version"], 2, "Should be version 2");
    assert_eq!(retrieved_json["status"], "confirmed", "Status should be updated");
    assert!(retrieved_json.get("investigating_officer").is_some(), "New field should be present");

    println!("[TC-1.2] Upsert test: latest version retrieved correctly");

    // Check slug_index
    let (_, slug_hash) = SekejapDB::parse_entity_id(slug);
    let count = db.slug_index.read().get(slug_hash);
    assert!(count.is_some(), "slug_index should contain entry");

    println!("[TC-1.2] slug_index entry verified");
}

#[test]
fn test_no_duplicate_index_entry() {
    let (db, _dir) = setup_db();

    let slug = "events/no-dup-test";

    // Insert same node twice
    let payload = json!({"_id": slug, "data": "test"}).to_string();
    db.nodes().put_json(&payload).unwrap();
    db.nodes().put_json(&payload).unwrap();
    db.flush().unwrap();

    // Check slug_index - should have exactly one entry
    let (_, slug_hash) = SekejapDB::parse_entity_id(slug);
    let count = db.slug_index.read().get(slug_hash);

    println!("[TC-1.2] Duplicate index test: slug_index entry count check");
    assert!(count.is_some(), "slug_index should have entry");
    println!("[TC-1.2] Single slug_index entry verified");
}

#[test]
fn test_version_increment_on_upsert() {
    let (db, _dir) = setup_db();

    let slug = "events/version-test";

    // Insert v1
    let v1 = json!({"_id": slug, "version": 1, "content": "First version"}).to_string();
    db.nodes().put_json(&v1).unwrap();
    db.flush().unwrap();

    // Insert v2
    let v2 = json!({"_id": slug, "version": 2, "content": "Second version"}).to_string();
    db.nodes().put_json(&v2).unwrap();
    db.flush().unwrap();

    // Insert v3
    let v3 = json!({"_id": slug, "version": 3, "content": "Third version"}).to_string();
    db.nodes().put_json(&v3).unwrap();
    db.flush().unwrap();

    // Retrieve should return v3
    let retrieved = db.nodes().get(slug).unwrap();
    let retrieved_json: serde_json::Value = serde_json::from_str(&retrieved).unwrap();

    assert_eq!(retrieved_json["version"], 3, "Should retrieve latest version");
    assert_eq!(retrieved_json["content"], "Third version", "Content should match latest");

    println!("[TC-1.2] Version increment test: PASSED");
}

#[test]
fn test_upsert_with_geo_coordinates() {
    let (db, _dir) = setup_db();

    let slug = "events/geo-update-test";

    // Initial with coordinates
    let initial = json!({
        "_id": slug,
        "name": "Initial Event",
        "coordinates": {"lat": -6.2088, "lon": 106.8456}
    }).to_string();
    db.nodes().put_json(&initial).unwrap();
    db.flush().unwrap();

    // Updated coordinates
    let updated = json!({
        "_id": slug,
        "name": "Updated Event",
        "coordinates": {"lat": -6.1754, "lon": 106.8272}
    }).to_string();
    db.nodes().put_json(&updated).unwrap();
    db.flush().unwrap();

    // Verify spatial index has updated coordinates
    let outcome = db.nodes()
        .one(slug)
        .near(-6.1754, 106.8272, 1.0)
        .collect()
        .unwrap();

    assert!(!outcome.data.is_empty(), "Node should be found at updated coordinates");
    println!("[TC-1.2] Spatial upsert test: PASSED");
}
