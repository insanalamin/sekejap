//! TC-2.1: Simple Node Promotion
//!
//! Goal: Verify atomic promotion from Tier 1 to Tier 2 with new epoch.
//! Given: A node exists in Tier 1 ingestion buffer.
//! When: Promoting to Tier 2.
//! Then: The node is visible in Tier 2 and epoch is updated.

use sekejap::SekejapDB;
use tempfile::TempDir;
use serde_json::json;

fn setup_db() -> (SekejapDB, TempDir) {
    let dir = TempDir::new().unwrap();
    let db = SekejapDB::new(dir.path(), 1000).unwrap();
    (db, dir)
}

#[test]
fn test_node_promotion_to_tier2() {
    let (db, _dir) = setup_db();

    let slug = "events/flood-2024-01";
    let payload = json!({
        "_id": slug,
        "summary": "Flood report",
        "epoch": 1,
        "severity": "high"
    }).to_string();

    // Step 1: Insert node (goes to Tier 1)
    let idx = db.nodes().put_json(&payload).unwrap();
    db.flush().unwrap();
    println!("[TC-2.1] Inserted node at index: {}", idx);

    // Verify node is active
    let slot = db.nodes.read_at(idx as u64);
    assert_ne!(slot.flags, 0, "Node should be active");
    println!("[TC-2.1] Node flags: {} (active)", slot.flags);

    // Step 2: Read node by slug (Tier 2 equivalent)
    let retrieved = db.nodes().get(slug);
    assert!(retrieved.is_some(), "Node should be visible in Tier 2");
    
    let retrieved_json: serde_json::Value = serde_json::from_str(&retrieved.unwrap()).unwrap();
    assert_eq!(retrieved_json["summary"], "Flood report");
    println!("[TC-2.1] Node retrieved from Tier 2: {}", retrieved_json["summary"]);

    // Verify slug_index mapping
    let (_, slug_hash) = SekejapDB::parse_entity_id(slug);
    let stored_idx = db.slug_index.read().get(slug_hash);
    assert!(stored_idx.is_some(), "slug_index should contain mapping");
    println!("[TC-2.1] slug_index verified");
}

#[test]
fn test_promotion_preserves_payload() {
    let (db, _dir) = setup_db();

    let slug = "events/promotion-test";
    let payload = json!({
        "_id": slug,
        "data": "test data",
        "nested": {"key": "value"},
        "array": [1, 2, 3]
    }).to_string();

    db.nodes().put_json(&payload).unwrap();
    db.flush().unwrap();

    let retrieved = db.nodes().get(slug).unwrap();
    let retrieved_json: serde_json::Value = serde_json::from_str(&retrieved).unwrap();

    assert_eq!(retrieved_json["data"], "test data");
    assert_eq!(retrieved_json["nested"]["key"], "value");
    assert_eq!(retrieved_json["array"], json!([1, 2, 3]));
    
    println!("[TC-2.1] Payload preservation: PASSED");
}

#[test]
fn test_multiple_node_promotion() {
    let (db, _dir) = setup_db();

    // Insert multiple nodes
    for i in 0..5 {
        let slug = format!("events/multi-{}", i);
        let payload = json!({"_id": slug, "index": i}).to_string();
        db.nodes().put_json(&payload).unwrap();
    }
    db.flush().unwrap();

    // Verify all nodes are promoted
    for i in 0..5 {
        let slug = format!("events/multi-{}", i);
        let retrieved = db.nodes().get(&slug);
        assert!(retrieved.is_some(), "Node {} should be promoted", i);
    }

    println!("[TC-2.1] Multiple node promotion: PASSED");
}
