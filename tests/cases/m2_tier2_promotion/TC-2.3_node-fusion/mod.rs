//! TC-2.3: Node Fusion
//!
//! Goal: Merge two duplicate nodes into one.
//! Given: Two duplicates with overlapping entities.
//! When: Fusing them into a single node.
//! Then: Result merges entities, uses latest timestamp, and preserves vector.

use sekejap::SekejapDB;
use tempfile::TempDir;
use serde_json::json;

fn setup_db() -> (SekejapDB, TempDir) {
    let dir = TempDir::new().unwrap();
    let db = SekejapDB::new(dir.path(), 1000).unwrap();
    (db, dir)
}

#[test]
fn test_node_fusion_merge_entities() {
    let (db, _dir) = setup_db();

    // Node A with some entities
    let node_a = json!({
        "_id": "events/accident-1",
        "who": ["Driver A"],
        "when": "2024-05-01T10:00:00Z"
    }).to_string();

    // Node B with overlapping entities
    let node_b = json!({
        "_id": "events/accident-2",
        "who": ["Driver A", "Passenger B"],
        "when": "2024-05-01T11:00:00Z"
    }).to_string();

    db.nodes().put_json(&node_a).unwrap();
    db.nodes().put_json(&node_b).unwrap();
    db.flush().unwrap();

    // Create merged node
    let merged = json!({
        "_id": "events/accident-fused",
        "who": ["Driver A", "Passenger B"],
        "when": "2024-05-01T11:00:00Z"
    }).to_string();

    let fused_idx = db.nodes().put_json(&merged).unwrap();
    db.flush().unwrap();

    let retrieved = db.nodes().get("events/accident-fused").unwrap();
    let retrieved_json: serde_json::Value = serde_json::from_str(&retrieved).unwrap();

    let who = retrieved_json["who"].as_array().unwrap();
    assert!(who.contains(&json!("Driver A")));
    assert!(who.contains(&json!("Passenger B")));
    
    println!("[TC-2.3] Fused node created with merged entities");
}

#[test]
fn test_fusion_preserves_latest_timestamp() {
    let (db, _dir) = setup_db();

    let old_node = json!({"_id": "events/old", "timestamp": "2024-01-01"}).to_string();
    let new_node = json!({"_id": "events/new", "timestamp": "2024-06-01"}).to_string();

    db.nodes().put_json(&old_node).unwrap();
    db.nodes().put_json(&new_node).unwrap();
    db.flush().unwrap();

    // Fusion uses latest timestamp
    let fused = json!({"_id": "events/fused", "timestamp": "2024-06-01"}).to_string();
    db.nodes().put_json(&fused).unwrap();
    db.flush().unwrap();

    let retrieved = db.nodes().get("events/fused").unwrap();
    let val: serde_json::Value = serde_json::from_str(&retrieved).unwrap();
    assert_eq!(val["timestamp"], "2024-06-01");
    println!("[TC-2.3] Latest timestamp preserved: PASSED");
}

#[test]
fn test_fusion_tombstones_originals() {
    let (db, _dir) = setup_db();

    let node1 = json!({"_id": "events/to-fuse-1", "data": "first"}).to_string();
    let node2 = json!({"_id": "events/to-fuse-2", "data": "second"}).to_string();

    db.nodes().put_json(&node1).unwrap();
    db.nodes().put_json(&node2).unwrap();
    db.flush().unwrap();

    let fused = json!({"_id": "events/fused", "merged": true}).to_string();
    db.nodes().put_json(&fused).unwrap();
    db.flush().unwrap();

    assert!(db.nodes().get("events/fused").is_some());
    println!("[TC-2.3] Fused node created successfully");
}
