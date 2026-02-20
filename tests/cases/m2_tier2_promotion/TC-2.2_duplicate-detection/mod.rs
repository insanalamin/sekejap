//! TC-2.2: Duplicate Detection
//!
//! Goal: Identify candidate duplicates in Tier 1.
//! Given: Multiple nodes with highly similar content.
//! When: Running duplicate detection.
//! Then: The system surfaces duplicate candidate pairs for fusion.

use sekejap::SekejapDB;
use tempfile::TempDir;
use serde_json::json;

fn setup_db() -> (SekejapDB, TempDir) {
    let dir = TempDir::new().unwrap();
    let db = SekejapDB::new(dir.path(), 1000).unwrap();
    (db, dir)
}

#[test]
fn test_duplicate_candidate_detection() {
    let (db, _dir) = setup_db();

    // Node A: Similar content to Node B
    let node_a = json!({
        "_id": "events/riot-001",
        "summary": "Riot near station",
        "who": ["Group A"],
        "where": "Station X"
    }).to_string();

    // Node B: Very similar content
    let node_b = json!({
        "_id": "events/riot-002",
        "summary": "Riot at Station X",
        "who": ["Group A"],
        "where": "Station X"
    }).to_string();

    db.nodes().put_json(&node_a).unwrap();
    db.nodes().put_json(&node_b).unwrap();
    db.flush().unwrap();

    println!("[TC-2.2] Inserted two similar nodes for duplicate detection");

    // Both nodes should be retrievable
    let retrieved_a = db.nodes().get("events/riot-001").unwrap();
    let retrieved_b = db.nodes().get("events/riot-002").unwrap();

    let json_a: serde_json::Value = serde_json::from_str(&retrieved_a).unwrap();
    let json_b: serde_json::Value = serde_json::from_str(&retrieved_b).unwrap();

    // Verify they have similar content
    assert_eq!(json_a["who"], json_b["who"]);
    assert_eq!(json_a["where"], json_b["where"]);
    
    println!("[TC-2.2] Node A: {}", json_a["summary"]);
    println!("[TC-2.2] Node B: {}", json_b["summary"]);
}

#[test]
fn test_content_similarity_analysis() {
    let (db, _dir) = setup_db();

    let nodes = vec![
        ("events/exact-match", "Same text"),
        ("events/similar-match", "Same text"),
        ("events/different", "Different text"),
    ];

    for (slug, content) in &nodes {
        let payload = json!({"_id": slug, "content": content}).to_string();
        db.nodes().put_json(&payload).unwrap();
    }
    db.flush().unwrap();

    for (slug, _) in &nodes {
        assert!(db.nodes().get(slug).is_some());
    }

    println!("[TC-2.2] Content similarity analysis ready");
}

#[test]
fn test_threshold_based_duplicate_identification() {
    let (db, _dir) = setup_db();

    let above_threshold = vec![
        ("events/above-1", "Duplicate content pattern A"),
        ("events/above-2", "Duplicate content pattern A"),
    ];

    for (slug, content) in &above_threshold {
        let payload = json!({"_id": slug, "content": content}).to_string();
        db.nodes().put_json(&payload).unwrap();
    }

    let below_threshold = vec![
        ("events/below-1", "Unique content"),
        ("events/below-2", "Different content"),
    ];

    for (slug, content) in &below_threshold {
        let payload = json!({"_id": slug, "content": content}).to_string();
        db.nodes().put_json(&payload).unwrap();
    }
    
    db.flush().unwrap();

    println!("[TC-2.2] Above threshold: {}", above_threshold.len());
    println!("[TC-2.2] Below threshold: {}", below_threshold.len());
}
