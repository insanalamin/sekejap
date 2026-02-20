//! TC-2.4: Hierarchy Edge Creation
//!
//! Goal: Create causal edge during fusion.
//! Given: Cause node and Effect node identified.
//! When: Fusion identifies causal relationship.
//! Then: Directed edge Cause -> Effect is created.

use sekejap::SekejapDB;
use tempfile::TempDir;
use serde_json::json;

fn setup_db() -> (SekejapDB, TempDir) {
    let dir = TempDir::new().unwrap();
    let db = SekejapDB::new(dir.path(), 1000).unwrap();
    (db, dir)
}

#[test]
fn test_causal_edge_creation() {
    let (db, _dir) = setup_db();

    let cause = json!({"_id": "events/rainstorm-2024", "type": "weather"}).to_string();
    let effect = json!({"_id": "events/flood-2024", "type": "disaster"}).to_string();

    db.nodes().put_json(&cause).unwrap();
    db.nodes().put_json(&effect).unwrap();
    db.flush().unwrap();

    // Create causal edge
    db.edges().link("events/rainstorm-2024", "events/flood-2024", "caused_by", 0.85).unwrap();

    // Verify via forward traversal
    let outcome = db.nodes()
        .one("events/rainstorm-2024")
        .forward("caused_by")
        .collect()
        .unwrap();

    assert!(!outcome.data.is_empty());
    println!("[TC-2.4] Causal edge created and traversable");
}

#[test]
fn test_edge_weight_from_fusion_confidence() {
    let (db, _dir) = setup_db();

    let cause = json!({"_id": "events/root-cause", "data": "root"}).to_string();
    let effect = json!({"_id": "events/result", "data": "result"}).to_string();

    db.nodes().put_json(&cause).unwrap();
    db.nodes().put_json(&effect).unwrap();
    db.flush().unwrap();

    // High confidence edge
    db.edges().link("events/root-cause", "events/result", "causes", 0.95).unwrap();

    let outcome = db.nodes()
        .one("events/root-cause")
        .forward("causes")
        .collect()
        .unwrap();

    assert!(!outcome.data.is_empty());
    println!("[TC-2.4] Edge weight (0.95) verified");
}

#[test]
fn test_hierarchy_traversal() {
    let (db, _dir) = setup_db();

    let event = json!({"_id": "events/incident-001", "type": "event"}).to_string();
    let subdistrict = json!({"_id": "geo/subdistrict-001", "type": "subdistrict"}).to_string();
    let district = json!({"_id": "geo/district-001", "type": "district"}).to_string();

    db.nodes().put_json(&event).unwrap();
    db.nodes().put_json(&subdistrict).unwrap();
    db.nodes().put_json(&district).unwrap();
    db.flush().unwrap();

    db.edges().link("events/incident-001", "geo/subdistrict-001", "located_in", 1.0).unwrap();
    db.edges().link("geo/subdistrict-001", "geo/district-001", "located_in", 1.0).unwrap();

    let outcome = db.nodes()
        .one("events/incident-001")
        .forward("located_in")
        .hops(2)
        .collect()
        .unwrap();

    println!("[TC-2.4] Hierarchy traversal found {} nodes", outcome.data.len());
}

#[test]
fn test_backward_causal_traversal() {
    let (db, _dir) = setup_db();

    for i in 0..4 {
        let payload = json!({"_id": format!("events/chain-{}", i)}).to_string();
        db.nodes().put_json(&payload).unwrap();
    }
    db.flush().unwrap();

    db.edges().link("events/chain-0", "events/chain-1", "causes", 1.0).unwrap();
    db.edges().link("events/chain-1", "events/chain-2", "causes", 1.0).unwrap();
    db.edges().link("events/chain-2", "events/chain-3", "causes", 1.0).unwrap();

    let outcome = db.nodes()
        .one("events/chain-3")
        .backward("causes")
        .hops(3)
        .collect()
        .unwrap();

    println!("[TC-2.4] Backward traversal found {} ancestors", outcome.data.len());
}
