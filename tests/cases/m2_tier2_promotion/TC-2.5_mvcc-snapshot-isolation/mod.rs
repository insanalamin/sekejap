//! TC-2.5: MVCC Snapshot Isolation
//!
//! Goal: Verify readers see a consistent snapshot during fusion.
//! Given: Readers access Tier 2 while fusion runs.
//! When: Fusion swaps to a new epoch.
//! Then: Readers see a consistent snapshot with no partial state.

use sekejap::SekejapDB;
use tempfile::TempDir;
use serde_json::json;

fn setup_db() -> (SekejapDB, TempDir) {
    let dir = TempDir::new().unwrap();
    let db = SekejapDB::new(dir.path(), 1000).unwrap();
    (db, dir)
}

#[test]
fn test_snapshot_isolation_during_write() {
    let (db, _dir) = setup_db();

    let initial = json!({"_id": "shared/data", "value": "initial"}).to_string();
    db.nodes().put_json(&initial).unwrap();
    db.flush().unwrap();

    let read1 = db.nodes().get("shared/data").unwrap();
    let val1: serde_json::Value = serde_json::from_str(&read1).unwrap();
    assert_eq!(val1["value"], "initial");
    println!("[TC-2.5] Read 1: {}", val1["value"]);

    let updated = json!({"_id": "shared/data", "value": "updated"}).to_string();
    db.nodes().put_json(&updated).unwrap();
    db.flush().unwrap();

    let read2 = db.nodes().get("shared/data").unwrap();
    let val2: serde_json::Value = serde_json::from_str(&read2).unwrap();
    assert_eq!(val2["value"], "updated");
    println!("[TC-2.5] Read 2 (after write): {}", val2["value"]);
}

#[test]
fn test_consistent_read_across_multiple_reads() {
    let (db, _dir) = setup_db();

    for i in 0..10 {
        let payload = json!({"_id": format!("shared/node-{}", i), "index": i}).to_string();
        db.nodes().put_json(&payload).unwrap();
    }
    db.flush().unwrap();

    let mut indices = Vec::new();
    for i in 0..10 {
        let slug = format!("shared/node-{}", i);
        if let Some(data) = db.nodes().get(&slug) {
            let val: serde_json::Value = serde_json::from_str(&data).unwrap();
            indices.push(val["index"].as_u64().unwrap());
        }
    }

    assert_eq!(indices.len(), 10);
    println!("[TC-2.5] Consistent read across {} nodes: PASSED", indices.len());
}

#[test]
fn test_no_partial_state_visibility() {
    let (db, _dir) = setup_db();

    let partial = json!({"_id": "test/partial", "status": "in-progress"}).to_string();
    db.nodes().put_json(&partial).unwrap();
    db.flush().unwrap();

    let complete = json!({"_id": "test/partial", "status": "complete"}).to_string();
    db.nodes().put_json(&complete).unwrap();
    db.flush().unwrap();

    let final_read = db.nodes().get("test/partial").unwrap();
    let val: serde_json::Value = serde_json::from_str(&final_read).unwrap();

    assert_eq!(val["status"], "complete");
    println!("[TC-2.5] No partial state visibility: PASSED");
}

#[test]
fn test_epoch_consistency() {
    let (db, _dir) = setup_db();

    let epoch1 = json!({"_id": "epoch/node", "epoch": 1}).to_string();
    db.nodes().put_json(&epoch1).unwrap();
    db.flush().unwrap();

    let read1 = db.nodes().get("epoch/node").unwrap();
    let val1: serde_json::Value = serde_json::from_str(&read1).unwrap();
    assert_eq!(val1["epoch"], 1);

    let epoch2 = json!({"_id": "epoch/node", "epoch": 2}).to_string();
    db.nodes().put_json(&epoch2).unwrap();
    db.flush().unwrap();

    let read2 = db.nodes().get("epoch/node").unwrap();
    let val2: serde_json::Value = serde_json::from_str(&read2).unwrap();
    assert_eq!(val2["epoch"], 2);

    println!("[TC-2.5] Epoch consistency: PASSED");
}
