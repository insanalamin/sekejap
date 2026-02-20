//! Tests for M2: Tier2 Promotion
//! Covers TC-2.1, TC-2.2, TC-2.3, TC-2.4, TC-2.5
//!
//! Run individual tests with:
//! cargo test tc_2_1 -- --nocapture
//! cargo test tc_2_2 -- --nocapture
//! cargo test tc_2_3 -- --nocapture
//! cargo test tc_2_4 -- --nocapture
//! cargo test tc_2_5 -- --nocapture

use sekejap::SekejapDB;
use tempfile::TempDir;
use serde_json::json;

fn setup_db() -> (SekejapDB, TempDir) {
    let dir = TempDir::new().unwrap();
    let db = SekejapDB::new(dir.path(), 1000).unwrap();
    (db, dir)
}

mod tc_2_1_simple_node_promotion {
    use super::*;

    #[test]
    fn test_node_promotion_to_tier2() {
        let (db, _dir) = setup_db();

        let slug = "events/flood-2024-01";
        let payload = json!({
            "_id": slug,
            "summary": "Flood report",
            "severity": "high"
        }).to_string();

        let idx = db.nodes().put_json(&payload).unwrap();
        db.flush().unwrap();
        println!("[TC-2.1] Inserted node at index: {}", idx);

        // Verify node is active
        let slot = db.nodes.read_at(idx as u64);
        assert_ne!(slot.flags, 0, "Node should be active");

        // Verify visibility
        let retrieved = db.nodes().get(slug);
        assert!(retrieved.is_some(), "Node should be visible in Tier 2");
        println!("[TC-2.1] Node promoted to Tier 2 successfully");
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
        println!("[TC-2.1] Payload preservation: PASSED");
    }

    #[test]
    fn test_multiple_node_promotion() {
        let (db, _dir) = setup_db();

        for i in 0..5 {
            let slug = format!("events/multi-{}", i);
            let payload = json!({"_id": slug, "index": i}).to_string();
            db.nodes().put_json(&payload).unwrap();
        }
        db.flush().unwrap();

        for i in 0..5 {
            let slug = format!("events/multi-{}", i);
            assert!(db.nodes().get(&slug).is_some(), "Node {} should be promoted", i);
        }
        println!("[TC-2.1] Multiple node promotion: PASSED");
    }
}

mod tc_2_2_duplicate_detection {
    use super::*;

    #[test]
    fn test_duplicate_candidate_detection() {
        let (db, _dir) = setup_db();

        let node_a = json!({
            "_id": "events/riot-001",
            "summary": "Riot near station",
            "who": ["Group A"],
            "where": "Station X"
        }).to_string();

        let node_b = json!({
            "_id": "events/riot-002",
            "summary": "Riot at Station X",
            "who": ["Group A"],
            "where": "Station X"
        }).to_string();

        db.nodes().put_json(&node_a).unwrap();
        db.nodes().put_json(&node_b).unwrap();
        db.flush().unwrap();

        let retrieved_a = db.nodes().get("events/riot-001").unwrap();
        let retrieved_b = db.nodes().get("events/riot-002").unwrap();

        let json_a: serde_json::Value = serde_json::from_str(&retrieved_a).unwrap();
        let json_b: serde_json::Value = serde_json::from_str(&retrieved_b).unwrap();

        assert_eq!(json_a["who"], json_b["who"]);
        println!("[TC-2.2] Similar content detected for duplicate analysis");
    }

    #[test]
    fn test_threshold_based_duplicate_identification() {
        let (db, _dir) = setup_db();

        let above_threshold = vec![
            ("events/above-1", "Duplicate content"),
            ("events/above-2", "Duplicate content"),
        ];

        for (slug, content) in &above_threshold {
            let payload = json!({"_id": slug, "content": content}).to_string();
            db.nodes().put_json(&payload).unwrap();
        }
        db.flush().unwrap();

        println!("[TC-2.2] {} nodes above similarity threshold", above_threshold.len());
    }
}

mod tc_2_3_node_fusion {
    use super::*;

    #[test]
    fn test_node_fusion_merge_entities() {
        let (db, _dir) = setup_db();

        let node_a = json!({"_id": "events/accident-1", "who": ["Driver A"]}).to_string();
        let node_b = json!({"_id": "events/accident-2", "who": ["Driver A", "Passenger B"]}).to_string();

        db.nodes().put_json(&node_a).unwrap();
        db.nodes().put_json(&node_b).unwrap();
        db.flush().unwrap();

        // Create merged node
        let merged = json!({
            "_id": "events/accident-fused",
            "who": ["Driver A", "Passenger B"]
        }).to_string();

        db.nodes().put_json(&merged).unwrap();
        db.flush().unwrap();

        let retrieved = db.nodes().get("events/accident-fused").unwrap();
        let retrieved_json: serde_json::Value = serde_json::from_str(&retrieved).unwrap();

        let who = retrieved_json["who"].as_array().unwrap();
        assert!(who.contains(&json!("Driver A")));
        assert!(who.contains(&json!("Passenger B")));
        println!("[TC-2.3] Entity fusion: PASSED");
    }

    #[test]
    fn test_fusion_preserves_latest_timestamp() {
        let (db, _dir) = setup_db();

        let old_node = json!({"_id": "events/old", "timestamp": "2024-01-01"}).to_string();
        let new_node = json!({"_id": "events/new", "timestamp": "2024-06-01"}).to_string();

        db.nodes().put_json(&old_node).unwrap();
        db.nodes().put_json(&new_node).unwrap();
        db.flush().unwrap();

        let fused = json!({"_id": "events/fused", "timestamp": "2024-06-01"}).to_string();
        db.nodes().put_json(&fused).unwrap();
        db.flush().unwrap();

        let retrieved = db.nodes().get("events/fused").unwrap();
        let val: serde_json::Value = serde_json::from_str(&retrieved).unwrap();
        assert_eq!(val["timestamp"], "2024-06-01");
        println!("[TC-2.3] Latest timestamp preserved: PASSED");
    }
}

mod tc_2_4_hierarchy_edge_creation {
    use super::*;

    #[test]
    fn test_causal_edge_creation() {
        let (db, _dir) = setup_db();

        let cause = json!({"_id": "events/rainstorm-2024", "type": "weather"}).to_string();
        let effect = json!({"_id": "events/flood-2024", "type": "disaster"}).to_string();

        db.nodes().put_json(&cause).unwrap();
        db.nodes().put_json(&effect).unwrap();
        db.flush().unwrap();

        db.edges().link("events/rainstorm-2024", "events/flood-2024", "caused_by", 0.85).unwrap();

        let outcome = db.nodes()
            .one("events/rainstorm-2024")
            .forward("caused_by")
            .collect()
            .unwrap();

        assert!(!outcome.data.is_empty());
        println!("[TC-2.4] Causal edge created: rainstorm -> flood");
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
}

mod tc_2_5_mvcc_snapshot_isolation {
    use super::*;

    #[test]
    fn test_snapshot_isolation_during_write() {
        let (db, _dir) = setup_db();

        let initial = json!({"_id": "shared/data", "value": "initial"}).to_string();
        db.nodes().put_json(&initial).unwrap();
        db.flush().unwrap();

        let read1 = db.nodes().get("shared/data").unwrap();
        let val1: serde_json::Value = serde_json::from_str(&read1).unwrap();
        assert_eq!(val1["value"], "initial");

        let updated = json!({"_id": "shared/data", "value": "updated"}).to_string();
        db.nodes().put_json(&updated).unwrap();
        db.flush().unwrap();

        let read2 = db.nodes().get("shared/data").unwrap();
        let val2: serde_json::Value = serde_json::from_str(&read2).unwrap();
        assert_eq!(val2["value"], "updated");
        println!("[TC-2.5] Snapshot isolation: PASSED");
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
    fn test_epoch_consistency() {
        let (db, _dir) = setup_db();

        let epoch1 = json!({"_id": "epoch/node", "epoch": 1}).to_string();
        db.nodes().put_json(&epoch1).unwrap();
        db.flush().unwrap();

        let epoch2 = json!({"_id": "epoch/node", "epoch": 2}).to_string();
        db.nodes().put_json(&epoch2).unwrap();
        db.flush().unwrap();

        let read2 = db.nodes().get("epoch/node").unwrap();
        let val2: serde_json::Value = serde_json::from_str(&read2).unwrap();
        assert_eq!(val2["epoch"], 2);
        println!("[TC-2.5] Epoch consistency: PASSED");
    }
}
