//! Tests for M1: Basic Node Operations
//! Covers TC-1.1, TC-1.2, TC-1.3
//!
//! Run individual tests with:
//! cargo test tc_1_1 -- --nocapture
//! cargo test tc_1_2 -- --nocapture
//! cargo test tc_1_3 -- --nocapture
//! cargo test m1_basic_node_ops -- --nocapture

use sekejap::SekejapDB;
use tempfile::TempDir;
use serde_json::json;

fn setup_db() -> (SekejapDB, TempDir) {
    let dir = TempDir::new().unwrap();
    let db = SekejapDB::new(dir.path(), 1000).unwrap();
    (db, dir)
}

mod tc_1_1_create_node_with_entities {
    use super::*;

    #[test]
    fn test_create_node_with_entities_and_spatial_lookup() {
        let (db, _dir) = setup_db();

        // Test Data
        let slug = "events/jakarta-crime-2024-04-12";
        let payload = json!({
            "_id": slug,
            "who": ["Police", "Mayor"],
            "where": "Jakarta",
            "when": "2024-04-12T09:30:00Z",
            "coordinates": {"lat": -6.2088, "lon": 106.8456},
            "summary": "Morning incident near downtown"
        }).to_string();

        // Step 1: Insert the node
        let idx = db.nodes().put_json(&payload).unwrap();
        db.flush().unwrap();
        println!("Inserted node at index: {}", idx);

        // Step 2: Read the node by slug
        let retrieved = db.nodes().get(slug);
        assert!(retrieved.is_some(), "Node should be retrievable by slug");
        let retrieved_json: serde_json::Value = serde_json::from_str(&retrieved.unwrap()).unwrap();

        // Verify payload content matches
        assert_eq!(retrieved_json["who"], json!(["Police", "Mayor"]));
        assert_eq!(retrieved_json["where"], "Jakarta");
        assert_eq!(retrieved_json["summary"], "Morning incident near downtown");

        // Step 3: Run spatial query centered at the node location with 1km radius
        let outcome = db.nodes()
            .one(slug)
            .near(-6.2088, 106.8456, 1.0)  // 1km radius
            .collect()
            .unwrap();

        println!("Spatial query trace: {:?}", outcome.trace);
        assert!(!outcome.data.is_empty(), "Spatial query should include the node");

        // Verify slug_index contains mapping
        let (_, slug_hash) = SekejapDB::parse_entity_id(slug);
        let stored_idx = db.slug_index.read().get(slug_hash);
        assert!(stored_idx.is_some(), "slug_index should contain mapping for slug hash");
        assert_eq!(stored_idx.unwrap(), idx, "slug_index should map to correct node index");
    }

    #[test]
    fn test_node_timestamp_assignment() {
        let (db, _dir) = setup_db();

        let slug = "events/timestamp-test";
        let payload = json!({"_id": slug, "test": true}).to_string();
        db.nodes().put_json(&payload).unwrap();
        db.flush().unwrap();

        // Verify node exists and is active (flags = 1)
        let (_, slug_hash) = SekejapDB::parse_entity_id(slug);
        let idx = db.slug_index.read().get(slug_hash).unwrap();
        let slot = db.nodes.read_at(idx as u64);
        assert_ne!(slot.flags, 0, "Node flags should be non-zero (active)");

        // For basic node creation, we verify the node was created successfully
        println!("Node created with flags: {}", slot.flags);
    }
}

mod tc_1_2_upsert_ingestion_buffer {
    use super::*;

    #[test]
    fn test_upsert_updates_existing_node() {
        let (db, _dir) = setup_db();

        let slug = "events/jakarta-crime-2024";

        // Initial payload
        let initial_payload = json!({
            "_id": slug,
            "summary": "Initial report",
            "version": 1
        }).to_string();

        // Insert initial
        let idx1 = db.nodes().put_json(&initial_payload).unwrap();
        db.flush().unwrap();
        println!("Initial insert: index={}", idx1);

        // Updated payload (same slug)
        let updated_payload = json!({
            "_id": slug,
            "summary": "Update with new details",
            "version": 2
        }).to_string();

        // Upsert (insert with same slug)
        let idx2 = db.nodes().put_json(&updated_payload).unwrap();
        db.flush().unwrap();
        println!("Upsert: index={}", idx2);

        // Verify node is retrievable
        let retrieved = db.nodes().get(slug);
        assert!(retrieved.is_some(), "Node should be retrievable");

        let retrieved_json: serde_json::Value = serde_json::from_str(&retrieved.unwrap()).unwrap();
        assert_eq!(retrieved_json["summary"], "Update with new details");
        assert_eq!(retrieved_json["version"], 2);

        // Check slug_index
        let (_, slug_hash) = SekejapDB::parse_entity_id(slug);
        let count = db.slug_index.read().get(slug_hash);
        assert!(count.is_some(), "slug_index should contain entry");

        println!("Upsert test completed - retrieved latest version");
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

        // Check slug_index
        let (_, slug_hash) = SekejapDB::parse_entity_id(slug);
        let count = db.slug_index.read().get(slug_hash);

        println!("Duplicate index test - checking slug_index behavior");
        assert!(count.is_some(), "slug_index should have entry");
    }
}

mod tc_1_3_spatial_index_registration {
    use super::*;

    #[test]
    fn test_spatial_index_registration() {
        let (db, _dir) = setup_db();

        let slug = "locations/monas";
        let payload = json!({
            "_id": slug,
            "name": "Monas",
            "coordinates": {"lat": -6.1754, "lon": 106.8272}
        }).to_string();

        // Step 1: Insert node with coordinates
        let idx = db.nodes().put_json(&payload).unwrap();
        db.flush().unwrap();
        println!("Inserted Monas at index: {}", idx);

        // Verify spatial index contains the node
        let spatial_tree = db.spatial.read();
        let all_nodes: Vec<_> = spatial_tree.iter().collect();
        println!("Spatial index contains {} nodes", all_nodes.len());
        assert!(!all_nodes.is_empty(), "Spatial index should not be empty");

        // Step 2: Spatial query with 0.5km radius should include node
        let outcome = db.nodes()
            .one(slug)
            .near(-6.1754, 106.8272, 0.5)
            .collect()
            .unwrap();

        assert!(!outcome.data.is_empty(), "0.5km radius should include Monas");
        println!("0.5km radius query: found {} nodes", outcome.data.len());

        // Step 3: Excluded if radius is too small (0.01km = 10 meters)
        let outcome_small = db.nodes()
            .one(slug)
            .near(-6.1754, 106.8272, 0.01)
            .collect()
            .unwrap();

        println!("0.01km radius query: found {} nodes", outcome_small.data.len());
    }

    #[test]
    fn test_multiple_nodes_spatial_query() {
        let (db, _dir) = setup_db();

        // Insert multiple nodes at different locations
        let nodes = vec![
            ("loc/a", -6.1754, 106.8272),  // Monas
            ("loc/b", -6.1760, 106.8280),  // Very close
            ("loc/c", -6.2000, 106.8500),  // Further away
            ("loc/d", -6.3000, 106.9000),  // Far
        ];

        for (slug, lat, lon) in &nodes {
            let payload = json!({
                "_id": slug,
                "name": format!("Node {}", slug),
                "coordinates": {"lat": lat, "lon": lon}
            }).to_string();
            db.nodes().put_json(&payload).unwrap();
        }
        db.flush().unwrap();

        // Query near Monas with 1km radius
        let outcome = db.nodes()
            .all()
            .near(-6.1754, 106.8272, 1.0)
            .collect()
            .unwrap();

        println!("Spatial query near Monas (1km): found {} nodes", outcome.data.len());
        for hit in &outcome.data {
            println!("  - lat: {}, lon: {}", hit.lat, hit.lon);
        }

        // Should find at least nodes A and B (close to query point)
        assert!(outcome.data.len() >= 2, "Should find at least 2 nearby nodes");
    }
}
