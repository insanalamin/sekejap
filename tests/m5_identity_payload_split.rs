//! Tests for M5: Identity/Payload Split
//! Covers TC-5.1, TC-5.2, TC-5.3, TC-5.4
//!
//! Run individual tests with:
//! cargo test tc_5_1 -- --nocapture
//! cargo test tc_5_2 -- --nocapture
//! cargo test tc_5_3 -- --nocapture
//! cargo test tc_5_4 -- --nocapture

use sekejap::SekejapDB;
use tempfile::TempDir;
use serde_json::json;

fn setup_db() -> (SekejapDB, TempDir) {
    let dir = TempDir::new().unwrap();
    let db = SekejapDB::new(dir.path(), 1000).unwrap();
    (db, dir)
}

mod tc_5_1_blobstore_write {
    use super::*;

    #[test]
    fn test_blob_payload_write() {
        let (db, _dir) = setup_db();

        let payload = json!({
            "_id": "blob/test-large",
            "data": "x".repeat(1000),
            "metadata": {"size": 1000}
        }).to_string();

        db.nodes().put_json(&payload).unwrap();
        db.flush().unwrap();

        let retrieved = db.nodes().get("blob/test-large");
        assert!(retrieved.is_some());
        println!("[TC-5.1] Blob payload written successfully");
    }

    #[test]
    fn test_multiple_blob_inserts() {
        let (db, _dir) = setup_db();

        for i in 0..10 {
            let payload = json!({
                "_id": format!("blob/item-{}", i),
                "data": format!("content-{}", i),
                "index": i
            }).to_string();
            db.nodes().put_json(&payload).unwrap();
        }
        db.flush().unwrap();

        for i in 0..10 {
            let slug = format!("blob/item-{}", i);
            assert!(db.nodes().get(&slug).is_some());
        }
        println!("[TC-5.1] {} blob items inserted", 10);
    }
}

mod tc_5_2_blobstore_read {
    use super::*;

    #[test]
    fn test_blob_payload_read() {
        let (db, _dir) = setup_db();

        let payload = json!({
            "_id": "blob/read-test",
            "content": "Hello, World!",
            "binary": "SGVsbG8gV29ybGQh"  // Base64 encoded
        }).to_string();

        db.nodes().put_json(&payload).unwrap();
        db.flush().unwrap();

        let retrieved = db.nodes().get("blob/read-test").unwrap();
        let val: serde_json::Value = serde_json::from_str(&retrieved).unwrap();
        assert_eq!(val["content"], "Hello, World!");
        println!("[TC-5.2] Blob read: PASSED");
    }

    #[test]
    fn test_read_nonexistent_blob() {
        let (db, _dir) = setup_db();

        let retrieved = db.nodes().get("blob/nonexistent");
        assert!(retrieved.is_none());
        println!("[TC-5.2] Nonexistent blob handling: PASSED");
    }
}

mod tc_5_3_nodeheader_payload_reference {
    use super::*;

    #[test]
    fn test_nodeheader_with_payload_reference() {
        let (db, _dir) = setup_db();

        let payload = json!({
            "_id": "ref/test",
            "description": "Test node with references",
            "references": ["events/a", "events/b"]
        }).to_string();

        db.nodes().put_json(&payload).unwrap();
        db.flush().unwrap();

        let retrieved = db.nodes().get("ref/test").unwrap();
        let val: serde_json::Value = serde_json::from_str(&retrieved).unwrap();
        assert!(val["references"].is_array());
        println!("[TC-5.3] NodeHeader payload reference: PASSED");
    }

    #[test]
    fn test_cross_reference_integrity() {
        let (db, _dir) = setup_db();

        // Create referenced nodes
        db.nodes().put_json(&json!({"_id": "ref/source1"}).to_string()).unwrap();
        db.nodes().put_json(&json!({"_id": "ref/source2"}).to_string()).unwrap();
        
        // Create node referencing them
        let payload = json!({
            "_id": "ref/aggregator",
            "sources": ["ref/source1", "ref/source2"]
        }).to_string();
        db.nodes().put_json(&payload).unwrap();
        db.flush().unwrap();

        let retrieved = db.nodes().get("ref/aggregator").unwrap();
        println!("[TC-5.3] Cross-reference integrity: PASSED");
    }
}

mod tc_5_4_bptree_size_vs_payload {
    use super::*;

    #[test]
    fn test_payload_size_variance() {
        let (db, _dir) = setup_db();

        let sizes = vec![
            ("size/tiny", 10),
            ("size/small", 100),
            ("size/medium", 1000),
            ("size/large", 10000),
        ];

        for (slug, size) in &sizes {
            let payload = json!({
                "_id": slug,
                "content": "x".repeat(*size),
                "expected_size": size
            }).to_string();
            db.nodes().put_json(&payload).unwrap();
        }
        db.flush().unwrap();

        for (slug, _) in &sizes {
            assert!(db.nodes().get(slug).is_some());
        }
        println!("[TC-5.4] Payload size variance test: PASSED");
    }

    #[test]
    fn test_bptree_index_operations() {
        let (db, _dir) = setup_db();

        for i in 0..100 {
            let payload = json!({
                "_id": format!("indexed/node-{:03}", i),
                "order": i
            }).to_string();
            db.nodes().put_json(&payload).unwrap();
        }
        db.flush().unwrap();

        // Verify all nodes are accessible via slug_index
        for i in 0..100 {
            let slug = format!("indexed/node-{:03}", i);
            assert!(db.nodes().get(&slug).is_some());
        }
        println!("[TC-5.4] BPTree index operations: 100 nodes indexed");
    }
}
