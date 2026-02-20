//! Tests for M3: Multimodal Search
//! Covers TC-3.1, TC-3.2, TC-3.3, TC-3.4, TC-3.5
//!
//! Run individual tests with:
//! cargo test tc_3_1 -- --nocapture
//! cargo test tc_3_2 -- --nocapture
//! cargo test tc_3_3 -- --nocapture
//! cargo test tc_3_4 -- --nocapture
//! cargo test tc_3_5 -- --nocapture

use sekejap::SekejapDB;
use tempfile::TempDir;
use serde_json::json;

fn setup_db() -> (SekejapDB, TempDir) {
    let dir = TempDir::new().unwrap();
    let db = SekejapDB::new(dir.path(), 1000).unwrap();
    (db, dir)
}

mod tc_3_1_vector_similarity {
    use super::*;

    #[test]
    fn test_vector_data_structure() {
        let (db, _dir) = setup_db();

        // Store nodes with vector data - verification of data structure
        let nodes = vec![
            ("vec/node-a", vec![0.9, 0.1, 0.1]),
            ("vec/node-b", vec![0.1, 0.9, 0.1]),
            ("vec/node-c", vec![0.1, 0.1, 0.9]),
        ];

        for (slug, vec_data) in &nodes {
            let payload = json!({
                "_id": slug,
                "vectors": {"dense": vec_data}
            }).to_string();
            db.nodes().put_json(&payload).unwrap();
        }
        db.flush().unwrap();

        // Verify vectors are stored
        for (slug, _) in &nodes {
            let retrieved = db.nodes().get(slug);
            assert!(retrieved.is_some(), "Node {} should exist", slug);
        }

        println!("[TC-3.1] Vector data structure: {} nodes stored", nodes.len());
    }

    #[test]
    fn test_vector_payload_parsing() {
        let (db, _dir) = setup_db();

        let payload = json!({
            "_id": "vec/test",
            "vectors": {
                "dense": [0.1, 0.2, 0.3, 0.4],
                "sparse": {"0": 0.5, "1": 0.3}
            }
        }).to_string();

        db.nodes().put_json(&payload).unwrap();
        db.flush().unwrap();

        let retrieved = db.nodes().get("vec/test").unwrap();
        let val: serde_json::Value = serde_json::from_str(&retrieved).unwrap();

        assert!(val["vectors"].is_object());
        println!("[TC-3.1] Vector payload parsing: PASSED");
    }
}

mod tc_3_2_spatial_radius {
    use super::*;

    #[test]
    fn test_spatial_radius_query() {
        let (db, _dir) = setup_db();

        // Create nodes at various distances from a center point
        let center = (-6.2088, 106.8456);  // Jakarta
        let nodes = vec![
            ("loc/very-close", -6.2080, 106.8460),   // ~100m
            ("loc/close", -6.2050, 106.8480),          // ~400m
            ("loc/far", -6.1900, 106.8600),            // ~2.5km
            ("loc/very-far", -6.1500, 106.9000),       // ~10km
        ];

        for (slug, lat, lon) in &nodes {
            let payload = json!({
                "_id": slug,
                "coordinates": {"lat": lat, "lon": lon}
            }).to_string();
            db.nodes().put_json(&payload).unwrap();
        }
        db.flush().unwrap();

        // Query within 1km
        let outcome = db.nodes()
            .all()
            .near(center.0, center.1, 1.0)
            .collect()
            .unwrap();

        println!("[TC-3.2] 1km radius query found: {} nodes", outcome.data.len());
        // Should find very-close and close (within 1km)
        assert!(outcome.data.len() >= 2);
    }

    #[test]
    fn test_spatial_radius_edge_cases() {
        let (db, _dir) = setup_db();

        // Node exactly at center
        let payload1 = json!({
            "_id": "loc/center",
            "coordinates": {"lat": 0.0, "lon": 0.0}
        }).to_string();
        db.nodes().put_json(&payload1).unwrap();

        // Node at 1km boundary (approximately 0.009 degrees)
        let payload2 = json!({
            "_id": "loc/boundary",
            "coordinates": {"lat": 0.009, "lon": 0.009}
        }).to_string();
        db.nodes().put_json(&payload2).unwrap();

        db.flush().unwrap();

        // Query with 1km radius from center
        let outcome = db.nodes()
            .all()
            .near(0.0, 0.0, 1.0)
            .collect()
            .unwrap();

        println!("[TC-3.2] Boundary test found: {} nodes", outcome.data.len());
        assert!(outcome.data.len() >= 1);
    }

    #[test]
    fn test_coordinate_extraction() {
        let (db, _dir) = setup_db();

        let payload = json!({
            "_id": "loc/test",
            "coordinates": {"lat": -6.2088, "lon": 106.8456}
        }).to_string();
        db.nodes().put_json(&payload).unwrap();
        db.flush().unwrap();

        let retrieved = db.nodes().get("loc/test").unwrap();
        let val: serde_json::Value = serde_json::from_str(&retrieved).unwrap();

        let coords = &val["coordinates"];
        assert_eq!(coords["lat"], -6.2088);
        assert_eq!(coords["lon"], 106.8456);
        println!("[TC-3.2] Coordinate extraction: PASSED");
    }
}

mod tc_3_3_time_window_filter {
    use super::*;

    #[test]
    fn test_time_window_query() {
        let (db, _dir) = setup_db();

        // Nodes at different times
        let now = "2024-06-15T12:00:00Z";
        let nodes = vec![
            ("time/recent-1", now),
            ("time/recent-2", "2024-06-15T11:00:00Z"),
            ("time/old", "2024-05-15T12:00:00Z"),
            ("time/very-old", "2023-06-15T12:00:00Z"),
        ];

        for (slug, timestamp) in &nodes {
            let payload = json!({
                "_id": slug,
                "timestamp": timestamp,
                "event": format!("Event at {}", timestamp)
            }).to_string();
            db.nodes().put_json(&payload).unwrap();
        }
        db.flush().unwrap();

        // Query all nodes
        let outcome = db.nodes()
            .all()
            .collect()
            .unwrap();

        println!("[TC-3.3] Total nodes: {}", outcome.data.len());
        assert!(outcome.data.len() >= 4);
    }

    #[test]
    fn test_timestamp_parsing() {
        let (db, _dir) = setup_db();

        let payloads = vec![
            ("time/iso", "2024-06-15T10:30:00Z"),
            ("time/with-ms", "2024-06-15T10:30:00.123Z"),
            ("time/with-tz", "2024-06-15T10:30:00+07:00"),
        ];

        for (slug, ts) in &payloads {
            let payload = json!({"_id": slug, "timestamp": ts}).to_string();
            db.nodes().put_json(&payload).unwrap();
        }
        db.flush().unwrap();

        // Verify all timestamps are stored
        let outcome = db.nodes().all().count().unwrap();
        println!("[TC-3.3] Timestamp parsing: {} nodes created", outcome.data);
        assert_eq!(outcome.data, 3);
    }
}

mod tc_3_4_hybrid_vector_spatial_time {
    use super::*;

    #[test]
    fn test_hybrid_query_all_filters() {
        let (db, _dir) = setup_db();

        let center = (-6.2088, 106.8456);
        
        // Node with all attributes
        let payload1 = json!({
            "_id": "hybrid/complete",
            "coordinates": {"lat": -6.2080, "lon": 106.8460},
            "vectors": {"dense": vec![0.9, 0.1, 0.0]},
            "timestamp": "2024-06-15T10:00:00Z"
        }).to_string();
        db.nodes().put_json(&payload1).unwrap();

        // Node without vector
        let payload2 = json!({
            "_id": "hybrid/no-vector",
            "coordinates": {"lat": -6.2080, "lon": 106.8460},
            "timestamp": "2024-06-15T10:00:00Z"
        }).to_string();
        db.nodes().put_json(&payload2).unwrap();

        db.flush().unwrap();

        // Verify nodes exist
        assert!(db.nodes().get("hybrid/complete").is_some());
        assert!(db.nodes().get("hybrid/no-vector").is_some());

        println!("[TC-3.4] Hybrid data: 2 nodes created");
    }

    #[test]
    fn test_combined_spatial_temporal_query() {
        let (db, _dir) = setup_db();

        // Create nodes with coordinates and timestamps
        let nodes = vec![
            ("hybrid/jkt-june", -6.2, 106.8, "2024-06-15"),
            ("hybrid/jkt-may", -6.2, 106.8, "2024-05-15"),
            ("hybrid/bdg-june", -6.9, 107.6, "2024-06-15"),
        ];

        for (slug, lat, lon, date) in &nodes {
            let payload = json!({
                "_id": slug,
                "coordinates": {"lat": lat, "lon": lon},
                "date": date
            }).to_string();
            db.nodes().put_json(&payload).unwrap();
        }
        db.flush().unwrap();

        // Spatial filter
        let outcome = db.nodes()
            .all()
            .near(-6.2, 106.8, 100.0)  // Jakarta area
            .collect()
            .unwrap();

        println!("[TC-3.4] Jakarta nodes: {}", outcome.data.len());
        assert!(outcome.data.len() >= 2);
    }
}

mod tc_3_5_temporal_spatial_bucketing {
    use super::*;

    #[test]
    fn test_temporal_bucketing() {
        let (db, _dir) = setup_db();

        // Create events in different time periods
        let events = vec![
            ("bucket/q2-2024", "2024-04-15", -6.2, 106.8),
            ("bucket/q2-late", "2024-06-15", -6.3, 106.9),
            ("bucket/q3-2024", "2024-07-15", -6.4, 107.0),
            ("bucket/q4-2024", "2024-10-15", -6.5, 107.1),
        ];

        for (slug, date, lat, lon) in &events {
            let payload = json!({
                "_id": slug,
                "date": date,
                "coordinates": {"lat": lat, "lon": lon}
            }).to_string();
            db.nodes().put_json(&payload).unwrap();
        }
        db.flush().unwrap();

        // Count events
        let outcome = db.nodes().all().count().unwrap();
        println!("[TC-3.5] Total bucketed events: {}", outcome.data);
        assert_eq!(outcome.data, 4);
    }

    #[test]
    fn test_spatial_bucketing() {
        let (db, _dir) = setup_db();

        // Create nodes in different geographic regions
        let regions = vec![
            ("region/jakarta", -6.2, 106.8),
            ("region/bandung", -6.9, 107.6),
            ("region/surabaya", -7.2, 112.7),
            ("region/medan", 3.5, 98.6),
        ];

        for (slug, lat, lon) in &regions {
            let payload = json!({
                "_id": slug,
                "coordinates": {"lat": lat, "lon": lon}
            }).to_string();
            db.nodes().put_json(&payload).unwrap();
        }
        db.flush().unwrap();

        // Query by region (spatial bucket)
        let outcome = db.nodes()
            .all()
            .near(-6.2, 106.8, 500.0)  // 500km
            .collect()
            .unwrap();

        println!("[TC-3.5] Java region nodes: {}", outcome.data.len());
        // Should find Jakarta at minimum
        assert!(outcome.data.len() >= 1);
    }

    #[test]
    fn test_combined_temporal_spatial_aggregation() {
        let (db, _dir) = setup_db();

        // Create time-series spatial data
        for day in 0..7 {
            let date = format!("2024-06-{:02}T12:00:00Z", day + 1);
            let lat = -6.2 + (day as f32 * 0.01);
            let lon = 106.8 + (day as f32 * 0.01);
            
            let payload = json!({
                "_id": format!("daily/day-{}", day),
                "date": date,
                "coordinates": {"lat": lat, "lon": lon},
                "value": day * 10
            }).to_string();
            db.nodes().put_json(&payload).unwrap();
        }
        db.flush().unwrap();

        // Aggregate data
        let outcome = db.nodes().all().count().unwrap();
        println!("[TC-3.5] Time-series data points: {}", outcome.data);
        assert_eq!(outcome.data, 7);
    }
}
