//! Integration Test: Milestone 1 - Basic CRUD Operations
//!
//! Tests basic write, read, update, and delete operations.

use sekejap::SekejapDB;
use tempfile::TempDir;

#[test]
fn test_write_single_event() {
    let temp_dir = TempDir::new().unwrap();
    let mut db = SekejapDB::new(temp_dir.path()).unwrap();

    let crime_data = r#"{
        "title": "Theft Incident - Jakarta",
        "entities": ["person", "vehicle"],
        "coordinates": {"lat": -6.2088, "lon": 106.8456}
    }"#;

    let node_id = db.write("jakarta-crime-001", crime_data).unwrap();
    assert!(node_id > 0);

    // Verify it's in ingestion
    assert_eq!(db.ingestion().len(), 1);
}

#[test]
fn test_write_many_events() {
    let temp_dir = TempDir::new().unwrap();
    let mut db = SekejapDB::new(temp_dir.path()).unwrap();

    let events: Vec<(String, String)> = vec![
        ("event-1".to_string(), r#"{"title": "Event 1"}"#.to_string()),
        ("event-2".to_string(), r#"{"title": "Event 2"}"#.to_string()),
        ("event-3".to_string(), r#"{"title": "Event 3"}"#.to_string()),
    ];

    let node_ids = db.write_many(events).unwrap();
    assert_eq!(node_ids.len(), 3);
    assert_eq!(db.ingestion().len(), 3);
}

#[test]
fn test_read_event() {
    let temp_dir = TempDir::new().unwrap();
    let mut db = SekejapDB::new(temp_dir.path()).unwrap();

    let data = r#"{
        "title": "Test Event",
        "tags": ["test", "sample"]
    }"#;

    db.write_with_options("test-event", data, sekejap::WriteOptions {
        publish_now: true,
        ..Default::default()
    }).unwrap();

    // Read from Tier 2
    let result = db.read("test-event").unwrap();
    assert!(result.is_some());
    assert!(result.unwrap().contains("Test Event"));
}

#[test]
fn test_read_nonexistent() {
    let temp_dir = TempDir::new().unwrap();
    let db = SekejapDB::new(temp_dir.path()).unwrap();

    let result = db.read("nonexistent").unwrap();
    assert!(result.is_none());
}

#[test]
fn test_update_event() {
    let temp_dir = TempDir::new().unwrap();
    let mut db = SekejapDB::new(temp_dir.path()).unwrap();

    db.write_with_options("test-event", r#"{"title": "Original"}"#,
        sekejap::WriteOptions { publish_now: true, ..Default::default() }
    ).unwrap();

    // Update
    db.write_with_options("test-event", r#"{"title": "Updated"}"#,
        sekejap::WriteOptions { publish_now: true, ..Default::default() }
    ).unwrap();

    // Read updated
    let result = db.read("test-event").unwrap().unwrap();
    assert!(result.contains("Updated"));
}

#[test]
fn test_delete_event() {
    let temp_dir = TempDir::new().unwrap();
    let mut db = SekejapDB::new(temp_dir.path()).unwrap();

    db.write_with_options("test-event", r#"{"title": "To Delete"}"#,
        sekejap::WriteOptions { publish_now: true, ..Default::default() }
    ).unwrap();

    // Delete
    db.delete("test-event").unwrap();

    // Should not be readable
    let result = db.read("test-event").unwrap();
    assert!(result.is_none());
}


#[test]
fn test_write_with_coordinates() {
    let temp_dir = TempDir::new().unwrap();
    let mut db = SekejapDB::new(temp_dir.path()).unwrap();

    let data = r#"{"title": "Location Event"}"#;

    let node_id = db.write_with_options("location-1", data,
        sekejap::WriteOptions {
            latitude: -6.2088,
            longitude: 106.8456,
            publish_now: true,
            ..Default::default()
        }
    ).unwrap();

    assert!(node_id > 0);
}

#[test]
fn test_mvcc_versioning() {
    let temp_dir = TempDir::new().unwrap();
    let mut db = SekejapDB::new(temp_dir.path()).unwrap();

    // First write
    db.write_with_options("test", r#"{"title": "V1", "version": 1}"#,
        sekejap::WriteOptions { publish_now: true, ..Default::default() }
    ).unwrap();

    // Second write (update)
    db.write_with_options("test", r#"{"title": "V2", "version": 2}"#,
        sekejap::WriteOptions { publish_now: true, ..Default::default() }
    ).unwrap();

    // Read should return latest
    let result = db.read("test").unwrap().unwrap();
    assert!(result.contains("V2"));
}
