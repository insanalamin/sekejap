//! Integration Test: Milestone 5 - Backup and Restore
//!
//! Tests backup/restore functionality for data durability.

use sekejap::SekejapDB;
use tempfile::TempDir;
use std::fs;

#[test]
fn test_backup_and_restore() {
    let temp_dir = TempDir::new().unwrap();
    let mut db = SekejapDB::new(temp_dir.path()).unwrap();

    // Write test data
    let events = vec![
        ("event-1", r#"{"title": "Event 1", "data": "value1"}"#),
        ("event-2", r#"{"title": "Event 2", "data": "value2"}"#),
        ("event-3", r#"{"title": "Event 3", "data": "value3"}"#),
    ];

    for (slug, data) in &events {
        db.write_with_options(slug, data,
            sekejap::WriteOptions { publish_now: true, ..Default::default() }
        ).unwrap();
    }

    // Add edges
    db.add_edge("event-1", "event-2", 0.8, "causal".to_string())
        .unwrap();

    // Backup
    let backup_path = temp_dir.path().join("backup.json");
    db.backup(&backup_path).unwrap();

    // Verify backup file exists
    assert!(backup_path.exists());

    // Create new database and restore
    let restore_dir = temp_dir.path().join("restore");
    fs::create_dir_all(&restore_dir).unwrap();
    let mut restore_db = SekejapDB::new(&restore_dir).unwrap();
    restore_db.restore(&backup_path).unwrap();

    // Verify restored data
    assert_eq!(restore_db.storage().len(), 3);

    let result = restore_db.read("event-1").unwrap();
    assert!(result.is_some());
    assert!(result.unwrap().contains("Event 1"));
}

#[test]
fn test_backup_preserves_edges() {
    let temp_dir = TempDir::new().unwrap();
    let mut db = SekejapDB::new(temp_dir.path()).unwrap();

    // Create nodes and edges
    db.write_with_options("cause", r#"{"title": "Cause"}"#,
        sekejap::WriteOptions { publish_now: true, ..Default::default() }
    ).unwrap();
    db.write_with_options("effect", r#"{"title": "Effect"}"#,
        sekejap::WriteOptions { publish_now: true, ..Default::default() }
    ).unwrap();
    db.add_edge("cause", "effect", 0.85, "causal".to_string()).unwrap();

    // Backup
    let backup_path = temp_dir.path().join("backup_edges.json");
    db.backup(&backup_path).unwrap();

    // Restore
    let restore_dir = temp_dir.path().join("restore2");
    fs::create_dir_all(&restore_dir).unwrap();
    let mut restore_db = SekejapDB::new(&restore_dir).unwrap();
    restore_db.restore(&backup_path).unwrap();

    // Verify edge exists in restored database
    let edges = restore_db.graph().get_edges_from_slug("cause");

    assert_eq!(edges.len(), 1);
    assert_eq!(edges[0].weight, 0.85);
}

#[test]
fn test_backup_format() {
    let temp_dir = TempDir::new().unwrap();
    let mut db = SekejapDB::new(temp_dir.path()).unwrap();

    db.write_with_options("test", r#"{"title": "Test"}"#,
        sekejap::WriteOptions { publish_now: true, ..Default::default() }
    ).unwrap();

    // Backup
    let backup_path = temp_dir.path().join("backup_format.json");
    db.backup(&backup_path).unwrap();

    // Read backup and verify it's valid JSON
    let content = fs::read_to_string(&backup_path).unwrap();
    let json: serde_json::Value = serde_json::from_str(&content).unwrap();

    assert!(json.is_object());
    assert!(json.get("nodes").is_some());
    assert!(json.get("edges").is_some());
}

#[test]
fn test_multiple_backups() {
    let temp_dir = TempDir::new().unwrap();
    let mut db = SekejapDB::new(temp_dir.path()).unwrap();

    // Initial data
    db.write_with_options("v1", r#"{"title": "Version 1"}"#,
        sekejap::WriteOptions { publish_now: true, ..Default::default() }
    ).unwrap();

    // First backup
    let backup1 = temp_dir.path().join("backup1.json");
    db.backup(&backup1).unwrap();

    // Add more data
    db.write_with_options("v2", r#"{"title": "Version 2"}"#,
        sekejap::WriteOptions { publish_now: true, ..Default::default() }
    ).unwrap();

    // Second backup
    let backup2 = temp_dir.path().join("backup2.json");
    db.backup(&backup2).unwrap();

    // Restore first backup
    let restore_dir = temp_dir.path().join("restore1");
    fs::create_dir_all(&restore_dir).unwrap();
    let mut restore1 = SekejapDB::new(&restore_dir).unwrap();
    restore1.restore(&backup1).unwrap();

    assert_eq!(restore1.storage().len(), 1);

    // Restore second backup
    let restore_dir2 = temp_dir.path().join("restore2");
    fs::create_dir_all(&restore_dir2).unwrap();
    let mut restore2 = SekejapDB::new(&restore_dir2).unwrap();
    restore2.restore(&backup2).unwrap();

    assert_eq!(restore2.storage().len(), 2);
}
