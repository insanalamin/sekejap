#[cfg(feature = "fulltext")]
mod tests {
    use sekejap::{SekejapDB, WriteOptions};
    use std::path::Path;
    use tempfile::TempDir;

    #[test]
    fn test_fulltext_integration() {
        let temp_dir = TempDir::new().unwrap();
        let mut db = SekejapDB::new(temp_dir.path()).unwrap();

        // Write documents
        let doc1_id = db
            .write(
                "doc-1",
                r#"{
            "title": "Jakarta Floods",
            "content": "Heavy rain caused severe flooding in Jakarta today."
        }"#,
            )
            .unwrap();

        let doc2_id = db
            .write(
                "doc-2",
                r#"{
            "title": "Bandung Traffic",
            "content": "Traffic is smooth in Bandung this morning."
        }"#,
            )
            .unwrap();

        let doc3_id = db
            .write(
                "doc-3",
                r#"{
            "title": "Jakarta Traffic Update",
            "content": "Jakarta traffic is heavy due to the rain."
        }"#,
            )
            .unwrap();

        // Search for "Jakarta" (should find doc-1 and doc-3)
        let results = db.search_text("Jakarta", 10).unwrap();
        assert_eq!(results.len(), 2);
        assert!(results.contains(&doc1_id));
        assert!(results.contains(&doc3_id));
        assert!(!results.contains(&doc2_id));

        // Search for "rain" (should find doc-1 and doc-3)
        let results = db.search_text("rain", 10).unwrap();
        assert_eq!(results.len(), 2);
        assert!(results.contains(&doc1_id));
        assert!(results.contains(&doc3_id));

        // Search for "Bandung" (should find doc-2)
        let results = db.search_text("Bandung", 10).unwrap();
        assert_eq!(results.len(), 1);
        assert!(results.contains(&doc2_id));
    }

    #[test]
    fn test_fulltext_write_batch() {
        let temp_dir = TempDir::new().unwrap();
        let mut db = SekejapDB::new(temp_dir.path()).unwrap();

        let items = vec![
            (
                "item-1".to_string(),
                r#"{"title": "Item One", "content": "First item content"}"#.to_string(),
            ),
            (
                "item-2".to_string(),
                r#"{"title": "Item Two", "content": "Second item content"}"#.to_string(),
            ),
            (
                "item-3".to_string(),
                r#"{"title": "Item Three", "content": "Third item content"}"#.to_string(),
            ),
        ];

        let node_ids = db.write_batch(items, true).unwrap();

        // Search for "content" (should find all 3)
        let results = db.search_text("content", 10).unwrap();
        assert_eq!(results.len(), 3);

        // Search for "Second" (should find item-2)
        let results = db.search_text("Second", 10).unwrap();
        assert_eq!(results.len(), 1);
        assert!(results.contains(&node_ids[1]));
    }

    #[test]
    fn test_fulltext_write_json() {
        let temp_dir = TempDir::new().unwrap();
        let mut db = SekejapDB::new(temp_dir.path()).unwrap();

        let node_id = db
            .write_json(
                r#"{
            "_id": "news/test-fulltext",
            "title": "Fulltext Test",
            "content": "Testing the write_json integration with fulltext search.",
            "props": { "author": "tester" }
        }"#,
            )
            .unwrap();

        let results = db.search_text("integration", 10).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], node_id);
    }
}
