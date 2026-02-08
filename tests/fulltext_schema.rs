#[cfg(feature = "fulltext")]
mod tests {
    use sekejap::SekejapDB;
    use tempfile::TempDir;

    #[test]
    fn test_schema_aware_fulltext() {
        let temp_dir = TempDir::new().unwrap();
        let mut db = SekejapDB::new(temp_dir.path()).unwrap();

        // 1. Define Collection Schema with custom fulltext fields
        db.define_collection(
            r#"{
            "books": {
                "fulltext": ["title", "props.author", "props.genre"]
            }
        }"#,
        )
        .unwrap();

        // 2. Write Document
        let doc_id = db
            .write_json(
                r#"{
            "_id": "books/dune",
            "title": "Dune",
            "content": "A story about spice.",
            "props": {
                "author": "Frank Herbert",
                "genre": "Sci-Fi",
                "year": 1965
            }
        }"#,
            )
            .unwrap();

        // 3. Search by Author (should work because it's in schema)
        // Note: JSON fields in Tantivy are searched as `field.key:value`
        let results = db
            .search_text("attributes.props.author:\"Frank Herbert\"", 10)
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], doc_id);

        // 4. Search by Genre
        let results = db.search_text("attributes.props.genre:Sci-Fi", 10).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], doc_id);

        // 5. Search by Year (should NOT work because not in schema)
        // Tantivy might error on query parsing if field doesn't exist, or return 0
        let results = db
            .search_text("attributes.props.year:1965", 10)
            .unwrap_or_default();
        assert_eq!(results.len(), 0);
    }
}
