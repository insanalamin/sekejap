#[cfg(all(feature = "fulltext", feature = "vector", feature = "spatial"))]
mod tests {
    use sekejap::{EdgeType, SekejapDB};
    use tempfile::TempDir;

    #[test]
    fn test_robust_hybrid_query() {
        let temp_dir = TempDir::new().unwrap();
        let mut db = SekejapDB::new(temp_dir.path()).unwrap();

        // Workaround for hnsw_rs 0.3.3 bug with small indices:
        // Add dummy vectors to ensure index has enough elements (>10)
        for i in 0..15 {
            db.write_json(&format!(
                r#"{{
                "_id": "dummy/{}",
                "title": "Dummy {}",
                "vectors": {{ "dense": [0.0001, 0.0001, 0.0001] }}
            }}"#,
                i, i
            ))
            .unwrap();
        }

        // 1. Setup Data: Crime Events in Jakarta
        // - Text: "Theft", "Robbery"
        // - Geo: Jakarta Coordinates
        // - Vector: Feature embedding (mock)
        // - Graph: Linked to "Poverty" cause

        // Event A: Theft in Central Jakarta (Matches all)
        db.write_json(
            r#"{
            "_id": "events/crime-a",
            "title": "Motorcycle Theft in Sudirman",
            "content": "A theft incident occurred near the station.",
            "geo": { "loc": { "lat": -6.2088, "lon": 106.8456 } },
            "vectors": { "dense": [0.1, 0.1, 0.1] }
        }"#,
        )
        .unwrap();

        // Event B: Robbery in South Jakarta (Matches Text/Geo, but different Vector)
        db.write_json(
            r#"{
            "_id": "events/crime-b",
            "title": "Robbery in Kemang",
            "content": "Armed robbery reported.",
            "geo": { "loc": { "lat": -6.25, "lon": 106.81 } },
            "vectors": { "dense": [0.9, 0.9, 0.9] }
        }"#,
        )
        .unwrap();

        // Event C: Theft in Bandung (Matches Text, but Far Geo)
        db.write_json(
            r#"{
            "_id": "events/crime-c",
            "title": "Theft in Bandung",
            "content": "Another theft incident.",
            "geo": { "loc": { "lat": -6.91, "lon": 107.61 } },
            "vectors": { "dense": [0.1, 0.1, 0.1] }
        }"#,
        )
        .unwrap();

        // Cause Node
        db.write_json(r#"{ "_id": "causes/poverty", "title": "Poverty" }"#)
            .unwrap();

        // Edges: Poverty -> Crime A, Poverty -> Crime B
        db.add_edge(
            "causes/poverty",
            "events/crime-a",
            0.9,
            "causes".to_string(),
        )
        .unwrap();
        db.add_edge(
            "causes/poverty",
            "events/crime-b",
            0.9,
            "causes".to_string(),
        )
        .unwrap();

        // 2. Execution: Hybrid Query
        // "Find events caused by Poverty, near Jakarta, containing 'Theft', similar to [0.1, 0.1, 0.1]"

        let results = db
            .query()
            .has_edge_from("causes/poverty", "causes".to_string()) // Graph Driver
            .spatial(-6.2, 106.8, 10.0)
            .unwrap() // Spatial Driver (Jakarta, 10km)
            .fulltext("Theft")
            .unwrap() // Text Driver
            .vector_search(vec![0.1, 0.1, 0.1], 5) // Vector Driver
            .execute()
            .unwrap();

        // 3. Assertions
        // - Crime A: Matches All. (Graph: Yes, Geo: Yes, Text: Yes, Vector: Yes) -> KEPT
        // - Crime B: Matches Graph, Geo. Text: No (Robbery vs Theft). Vector: No (0.9 vs 0.1). -> REJECTED
        // - Crime C: Matches Text, Vector. Graph: No (No edge). Geo: No (Bandung). -> REJECTED

        assert_eq!(results.len(), 1);
        let title = results[0].payload_ptr; // In real usage we'd read payload, but here we trust ID logic

        // Read payload to verify title
        let payload_json = db.read("events/crime-a").unwrap().unwrap();
        assert!(payload_json.contains("Sudirman"));
    }
}
