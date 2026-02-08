//! TDD Test Suite for Sekejap-DB Core Components
//!
//! Run with: cargo test --test tdd
//!
//! This test file follows Test-Driven Development principles to verify
//! core components of the Sekejap-DB knowledge graph database.

use sekejap::{
    Collection, CollectionId, CollectionSchema, EdgePayload, EntityId, SekejapDB, WeightedEdge,
    WriteOptions, atoms::*, index::SlugIndex, sekejapql::SekejapQL,
};
use tempfile::TempDir;

// ============================================================================
// SECTION 1: EntityId and Collection Tests
// ============================================================================

#[cfg(test)]
mod entity_id_tests {
    use super::*;

    #[test]
    fn test_entity_id_creation() {
        let id = EntityId::new("news", "article-001");
        assert_eq!(id.as_str(), "news/article-001");
        assert_eq!(id.collection(), "news");
        assert_eq!(id.key(), "article-001");
    }

    #[test]
    fn test_entity_id_parse() {
        let id = EntityId::parse("terms/flood-2026").unwrap();
        assert_eq!(id.collection(), "terms");
        assert_eq!(id.key(), "flood-2026");
    }

    #[test]
    fn test_entity_id_parse_invalid() {
        assert!(EntityId::parse("invalid").is_err());
        assert!(EntityId::parse("a/b/c").is_err());
    }

    #[test]
    fn test_entity_id_display() {
        let id = EntityId::new("places", "jakarta");
        assert_eq!(format!("{}", id), "places/jakarta");
    }

    #[test]
    fn test_entity_id_equality() {
        let id1 = EntityId::new("news", "test-001");
        let id2 = EntityId::new("news", "test-001");
        let id3 = EntityId::new("news", "test-002");

        assert_eq!(id1, id2);
        assert_ne!(id1, id3);
    }

    #[test]
    fn test_entity_id_hash() {
        use std::collections::HashMap;

        let mut map = HashMap::new();
        let id = EntityId::new("news", "article-001");
        map.insert(id.clone(), "value");

        assert_eq!(map.get(&id), Some(&"value"));
    }
}

#[cfg(test)]
mod collection_tests {
    use super::*;

    #[test]
    fn test_collection_creation() {
        let collection = Collection::new(CollectionId::new("news"));
        assert_eq!(collection.name(), "news");
        assert!(collection.is_flex_mode());
        assert!(!collection.has_schema());
    }

    #[test]
    fn test_collection_with_schema() {
        let mut collection = Collection::new(CollectionId::new("news"));
        let schema = CollectionSchema::new();
        collection.set_schema(schema);

        assert!(collection.has_schema());
        assert!(!collection.is_flex_mode());
    }

    #[test]
    fn test_collection_metadata() {
        let mut collection = Collection::new(CollectionId::new("news"));
        collection
            .metadata_mut()
            .set("description", serde_json::json!("News articles"));

        assert_eq!(
            collection.metadata().get_str("description").unwrap(),
            "News articles"
        );
    }
}

// ============================================================================
// SECTION 2: WeightedEdge Tests
// ============================================================================

#[cfg(test)]
mod weighted_edge_tests {
    use super::*;

    #[test]
    fn test_edge_creation() {
        let edge = WeightedEdge::new(
            EntityId::new("news", "event-001"),
            EntityId::new("terms", "banjir"),
            0.85,
            "mentions".to_string(),
            100,
            1700000000000,
            None,
        );

        assert_eq!(edge.source_collection(), "news");
        assert_eq!(edge.target_collection(), "terms");
        assert_eq!(edge.source_key(), "event-001");
        assert_eq!(edge.target_key(), "banjir");
        assert_eq!(edge.weight, 0.85);
        assert_eq!(edge._type, "mentions");
    }

    #[test]
    fn test_edge_with_payload() {
        let payload = EdgePayload::new("caused_by")
            .with_title("Causal relationship")
            .with_prop("confidence", serde_json::json!(0.95))
            .with_prop("method", serde_json::json!("regression_analysis"));

        let edge = WeightedEdge::new_with_payload(
            EntityId::new("crime", "theft-001"),
            EntityId::new("causes", "poverty"),
            0.7,
            "caused_by".to_string(),
            100,
            1700000000000,
            None,
            Some(payload),
        );

        assert_eq!(edge._type, "caused_by");
        assert!(edge.payload.is_some());
        assert_eq!(
            edge.get_metadata("confidence").unwrap(),
            &serde_json::json!(0.95)
        );
    }

    #[test]
    fn test_edge_validity() {
        let edge = WeightedEdge::new(
            EntityId::new("a", "b"),
            EntityId::new("c", "d"),
            0.85,
            "hierarchy".to_string(),
            100,
            1700000000000,
            Some(1700000100000),
        );

        // Before valid range
        assert!(!edge.is_valid_at(1699999999999));

        // Inside valid range
        assert!(edge.is_valid_at(1700000005000));

        // After valid range
        assert!(!edge.is_valid_at(1700000100001));
    }

    #[test]
    fn test_weight_threshold() {
        let edge = WeightedEdge::new(
            EntityId::new("a", "b"),
            EntityId::new("c", "d"),
            0.7,
            "custom-type".to_string(),
            100,
            1700000000000,
            None,
        );

        assert!(edge.meets_threshold(0.5));
        assert!(edge.meets_threshold(0.7));
        assert!(!edge.meets_threshold(0.8));
    }

    #[test]
    fn test_edge_display() {
        let edge = WeightedEdge::new(
            EntityId::new("news", "event"),
            EntityId::new("terms", "term"),
            0.9,
            "related".to_string(),
            100,
            1700000000000,
            None,
        );

        let display = format!("{}", edge);
        assert!(display.contains("->"));
        assert!(display.contains("weight=0.90"));
    }
}

// ============================================================================
// SECTION 3: Atom Functions Tests
// ============================================================================

#[cfg(test)]
mod atom_tests {
    use super::*;

    #[test]
    fn test_haversine_distance() {
        // Jakarta to Bogor (actual distance ~43km based on coordinates)
        let dist = haversine_distance(-6.2088, 106.8456, -6.5950, 106.8170);
        // Allow 5km tolerance
        assert!(
            (dist - 43.0).abs() < 5.0,
            "Distance should be ~43km, got: {:.2}km",
            dist
        );

        // Test zero distance
        let zero_dist = haversine_distance(-6.2088, 106.8456, -6.2088, 106.8456);
        assert!(zero_dist.abs() < 0.1, "Zero distance should be ~0");
    }

    #[test]
    fn test_cosine_similarity() {
        #[cfg(feature = "vector")]
        {
            let vec1 = vec![1.0, 0.0, 0.0];
            let vec2 = vec![1.0, 0.0, 0.0];
            let sim = cosine_similarity(&vec1, &vec2);
            assert!(
                (sim - 1.0).abs() < 0.001,
                "Identical vectors should have similarity 1.0"
            );

            let vec3 = vec![0.0, 1.0, 0.0];
            let sim2 = cosine_similarity(&vec1, &vec3);
            assert!(
                sim2 < 0.5,
                "Perpendicular vectors should have low similarity"
            );
        }
    }
}

// ============================================================================
// SECTION 4: Graph Traversal Tests
// ============================================================================

#[cfg(test)]
mod graph_traversal_tests {
    use super::*;

    #[test]
    fn test_bfs_traversal() {
        let temp_dir = TempDir::new().unwrap();
        let mut db = SekejapDB::new(temp_dir.path()).unwrap();

        // Setup: west-java -> bandung, west-java -> jakarta
        db.write_with_options(
            "west-java",
            r#"{"title": "West Java"}"#,
            WriteOptions {
                publish_now: true,
                ..Default::default()
            },
        )
        .unwrap();
        db.write_with_options(
            "bandung",
            r#"{"title": "Bandung"}"#,
            WriteOptions {
                publish_now: true,
                ..Default::default()
            },
        )
        .unwrap();
        db.write_with_options(
            "jakarta",
            r#"{"title": "Jakarta"}"#,
            WriteOptions {
                publish_now: true,
                ..Default::default()
            },
        )
        .unwrap();

        db.add_edge("west-java", "bandung", 0.8, "Hierarchy".to_string())
            .unwrap();
        db.add_edge("west-java", "jakarta", 0.9, "Hierarchy".to_string())
            .unwrap();

        // Traverse from west-java (forward direction)
        let nodes = traverse_bfs(&db, "west-java", 2);

        // Should find west-java (start) + bandung + jakarta = 3 nodes
        // traverse_bfs includes the starting node
        assert_eq!(nodes.len(), 3);
    }

    #[test]
    fn test_traversal_depth_limit() {
        let temp_dir = TempDir::new().unwrap();
        let mut db = SekejapDB::new(temp_dir.path()).unwrap();

        // Setup chain: a -> b -> c -> d
        for i in ['a', 'b', 'c', 'd'] {
            db.write_with_options(
                &i.to_string(),
                r#"{"title": "Test"}"#,
                WriteOptions {
                    publish_now: true,
                    ..Default::default()
                },
            )
            .unwrap();
        }
        db.add_edge("a", "b", 1.0, "next".to_string()).unwrap();
        db.add_edge("b", "c", 1.0, "next".to_string()).unwrap();
        db.add_edge("c", "d", 1.0, "next".to_string()).unwrap();

        // With depth 2, should find a, b, c
        let nodes = traverse_bfs(&db, "a", 2);
        assert_eq!(nodes.len(), 3);

        // With depth 1, should find a, b
        let nodes = traverse_bfs(&db, "a", 1);
        assert_eq!(nodes.len(), 2);
    }
}

// ============================================================================
// SECTION 5: SekejapQL Tests
// ============================================================================

#[cfg(test)]
mod sekejapql_tests {
    use super::*;

    #[test]
    fn test_simple_filter_query() {
        let temp_dir = TempDir::new().unwrap();
        let mut db = SekejapDB::new(temp_dir.path()).unwrap();

        // Write directly to Tier 2 for query tests
        db.write_with_options(
            "italian",
            r#"{"title": "Italian"}"#,
            WriteOptions {
                publish_now: true,
                ..Default::default()
            },
        )
        .unwrap();
        db.write_with_options(
            "restaurant-1",
            r#"{"title": "Luigi's Pizza"}"#,
            WriteOptions {
                publish_now: true,
                ..Default::default()
            },
        )
        .unwrap();
        db.add_edge("restaurant-1", "italian", 0.9, "related".to_string())
            .unwrap();

        let engine = SekejapQL::new(&db);

        // First verify we can get all nodes
        let all_query = r#"{}"#;
        let all_result = engine.execute(all_query).unwrap();
        // Should find italian, restaurant-1 = 2 nodes
        assert_eq!(all_result.nodes.len(), 2, "Expected 2 nodes in DB");

        // Now test the filter - restaurant-1 has edge TO italian
        // Note: edgeType is case-sensitive, using lowercase "related"
        let query = r#"{
            "filters": [
                {"type": "edge_to", "target": "italian", "edgeType": "related"}
            ]
        }"#;

        let result = engine.execute(query).unwrap();

        // Should find node that has an edge to "italian" (restaurant-1)
        assert_eq!(
            result.nodes.len(),
            1,
            "Expected 1 node with edge to italian"
        );
    }

    #[test]
    fn test_security_limits() {
        let temp_dir = TempDir::new().unwrap();
        let db = SekejapDB::new(temp_dir.path()).unwrap();

        let engine = SekejapQL::builder(&db)
            .max_nodes(5)
            .timeout_ms(1000)
            .read_only(true)
            .build();

        // Engine created successfully with custom limits
        // Security limits are enforced during query execution
    }

    #[test]
    fn test_traversal_query() {
        let temp_dir = TempDir::new().unwrap();
        let mut db = SekejapDB::new(temp_dir.path()).unwrap();

        // Write directly to Tier 2 for query tests
        db.write_with_options(
            "west-java",
            r#"{"title": "West Java"}"#,
            WriteOptions {
                publish_now: true,
                ..Default::default()
            },
        )
        .unwrap();
        db.write_with_options(
            "bandung",
            r#"{"title": "Bandung"}"#,
            WriteOptions {
                publish_now: true,
                ..Default::default()
            },
        )
        .unwrap();
        db.write_with_options(
            "jakarta",
            r#"{"title": "Jakarta"}"#,
            WriteOptions {
                publish_now: true,
                ..Default::default()
            },
        )
        .unwrap();

        db.add_edge("west-java", "bandung", 0.8, "Hierarchy".to_string())
            .unwrap();
        db.add_edge("west-java", "jakarta", 0.9, "Hierarchy".to_string())
            .unwrap();

        let engine = SekejapQL::new(&db);
        let query = r#"{
            "traversal": {
                "start": "west-java",
                "direction": "forward",
                "maxDepth": 2
            }
        }"#;

        let result = engine.execute(query).unwrap();

        // Should find west-java (start) + bandung + jakarta = 3 nodes
        // Traversal includes the starting node
        assert_eq!(result.nodes.len(), 3);
    }

    #[test]
    fn test_limit_and_offset() {
        let temp_dir = TempDir::new().unwrap();
        let mut db = SekejapDB::new(temp_dir.path()).unwrap();

        // Create multiple nodes
        for i in 1..=10 {
            db.write_with_options(
                &format!("node-{}", i),
                &format!(r#"{{"title": "Node {}"}}"#, i),
                WriteOptions {
                    publish_now: true,
                    ..Default::default()
                },
            )
            .unwrap();
        }

        let engine = SekejapQL::new(&db);
        let query = r#"{
            "limit": 5,
            "offset": 2
        }"#;

        let result = engine.execute(query).unwrap();
        assert_eq!(result.nodes.len(), 5);
    }
}

// ============================================================================
// SECTION 6: Index Tests
// ============================================================================

#[cfg(test)]
mod index_tests {
    use super::*;

    #[test]
    fn test_slug_index_insert_get() {
        let temp_dir = TempDir::new().unwrap();
        let index = SlugIndex::new(&temp_dir.path().join("slugs.redb")).unwrap();

        index.insert(12345, 1);
        index.insert(67890, 2);

        assert_eq!(index.get(12345), Some(1));
        assert_eq!(index.get(67890), Some(2));
        assert_eq!(index.get(99999), None);
    }

    #[test]
    fn test_slug_index_contains() {
        let temp_dir = TempDir::new().unwrap();
        let index = SlugIndex::new(&temp_dir.path().join("slugs.redb")).unwrap();

        index.insert(12345, 1);

        assert!(index.contains(12345));
        // contains() returns true if key exists (verify via get)
        assert!(index.get(99999).is_none());
    }

    #[test]
    fn test_slug_index_remove() {
        let temp_dir = TempDir::new().unwrap();
        let index = SlugIndex::new(&temp_dir.path().join("slugs.redb")).unwrap();

        index.insert(12345, 1);
        assert_eq!(index.get(12345), Some(1));

        index.remove(12345);
        assert_eq!(index.get(12345), None);
    }

    #[test]
    fn test_slug_index_len() {
        let temp_dir = TempDir::new().unwrap();
        let index = SlugIndex::new(&temp_dir.path().join("slugs.redb")).unwrap();

        assert!(index.is_empty());

        index.insert(1, 1);
        index.insert(2, 2);
        index.insert(3, 3);

        assert_eq!(index.len(), 3);
        assert!(!index.is_empty());
    }

    #[test]
    fn test_slug_index_clear() {
        let temp_dir = TempDir::new().unwrap();
        let index = SlugIndex::new(&temp_dir.path().join("slugs.redb")).unwrap();

        index.insert(1, 1);
        index.insert(2, 2);

        index.clear();

        assert!(index.is_empty());
        assert_eq!(index.len(), 0);
    }

    #[test]
    fn test_slug_index_iter() {
        let temp_dir = TempDir::new().unwrap();
        let index = SlugIndex::new(&temp_dir.path().join("slugs.redb")).unwrap();

        index.insert(100, 1);
        index.insert(200, 2);
        index.insert(300, 3);

        let entries = index.iter();
        assert_eq!(entries.len(), 3);
    }
}

// ============================================================================
// SECTION 7: Database CRUD Tests
// ============================================================================

#[cfg(test)]
mod database_crud_tests {
    use super::*;

    #[test]
    fn test_write_and_read() {
        let temp_dir = TempDir::new().unwrap();
        let mut db = SekejapDB::new(temp_dir.path()).unwrap();

        // Use publish_now to write directly to Tier 2 for immediate reading
        db.write_with_options(
            "article-1",
            r#"{"title": "Flood in Jakarta", "content": "Heavy rain caused flooding"}"#,
            WriteOptions {
                publish_now: true,
                ..Default::default()
            },
        )
        .unwrap();

        let result = db.read("article-1").unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_add_edge() {
        let temp_dir = TempDir::new().unwrap();
        let mut db = SekejapDB::new(temp_dir.path()).unwrap();

        // Use publish_now to write directly to Tier 2
        db.write_with_options(
            "source",
            r#"{"title": "Source"}"#,
            WriteOptions {
                publish_now: true,
                ..Default::default()
            },
        )
        .unwrap();
        db.write_with_options(
            "target",
            r#"{"title": "Target"}"#,
            WriteOptions {
                publish_now: true,
                ..Default::default()
            },
        )
        .unwrap();

        db.add_edge("source", "target", 0.8, "related".to_string())
            .unwrap();

        // Verify edge was added by traversing
        let edges = get_edges_from(&db, "source");
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].weight, 0.8);
    }

    #[test]
    fn test_delete() {
        let temp_dir = TempDir::new().unwrap();
        let mut db = SekejapDB::new(temp_dir.path()).unwrap();

        // Use publish_now to write directly to Tier 2
        db.write_with_options(
            "test-node",
            r#"{"title": "Test"}"#,
            WriteOptions {
                publish_now: true,
                ..Default::default()
            },
        )
        .unwrap();
        assert!(db.read("test-node").unwrap().is_some());

        db.delete("test-node").unwrap();
        assert!(db.read("test-node").unwrap().is_none());
    }
}
