//! Integration Test: Milestone 4 - Root Cause Analysis (Causal Traversal)
//!
//! Tests backward BFS traversal for finding root causes.

use sekejap::SekejapDB;
use tempfile::TempDir;

#[test]
fn test_backward_bfs_basic() {
    let temp_dir = TempDir::new().unwrap();
    let mut db = SekejapDB::new(temp_dir.path()).unwrap();

    // Create causal chain: A -> B -> C -> D (avoid hyphens in slugs for safety)
    let chain = vec!["rootcause", "intermediate1", "intermediate2", "effect"];
    for slug in &chain {
        let data = format!(r#"{{"title": "{}"}}"#, slug);
        db.write_with_options(
            slug,
            &data,
            sekejap::WriteOptions {
                publish_now: true,
                ..Default::default()
            },
        )
        .unwrap();
    }
    
    db.flush().unwrap();

    // Add edges: root -> inter1 -> inter2 -> effect
    db.add_edge("rootcause", "intermediate1", 0.8, "causes".to_string())
        .unwrap();
    db.add_edge("intermediate1", "intermediate2", 0.7, "causes".to_string())
        .unwrap();
    db.add_edge("intermediate2", "effect", 0.9, "causes".to_string())
        .unwrap();

    // Traverse backward from effect
    let result = db.traverse("effect", 10, 0.5, None).unwrap();

    // Should find all nodes in the chain (4 nodes: effect, inter2, inter1, rootcause)
    assert!(
        result.path.len() >= 4,
        "Expected at least 4 nodes, got {}",
        result.path.len()
    );
    assert!(
        result.edges.len() >= 3,
        "Expected at least 3 edges, got {}",
        result.edges.len()
    );
}

#[test]
fn test_weight_threshold_filtering() {
    let temp_dir = TempDir::new().unwrap();
    let mut db = SekejapDB::new(temp_dir.path()).unwrap();

    // Create chain with varying weights
    db.write_with_options(
        "cause-high",
        r#"{"title": "High Weight Cause"}"#,
        sekejap::WriteOptions {
            publish_now: true,
            ..Default::default()
        },
    )
    .unwrap();
    db.write_with_options(
        "cause-low",
        r#"{"title": "Low Weight Cause"}"#,
        sekejap::WriteOptions {
            publish_now: true,
            ..Default::default()
        },
    )
    .unwrap();
    db.write_with_options(
        "effect",
        r#"{"title": "Effect"}"#,
        sekejap::WriteOptions {
            publish_now: true,
            ..Default::default()
        },
    )
    .unwrap();

    db.flush().unwrap();

    db.add_edge("cause-high", "effect", 0.9, "causes".to_string())
        .unwrap();
    db.add_edge("cause-low", "effect", 0.3, "causes".to_string())
        .unwrap();

    // With high threshold, only cause-high should be found
    let result = db.traverse("effect", 10, 0.8, None).unwrap();
    assert_eq!(result.path.len(), 2); // effect + cause-high

    // With low threshold, both should be found
    let result = db.traverse("effect", 10, 0.2, None).unwrap();
    assert_eq!(result.path.len(), 3); // effect + cause-high + cause-low
}

#[test]
fn test_max_hops_limit() {
    let temp_dir = TempDir::new().unwrap();
    let mut db = SekejapDB::new(temp_dir.path()).unwrap();

    // Create chain of 5 nodes
    let nodes: Vec<String> = (0..5).map(|i| format!("node-{}", i)).collect();
    for (i, slug) in nodes.iter().enumerate() {
        db.write_with_options(
            slug,
            &format!(r#"{{"title": "Node {}", "level": {}}}"#, slug, i),
            sekejap::WriteOptions {
                publish_now: true,
                ..Default::default()
            },
        )
        .unwrap();
    }

    // Create chain
    for i in 0..4 {
        db.add_edge(&nodes[i], &nodes[i + 1], 0.8, "causes".to_string())
            .unwrap();
    }

    // Traverse with max_hops=2
    let result = db.traverse("node-4", 2, 0.5, None).unwrap();

    // Should only find node-4, node-3, node-2 (2 hops)
    assert!(result.path.len() <= 3);
}

#[test]
fn test_crime_cause_aggregation() {
    let temp_dir = TempDir::new().unwrap();
    let mut db = SekejapDB::new(temp_dir.path()).unwrap();

    // Create root causes
    let causes = vec![
        ("poverty", r#"{"title": "Poverty"}"#),
        ("unemployment", r#"{"title": "Unemployment"}"#),
        ("economic-crisis", r#"{"title": "Economic Crisis"}"#),
    ];

    for (slug, data) in &causes {
        db.write_with_options(
            slug,
            data,
            sekejap::WriteOptions {
                publish_now: true,
                ..Default::default()
            },
        )
        .unwrap();
    }

    // Create crimes linked to causes
    let crimes: Vec<(String, Vec<&str>)> = vec![
        ("theft-1".to_string(), vec!["poverty", "unemployment"]),
        ("theft-2".to_string(), vec!["poverty"]),
        ("theft-3".to_string(), vec!["economic-crisis", "poverty"]),
    ];

    for (slug, cause_slugs) in &crimes {
        db.write_with_options(
            slug,
            &format!(r#"{{"title": "Theft {}", "type": "theft"}}"#, slug),
            sekejap::WriteOptions {
                publish_now: true,
                ..Default::default()
            },
        )
        .unwrap();

        for cause in cause_slugs {
            db.add_edge(cause, slug, 0.8, "causes".to_string()).unwrap();
        }
    }

    db.flush().unwrap();

    // Aggregate: find all causes for theft crimes
    let mut all_causes = std::collections::HashSet::new();
    for (slug, _) in &crimes {
        let result = db.traverse(slug, 10, 0.5, None).unwrap();
        for node_id in result.path.into_iter() {
            all_causes.insert(node_id);
        }
    }

    // Should include poverty, unemployment, economic-crisis, and 3 crimes
    assert!(all_causes.len() >= 6);
}

#[test]
fn test_multiple_edge_types() {
    let temp_dir = TempDir::new().unwrap();
    let mut db = SekejapDB::new(temp_dir.path()).unwrap();

    db.write_with_options(
        "source",
        r#"{"title": "Source"}"#,
        sekejap::WriteOptions {
            publish_now: true,
            ..Default::default()
        },
    )
    .unwrap();
    db.write_with_options(
        "target",
        r#"{"title": "Target"}"#,
        sekejap::WriteOptions {
            publish_now: true,
            ..Default::default()
        },
    )
    .unwrap();

    db.add_edge("source", "target", 0.8, "causes".to_string())
        .unwrap();
    db.add_edge("source", "target", 0.6, "influences".to_string())
        .unwrap();

    let result = db.traverse("target", 10, 0.5, None).unwrap();

    // Should find source through either edge
    let source_id = sekejap::EntityId::new("nodes", "source");
    assert!(result.path.contains(&source_id));
}

#[test]
fn test_empty_graph_traversal() {
    let temp_dir = TempDir::new().unwrap();
    let db = SekejapDB::new(temp_dir.path()).unwrap();

    // Traverse non-existent node - start node is in path but no edges
    let result = db.traverse("nonexistent", 10, 0.5, None).unwrap();

    // Start node is always in path (even if not in graph), but no edges should be found
    assert!(
        result.edges.is_empty(),
        "Expected no edges for non-existent node"
    );
}
