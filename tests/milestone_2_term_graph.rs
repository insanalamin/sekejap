//! Integration Test: Milestone 2 - Term Graph Model
//!
//! Tests ArangoDB-style flexible node model with term nodes and edges.

use sekejap::SekejapDB;
use tempfile::TempDir;

#[test]
fn test_flexible_node_model() {
    let temp_dir = TempDir::new().unwrap();
    let mut db = SekejapDB::new(temp_dir.path()).unwrap();

    // Create a cuisine term node
    let cuisine_data = r#"{
        "title": "Italian Cuisine",
        "type": "cuisine",
        "region": "Europe"
    }"#;
    db.write_with_options(
        "italian",
        cuisine_data,
        sekejap::WriteOptions {
            publish_now: true,
            ..Default::default()
        },
    )
    .unwrap();

    // Create a restaurant node
    let restaurant_data = r#"{
        "title": "Luigi's Pizza",
        "type": "restaurant",
        "rating": 4.5
    }"#;
    db.write_with_options(
        "luigis-pizza",
        restaurant_data,
        sekejap::WriteOptions {
            publish_now: true,
            ..Default::default()
        },
    )
    .unwrap();

    // Connect restaurant to cuisine term
    db.add_edge("luigis-pizza", "italian", 0.9, "cuisine".to_string())
        .unwrap();

    // Verify edge exists
    let edges = db.graph().get_edges_from_slug("luigis-pizza");
    assert!(!edges.is_empty());
    assert_eq!(edges[0]._type, "cuisine");
}

#[test]
fn test_term_node_as_first_class() {
    let temp_dir = TempDir::new().unwrap();
    let mut db = SekejapDB::new(temp_dir.path()).unwrap();

    // Any node can be a term
    let terms = vec![
        (
            "cuisine-italian",
            r#"{"title": "Italian", "type": "cuisine"}"#,
        ),
        (
            "cuisine-japanese",
            r#"{"title": "Japanese", "type": "cuisine"}"#,
        ),
        (
            "location-jakarta",
            r#"{"title": "Jakarta", "type": "location"}"#,
        ),
        ("food-pizza", r#"{"title": "Pizza", "type": "food"}"#),
    ];

    for (slug, data) in &terms {
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

    assert_eq!(db.storage().len(), 4);
}

#[test]
fn test_edge_with_metadata() {
    let temp_dir = TempDir::new().unwrap();
    let mut db = SekejapDB::new(temp_dir.path()).unwrap();

    // Create nodes
    db.write_with_options(
        "cause-1",
        r#"{"title": "Poverty"}"#,
        sekejap::WriteOptions {
            publish_now: true,
            ..Default::default()
        },
    )
    .unwrap();
    db.write_with_options(
        "effect-1",
        r#"{"title": "Crime"}"#,
        sekejap::WriteOptions {
            publish_now: true,
            ..Default::default()
        },
    )
    .unwrap();

    // Add edge with weight
    db.add_edge("cause-1", "effect-1", 0.85, "causal".to_string())
        .unwrap();

    // Verify edge weight
    let edges = db.graph().get_edges_from_slug("cause-1");

    assert_eq!(edges.len(), 1);
    assert_eq!(edges[0].weight, 0.85);
}
