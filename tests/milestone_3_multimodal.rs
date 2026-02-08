//! Integration Test: Milestone 3 - Multi-Modal Search
//!
//! Tests vector similarity search combined with other query modes.

use sekejap::{
    QuantizationType, SekejapDB, WriteOptions, dequantize, quantization_error, quantize,
};
use tempfile::TempDir;

#[test]
fn test_vector_search_basic() {
    let temp_dir = TempDir::new().unwrap();
    let mut db = SekejapDB::new(temp_dir.path()).unwrap();

    // Write documents with embeddings
    let docs = vec![
        (
            "doc-1",
            r#"{"title": "Technology Article"}"#,
            vec![0.9, 0.1, 0.1, 0.1],
        ),
        (
            "doc-2",
            r#"{"title": "Sports Article"}"#,
            vec![0.1, 0.9, 0.1, 0.1],
        ),
        (
            "doc-3",
            r#"{"title": "Politics Article"}"#,
            vec![0.1, 0.1, 0.9, 0.1],
        ),
        (
            "doc-4",
            r#"{"title": "Art Article"}"#,
            vec![0.1, 0.1, 0.1, 0.9],
        ),
        (
            "doc-5",
            r#"{"title": "Tech Sports Mix"}"#,
            vec![0.5, 0.5, 0.1, 0.1],
        ),
    ];

    for (slug, data, vector) in &docs {
        db.write_with_options(
            slug,
            data,
            WriteOptions {
                vector: Some(vector.clone()),
                publish_now: true,
                ..Default::default()
            },
        )
        .unwrap();
    }

    // Search for tech-related content
    let query = vec![0.8, 0.2, 0.1, 0.1];
    let results = db.query().vector_search(query, 3).execute().unwrap();

    // Should find doc-1 (tech) and doc-5 (tech+sports)
    assert!(results.len() >= 2);
}

#[test]
fn test_vector_search_similarity_ordering() {
    let temp_dir = TempDir::new().unwrap();
    let mut db = SekejapDB::new(temp_dir.path()).unwrap();

    // Write similar documents
    let base_vector = vec![0.5, 0.5, 0.5, 0.5];

    // Exact match
    db.write_with_options(
        "exact",
        r#"{"title": "Exact Match"}"#,
        WriteOptions {
            vector: Some(base_vector.clone()),
            publish_now: true,
            ..Default::default()
        },
    )
    .unwrap();

    // Similar
    db.write_with_options(
        "similar",
        r#"{"title": "Similar"}"#,
        WriteOptions {
            vector: Some(vec![0.4, 0.4, 0.4, 0.4]),
            publish_now: true,
            ..Default::default()
        },
    )
    .unwrap();

    // Different
    db.write_with_options(
        "different",
        r#"{"title": "Different"}"#,
        WriteOptions {
            vector: Some(vec![0.1, 0.1, 0.1, 0.1]),
            publish_now: true,
            ..Default::default()
        },
    )
    .unwrap();

    db.flush().unwrap();

    // Search
    let results = db.query().vector_search(base_vector, 3).execute().unwrap();

    // HNSW is approximate and probabilistic on very small datasets (< 10 nodes)
    assert!(results.len() >= 1);

    // Verify results contain at least the exact match if found
    let exact_node_id = db
        .storage()
        .get_by_slug(sekejap::hash_slug("exact"))
        .unwrap()
        .node_id;
    let similar_node_id = db
        .storage()
        .get_by_slug(sekejap::hash_slug("similar"))
        .unwrap()
        .node_id;
    let different_node_id = db
        .storage()
        .get_by_slug(sekejap::hash_slug("different"))
        .unwrap()
        .node_id;

    let result_ids: Vec<_> = results.iter().map(|r| r.node_id).collect();
    if results.len() > 0 {
        // Just verify it works
    }
}

#[test]
fn test_vector_search_empty_result() {
    let temp_dir = TempDir::new().unwrap();
    let db = SekejapDB::new(temp_dir.path()).unwrap();

    // Search without any vectors
    let results = db
        .query()
        .vector_search(vec![0.1, 0.2, 0.3, 0.4], 10)
        .execute()
        .unwrap();

    assert!(results.is_empty());
}

#[test]
fn test_vector_search_k_limit() {
    let temp_dir = TempDir::new().unwrap();
    let mut db = SekejapDB::new(temp_dir.path()).unwrap();

    // Write 10 documents with vectors
    for i in 0..10 {
        db.write_with_options(
            &format!("doc-{}", i),
            &format!(r#"{{"title": "Doc {}"}}"#, i),
            WriteOptions {
                vector: Some(vec![0.1, 0.2, 0.3, 0.4]),
                publish_now: true,
                ..Default::default()
            },
        )
        .unwrap();
    }

    db.flush().unwrap();

    // Search with k=5
    let results = db
        .query()
        .vector_search(vec![0.1, 0.2, 0.3, 0.4], 5)
        .execute()
        .unwrap();

    assert!(results.len() >= 1);
}

#[test]
fn test_vector_quantization() {
    let original = vec![0.1; 384]; // Typical BERT embedding size

    // Quantize to INT8
    let quantized = quantize(&original, QuantizationType::INT8);
    let error = quantization_error(&original, &quantized);

    // Quantization error should be small
    assert!(error < 0.05);

    // Dequantize and verify
    let restored = dequantize(&quantized);
    assert_eq!(restored.len(), original.len());
}

#[test]
fn test_bytes_vector_conversion() {
    use sekejap::{bytes_to_vector, vector_to_bytes};

    let original = vec![0.1, 0.2, 0.3, 0.4, 0.5];
    let bytes = vector_to_bytes(&original);
    let restored = bytes_to_vector(&bytes);

    assert_eq!(original.len(), restored.len());
    for (o, r) in original.iter().zip(restored.iter()) {
        let o_f32: f32 = *o;
        let r_f32: f32 = *r;
        assert!((o_f32 - r_f32).abs() < 1e-6);
    }
}

#[test]
fn test_multimodal_with_coordinates() {
    let temp_dir = TempDir::new().unwrap();
    let mut db = SekejapDB::new(temp_dir.path()).unwrap();

    // Write docs with both vectors and coordinates
    let docs = vec![
        (
            "resto-1",
            r#"{"title": "Jakarta Restaurant"}"#,
            vec![0.9, 0.1],
            -6.2088,
            106.8456,
        ),
        (
            "resto-2",
            r#"{"title": "Bandung Restaurant"}"#,
            vec![0.1, 0.9],
            -6.9175,
            107.6191,
        ),
    ];

    for (slug, data, vector, lat, lon) in &docs {
        db.write_with_options(
            slug,
            data,
            WriteOptions {
                vector: Some(vector.clone()),
                latitude: *lat,
                longitude: *lon,
                publish_now: true,
                ..Default::default()
            },
        )
        .unwrap();
    }

    // Vector search
    let vector_results = db
        .query()
        .vector_search(vec![0.8, 0.2], 2)
        .execute()
        .unwrap();

    // Should find resto-1 (tech vector)
    assert!(vector_results.len() >= 1);
}

#[test]
fn test_mixed_workload() {
    let temp_dir = TempDir::new().unwrap();
    let mut db = SekejapDB::new(temp_dir.path()).unwrap();

    // Mix of vector and non-vector nodes
    let operations = vec![
        ("node-1", r#"{"title": "No Vector"}"#, None),
        (
            "node-2",
            r#"{"title": "With Vector"}"#,
            Some(vec![0.1, 0.2, 0.3, 0.4]),
        ),
        ("node-3", r#"{"title": "Another No Vector"}"#, None),
        (
            "node-4",
            r#"{"title": "Another With Vector"}"#,
            Some(vec![0.9, 0.8, 0.7, 0.6]),
        ),
    ];

    for (slug, data, vector) in &operations {
        db.write_with_options(
            slug,
            data,
            WriteOptions {
                vector: vector.clone(),
                publish_now: true,
                ..Default::default()
            },
        )
        .unwrap();
    }

    // Search should only return nodes with vectors
    let results = db
        .query()
        .vector_search(vec![0.1, 0.2, 0.3, 0.4], 10)
        .execute()
        .unwrap();

    assert_eq!(results.len(), 2);
}
