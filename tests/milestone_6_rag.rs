//! Integration Test: Milestone 6 - RAG (Retrieval Augmented Generation)
//!
//! Tests vector-based semantic search for LLM augmentation.

use sekejap::{QuantizationType, SekejapDB, WriteOptions, dequantize, quantize};
use tempfile::TempDir;

#[test]
fn test_rag_document_storage() {
    let temp_dir = TempDir::new().unwrap();
    let mut db = SekejapDB::new(temp_dir.path()).unwrap();

    // Store document chunks with embeddings
    let chunks = vec![
        (
            "doc-001",
            r#"{"title": "Introduction to Machine Learning",
               "content": "Machine learning is a subset of artificial intelligence.",
               "source": "textbook.pdf",
               "page": 1}"#,
            vec![0.9, 0.1, 0.1, 0.1, 0.1], // AI/ML related
        ),
        (
            "doc-002",
            r#"{"title": "Deep Learning Basics",
               "content": "Deep learning uses neural networks with multiple layers.",
               "source": "textbook.pdf",
               "page": 45}"#,
            vec![0.8, 0.2, 0.1, 0.1, 0.1], // Deep learning related
        ),
        (
            "doc-003",
            r#"{"title": "Natural Language Processing",
               "content": "NLP enables computers to understand human language.",
               "source": "paper.pdf",
               "page": 12}"#,
            vec![0.1, 0.9, 0.1, 0.1, 0.1], // NLP related
        ),
    ];

    for (slug, data, embedding) in &chunks {
        db.write_with_options(
            slug,
            data,
            WriteOptions {
                vector: Some(embedding.clone()),
                publish_now: true,
                ..Default::default()
            },
        )
        .unwrap();
    }

    assert_eq!(db.storage().len(), 3);
}

#[test]
fn test_rag_semantic_search() {
    let temp_dir = TempDir::new().unwrap();
    let mut db = SekejapDB::new(temp_dir.path()).unwrap();

    // Store documents with different topics
    let docs = vec![
        (
            "python-guide",
            r#"{"title": "Python Programming Guide"}"#,
            vec![0.9, 0.05, 0.05, 0.05, 0.05],
        ), // Programming
        (
            "rust-guide",
            r#"{"title": "Rust Programming Guide"}"#,
            vec![0.85, 0.1, 0.1, 0.05, 0.05],
        ), // Programming
        (
            "cooking-recipe",
            r#"{"title": "Italian Cooking Recipe"}"#,
            vec![0.05, 0.9, 0.05, 0.05, 0.05],
        ), // Cooking
    ];

    for (slug, data, embedding) in &docs {
        db.write_with_options(
            slug,
            data,
            WriteOptions {
                vector: Some(embedding.clone()),
                publish_now: true,
                ..Default::default()
            },
        )
        .unwrap();
    }

    // Query about programming
    let query = vec![0.95, 0.05, 0.05, 0.05, 0.05];
    let results = db.query().vector_search(query, 2).execute().unwrap();

    // Should return programming guides
    assert!(results.len() >= 1);
}

#[test]
fn test_rag_with_metadata_filtering() {
    let temp_dir = TempDir::new().unwrap();
    let mut db = SekejapDB::new(temp_dir.path()).unwrap();

    // Store docs from different sources
    let docs = vec![
        (
            "doc-a",
            r#"{"title": "Doc A", "source": "source1"}"#,
            vec![0.5, 0.5, 0.5, 0.5],
        ),
        (
            "doc-b",
            r#"{"title": "Doc B", "source": "source2"}"#,
            vec![0.5, 0.5, 0.5, 0.5],
        ),
    ];

    for (slug, data, embedding) in &docs {
        db.write_with_options(
            slug,
            data,
            WriteOptions {
                vector: Some(embedding.clone()),
                publish_now: true,
                ..Default::default()
            },
        )
        .unwrap();
    }

    // Search
    let results = db
        .query()
        .vector_search(vec![0.5, 0.5, 0.5, 0.5], 2)
        .execute()
        .unwrap();

    assert_eq!(results.len(), 2);
}

#[test]
fn test_rag_context_retrieval() {
    let temp_dir = TempDir::new().unwrap();
    let mut db = SekejapDB::new(temp_dir.path()).unwrap();

    // Simulate RAG document chunks
    let context_chunks = vec![
        (
            "chunk-1",
            "The capital of France is Paris.",
            vec![0.8, 0.2, 0.1, 0.1],
        ),
        (
            "chunk-2",
            "Paris is known for the Eiffel Tower.",
            vec![0.7, 0.3, 0.2, 0.1],
        ),
        (
            "chunk-3",
            "The population of Tokyo is about 14 million.",
            vec![0.1, 0.1, 0.9, 0.1],
        ),
    ];

    for (slug, content, embedding) in &context_chunks {
        let data = format!(
            r#"{{"title": "{}", "content": "{}", "type": "chunk"}}"#,
            slug, content
        );
        db.write_with_options(
            slug,
            &data,
            WriteOptions {
                vector: Some(embedding.clone()),
                publish_now: true,
                ..Default::default()
            },
        )
        .unwrap();
    }

    // Query about France/Paris
    let query = vec![0.9, 0.2, 0.1, 0.1];
    let results = db.query().vector_search(query, 2).execute().unwrap();

    // Should retrieve France/Paris related chunks
    assert!(results.len() >= 1);
}

#[test]
fn test_rag_prompt_construction() {
    let temp_dir = TempDir::new().unwrap();
    let mut db = SekejapDB::new(temp_dir.path()).unwrap();

    // Store relevant context
    db.write_with_options(
        "context-1",
        r#"{"title": "Context 1", "content": "Python uses indentation for blocks."}"#,
        WriteOptions {
            vector: Some(vec![0.9, 0.1, 0.1, 0.1]),
            publish_now: true,
            ..Default::default()
        },
    )
    .unwrap();

    // Retrieve context
    let context_results = db
        .query()
        .vector_search(vec![0.85, 0.15, 0.1, 0.1], 1)
        .execute()
        .unwrap();

    // Construct RAG prompt
    let system_prompt =
        "You are a helpful assistant. Use the following context to answer the user's question.";
    let user_question = "How does Python define code blocks?";

    let mut rag_prompt = format!("{}\n\nContext:\n", system_prompt);
    for result in &context_results {
        let node = db.storage().get_by_id(result.node_id, None).unwrap();
        let payload_bytes = db.blob_store().read(node.payload_ptr).unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&payload_bytes).unwrap();
        rag_prompt.push_str(&payload["content"].as_str().unwrap_or(""));
        rag_prompt.push_str("\n");
    }
    rag_prompt.push_str(&format!("\nQuestion: {}\nAnswer:", user_question));

    assert!(rag_prompt.contains("Python"));
    assert!(rag_prompt.contains("indentation"));
    assert!(rag_prompt.contains("Question:"));
}

#[test]
fn test_vector_quantization_for_rag() {
    // Simulate embedding compression for large-scale RAG
    let original = vec![0.1; 384]; // Typical BERT embedding size

    // Quantize to save memory
    let quantized = quantize(&original, QuantizationType::INT8);

    // Verify size reduction
    let original_size = original.len() * 4; // f32 = 4 bytes
    let quantized_size = quantized.data.len();

    assert!(quantized_size < original_size);

    // Dequantize for search
    let restored = dequantize(&quantized);

    // Check reconstruction quality
    let max_error: f32 = original
        .iter()
        .zip(restored.iter())
        .map(|(o, r): (&f32, &f32)| (o - r).abs())
        .fold(0.0, f32::max);

    assert!(max_error < 0.1); // INT8 quantization error should be small
}
