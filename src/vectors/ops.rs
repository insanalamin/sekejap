//! Vector Operations - Brute-force similarity search (Phase 1)
//!
//! This module provides brute-force vector similarity search by scanning all nodes.
//! It's slow (O(N)) but correct, and serves as foundation for HNSW index.

use crate::storage::single::SingleStorage;
use crate::types::BlobStore;
use crate::vectors::VectorSearchResult;

/// Brute-force vector similarity search
///
/// Scans all nodes in storage, computes cosine similarity with query vector,
/// and returns top-k most similar nodes.
///
/// # Performance
///
/// - Time complexity: O(N × D) where N = number of nodes, D = vector dimension
/// - Space complexity: O(k) for results
/// - Suitable for: Small datasets (< 10k vectors) or correctness validation
/// - Not suitable for: Large datasets (use HNSW index instead)
///
/// # Arguments
///
/// * `storage` - The database storage
/// * `query` - Query vector (must match vector dimensions)
/// * `k` - Number of results to return
///
/// # Returns
///
/// Vector of top-k similar nodes with similarity scores (0.0 to 1.0)
///
/// # Note
///
/// This is a low-level function. For normal usage, use `db.query().vector_search()`.
///
/// # Example
///
/// ```rust,ignore
/// use hsdl_sekejap::vectors::ops::brute_force_search;
///
/// // In practice, call via:
/// // db.query().vector_search(query_vec, k).execute()
/// ```
pub fn brute_force_search(
    storage: &SingleStorage,
    blob_store: &BlobStore,
    query: &[f32],
    k: usize,
) -> Result<Vec<VectorSearchResult>, Box<dyn std::error::Error>> {
    // Validate query vector
    if query.is_empty() {
        return Err("Query vector cannot be empty".into());
    }

    if k == 0 {
        return Ok(Vec::new());
    }

    // Compute query magnitude (for cosine similarity)
    let query_magnitude = magnitude(query);
    if query_magnitude == 0.0 {
        return Err("Query vector has zero magnitude".into());
    }

    // Collect all nodes with vectors
    let mut results = Vec::new();

    // Iterate through all nodes
    for header in storage.iter() {
        // Skip nodes without vectors
        if let Some(vector_ptr) = &header.vector_ptr {
            // Read vector from BlobStore
            match blob_store.read(*vector_ptr) {
                Ok(vector_bytes) => {
                    // Convert bytes to f32 vector
                    let vector = bytes_to_vector(&vector_bytes);

                    // Compute cosine similarity
                    let similarity = cosine_similarity(query, &vector);

                    results.push(VectorSearchResult {
                        node_id: header.node_id,
                        similarity,
                    });
                }
                Err(e) => {
                    // Log error but continue processing other nodes
                    log::warn!("Failed to read vector for node {}: {}", header.node_id, e);
                    continue;
                }
            }
        }
    }

    // Sort by similarity (descending) and take top-k
    results.sort_by(|a: &VectorSearchResult, b: &VectorSearchResult| {
        b.similarity.partial_cmp(&a.similarity).unwrap()
    });
    results.truncate(k);

    Ok(results)
}

/// Compute magnitude (L2 norm) of a vector
///
/// `||v|| = sqrt(sum(v[i]^2))`
fn magnitude(vec: &[f32]) -> f32 {
    vec.iter().map(|&x| x * x).sum::<f32>().sqrt()
}

/// Compute cosine similarity between two vectors
///
/// `cos(a, b) = (a · b) / (||a|| × ||b||)`
///
/// Returns value in range [-1.0, 1.0], where:
/// - 1.0: identical direction
/// - 0.0: orthogonal
/// - -1.0: opposite direction
fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(a.len(), b.len(), "Vectors must have same length");

    // Dot product
    let dot_product: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();

    // Magnitudes
    let mag_a = magnitude(a);
    let mag_b = magnitude(b);

    // Avoid division by zero
    if mag_a == 0.0 || mag_b == 0.0 {
        return 0.0;
    }

    // Cosine similarity
    dot_product / (mag_a * mag_b)
}

/// Convert byte slice to vector of f32
///
/// Assumes bytes are in little-endian f32 format
pub fn bytes_to_vector(bytes: &[u8]) -> Vec<f32> {
    assert_eq!(bytes.len() % 4, 0, "Byte length must be multiple of 4");

    bytes
        .chunks_exact(4)
        .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
        .collect()
}

/// Convert vector of f32 to byte slice (little-endian)
pub fn vector_to_bytes(vec: &[f32]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(vec.len() * 4);
    for &val in vec {
        bytes.extend_from_slice(&val.to_le_bytes());
    }
    bytes
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_magnitude() {
        let vec = vec![3.0, 4.0];
        assert!((magnitude(&vec) - 5.0).abs() < 1e-6);
    }

    #[test]
    fn test_cosine_similarity() {
        // Identical vectors
        let a = vec![1.0, 2.0, 3.0];
        let b = vec![1.0, 2.0, 3.0];
        assert!((cosine_similarity(&a, &b) - 1.0).abs() < 1e-6);

        // Orthogonal vectors
        let c = vec![1.0, 0.0];
        let d = vec![0.0, 1.0];
        assert!((cosine_similarity(&c, &d) - 0.0).abs() < 1e-6);

        // Opposite vectors
        let e = vec![1.0, 2.0];
        let f = vec![-1.0, -2.0];
        assert!((cosine_similarity(&e, &f) + 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_bytes_to_vector() {
        let vec = vec![1.0, 2.0, 3.0];
        let bytes = vector_to_bytes(&vec);
        let decoded = bytes_to_vector(&bytes);
        assert_eq!(vec, decoded);
    }

    #[test]
    fn test_vector_to_bytes() {
        let vec = vec![0.1, 0.2, 0.3];
        let bytes = vector_to_bytes(&vec);
        assert_eq!(bytes.len(), vec.len() * 4);
    }

    #[test]
    fn test_cosine_similarity_zero_magnitude() {
        let a = vec![0.0, 0.0, 0.0];
        let b = vec![1.0, 2.0, 3.0];
        assert_eq!(cosine_similarity(&a, &b), 0.0);
    }
}