//! Vector Operations Module
//!
//! Provides vector similarity search, quantization, and indexing for SekejapDB.
//!
//! # Architecture
//!
//! - **Vector Storage**: Vectors are stored as blobs in BlobStore (content-addressed)
//! - **MVCC**: Vectors are versioned with nodes via `NodeHeader::vector_ptr`
//! - **Search**: Brute-force scan (Phase 1) → HNSW index (Phase 2)
//! - **Quantization**: FP32 → FP16/INT8 for memory reduction (feature-gated)
//!
//! # Feature Flags
//!
//! ```toml
//! [features]
//! vector = []  # Enable vector operations
//! ```
//!
//! With this feature enabled:
//! - Brute-force vector similarity search
//! - FP16/INT8 quantization
//! - HNSW index for fast ANN search
//! - Batch vector operations
//!
//! Without this feature:
//! - Zero compile-time overhead
//! - No runtime cost
//!
//! # Usage
//!
//! ```rust
//! use sekejap::SekejapDB;
//! use std::path::Path;
//!
//! # fn main() {
//! let db = SekejapDB::new(Path::new("./data")).unwrap();
//!
//! // Search for similar nodes (brute-force, Phase 1)
//! let query = vec![0.1, 0.2, 0.3];
//! let results = db.query()
//!     .vector_search(query, 10)  // Find top 10 similar nodes
//!     .execute()
//!     .unwrap();
//! # }
//! ```

#[cfg(feature = "vector")]
pub mod ops;

#[cfg(feature = "vector")]
pub mod quantization;

#[cfg(feature = "vector")]
pub mod index;

#[cfg(feature = "vector")]
pub mod hnsw;

#[cfg(feature = "vector")]
pub use quantization::{QuantizationType, QuantizedVector};

#[cfg(feature = "vector")]
pub use index::{IndexBuildPolicy, VectorIndex};

/// Vector search result with similarity score
#[derive(Debug, Clone, PartialEq)]
#[cfg(feature = "vector")]
pub struct VectorSearchResult {
    pub node_id: crate::types::node::NodeId,
    pub similarity: f32,
}

#[cfg(feature = "vector")]
impl VectorSearchResult {
    pub fn new(node_id: crate::types::node::NodeId, similarity: f32) -> Self {
        Self {
            node_id,
            similarity,
        }
    }
}

#[cfg(all(test, feature = "vector"))]
mod tests {
    use super::*;

    #[test]
    fn test_vector_search_result() {
        let result = VectorSearchResult::new(123456789012345678u128, 0.95);
        assert_eq!(result.node_id, 123456789012345678u128);
        assert_eq!(result.similarity, 0.95);
    }
}
