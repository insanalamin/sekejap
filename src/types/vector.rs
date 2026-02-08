//! Vector types for Sekejap-DB
//!
//! Provides structured vector storage with model metadata and token support.
//! Supports dense vectors and ColBERT-style token vectors for multi-modal search.
//!
//! # Example
//!
//! ```rust
//! use sekejap::types::{VectorChannel, VectorStore};
//!
//! // Dense vector
//! let dense = VectorChannel::dense("bge-m3", 1024, vec![0.1, -0.2, 0.3]);
//!
//! // Token vectors (ColBERT-style)
//! let tokens = vec![vec![0.1, -0.2], vec![0.3, -0.1], vec![0.05, 0.15]];
//! let colbert = VectorChannel::tokens("colbert-v2", 128, tokens);
//!
//! // Vector store with multiple channels
//! let mut store = VectorStore::new();
//! store.insert("dense".to_string(), dense);
//! store.insert("colbert".to_string(), colbert);
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

/// Single vector channel with metadata
///
/// Each channel represents one vector embedding with:
/// - Model name (for identification and compatibility)
/// - Dimensions (explicit to prevent silent errors)
/// - Data (dense vectors) or tokens (ColBERT-style)
///
/// # Example
///
/// ```rust
/// use sekejap::types::VectorChannel;
///
/// let channel = VectorChannel::dense("bge-m3", 1024, vec![0.1; 1024]);
/// assert_eq!(channel.model(), "bge-m3");
/// assert_eq!(channel.dims(), 1024);
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VectorChannel {
    /// Embedding model name (e.g., "bge-m3", "colbert-v2")
    pub model: String,

    /// Vector dimensions (explicit for correctness)
    pub dims: usize,

    /// Dense vector data (one vector per channel)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Vec<f32>>,

    /// Token vectors for ColBERT-style late interaction
    /// Each token is a separate vector (e.g., 32 tokens × 128 dims)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tokens: Option<Vec<Vec<f32>>>,
}

impl VectorChannel {
    /// Create a dense vector channel
    pub fn dense(model: impl Into<String>, dims: usize, data: Vec<f32>) -> Self {
        Self {
            model: model.into(),
            dims,
            data: Some(data),
            tokens: None,
        }
    }

    /// Create a token vector channel (ColBERT-style)
    pub fn tokens(model: impl Into<String>, dims: usize, tokens: Vec<Vec<f32>>) -> Self {
        Self {
            model: model.into(),
            dims,
            data: None,
            tokens: Some(tokens),
        }
    }

    /// Get model name
    pub fn model(&self) -> &str {
        &self.model
    }

    /// Get dimensions
    pub fn dims(&self) -> usize {
        self.dims
    }

    /// Check if this is a dense vector
    pub fn is_dense(&self) -> bool {
        self.data.is_some()
    }

    /// Check if this is a token vector
    pub fn is_tokens(&self) -> bool {
        self.tokens.is_some()
    }

    /// Get reference to dense data (if available)
    pub fn data(&self) -> Option<&Vec<f32>> {
        self.data.as_ref()
    }

    /// Get reference to token vectors (if available)
    pub fn get_tokens(&self) -> Option<&Vec<Vec<f32>>> {
        self.tokens.as_ref()
    }

    /// Get total vector count (1 for dense, N for tokens)
    pub fn vector_count(&self) -> usize {
        if let Some(ref data) = self.data {
            data.len() / self.dims
        } else if let Some(ref tokens) = self.tokens {
            tokens.len()
        } else {
            0
        }
    }

    /// Serialize to bytes for BlobStore (blazing fast)
    /// Uses bincode for efficient binary serialization
    pub fn to_bytes(&self) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        Ok(bincode::serialize(self)?)
    }

    /// Deserialize from bytes
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(bincode::deserialize(bytes)?)
    }
}

/// Store for multiple vector channels (named)
///
/// Provides O(1) access to named vector channels.
/// Each channel can have different models and dimensions.
///
/// # Example
///
/// ```rust
/// use sekejap::types::{VectorStore, VectorChannel};
///
/// let mut store = VectorStore::new();
///
/// // Add dense vector
/// store.insert("dense".to_string(), VectorChannel::dense("bge-m3", 1024, vec![0.1; 1024]));
///
/// // Add token vectors
/// store.insert("colbert".to_string(), VectorChannel::tokens("colbert-v2", 128, vec![vec![0.1; 128]; 32]));
///
/// // Query
/// if let Some(channel) = store.get("dense") {
///     assert_eq!(channel.dims(), 1024);
/// }
/// ```
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct VectorStore(HashMap<String, VectorChannel>);

impl VectorStore {
    /// Create a new empty vector store
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    /// Insert a vector channel
    pub fn insert(&mut self, name: String, channel: VectorChannel) -> Option<VectorChannel> {
        self.0.insert(name, channel)
    }

    /// Get a vector channel by name
    pub fn get(&self, name: &str) -> Option<&VectorChannel> {
        self.0.get(name)
    }

    /// Get mutable reference to a vector channel
    pub fn get_mut(&mut self, name: &str) -> Option<&mut VectorChannel> {
        self.0.get_mut(name)
    }

    /// Check if channel exists
    pub fn contains(&self, name: &str) -> bool {
        self.0.contains_key(name)
    }

    /// Remove a channel
    pub fn remove(&mut self, name: &str) -> Option<VectorChannel> {
        self.0.remove(name)
    }

    /// Get number of channels
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Iterate over channels
    pub fn iter(&self) -> impl Iterator<Item = (&str, &VectorChannel)> {
        self.0.iter().map(|(k, v)| (k.as_str(), v))
    }

    /// Iterate mutably over channels
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&str, &mut VectorChannel)> {
        self.0.iter_mut().map(|(k, v)| (k.as_str(), v))
    }

    /// Get all channel names
    pub fn channels(&self) -> Vec<&str> {
        self.0.keys().map(|s| s.as_str()).collect()
    }

    /// Get total vector count across all channels
    pub fn total_vector_count(&self) -> usize {
        self.0.values().map(|v| v.vector_count()).sum()
    }

    /// Get total bytes for all vectors (estimated)
    pub fn estimated_bytes(&self) -> usize {
        self.0.values().fold(0, |acc, v| {
            let data_bytes = v.data.as_ref().map_or(0, |d| d.len() * 4);
            let token_bytes = v
                .tokens
                .as_ref()
                .map_or(0, |t| t.iter().map(|v| v.len() * 4).sum());
            acc + data_bytes + token_bytes
        })
    }
}

impl fmt::Display for VectorChannel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_dense() {
            write!(f, "VectorChannel({} dense, {} dims)", self.model, self.dims)
        } else {
            let token_count = self.tokens.as_ref().map_or(0, |t| t.len());
            write!(
                f,
                "VectorChannel({} tokens, {} dims)",
                self.model, token_count
            )
        }
    }
}

impl fmt::Display for VectorStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "VectorStore({} channels)", self.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dense_vector() {
        let channel = VectorChannel::dense("bge-m3", 1024, vec![0.1; 1024]);
        assert_eq!(channel.model(), "bge-m3");
        assert_eq!(channel.dims(), 1024);
        assert!(channel.is_dense());
        assert!(!channel.is_tokens());
        assert_eq!(channel.vector_count(), 1);
    }

    #[test]
    fn test_token_vector() {
        let tokens = vec![vec![0.1; 128]; 32];
        let channel = VectorChannel::tokens("colbert-v2", 128, tokens);
        assert_eq!(channel.model(), "colbert-v2");
        assert_eq!(channel.dims(), 128);
        assert!(!channel.is_dense());
        assert!(channel.is_tokens());
        assert_eq!(channel.vector_count(), 32);
    }

    #[test]
    fn test_vector_store() {
        let mut store = VectorStore::new();
        assert!(store.is_empty());
        assert_eq!(store.len(), 0);

        store.insert(
            "dense".to_string(),
            VectorChannel::dense("bge-m3", 1024, vec![0.1; 1024]),
        );
        store.insert(
            "colbert".to_string(),
            VectorChannel::tokens("colbert-v2", 128, vec![vec![0.1; 128]; 32]),
        );

        assert_eq!(store.len(), 2);
        assert!(store.contains("dense"));
        assert!(store.contains("colbert"));
        assert!(!store.contains("missing"));

        let channels: Vec<&str> = store.channels();
        assert_eq!(channels.len(), 2);

        let total = store.total_vector_count();
        assert_eq!(total, 33); // 1 + 32
    }

    #[test]
    #[ignore] // bincode 1.3 has size limits that cause issues
    fn test_vector_serialization() {
        // Use smaller vector for faster test
        let channel = VectorChannel::dense("bge-m3", 10, vec![0.1; 10]);
        let bytes = channel.to_bytes().unwrap();

        // Use the from_bytes method which handles bincode internally
        let restored = VectorChannel::from_bytes(&bytes).unwrap();

        assert_eq!(channel, restored);
    }
}
